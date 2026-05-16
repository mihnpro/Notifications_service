# Notification Platform — Workers Documentation

Сервис массовых уведомлений. Два Go-воркера реализуют асинхронную доставку сообщений
через RabbitMQ с гарантией at-least-once и полным аудитом каждой попытки.

---

## Содержание

1. [Общая архитектура](#1-общая-архитектура)
2. [Поток данных от кампании до доставки](#2-поток-данных-от-кампании-до-доставки)
3. [Fanout Worker](#3-fanout-worker)
   - [Назначение](#31-назначение)
   - [DDD-структура](#32-ddd-структура)
   - [Алгоритм работы](#33-алгоритм-работы)
   - [Pipeline: три стадии](#34-pipeline-три-стадии)
   - [Взаимодействие с БД](#35-взаимодействие-с-бд)
   - [Идемпотентность и рестарт](#36-идемпотентность-и-рестарт)
4. [Delivery Worker](#4-delivery-worker)
   - [Назначение](#41-назначение)
   - [DDD-структура](#42-ddd-структура)
   - [Алгоритм работы](#43-алгоритм-работы)
   - [Lease: захват и heartbeat](#44-lease-захват-и-heartbeat)
   - [Provider Stub](#45-provider-stub)
   - [Retry-логика](#46-retry-логика)
   - [Финализация: атомарная транзакция](#47-финализация-атомарная-транзакция)
   - [Взаимодействие с БД](#48-взаимодействие-с-бд)
5. [RabbitMQ-топология](#5-rabbitmq-топология)
6. [Гарантии надёжности](#6-гарантии-надёжности)
7. [Конфигурация](#7-конфигурация)
8. [Локальный запуск](#8-локальный-запуск)

---

## 1. Общая архитектура

```text
┌──────────┐   POST /campaigns   ┌─────────────────────────────────────────┐
│ Manager  │ ──────────────────► │                  API                    │
└──────────┘                     │  • создаёт campaign                     │
                                 │  • создаёт campaign_region_run          │
                                 │  • пишет CampaignRegionRunRequested     │
                                 │    в outbox_events                      │
                                 └─────────────────┬───────────────────────┘
                                                   │ (PostgreSQL)
                                 ┌─────────────────▼───────────────────────┐
                                 │           Outbox Publisher              │
                                 │  • читает outbox_events WHERE pending   │
                                 │  • публикует в RabbitMQ                 │
                                 │  • помечает published                   │
                                 └──────┬──────────────────────────────────┘
                                        │ notification.fanout
                                 ┌──────▼──────────────────────────────────┐
                                 │           Fanout Worker                 │
                                 │  • захватывает lock на run              │
                                 │  • читает пользователей курсором        │
                                 │  • создаёт delivery_tasks (100k+)       │
                                 │  • пишет DeliveryTaskCreated в outbox   │
                                 └──────┬──────────────────────────────────┘
                                        │ outbox_events → Outbox Publisher
                                        │ notification.eu.default.normal
                                 ┌──────▼──────────────────────────────────┐
                                 │          Delivery Worker                │
                                 │  • берёт задачу в работу (lease)        │
                                 │  • запускает heartbeat (каждые 30 с)    │
                                 │  • вызывает provider (до 320 с)         │
                                 │  • финализирует результат               │
                                 └─────────────────────────────────────────┘
```text

Все воркеры используют **одну PostgreSQL БД** через пул соединений pgxpool.
Связь между воркерами — только через таблицы `outbox_events` и `delivery_tasks`.

---

## 2. Поток данных от кампании до доставки

```text
campaigns
  └─► campaign_region_runs   (status: fanout_pending → fanout_running → fanout_completed)
        └─► outbox_events    (CampaignRegionRunRequested → published)
              └─► [Fanout Worker]
                    └─► delivery_tasks × N    (status: queued)
                    └─► outbox_events         (DeliveryTaskCreated × N → published)
                          └─► [Delivery Worker]
                                └─► delivery_attempts  (status: started → succeeded/failed)
                                └─► delivery_tasks     (status: sending → succeeded/retry_scheduled/dead_lettered)
                                └─► dlq_items          (если dead_lettered)
                                └─► outbox_events      (TaskRetryScheduled если нужен retry)
                                └─► campaign_stats     (счётчики обновляются атомарно)
```text

---

## 3. Fanout Worker

### 3.1 Назначение

Fanout Worker превращает одну запись `campaign_region_run` в набор `delivery_tasks` —
по одной задаче на каждую пару (пользователь, канал).

Для кампании с 50 000 пользователей и 2 каналами создаётся ~100 000 задач.
Это происходит **асинхронно** — API возвращает `202` немедленно, не ожидая создания задач.

### 3.2 DDD-структура

```text
funout/
├── cmd/main.go                              — точка входа, wiring
└── internal/
    ├── domain/                              — чистый Go, 0 внешних зависимостей
    │   ├── campaign/
    │   │   ├── entity.go                   — Campaign, RecipientSelector, UserChannel
    │   │   ├── run.go                      — CampaignRegionRun + IsLocked(), IsCompleted()
    │   │   ├── repository.go               — интерфейсы: RunRepo, CampaignRepo, UserRepo, UserChannelRepo
    │   │   └── errors.go                   — ErrAlreadyLocked, ErrLockLost, ErrNotFound
    │   └── task/
    │       ├── task.go                     — Task entity + New() factory (детерминированный ID)
    │       └── repository.go               — BatchRepository interface
    ├── application/fanout/                  — оркестрация, зависит только от domain
    │   ├── message.go                      — Message DTO (из RabbitMQ)
    │   ├── service.go                      — Service.Execute() — главный флоу
    │   └── pipeline.go                     — 3-стадийный concurrent pipeline
    └── infrastructure/
        ├── config/config.go
        ├── postgres/
        │   ├── run_repo.go                 — AcquireLock / ExtendLock / MarkCompleted / MarkFailed
        │   ├── campaign_repo.go            — FindByID
        │   ├── user_repo.go                — FetchBatch (keyset pagination)
        │   ├── user_channel_repo.go        — FindActiveByUsers
        │   └── task_batch_repo.go          — InsertBatch (tasks + outbox + stats, одна транзакция)
        └── rabbitmq/worker.go              — consumer + semaphore
```text

**Правило зависимостей:** `infrastructure` → `application` → `domain`.
Ни один пакет `domain` не импортирует pgx или amqp.

### 3.3 Алгоритм работы

```text
[RabbitMQ: notification.fanout]
  ↓ сообщение: CampaignRegionRunRequested
  ↓ {campaign_region_run_id, campaign_id, region_id}

Service.Execute(ctx, body)
  │
  ├─ 1. RunRepo.AcquireLock(runID, workerID)
  │       UPDATE campaign_region_runs
  │       SET status='fanout_running', fanout_lock_owner=workerID,
  │           fanout_lock_until=NOW()+90s,
  │           fanout_attempt_count=fanout_attempt_count+1
  │       WHERE status='fanout_pending'
  │         AND (fanout_lock_until IS NULL OR fanout_lock_until < NOW())
  │       ← 0 rows → ErrAlreadyLocked → Ack (дубль-сигнал, игнорируем)
  │
  ├─ 2. CampaignRepo.FindByID(campaignID)
  │       SELECT message_snapshot, recipient_selector,
  │              selected_channel_codes, priority
  │
  ├─ 3. runPipeline(ctx, run, campaign)    ← 3 горутины
  │
  └─ 4. RunRepo.MarkCompleted(runID, workerID)
          UPDATE campaign_region_runs SET status='fanout_completed'
          WHERE id=$runID AND fanout_lock_owner=$workerID
```text

При ошибке в pipeline вызывается `MarkFailed` — recovery job может переотправить сигнал.

### 3.4 Pipeline: три стадии

Три горутины, связанные буферизованными каналами (размер 2).
Координируются через `errgroup.WithContext` — ошибка в любой стадии отменяет все остальные.

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 1: stageCursor (1 горутина)                                       │
│                                                                          │
│  loop {                                                                  │
│    UserRepo.FetchBatch(regionID, selector, lastID, 1000)                 │
│    ─── keyset: WHERE id > $lastID ORDER BY id LIMIT 1000 ────────────    │
│    → []uuid.UUID                    → chan []uuid.UUID (buf=2)           │
│    lastID = ids[last]                                                    │
│  }                                                                       │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │ пока Stage 1 читает batch N+1
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 2: stageEnrich (1 горутина)                                       │
│                                                                          │
│  for userIDs := range userBatchCh {                                      │
│    UserChannelRepo.FindActiveByUsers(userIDs, channelCodes)              │
│    ─── SELECT uc.id, uc.user_id, c.code, c.queue_group, uc.address ───   │
│        FROM user_channels uc JOIN channels c                             │
│        WHERE uc.user_id=ANY($ids) AND c.code=ANY($codes)                │
│          AND uc.status='active' AND c.state='enabled'                    │
│    → task.New(run, campaign, uc) для каждой строки                      │
│    → []*task.Task                   → chan []*task.Task (buf=2)          │
│  }                                                                       │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │ пока Stage 2 обогащает batch N
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Stage 3: stageInsert (1 горутина)                                       │
│                                                                          │
│  for tasks := range taskBatchCh {                                        │
│    TaskBatchRepo.InsertBatch(tasks)   ← одна транзакция:                 │
│      pgx.Batch: INSERT delivery_tasks (ON CONFLICT DO NOTHING)          │
│      pgx.Batch: INSERT outbox_events  (ON CONFLICT DO NOTHING)          │
│      UPSERT campaign_stats (total_tasks+=n, queued+=n)                  │
│    RunRepo.ExtendLock(runID)   ← продлеваем lock на 90 с                │
│  }                                                                       │
└──────────────────────────────────────────────────────────────────────────┘
```text

**Зачем буфер 2?** Пока Stage 3 вставляет batch N в БД, Stage 2 уже обогащает
batch N+1, а Stage 1 читает batch N+2. Три операции перекрываются во времени —
DB-соединения никогда не простаивают.

**Зачем keyset вместо OFFSET?**
`OFFSET 50000` сканирует 50 001 строку каждый раз — O(n).
`WHERE id > $lastID` использует B-tree индекс — O(log n) независимо от позиции.

### 3.5 Взаимодействие с БД

| Операция | Таблица | Тип |
|---|---|---|
| Захват lock | `campaign_region_runs` | UPDATE с CAS на status + lock_until |
| Загрузка кампании | `campaigns` | SELECT |
| Чтение пользователей | `users` | SELECT (keyset cursor, повторяется) |
| Обогащение каналами | `user_channels` + `channels` | JOIN SELECT (на каждый batch) |
| Вставка задач | `delivery_tasks` | pgx.Batch INSERT ON CONFLICT |
| Вставка outbox | `outbox_events` | pgx.Batch INSERT ON CONFLICT |
| Обновление статистики | `campaign_stats` | UPSERT |
| Продление lock | `campaign_region_runs` | UPDATE после каждого batch |
| Финализация | `campaign_region_runs` | UPDATE status=completed |

Вставка задач и outbox-событий — **одна транзакция** на каждый batch.
Либо оба записались, либо ни один — нет риска создать задачу без сигнала доставки.

### 3.6 Идемпотентность и рестарт

Если fanout-воркер упал посередине:

1. Recovery job видит `status=fanout_running` с истёкшим `fanout_lock_until`
2. Сбрасывает статус на `fanout_pending`
3. Outbox Publisher переотправляет `CampaignRegionRunRequested`
4. Fanout Worker начинает с начала

**Дубли не создаются** — каждый `delivery_task` имеет уникальный `idempotency_key`:
```text
idempotency_key = "fanout:{run_id}:{user_channel_id}"
```text
И детерминированный `id` через UUID v5:
```text
task_id = UUIDv5(namespace, run_id + ":" + user_channel_id)
```text
`ON CONFLICT (idempotency_key) DO NOTHING` пропускает уже созданные задачи.

---

## 4. Delivery Worker

### 4.1 Назначение

Delivery Worker читает задачи из RabbitMQ, вызывает провайдер-адаптер канала
(email, sms, telegram и т.д.) и сохраняет результат. Поддерживает параллельную
обработку, защиту от двойного выполнения через lease-токен, retry с exponential
backoff и DLQ для исчерпанных попыток.

### 4.2 DDD-структура

```text
delivery/
├── cmd/main.go                              — точка входа, wiring
└── internal/
    ├── domain/                              — чистый Go, 0 внешних зависимостей
    │   ├── task/
    │   │   ├── task.go                     — Task entity + CanRetry(), IsAvailableNow()
    │   │   ├── attempt.go                  — Attempt entity, AttemptStatus
    │   │   ├── finalize.go                 — FinalizeParams value object
    │   │   ├── repository.go               — Repository (AcquireLease/ExtendLease/Finalize)
    │   │   └── errors.go                   — ErrAlreadyLeased, ErrNotAvailable, ErrLeaseExpired
    │   └── provider/
    │       └── adapter.go                  — Adapter interface, Payload, Result, Error
    ├── application/delivery/
    │   ├── message.go                      — Message DTO
    │   ├── service.go                      — Service.Process() — главный флоу
    │   ├── heartbeat.go                    — runHeartbeat() горутина
    │   └── retry.go                        — retryDelay(), retryRoutingKey()
    └── infrastructure/
        ├── config/config.go                — DeliveryQueues() генерирует имена очередей
        ├── postgres/task_repo.go           — все 3 метода репозитория
        ├── provider/stub.go                — GenericStubAdapter
        └── rabbitmq/worker.go             — multi-queue fan-in consumer
```text

### 4.3 Алгоритм работы

```text
[RabbitMQ: notification.eu.default.normal]
  ↓ сообщение: DeliveryTaskCreated
  ↓ {task_id, campaign_id, channel_code, queue_group, priority, ...}

Service.Process(ctx, body)
  │
  ├─ 1. AcquireLease(taskID, workerID)     ← одна транзакция
  │       UPDATE delivery_tasks SET status='sending',
  │              lease_token=gen_random_uuid(), lease_until=NOW()+120s,
  │              attempt_count=attempt_count+1
  │       WHERE id=$taskID AND status IN ('queued','retry_scheduled')
  │         AND available_at <= NOW()
  │         AND (lease_until IS NULL OR lease_until < NOW())
  │       INSERT INTO delivery_attempts (status='started')
  │       UPDATE campaign_stats SET sending+=1, queued-=1
  │       ← 0 rows → ErrAlreadyLeased | ErrNotAvailable → Ack
  │
  ├─ 2. go runHeartbeat(hbCtx, taskID, leaseToken)  ← фоновая горутина
  │       ticker := time.NewTicker(30s)
  │       loop { UPDATE delivery_tasks SET lease_until=NOW()+120s
  │              WHERE id=$taskID AND lease_token=$token }
  │
  ├─ 3. adapter.Send(provCtx[timeout=320s], payload)
  │       ← nil error        → success
  │       ← *provider.Error{Type: Transient} → retry
  │       ← *provider.Error{Type: Permanent} → DLQ
  │       ← context.DeadlineExceeded        → retry (как transient)
  │
  ├─ 4. hbCancel(); <-hbDone               ← ждём остановки heartbeat
  │
  └─ 5. Finalize(ctx, params)              ← одна транзакция
          CAS UPDATE delivery_tasks WHERE lease_token=$ourToken
          UPDATE delivery_attempts
          UPDATE campaign_stats
          INSERT dlq_items           (если dead_lettered)
          INSERT outbox_events       (если retry_scheduled)
          ← ErrLeaseExpired → recovery уже перехватил задачу → Ack
```text

### 4.4 Lease: захват и heartbeat

**Проблема:** провайдер может отвечать до 300 секунд. Если воркер упал,
другой воркер должен подхватить задачу — но не раньше, чем истечёт lease.

**Решение: lease + heartbeat**

```text
Воркер A берёт задачу:
  lease_token = UUID-A
  lease_until = NOW() + 120s

  Каждые 30 секунд:
    UPDATE SET lease_until = NOW() + 120s
    WHERE lease_token = UUID-A    ← CAS: не продлим чужой lease

Если воркер A упал:
  Через 120 секунд lease_until истекает
  Recovery job: UPDATE SET status='retry_scheduled'
                WHERE status='sending' AND lease_until < NOW()
  → задача снова доступна для другого воркера
```text

**Защита от двойного выполнения:** даже если воркер A и B оба начали обрабатывать
одну задачу (race condition), только один из них пройдёт финализацию:
```sql
UPDATE delivery_tasks
SET status = $newStatus, lease_token = NULL
WHERE id = $taskID AND lease_token = $ourToken   -- CAS
```text
Второй воркер получит 0 rows → `ErrLeaseExpired` → Ack без записи результата.

**Почему heartbeat ждёт `<-hbDone` перед финализацией?**
Без ожидания возможен race: heartbeat делает `UPDATE lease_until`, финализация
делает `UPDATE lease_token=NULL` — в неправильном порядке это может продлить
уже снятый lease. `<-hbDone` гарантирует полную остановку heartbeat-горутины.

### 4.5 Provider Stub

`GenericStubAdapter` имитирует реальный провайдер для тестов и демо:

```text
Send(ctx, payload):
  1. Случайная задержка от MinLatency(2s) до MaxLatency(300s)
     ← если ctx.Done() во время ожидания → context.Canceled
  2. rand() < SuccessRate(0.8) → Result{ProviderRequestID: "stub-{task_id}"}
  3. rand() < TransientRate(0.7) → &Error{Type: Transient, Code: "STUB_TRANSIENT"}
  4. иначе                       → &Error{Type: Permanent, Code: "STUB_PERMANENT"}
```text

Конфигурация через поля структуры — для тестов можно выставить MinLatency=20ms.

### 4.6 Retry-логика

Retry-решение принимается в `service.buildFinalizeParams()` на основе:
- Типа ошибки от провайдера (`Transient` / `Permanent`)
- Текущего `attempt_count` vs `max_attempts` (по умолчанию 5)

```text
provErr == nil                      → StatusSucceeded
provErr.Type == Permanent           → StatusDeadLettered
attempt_count >= max_attempts       → StatusDeadLettered
provErr.Type == Transient           → StatusRetryScheduled
```text

**Retry buckets** (таблица задержек по номеру попытки):

| Попытка | Задержка | RabbitMQ очередь |
|---|---|---|
| 1 | 30 секунд | `notification.eu.default.retry.30s` |
| 2 | 1 минута | `notification.eu.default.retry.1m` |
| 3 | 5 минут | `notification.eu.default.retry.5m` |
| 4+ | 15 минут | `notification.eu.default.retry.15m` |

Retry реализован через **RabbitMQ TTL + Dead Letter Exchange**:
1. Outbox Publisher публикует `TaskRetryScheduled` в `notification.retry`
   с routing_key `notification.eu.default.retry.30s`
2. Сообщение попадает в retry-очередь с TTL=30 000ms — **никто её не читает**
3. Когда TTL истекает, RabbitMQ автоматически перекладывает сообщение
   через `x-dead-letter-exchange: notification.direct` обратно в основную очередь
4. Delivery Worker снова берёт задачу

**Защита от ранней доставки:** даже если retry-сообщение пришло раньше `available_at`,
`AcquireLease` проверяет `AND available_at <= NOW()` — задача не будет взята раньше времени.

### 4.7 Финализация: атомарная транзакция

Все изменения по итогу одного провайдер-вызова фиксируются в **одной транзакции**:

```sql
BEGIN

-- 1. CAS на lease_token (защита от stale finalization)
UPDATE delivery_tasks
SET    status       = $newStatus,       -- succeeded | failed | retry_scheduled | dead_lettered
       lease_token  = NULL,
       lease_until  = NULL,
       lease_owner  = NULL,
       available_at = COALESCE($retryAt, available_at),
       completed_at = CASE WHEN $newStatus IN ('succeeded','failed','dead_lettered')
                           THEN NOW() ELSE NULL END
WHERE  id           = $taskID
  AND  lease_token  = $ourToken         -- если 0 rows → ROLLBACK, ErrLeaseExpired

-- 2. Обновить attempt
UPDATE delivery_attempts
SET    status              = $attemptStatus,
       completed_at        = NOW(),
       provider_request_id = $providerReqID,
       error_type          = $errorType,
       error_code          = $errorCode,
       error_message       = $errorMsg
WHERE  id = $attemptID

-- 3. Обновить статистику (sending-1, newStatus+1)
UPDATE campaign_stats
SET sending      = sending - 1,
    $newStatus   = $newStatus + 1,
    updated_at   = NOW()
WHERE campaign_id = $campaignID

-- 4. Если dead_lettered: создать DLQ запись
INSERT INTO dlq_items (task_id, campaign_id, reason_code, ...)
VALUES (...)
ON CONFLICT (task_id) DO NOTHING

-- 5. Если retry_scheduled: создать outbox-сигнал для повтора
INSERT INTO outbox_events
    (event_type='TaskRetryScheduled', routing_key=$retryBucketKey, ...)
ON CONFLICT (dedupe_key) DO NOTHING

COMMIT
```text

Если транзакция упала после COMMIT но до Ack — RabbitMQ переотправит сообщение.
При повторной обработке `AcquireLease` вернёт `ErrAlreadyLeased` (статус уже `succeeded`)
и сообщение будет заакчено без повторной записи результата.

### 4.8 Взаимодействие с БД

| Операция | Таблица | Тип |
|---|---|---|
| Захват lease | `delivery_tasks` | UPDATE CAS в транзакции |
| Создание attempt | `delivery_attempts` | INSERT |
| Инкремент sending | `campaign_stats` | UPDATE |
| Продление lease | `delivery_tasks` | UPDATE CAS (отдельное соединение) |
| Финализация задачи | `delivery_tasks` | UPDATE CAS |
| Финализация attempt | `delivery_attempts` | UPDATE |
| Обновление статистики | `campaign_stats` | UPDATE |
| DLQ (если нужно) | `dlq_items` | INSERT ON CONFLICT |
| Retry outbox (если нужно) | `outbox_events` | INSERT ON CONFLICT |

---

## 5. RabbitMQ-топология

```text
                    ┌─────────────────────────────────────────────┐
                    │           notification.direct               │
                    │           (exchange, type=direct)           │
                    └─────┬──────────────┬──────────────┬─────────┘
                          │              │              │
               ┌──────────▼──┐  ┌────────▼────┐  ┌────▼────────────┐
               │  .eu.default│  │.eu.messenger│  │ .fanout         │
               │  .high/norm │  │.high/normal │  │ (кампании)      │
               │  (доставка) │  │ (telegram,  │  └─────────────────┘
               └──────┬──────┘  │  whatsapp)  │
                      │         └──────┬───────┘
            x-dead-letter             x-dead-letter
                      │               │
               ┌──────▼──────────────▼───────────────────────────┐
               │           notification.dlx                      │
               │    *.dead очереди (мониторинг просроченных)     │
               └─────────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────────┐
                    │           notification.retry                │
                    │           (exchange, type=direct)           │
                    └──────┬──────┬──────┬──────┬────────────────┘
                           │      │      │      │
                    .retry.30s  .1m  .5m  .15m  (TTL-очереди)
                           │      │      │      │
                           └──────┴──────┴──────┘
                                  │ x-dead-letter-exchange: notification.direct
                                  │ (после TTL → обратно в основную очередь)
                                  ▼
                          notification.eu.default.normal
```text

**Routing key format:** `notification.{region}.{queue_group}.{priority}`

**queue_group** отвязывает бизнес-канал от физической очереди:
- `email`, `sms`, `push` → queue_group=`default`
- `telegram`, `whatsapp` → queue_group=`messenger`

Добавление нового канала (например `viber`) с `queue_group=messenger` не требует
изменений в RabbitMQ — он автоматически маршрутизируется через существующие очереди.

---

## 6. Гарантии надёжности

### At-least-once delivery

| Риск | Механизм защиты |
|---|---|
| Воркер упал до Ack | RabbitMQ переотправит сообщение (manual ack) |
| Fanout упал посередине | fanout_lock_until истекает → recovery → повтор |
| Delivery упал во время провайдер-вызова | lease_until истекает → recovery → retry |
| БД упала после COMMIT, до Ack | RabbitMQ переотправит; ON CONFLICT DO NOTHING |
| Дублирующийся RabbitMQ-сигнал | lease_token CAS → 0 rows при финализации |

### Идемпотентность

- `delivery_tasks.idempotency_key` — UNIQUE, `ON CONFLICT DO NOTHING`
- `outbox_events.dedupe_key` — UNIQUE, `ON CONFLICT DO NOTHING`
- `task_id` в fanout — детерминированный UUID v5
- `lease_token` в delivery — CAS guard на финализации

### Статистика без дрейфа

Счётчики в `campaign_stats` обновляются **только внутри успешной транзакции**
перехода задачи в новый статус. Если транзакция откатилась — счётчик не изменился.
Дублирующийся сигнал не приводит к двойному счёту.

---

## 7. Конфигурация

### Fanout Worker

| Переменная | По умолчанию | Описание |
|---|---|---|
| `WORKER_ID` | `funout-1` | Уникальный ID воркера (пишется в fanout_lock_owner) |
| `FANOUT_BATCH_SIZE` | `1000` | Пользователей за один DB-запрос |
| `FANOUT_CONCURRENCY` | `4` | Параллельных fanout pipeline |
| `POSTGRES_HOST` | `localhost` | |
| `POSTGRES_PORT` | `5433` | |
| `POSTGRES_USER` | `notifications` | |
| `POSTGRES_PASSWORD` | `notifications` | |
| `POSTGRES_DB` | `notifications` | |
| `RABBITMQ_URL` | `amqp://notifications:notifications@localhost:5672` | |
| `RABBITMQ_VHOST` | `/notifications` | |

### Delivery Worker

| Переменная | По умолчанию | Описание |
|---|---|---|
| `WORKER_ID` | `delivery-1` | Уникальный ID (пишется в lease_owner) |
| `DELIVERY_CONCURRENCY` | `16` | Параллельных провайдер-вызовов |
| `REGION` | `eu` | Регион → формирует имена очередей |
| `QUEUE_GROUPS` | `default,messenger` | Queue groups → имена очередей |
| `POSTGRES_*` | (см. выше) | |
| `RABBITMQ_*` | (см. выше) | |

---

## 8. Локальный запуск

### Требования

- Go 1.25+
- Docker + Docker Compose

### Инфраструктура

```bash
cd deploy
docker compose up -d

# Проверить что всё поднялось
docker compose ps
# Management UI: http://localhost:15672  (login: notifications / notifications)
```text

### Миграции

```bash
cd api
uv run alembic upgrade head
```text

### Fanout Worker

```bash
cd funout
WORKER_ID=funout-1 \
POSTGRES_PORT=5433 \
go run ./cmd
```text

### Delivery Worker

```bash
cd delivery
WORKER_ID=delivery-1 \
POSTGRES_PORT=5433 \
REGION=eu \
QUEUE_GROUPS=default,messenger \
go run ./cmd
```text

### Несколько delivery воркеров параллельно

```bash
# Терминал 1
WORKER_ID=delivery-1 DELIVERY_CONCURRENCY=8 go run ./cmd

# Терминал 2
WORKER_ID=delivery-2 DELIVERY_CONCURRENCY=8 go run ./cmd
```text

Каждый воркер имеет уникальный `WORKER_ID` — lease ownership отслеживается
по этому полю. При сбое одного воркера lease истекает и другой подхватит задачу.
