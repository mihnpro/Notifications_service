import React, { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  Ban,
  BarChart3,
  Bell,
  CheckCircle2,
  ChevronRight,
  Clock,
  Copy,
  Eye,
  FileText,
  Filter,
  LayoutDashboard,
  Mail,
  MessageCircle,
  MoreHorizontal,
  PauseCircle,
  PlayCircle,
  Plus,
  RefreshCw,
  Search,
  Send,
  Settings,
  ShieldAlert,
  SlidersHorizontal,
  Smartphone,
  Upload,
  Users,
} from "lucide-react";
import { motion } from "framer-motion";

import { api } from "./api/client";
import { adaptCampaign, adaptChannel, adaptIssue, adaptResult, toCreateCampaignPayload } from "./api/adapters";
import { clearStoredToken, getStoredToken, setStoredToken } from "./auth/session";

const audiences = [
  { id: "all", title: "Все активные пользователи", size: "все", hint: "Подходит для массовой рассылки" },
  { id: "selected", title: "Выбранные пользователи", size: "список", hint: "Через external IDs, разделитель - запятая" },
];

const navItems = [
  { id: "overview", label: "Обзор", icon: LayoutDashboard },
  { id: "campaign-new", label: "Новая рассылка", icon: Send },
  { id: "campaigns", label: "Рассылки", icon: BarChart3 },
  { id: "campaign-detail", label: "Карточка рассылки", icon: FileText },
  { id: "issues", label: "Проблемы доставки", icon: ShieldAlert },
  { id: "recipients", label: "Получатели", icon: Users },
  { id: "channels", label: "Каналы", icon: Settings },
];

function cx(...classes) {
  return classes.filter(Boolean).join(" ");
}

function toneClasses(tone = "neutral") {
  return {
    neutral: "bg-slate-100 text-slate-700 ring-slate-200",
    good: "bg-emerald-50 text-emerald-700 ring-emerald-200",
    warn: "bg-amber-50 text-amber-700 ring-amber-200",
    bad: "bg-rose-50 text-rose-700 ring-rose-200",
    info: "bg-blue-50 text-blue-700 ring-blue-200",
    violet: "bg-violet-50 text-violet-700 ring-violet-200",
  }[tone] || "bg-slate-100 text-slate-700 ring-slate-200";
}

function statusTone(status) {
  if (["enabled", "active", "sending", "in_progress"].includes(status)) return "info";
  if (["completed", "delivered"].includes(status)) return "good";
  if (["will_retry", "limited", "needs_attention", "paused", "cancelling"].includes(status)) return "warn";
  if (["failed", "disabled", "blocked", "cancelled"].includes(status)) return "bad";
  return "neutral";
}

function formatStatus(status) {
  return {
    sending: "идёт отправка",
    cancelling: "останавливается",
    completed: "завершена",
    needs_attention: "требует внимания",
    cancelled: "отменена",
    delivered: "доставлено",
    will_retry: "повторим позже",
    failed: "не доставлено",
    in_progress: "в процессе",
    enabled: "доступен",
    disabled: "отключён",
    limited: "есть ограничения",
    active: "активен",
    blocked: "заблокирован",
    normal: "обычный",
    high: "важный",
    low: "низкий",
  }[status] || status;
}

function formatChannel(channel) {
  return { email: "Email", sms: "SMS", telegram: "Telegram", whatsapp: "WhatsApp", push: "Push" }[channel] || channel;
}

function formatPriority(priority) {
  return { low: "низкий", normal: "обычный", high: "важный" }[priority] || priority;
}

function mapCampaignStatus(status) {
  return {
    running: "sending",
    completed: "completed",
    partially_failed: "needs_attention",
    failed: "needs_attention",
    cancelling: "cancelling",
    cancelled: "cancelled",
  }[status] || status || "sending";
}

function normalizeCampaignStats(stats = {}) {
  return {
    total: stats.totalTasks || stats.total || 0,
    delivered: stats.succeeded || stats.delivered || 0,
    inProgress: (stats.queued || 0) + (stats.sending || 0) + (stats.inProgress || 0),
    waiting: stats.retryScheduled || stats.waiting || 0,
    failed: (stats.failed || 0) + (stats.deadLettered || 0) + (stats.cancelled || 0),
  };
}

function Badge({ children, tone = "neutral" }) {
  return <span className={cx("inline-flex items-center whitespace-nowrap rounded-full px-2.5 py-1 text-xs font-medium ring-1", toneClasses(tone))}>{children}</span>;
}

function Button({ children, tone = "dark", size = "md", icon: Icon, className = "", ...props }) {
  const tones = {
    dark: "bg-slate-950 text-white hover:bg-slate-800 border-slate-950",
    primary: "bg-blue-600 text-white hover:bg-blue-500 border-blue-600",
    good: "bg-emerald-600 text-white hover:bg-emerald-500 border-emerald-600",
    warn: "bg-amber-500 text-white hover:bg-amber-400 border-amber-500",
    bad: "bg-rose-600 text-white hover:bg-rose-500 border-rose-600",
    ghost: "bg-white text-slate-700 hover:bg-slate-50 border-slate-200",
    soft: "bg-slate-100 text-slate-700 hover:bg-slate-200 border-slate-100",
  };
  const sizes = { sm: "px-3 py-2 text-xs", md: "px-4 py-2.5 text-sm", lg: "px-5 py-3 text-sm" };
  return (
    <button className={cx("inline-flex items-center justify-center gap-2 rounded-2xl border font-semibold shadow-sm transition", tones[tone], sizes[size], className)} {...props}>
      {Icon ? <Icon size={16} /> : null}
      {children}
    </button>
  );
}

function Card({ children, className = "" }) {
  return <div className={cx("rounded-2xl border border-slate-200 bg-white p-5 shadow-sm", className)}>{children}</div>;
}

function Field({ label, children, hint }) {
  return (
    <label className="block space-y-2">
      <span className="text-sm font-medium text-slate-700">{label}</span>
      {children}
      {hint ? <span className="block text-xs text-slate-400">{hint}</span> : null}
    </label>
  );
}

function Input(props) {
  return <input className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm outline-none ring-blue-100 transition placeholder:text-slate-400 focus:border-blue-300 focus:ring-4" {...props} />;
}

function Textarea(props) {
  return <textarea className="w-full resize-none rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm outline-none ring-blue-100 transition placeholder:text-slate-400 focus:border-blue-300 focus:ring-4" {...props} />;
}

function Select(props) {
  return <select className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm outline-none ring-blue-100 transition focus:border-blue-300 focus:ring-4" {...props} />;
}

function PageHeader({ eyebrow, title, description, children }) {
  return (
    <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
      <div>
        {eyebrow ? <p className="text-sm font-medium text-blue-600">{eyebrow}</p> : null}
        <h1 className="mt-1 text-3xl font-semibold tracking-tight text-slate-950">{title}</h1>
        {description ? <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-500">{description}</p> : null}
      </div>
      {children ? <div className="flex flex-wrap gap-2">{children}</div> : null}
    </div>
  );
}

function StatCard({ icon: Icon, label, value, hint, tone = "neutral" }) {
  const iconTone = {
    neutral: "bg-slate-50 text-slate-700",
    good: "bg-emerald-50 text-emerald-700",
    warn: "bg-amber-50 text-amber-700",
    bad: "bg-rose-50 text-rose-700",
    info: "bg-blue-50 text-blue-700",
    violet: "bg-violet-50 text-violet-700",
  }[tone] || "bg-slate-50 text-slate-700";
  return (
    <Card className="p-4">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm text-slate-500">{label}</p>
          <p className="mt-1 text-2xl font-semibold tracking-tight text-slate-950">{value}</p>
          <p className="mt-1 text-xs text-slate-400">{hint}</p>
        </div>
        <div className={cx("rounded-xl p-2", iconTone)}><Icon size={18} /></div>
      </div>
    </Card>
  );
}

function SearchBox({ placeholder = "Поиск", value = "", onChange = () => {} }) {
  return (
    <div className="flex min-w-56 items-center gap-2 rounded-2xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-500 shadow-sm">
      <Search size={16} />
      <input className="w-full bg-transparent outline-none placeholder:text-slate-400" placeholder={placeholder} value={value} onChange={(event) => onChange(event.target.value)} />
    </div>
  );
}

function Table({ columns, rows, renderRow }) {
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-200">
      <table className="w-full text-left text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>{columns.map((column) => <th key={column} className="px-4 py-3 font-semibold">{column}</th>)}</tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">{rows.map(renderRow)}</tbody>
      </table>
    </div>
  );
}

function ProgressBar({ parts }) {
  const total = parts.reduce((sum, part) => sum + part.value, 0) || 1;
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-200">
      <div className="flex h-4 w-full">
        {parts.map((part) => <div key={part.label} className={part.color} style={{ width: `${Math.max((part.value / total) * 100, part.value ? 2 : 0)}%` }} />)}
      </div>
      <div className="grid grid-cols-2 gap-px bg-slate-200 md:grid-cols-4">
        {parts.map((part) => (
          <div key={part.label} className="bg-white p-4">
            <p className="text-xs text-slate-500">{part.label}</p>
            <p className="mt-1 text-lg font-semibold">{part.value.toLocaleString()}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function LoginScreen({ onLogin, loading, error }) {
  const [login, setLogin] = useState("admin");
  const [password, setPassword] = useState("change-me-now");

  return (
    <div className="min-h-screen bg-slate-50 px-4 py-20">
      <div className="mx-auto max-w-md rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
        <h1 className="text-2xl font-semibold">Вход менеджера</h1>
        <p className="mt-1 text-sm text-slate-500">Авторизация через `POST /auth/login`</p>
        {error ? <p className="mt-3 rounded-xl bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p> : null}
        <div className="mt-4 space-y-3">
          <Field label="Логин"><Input value={login} onChange={(event) => setLogin(event.target.value)} /></Field>
          <Field label="Пароль"><Input type="password" value={password} onChange={(event) => setPassword(event.target.value)} /></Field>
        </div>
        <Button tone="dark" className="mt-4 w-full" disabled={loading} onClick={() => onLogin(login, password)}>
          {loading ? "Входим..." : "Войти"}
        </Button>
      </div>
    </div>
  );
}

function Shell({ screen, setScreen, children, onRefresh, onLogout, health, isBusy, flash }) {
  const current = navItems.find((item) => item.id === screen) || navItems[0];
  return (
    <div className="min-h-screen bg-slate-50 text-slate-950">
      <aside className="fixed inset-y-0 left-0 z-30 hidden w-72 border-r border-slate-200 bg-white lg:block">
        <div className="flex h-16 items-center gap-3 border-b border-slate-200 px-5">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-slate-950 text-white"><Bell size={19} /></div>
          <div>
            <p className="text-sm font-semibold">Кабинет рассылок</p>
            <p className="text-xs text-slate-500">для менеджера</p>
          </div>
        </div>
        <nav className="space-y-1 p-3">
          {navItems.map((item) => {
            const Icon = item.icon;
            const active = item.id === screen;
            return (
              <button key={item.id} onClick={() => setScreen(item.id)} className={cx("flex w-full items-center gap-3 rounded-2xl px-3 py-2.5 text-left text-sm font-medium transition", active ? "bg-slate-950 text-white shadow-sm" : "text-slate-600 hover:bg-slate-100 hover:text-slate-950")}>
                <Icon size={18} />
                {item.label}
              </button>
            );
          })}
        </nav>
        <div className="absolute bottom-0 left-0 right-0 border-t border-slate-200 p-4">
          <div className="rounded-2xl bg-slate-50 p-4">
            <div className="flex items-center gap-2"><CheckCircle2 size={16} className="text-emerald-600" /><p className="text-sm font-medium">{health.ready ? "API готов" : "API недоступно"}</p></div>
            <p className="mt-1 text-xs text-slate-500">healthz: {health.health ? "ok" : "fail"}, readyz: {health.ready ? "ok" : "fail"}</p>
          </div>
        </div>
      </aside>

      <div className="lg:pl-72">
        <header className="sticky top-0 z-20 border-b border-slate-200 bg-white/90 backdrop-blur">
          <div className="flex min-h-16 flex-col gap-3 px-4 py-3 md:flex-row md:items-center md:justify-between lg:px-6">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-slate-950 text-white lg:hidden"><Bell size={19} /></div>
              <div>
                <div className="flex items-center gap-2 text-sm text-slate-500"><span>Кабинет рассылок</span><ChevronRight size={14} /><span>{current.label}</span></div>
                <p className="text-sm font-semibold text-slate-950">Управление массовыми уведомлениями</p>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button tone="ghost" icon={RefreshCw} onClick={onRefresh} disabled={isBusy}>Обновить</Button>
              <Button tone="primary" icon={Plus} onClick={() => setScreen("campaign-new")}>Новая рассылка</Button>
              <Button tone="soft" onClick={onLogout}>Выйти</Button>
            </div>
          </div>
          {flash ? <div className="px-4 pb-3 text-sm text-slate-600 lg:px-6">{flash}</div> : null}
          <div className="flex gap-1 overflow-x-auto px-4 pb-3 lg:hidden">
            {navItems.map((item) => <Button key={item.id} tone={screen === item.id ? "dark" : "soft"} size="sm" onClick={() => setScreen(item.id)}>{item.label}</Button>)}
          </div>
        </header>
        <main className="px-4 py-6 lg:px-6">
          <motion.div key={screen} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.2 }}>
            {children}
          </motion.div>
        </main>
      </div>
    </div>
  );
}

function OverviewScreen({ campaigns, issues, setScreen }) {
  const activeCampaigns = campaigns.filter((campaign) => campaign.status === "sending" || campaign.status === "cancelling").length;
  const totalRecipients = campaigns.reduce((sum, campaign) => sum + campaign.stats.total, 0);
  const delivered = campaigns.reduce((sum, campaign) => sum + campaign.stats.delivered, 0);
  const failed = campaigns.reduce((sum, campaign) => sum + campaign.stats.failed, 0);

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Обзор" title="Рассылки и доставка" description="Здесь менеджер видит, сколько рассылок сейчас отправляется, сколько сообщений доставлено и где требуется внимание.">
        <Button tone="primary" icon={Send} onClick={() => setScreen("campaign-new")}>Создать рассылку</Button>
        <Button tone="ghost" icon={ShieldAlert} onClick={() => setScreen("issues")}>Проблемы доставки</Button>
      </PageHeader>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatCard icon={BarChart3} label="Активные рассылки" value={activeCampaigns} hint="идёт отправка" tone="info" />
        <StatCard icon={Users} label="Получателей всего" value={totalRecipients.toLocaleString()} hint="во всех рассылках" tone="violet" />
        <StatCard icon={CheckCircle2} label="Доставлено" value={delivered.toLocaleString()} hint="успешные сообщения" tone="good" />
        <StatCard icon={AlertTriangle} label="Не доставлено" value={failed.toLocaleString()} hint="можно повторить отправку" tone="warn" />
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <Card className="xl:col-span-2">
          <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div><h2 className="text-lg font-semibold">Последние рассылки</h2><p className="text-sm text-slate-500">Прогресс обновляется автоматически.</p></div>
            <Button tone="ghost" size="sm" onClick={() => setScreen("campaigns")}>Все рассылки</Button>
          </div>
          <CampaignsTable rows={campaigns} onOpen={() => { setScreen("campaign-detail"); }} />
        </Card>

        <Card>
          <h2 className="text-lg font-semibold">Что требует внимания</h2>
          <div className="mt-4 space-y-3">
            {issues.slice(0, 3).map((issue) => (
              <div key={issue.id} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="font-medium">{issue.campaign}</p>
                    <p className="mt-1 text-sm text-slate-500">{issue.channel}: {issue.reason}</p>
                  </div>
                  <Badge tone={issue.severity}>{issue.count}</Badge>
                </div>
                <Button tone="ghost" size="sm" icon={RefreshCw} className="mt-3">{issue.action}</Button>
              </div>
            ))}
            {issues.length === 0 ? <p className="text-sm text-slate-500">Проблем пока нет.</p> : null}
          </div>
        </Card>
      </div>
    </div>
  );
}

function CampaignCreateScreen({ channels, onCreate, loading }) {
  const [audience, setAudience] = useState("all");
  const [priority, setPriority] = useState("normal");
  const [name, setName] = useState("Майская акция");
  const [subject, setSubject] = useState("Акция");
  const [body, setBody] = useState("Здравствуйте! Для вас новое предложение.");
  const [audienceInput, setAudienceInput] = useState("");
  const [selectedChannels, setSelectedChannels] = useState(["email", "sms"]);

  const availableChannels = channels.length > 0 ? channels : [
    { id: "email", code: "email", displayName: "Email", state: "enabled", description: "Письма на email" },
    { id: "sms", code: "sms", displayName: "SMS", state: "enabled", description: "SMS" },
  ];

  const selectedAudience = audiences.find((item) => item.id === audience) || audiences[0];
  const selectedChannelLabels = selectedChannels.map(formatChannel).join(", ");
  const parsedExternalIds = audienceInput
    .split(/[\n,;]+/)
    .map((value) => value.trim())
    .filter(Boolean);
  const selectedUsersCount = parsedExternalIds.length;
  const canSubmit = audience !== "selected" || selectedUsersCount > 0;
  const [estimate, setEstimate] = useState({
    users: null,
    tasks: null,
    loading: false,
    error: false,
  });

  const toggleChannel = (code) => {
    setSelectedChannels((current) => current.includes(code) ? current.filter((item) => item !== code) : [...current, code]);
  };

  const toRecipientSelector = () => {
    if (audience === "all") return { type: "all" };
    return { type: "external_ids", externalIds: parsedExternalIds };
  };

  const submit = async () => {
    await onCreate(toCreateCampaignPayload({
      name,
      subject,
      body,
      regionIds: ["default"],
      recipientSelector: toRecipientSelector(),
      channels: selectedChannels,
      priority,
    }));
  };

  useEffect(() => {
    let cancelled = false;
    const timer = setTimeout(async () => {
      if (selectedChannels.length === 0) {
        setEstimate({ users: 0, tasks: 0, loading: false, error: false });
        return;
      }
      if (audience === "selected" && selectedUsersCount === 0) {
        setEstimate({ users: 0, tasks: 0, loading: false, error: false });
        return;
      }

      setEstimate((prev) => ({ ...prev, loading: true, error: false }));
      try {
        const recipientSelector = audience === "all"
          ? { type: "all" }
          : { type: "external_ids", externalIds: parsedExternalIds };
        const response = await api.estimateRecipients({
          regionId: "default",
          channels: selectedChannels,
          recipientSelector,
        });
        if (cancelled) return;
        setEstimate({
          users: response?.estimatedUsers ?? 0,
          tasks: response?.estimatedTasks ?? 0,
          loading: false,
          error: false,
        });
      } catch {
        if (cancelled) return;
        setEstimate((prev) => ({ ...prev, loading: false, error: true }));
      }
    }, 250);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [audience, audienceInput, selectedChannels, selectedUsersCount]);

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Новая рассылка" title="Создание рассылки" description="Менеджер выбирает аудиторию, каналы и текст сообщения. Система сама отправит уведомления и покажет прогресс.">
        <Button tone="ghost">Сохранить черновик</Button>
        <Button tone="primary" icon={Send} onClick={submit} disabled={loading || !canSubmit}>Запустить рассылку</Button>
      </PageHeader>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-5">
        <div className="space-y-6 xl:col-span-3">
          <Card>
            <h2 className="text-lg font-semibold">Основное</h2>
            <p className="mt-1 text-sm text-slate-500">Название и сообщение, которое увидят получатели.</p>
            <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-2">
              <Field label="Название рассылки"><Input value={name} onChange={(event) => setName(event.target.value)} /></Field>
              <Field label="Важность"><Select value={priority} onChange={(event) => setPriority(event.target.value)}><option value="low">низкая</option><option value="normal">обычная</option><option value="high">важная</option></Select></Field>
              <Field label="Тема"><Input value={subject} onChange={(event) => setSubject(event.target.value)} /></Field>
              <Field label="Когда отправить"><Select defaultValue="now"><option value="now">сразу</option></Select></Field>
              <Field label="Текст сообщения"><Textarea rows={5} value={body} onChange={(event) => setBody(event.target.value)} /></Field>
            </div>
          </Card>

          <Card>
            <h2 className="text-lg font-semibold">Аудитория</h2>
            <p className="mt-1 text-sm text-slate-500">Кому отправить рассылку.</p>
            <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-2">
              {audiences.map((item) => (
                <button key={item.id} onClick={() => setAudience(item.id)} className={cx("rounded-2xl border p-4 text-left transition", audience === item.id ? "border-blue-300 bg-blue-50" : "border-slate-200 bg-white hover:bg-slate-50")}>
                  <div className="flex items-start justify-between gap-3">
                    <div><p className="font-medium">{item.title}</p><p className="mt-1 text-sm text-slate-500">{item.hint}</p></div>
                    <Badge tone={audience === item.id ? "info" : "neutral"}>
                      {item.id === "selected"
                        ? `${selectedUsersCount}`
                        : item.id === "all" && estimate.users !== null
                          ? estimate.users.toLocaleString("ru-RU")
                          : item.size}
                    </Badge>
                  </div>
                </button>
              ))}
            </div>
            <div className="mt-4">
              {audience === "selected" ? (
                <Field label="External IDs" hint="Можно указывать через запятую, точку с запятой или с новой строки.">
                  <Textarea
                    rows={4}
                    value={audienceInput}
                    onChange={(event) => setAudienceInput(event.target.value)}
                    placeholder={"user-1\nuser-2\nuser-3"}
                  />
                  <div className="mt-3 flex items-center justify-between gap-3">
                    <p className="text-xs text-slate-500">Найдено ID: {parsedExternalIds.length}</p>
                  </div>
                  {selectedUsersCount === 0 ? (
                    <p className="mt-2 text-xs text-rose-600">Добавьте хотя бы один external ID, чтобы запустить рассылку.</p>
                  ) : null}
                  {parsedExternalIds.length > 0 ? (
                    <div className="mt-2 flex flex-wrap gap-2">
                      {parsedExternalIds.slice(0, 20).map((id) => (
                        <span key={id} className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs text-slate-700">
                          {id}
                        </span>
                      ))}
                      {parsedExternalIds.length > 20 ? (
                        <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-500">
                          +{parsedExternalIds.length - 20} ещё
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                </Field>
              ) : null}
            </div>
          </Card>

          <Card>
            <h2 className="text-lg font-semibold">Каналы отправки</h2>
            <p className="mt-1 text-sm text-slate-500">Выберите, куда отправить сообщение.</p>
            <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-2">
              {availableChannels.map((channel) => {
                const disabled = channel.state === "disabled";
                const selected = selectedChannels.includes(channel.code);
                return (
                  <button key={channel.code} disabled={disabled} onClick={() => toggleChannel(channel.code)} className={cx("rounded-2xl border p-4 text-left transition", selected ? "border-emerald-300 bg-emerald-50" : "border-slate-200 bg-white hover:bg-slate-50", disabled ? "cursor-not-allowed opacity-50" : "")}>
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex items-center gap-3">
                        <div className="rounded-xl bg-white p-2 text-slate-700 shadow-sm">{channel.code === "email" ? <Mail size={18} /> : channel.code === "sms" ? <Smartphone size={18} /> : <MessageCircle size={18} />}</div>
                        <div>
                          <p className="font-medium">{channel.displayName}</p>
                          <p className="text-xs text-slate-400">code: {channel.code}</p>
                          <p className="text-sm text-slate-500">{channel.description}</p>
                        </div>
                      </div>
                      <Badge tone={statusTone(channel.state)}>{formatStatus(channel.state)}</Badge>
                    </div>
                  </button>
                );
              })}
            </div>
          </Card>
        </div>

        <div className="space-y-6 xl:col-span-2">
          <Card className="sticky top-24">
            <h2 className="text-lg font-semibold">Предпросмотр</h2>
            <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-xs uppercase tracking-wide text-slate-400">Сообщение</p>
              <p className="mt-2 font-semibold">{subject || "Без темы"}</p>
              <p className="mt-1 text-sm text-slate-600">{body || "Без текста"}</p>
            </div>
            <div className="mt-4 space-y-3">
              <SummaryRow label="Аудитория" value={`${selectedAudience.title}`} />
              <SummaryRow
                label="Выбрано пользователей"
                value={
                  audience === "selected"
                    ? selectedUsersCount.toLocaleString("ru-RU")
                    : audience === "all"
                      ? "все активные"
                      : "—"
                }
              />
              <SummaryRow
                label="Получателей (оценка)"
                value={
                  estimate.loading
                    ? "считаем..."
                    : estimate.users !== null
                      ? estimate.users.toLocaleString("ru-RU")
                      : "—"
                }
              />
              <SummaryRow
                label="Сообщений (оценка)"
                value={
                  estimate.loading
                    ? "считаем..."
                    : estimate.tasks !== null
                      ? estimate.tasks.toLocaleString("ru-RU")
                      : "—"
                }
              />
              {estimate.error ? <p className="text-xs text-amber-600">Не удалось посчитать оценку, попробуйте обновить.</p> : null}
              <SummaryRow label="Каналы" value={selectedChannelLabels || "не выбраны"} />
              <SummaryRow label="Важность" value={formatPriority(priority)} />
              <SummaryRow label="Старт" value="сразу после запуска" />
            </div>
            <Button tone="primary" icon={Send} className="mt-5 w-full" onClick={submit} disabled={loading || !canSubmit}>Запустить рассылку</Button>
          </Card>
        </div>
      </div>
    </div>
  );
}

function CampaignsScreen({ campaigns, onOpenCampaign }) {
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Рассылки" title="Все рассылки" description="Список запущенных, завершённых и требующих внимания рассылок." />
      <Card>
        <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <SearchBox placeholder="Поиск по названию" />
          <div className="flex gap-2"><Button tone="ghost" icon={Filter}>Статус</Button><Button tone="ghost" icon={SlidersHorizontal}>Период</Button></div>
        </div>
        <CampaignsTable rows={campaigns} onOpen={onOpenCampaign} />
      </Card>
    </div>
  );
}

function CampaignsTable({ rows, onOpen }) {
  return <Table columns={["Рассылка", "Статус", "Аудитория", "Каналы", "Доставлено", "Создана", ""]} rows={rows} renderRow={(campaign) => {
    const progress = campaign.stats.total > 0 ? Math.round((campaign.stats.delivered / campaign.stats.total) * 100) : 0;
    return (
      <tr key={campaign.id}>
        <td className="px-4 py-3"><p className="font-medium">{campaign.name}</p><p className="text-xs text-slate-500">важность: {formatPriority(campaign.priority)}</p></td>
        <td className="px-4 py-3"><Badge tone={statusTone(campaign.status)}>{formatStatus(campaign.status)}</Badge></td>
        <td className="px-4 py-3 text-slate-600">{campaign.audience}</td>
        <td className="px-4 py-3"><div className="flex flex-wrap gap-1">{campaign.channels.map((channel) => <Badge key={`${campaign.id}-${channel}`}>{formatChannel(channel)}</Badge>)}</div></td>
        <td className="px-4 py-3"><div className="flex items-center gap-3"><div className="h-2 w-28 rounded-full bg-slate-100"><div className="h-2 rounded-full bg-emerald-500" style={{ width: `${progress}%` }} /></div><span className="text-xs text-slate-500">{progress}%</span></div></td>
        <td className="px-4 py-3 text-slate-500">{campaign.createdAt}</td>
        <td className="px-4 py-3 text-right"><Button tone="ghost" size="sm" icon={Eye} onClick={() => onOpen(campaign.id)}>Открыть</Button></td>
      </tr>
    );
  }} />;
}

function CampaignDetailScreen({ campaign, rows, issues, onCancel }) {
  const [tab, setTab] = useState("summary");
  if (!campaign) return <Card><p className="text-sm text-slate-500">Выберите рассылку.</p></Card>;

  const parts = [
    { label: "Доставлено", value: campaign.stats.delivered, color: "bg-emerald-500" },
    { label: "В процессе", value: campaign.stats.inProgress, color: "bg-blue-500" },
    { label: "Ожидает повтора", value: campaign.stats.waiting, color: "bg-amber-500" },
    { label: "Не доставлено", value: campaign.stats.failed, color: "bg-rose-500" },
  ];
  const tabs = [
    ["summary", "Сводка"],
    ["recipients", "Получатели"],
    ["results", "Результаты"],
    ["issues", "Проблемы"],
    ["settings", "Настройки"],
  ];

  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Карточка рассылки" title={campaign.name} description="Здесь видно, как идёт отправка, кому сообщение уже доставлено и где есть проблемы.">
        <Button tone="ghost" icon={Copy}>Скопировать ссылку</Button>
        <Button tone="bad" icon={Ban} onClick={onCancel}>Остановить рассылку</Button>
      </PageHeader>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <StatCard icon={Users} label="Получателей" value={campaign.stats.total.toLocaleString()} hint={campaign.audience} tone="info" />
        <StatCard icon={CheckCircle2} label="Доставлено" value={campaign.stats.delivered.toLocaleString()} hint="успешно получили" tone="good" />
        <StatCard icon={Clock} label="В процессе" value={campaign.stats.inProgress.toLocaleString()} hint="ещё отправляется" tone="violet" />
        <StatCard icon={AlertTriangle} label="Проблемы" value={campaign.stats.failed.toLocaleString()} hint="можно повторить" tone="warn" />
      </div>

      <Card>
        <div className="mb-5 flex flex-wrap gap-2">
          {tabs.map(([tabId, label]) => <button key={tabId} onClick={() => setTab(tabId)} className={cx("rounded-2xl px-4 py-2 text-sm font-semibold transition", tab === tabId ? "bg-slate-950 text-white" : "bg-slate-100 text-slate-600 hover:bg-slate-200")}>{label}</button>)}
        </div>
        {tab === "summary" ? <CampaignSummary campaign={campaign} parts={parts} /> : null}
        {tab === "recipients" ? <RecipientsResultTable rows={rows} /> : null}
        {tab === "results" ? <ResultsTable rows={rows} /> : null}
        {tab === "issues" ? <IssuesInnerList rows={issues} /> : null}
        {tab === "settings" ? <CampaignSettings campaign={campaign} onCancel={onCancel} /> : null}
      </Card>
    </div>
  );
}

function CampaignSummary({ campaign, parts }) {
  return (
    <div className="space-y-5">
      <div><h2 className="text-lg font-semibold">Прогресс отправки</h2><p className="text-sm text-slate-500">Сводка по всем выбранным каналам.</p></div>
      <ProgressBar parts={parts} />
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <SummaryCard title="Сообщение" text={`${campaign.message.subject}: ${campaign.message.body}`} />
        <SummaryCard title="Каналы" text={campaign.channels.map(formatChannel).join(", ")} />
        <SummaryCard title="Статус" text={formatStatus(campaign.status)} />
      </div>
    </div>
  );
}

function RecipientsResultTable({ rows }) {
  return <div className="space-y-4"><div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between"><SearchBox placeholder="получатель / канал / статус" /><Button tone="ghost" icon={Filter}>Фильтр</Button></div><Table columns={["Получатель", "Канал", "Статус", "Попытки", "Время", "Действие"]} rows={rows} renderRow={(row) => <tr key={row.id}><td className="px-4 py-3 font-medium">{row.recipient}</td><td className="px-4 py-3"><Badge>{formatChannel(row.channel)}</Badge></td><td className="px-4 py-3"><Badge tone={statusTone(row.status)}>{formatStatus(row.status)}</Badge></td><td className="px-4 py-3">{row.attempts}</td><td className="px-4 py-3 text-slate-500">{row.sentAt}</td><td className="px-4 py-3"><Button tone="ghost" size="sm" icon={MoreHorizontal}>Действия</Button></td></tr>} /></div>;
}

function ResultsTable({ rows }) {
  return <div className="space-y-4"><div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between"><SearchBox placeholder="получатель / статус / канал" /><Button tone="ghost" icon={Upload}>Экспорт</Button></div><Table columns={["Получатель", "Канал", "Сообщение", "Статус", "Время"]} rows={rows} renderRow={(row) => <tr key={row.id}><td className="px-4 py-3 font-medium">{row.recipient}</td><td className="px-4 py-3">{formatChannel(row.channel)}</td><td className="px-4 py-3 text-slate-600">{row.message}</td><td className="px-4 py-3"><Badge tone={statusTone(row.status)}>{formatStatus(row.status)}</Badge></td><td className="px-4 py-3 text-slate-500">{row.sentAt}</td></tr>} /></div>;
}

function IssuesInnerList({ rows }) {
  return <div className="space-y-4"><div className="flex items-center justify-between"><div><h2 className="text-lg font-semibold">Проблемы этой рассылки</h2><p className="text-sm text-slate-500">Можно повторить отправку для выбранных получателей.</p></div><Button tone="primary" icon={RefreshCw}>Повторить все</Button></div><IssuesTable rows={rows} /></div>;
}

function CampaignSettings({ campaign, onCancel }) {
  return <div className="grid grid-cols-1 gap-6 lg:grid-cols-2"><div><h2 className="text-lg font-semibold">Настройки рассылки</h2><p className="mt-1 text-sm text-slate-500">Менеджер может остановить рассылку или повторить отправку по проблемным получателям.</p><div className="mt-5 space-y-4"><SummaryRow label="Название" value={campaign.name} /><SummaryRow label="Важность" value={formatPriority(campaign.priority)} /><SummaryRow label="Каналы" value={campaign.channels.map(formatChannel).join(", ")} /></div></div><div className="rounded-2xl border border-rose-200 bg-rose-50 p-5"><h3 className="font-semibold text-rose-700">Опасная зона</h3><p className="mt-2 text-sm text-rose-700">Остановка рассылки прекратит новые отправки, но уже доставленные сообщения останутся в истории.</p><Button tone="bad" icon={Ban} className="mt-4" onClick={onCancel}>Остановить рассылку</Button></div></div>;
}

function IssuesScreen({ issues, onReplay }) {
  const total = issues.reduce((sum, i) => sum + i.count, 0);
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Проблемы доставки" title="Что не удалось доставить" description="Здесь менеджер видит понятные причины проблем и может повторить отправку без технических деталей.">
        <Button tone="primary" icon={RefreshCw} onClick={onReplay}>Повторить выбранные</Button>
      </PageHeader>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        <StatCard icon={AlertTriangle} label="Требуют внимания" value={total} hint="по всем рассылкам" tone="warn" />
        <StatCard icon={RefreshCw} label="Можно повторить" value={total} hint="временные ошибки" tone="info" />
        <StatCard icon={Ban} label="Недоступные адреса" value={issues.filter((i) => i.reason.includes("Адрес")).length} hint="лучше исключить" tone="bad" />
      </div>
      <Card><div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between"><SearchBox placeholder="кампания / канал / причина" /><div className="flex gap-2"><Button tone="ghost" icon={Filter}>Тип проблемы</Button><Button tone="ghost" icon={SlidersHorizontal}>Период</Button></div></div><IssuesTable rows={issues} /></Card>
    </div>
  );
}

function IssuesTable({ rows }) {
  return <Table columns={["Рассылка", "Канал", "Причина", "Получателей", "Действие"]} rows={rows} renderRow={(row) => <tr key={row.id}><td className="px-4 py-3 font-medium">{row.campaign}</td><td className="px-4 py-3"><Badge>{row.channel}</Badge></td><td className="px-4 py-3 text-slate-600">{row.reason}</td><td className="px-4 py-3"><Badge tone={row.severity}>{row.count}</Badge></td><td className="px-4 py-3"><Button tone="ghost" size="sm" icon={row.action.includes("Повторить") ? RefreshCw : Eye}>{row.action}</Button></td></tr>} />;
}

function RecipientsScreen({ onBulkImport, bulkJson, setBulkJson, bulkResult, loading }) {
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Получатели" title="Получатели и сегменты" description="Импорт получателей через `POST /users/bulk`.">
        <Button tone="ghost" icon={Upload} onClick={onBulkImport} disabled={loading}>Импортировать список</Button>
      </PageHeader>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-4"><StatCard icon={Users} label="Импорт" value="users/bulk" hint="endpoint" tone="good" /><StatCard icon={Mail} label="Mode" value="skip/upsert" hint="payload.mode" tone="info" /><StatCard icon={Smartphone} label="Region" value="default" hint="MVP" tone="info" /><StatCard icon={MessageCircle} label="Идемпотентность" value="on" hint="Idempotency-Key" tone="violet" /></div>
      <Card>
        <Field label="JSON payload">
          <Textarea rows={12} value={bulkJson} onChange={(event) => setBulkJson(event.target.value)} />
        </Field>
      </Card>
      {bulkResult ? <Card><pre className="overflow-auto text-xs">{JSON.stringify(bulkResult, null, 2)}</pre></Card> : null}
    </div>
  );
}

function ChannelsScreen({ channels, onToggleChannel }) {
  return (
    <div className="space-y-6">
      <PageHeader eyebrow="Каналы" title="Доступные каналы отправки" description="Менеджер видит, какие каналы доступны для новых рассылок.">
        <Button tone="primary" icon={Plus}>Запросить новый канал</Button>
      </PageHeader>
      <Card>
        <Table columns={["Канал", "Описание", "Статус", "По умолчанию", "Действия"]} rows={channels} renderRow={(channel) => <tr key={channel.id}><td className="px-4 py-3"><div className="flex items-center gap-3"><div className="rounded-xl bg-slate-50 p-2">{channel.code === "email" ? <Mail size={18} /> : channel.code === "sms" ? <Smartphone size={18} /> : <MessageCircle size={18} />}</div><p className="font-medium">{channel.displayName}</p></div></td><td className="px-4 py-3 text-slate-600">{channel.description}</td><td className="px-4 py-3"><Badge tone={statusTone(channel.state)}>{formatStatus(channel.state)}</Badge></td><td className="px-4 py-3">{channel.selectedByDefault ? "да" : "нет"}</td><td className="px-4 py-3"><div className="flex gap-2"><Button tone="ghost" size="sm" icon={channel.state === "enabled" ? PauseCircle : PlayCircle} onClick={() => onToggleChannel(channel)}>{channel.state === "enabled" ? "Отключить" : "Включить"}</Button></div></td></tr>} />
      </Card>
    </div>
  );
}

function SummaryRow({ label, value }) {
  return <div className="flex items-start justify-between gap-4 rounded-2xl border border-slate-200 bg-slate-50 p-3"><p className="text-sm text-slate-500">{label}</p><p className="text-right text-sm font-medium text-slate-800">{value}</p></div>;
}

function SummaryCard({ title, text }) {
  return <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4"><p className="text-sm text-slate-500">{title}</p><p className="mt-1 font-semibold text-slate-900">{text}</p></div>;
}

function toIssueSummary(rawErrors, campaignById) {
  const map = new Map();
  rawErrors.forEach((row) => {
    const issue = adaptIssue(row);
    const key = `${issue.campaign}|${issue.channel}|${issue.reason}`;
    const prev = map.get(key);
    if (!prev) {
      map.set(key, { ...issue, count: 1, campaign: campaignById.get(row.campaignId) || issue.campaign });
      return;
    }
    prev.count += 1;
  });
  return Array.from(map.values());
}

export default function NotificationPlatformFrontend() {
  const [screen, setScreen] = useState("overview");
  const [token, setToken] = useState(() => getStoredToken());
  const [busy, setBusy] = useState(false);
  const [authError, setAuthError] = useState("");
  const [flash, setFlash] = useState("");
  const [health, setHealth] = useState({ health: false, ready: false });

  const [campaigns, setCampaigns] = useState([]);
  const [channels, setChannels] = useState([]);
  const [selectedCampaignId, setSelectedCampaignId] = useState("");
  const [campaignRows, setCampaignRows] = useState([]);
  const [rawCampaignErrors, setRawCampaignErrors] = useState([]);
  const [bulkJson, setBulkJson] = useState(JSON.stringify({ mode: "skip_duplicates", items: [{ externalId: "demo-user-1", status: "active", channels: [{ channel: "email", address: "demo@example.com", status: "active", verified: true }] }] }, null, 2));
  const [bulkResult, setBulkResult] = useState(null);

  const isAuthed = Boolean(token);

  const setBusyMessage = async (fn, okText) => {
    setBusy(true);
    setFlash("");
    setAuthError("");
    try {
      await fn();
      if (okText) setFlash(okText);
    } catch (err) {
      if (err?.status === 401) {
        clearStoredToken();
        setToken("");
        setAuthError("Сессия истекла. Войдите заново.");
      } else {
        setFlash(err?.message || "Ошибка запроса");
      }
    } finally {
      setBusy(false);
    }
  };

  const loadHealth = async () => {
    const healthOk = await api.health().then(() => true).catch(() => false);
    const readyOk = await api.ready().then(() => true).catch(() => false);
    try {
      setHealth({ health: healthOk, ready: readyOk });
    } catch {
      // noop
    }
  };

  const loadCampaigns = async () => {
    const data = await api.listCampaigns({ limit: 50 });
    const baseItems = data?.items || [];
    const items = await Promise.all(baseItems.map(async (item) => {
      const stats = await api.getCampaignStats(item.campaignId).catch(() => null);
      return adaptCampaign(item, stats);
    }));
    setCampaigns(items);
    if (!selectedCampaignId && items[0]?.id) setSelectedCampaignId(items[0].id);
  };

  const loadChannels = async () => {
    const data = await api.listChannels();
    setChannels((data?.items || []).map((row) => adaptChannel(row)));
  };

  const loadCampaignDetails = async (campaignId) => {
    if (!campaignId) return;
    const [statsRes, tasksRes, resultsRes, errorsRes] = await Promise.all([
      api.getCampaignStats(campaignId),
      api.getCampaignTasks(campaignId, { limit: 100 }),
      api.getCampaignResults(campaignId, { limit: 100 }),
      api.getCampaignErrors(campaignId, { limit: 100 }),
    ]);

    const rowById = new Map();
    const resultRows = (resultsRes?.items || []).map((row) => adaptResult(row));
    const taskRows = (tasksRes?.items || []).map((row) => adaptResult(row));
    [...taskRows, ...resultRows].forEach((row) => rowById.set(row.id, row));

    setCampaigns((prev) => prev.map((campaign) => {
      if (campaign.id !== campaignId) return campaign;
      return {
        ...campaign,
        status: mapCampaignStatus(statsRes?.status || campaign.status),
        stats: normalizeCampaignStats(statsRes?.stats || campaign.stats || {}),
      };
    }));

    setCampaignRows(Array.from(rowById.values()));
    setRawCampaignErrors((errorsRes?.items || []).map((row) => ({ ...row, campaignId })));
  };

  const waitCampaignCancelFinalized = async (campaignId, timeoutMs = 12000, pollIntervalMs = 1000) => {
    const startedAt = Date.now();
    while (Date.now() - startedAt < timeoutMs) {
      const statsRes = await api.getCampaignStats(campaignId).catch(() => null);
      const rawStatus = statsRes?.status;
      if (rawStatus) {
        const mappedStatus = mapCampaignStatus(rawStatus);
        setCampaigns((prev) => prev.map((campaign) => {
          if (campaign.id !== campaignId) return campaign;
          return {
            ...campaign,
            status: mappedStatus,
            stats: normalizeCampaignStats(statsRes?.stats || campaign.stats || {}),
          };
        }));
      }
      if (rawStatus === "cancelled") {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  };

  const refreshAll = async () => {
    await loadHealth();
    if (!isAuthed) return;
    await Promise.all([loadCampaigns(), loadChannels()]);
  };

  useEffect(() => {
    const timer = setTimeout(() => {
      void loadHealth();
    }, 0);
    return () => clearTimeout(timer);
  }, []);

  useEffect(() => {
    if (!isAuthed) return;
    const timer = setTimeout(() => {
      void setBusyMessage(refreshAll, "Данные обновлены");
    }, 0);
    return () => clearTimeout(timer);
  }, [isAuthed]);

  useEffect(() => {
    if (!isAuthed || !selectedCampaignId) return;
    const timer = setTimeout(() => {
      void setBusyMessage(() => loadCampaignDetails(selectedCampaignId));
    }, 0);
    return () => clearTimeout(timer);
  }, [isAuthed, selectedCampaignId]);

  const issues = useMemo(() => {
    const map = new Map(campaigns.map((c) => [c.id, c.name]));
    return toIssueSummary(rawCampaignErrors, map);
  }, [rawCampaignErrors, campaigns]);

  const selectedCampaign = useMemo(() => campaigns.find((c) => c.id === selectedCampaignId) || campaigns[0], [campaigns, selectedCampaignId]);

  const login = async (loginValue, password) => {
    await setBusyMessage(async () => {
      const response = await api.login({ login: loginValue, password });
      const accessToken = response?.accessToken || response?.access_token;
      if (!accessToken) throw new Error("Токен не получен");
      setStoredToken(accessToken);
      setToken(accessToken);
      setFlash("Успешный вход");
    });
  };

  const logout = () => {
    clearStoredToken();
    setToken("");
    setFlash("Вы вышли из аккаунта");
  };

  const createCampaign = async (payload) => {
    await setBusyMessage(async () => {
      await api.createCampaign(payload);
      await loadCampaigns();
      setScreen("campaigns");
    }, "Рассылка создана");
  };

  const cancelCampaign = async () => {
    if (!selectedCampaign) return;
    await setBusyMessage(async () => {
      await api.cancelCampaign(selectedCampaign.id, { reason: "Manual stop from UI" });
      await waitCampaignCancelFinalized(selectedCampaign.id);
      await loadCampaigns();
      await loadCampaignDetails(selectedCampaign.id);
    }, "Запрос на отмену отправлен");
  };

  const toggleChannel = async (channel) => {
    await setBusyMessage(async () => {
      if (channel.state === "enabled") await api.disableChannel(channel.id);
      else await api.enableChannel(channel.id);
      await loadChannels();
    }, "Статус канала обновлен");
  };

  const replayDlq = async () => {
    await setBusyMessage(async () => {
      const campaignId = selectedCampaign?.id;
      await api.replayDlq({
        regionId: "default",
        filter: campaignId ? { campaignId } : {},
        limit: 100,
        additionalAttempts: 0,
        reason: "manual replay",
      });
    }, "DLQ replay запущен");
  };

  const usersBulkImport = async () => {
    await setBusyMessage(async () => {
      const payload = JSON.parse(bulkJson);
      const result = await api.usersBulkImport(payload);
      setBulkResult(result);
    }, "Импорт получателей выполнен");
  };

  if (!isAuthed) {
    return <LoginScreen onLogin={login} loading={busy} error={authError || flash} />;
  }

  const screenNode = {
    overview: <OverviewScreen campaigns={campaigns} issues={issues} setScreen={setScreen} />,
    "campaign-new": <CampaignCreateScreen channels={channels} onCreate={createCampaign} loading={busy} />,
    campaigns: <CampaignsScreen campaigns={campaigns} onOpenCampaign={(id) => { setSelectedCampaignId(id); setScreen("campaign-detail"); }} />,
    "campaign-detail": <CampaignDetailScreen campaign={selectedCampaign} rows={campaignRows} issues={issues.filter((x) => x.campaign === selectedCampaign?.name)} onCancel={cancelCampaign} />,
    issues: <IssuesScreen issues={issues} onReplay={replayDlq} />,
    recipients: <RecipientsScreen onBulkImport={usersBulkImport} bulkJson={bulkJson} setBulkJson={setBulkJson} bulkResult={bulkResult} loading={busy} />,
    channels: <ChannelsScreen channels={channels} onToggleChannel={toggleChannel} />,
  }[screen] || <OverviewScreen campaigns={campaigns} issues={issues} setScreen={setScreen} />;

  return (
    <Shell
      screen={screen}
      setScreen={setScreen}
      onRefresh={() => setBusyMessage(refreshAll, "Обновлено")}
      onLogout={logout}
      health={health}
      isBusy={busy}
      flash={flash}
    >
      {screenNode}
    </Shell>
  );
}
