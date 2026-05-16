# Frontend

## Environment

- `VITE_PROXY_TARGET`: dev-only proxy target for Vite (`/api` forwarding).
- `VITE_API_BASE_URL`: production API base URL, injected during Docker image build.

Example:

```env
VITE_API_BASE_URL=https://api.example.com
```

## Local development

```bash
cd Notifications_service/frontend
npm ci
npm run dev
```

## Docker build (production)

Frontend image is built from `./Dockerfile` and served by nginx with SPA fallback.
