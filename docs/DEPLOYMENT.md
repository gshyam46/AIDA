# Deploying AIDA

This guide covers:

1. Publishing the frontend on Vercel.
2. Recording sign-up and sign-in interest while accounts are unavailable.
3. Whether the Python backend can run on Vercel.
4. What must change before the backend is hosted publicly anywhere.

## 1. Frontend on Vercel

The frontend deploys on its own. With `NEXT_PUBLIC_AIDA_MODE=preview` it serves the landing page and `/benchmarks`, while sign-in, sign-up, onboarding and the workspace show a "Currently unavailable" page that records early-access interest. No backend, database or model key is needed for that.

### Import the repository

1. On vercel.com, choose **Add New → Project** and import `gshyam46/AIDA`.
2. Set **Root Directory** to `frontend`. Vercel detects Next.js; keep the default build settings.
3. Add the environment variables below, then deploy.
4. The site code is on the `mvp2.0` branch. Either set **Settings → Environments → Production → Branch Tracking** to `mvp2.0` and redeploy, or merge `mvp2.0` into `main`.
5. Optional: add a subdomain such as `aida.yourdomain.com` under **Settings → Domains**, and create the CNAME record Vercel shows at your DNS provider.

To use the CLI instead, run `npx vercel login` once, then from `frontend/`:

```powershell
npx vercel deploy --prod --build-env NEXT_PUBLIC_AIDA_MODE=preview --env NEXT_PUBLIC_AIDA_MODE=preview
```

`frontend/.vercelignore` keeps local `.env` files out of the upload.

### Environment variables

| Variable | Where it is used | Value |
| --- | --- | --- |
| `NEXT_PUBLIC_AIDA_MODE` | Build time, browser | `preview` for a frontend-only deployment; leave unset when a backend is available |
| `SUPABASE_URL` | Server only | Your Supabase project URL, for interest capture |
| `SUPABASE_SERVICE_ROLE_KEY` | Server only | The project's secret (service role) key. Never prefix it with `NEXT_PUBLIC_` |
| `SUPABASE_INTEREST_TABLE` | Server only | Optional; defaults to `aida_interest` |
| `INTEREST_WEBHOOK_URL` | Server only | Optional alternative to Supabase: every submission is POSTed there as JSON |
| `BACKEND_URL` | Server only | The AIDA backend base URL, once one is hosted |

`frontend/.env.example` lists the same variables. Vercel's Hobby plan is for personal, non-commercial projects.

## 2. Recording sign-up and sign-in interest

### When the backend is running

Accounts, sessions, onboarding answers and security events are stored by the backend in `backend/data/auth.sqlite`. Security events include sign-ups, successful and failed sign-ins, lockouts and rate limits; passwords and question text are never stored. The owner account can read them at `GET /api/v1/security/events`.

### When accounts are unavailable

Every account page first calls `/api/v1/health`. The page switches to **Currently unavailable** when any of these happen:

- the build is in preview mode;
- the backend is not deployed or does not answer within five seconds;
- a request fails with a network error or a 5xx response, including a sign-up, sign-in or onboarding submission that fails part-way.

The page thanks the visitor for their interest and offers a short form. Name and email are carried over from the form they were filling in. The form sends its data to `POST /api/interest`, a Next.js route that runs on Vercel itself:

- **Stored fields:** email, name, company, role, what they want to analyze, the page they came from (`signup`, `login`, `onboarding` or `workspace`), consent to be emailed, and the time.
- **Passwords are never sent or stored.** The test in `scripts/e2e-availability.cjs` checks this.
- **Storage:** Supabase via its REST API using the server-side key, otherwise the webhook. A repeat submission with the same email updates the existing row.
- **Honest confirmation:** the visitor sees "Thank you" only after the save succeeds. If storage is not configured or fails, they see "We could not save your details right now."
- **Protections:**
  - same-origin submissions only;
  - bodies up to 4 KB;
  - a hidden bot-trap field;
  - email and consent validation;
  - a best-effort limit of 5 submissions per client per 10 minutes, kept in memory per serverless instance, using the `x-real-ip` header Vercel sets.

### Create the Supabase table

In the Supabase SQL editor:

```sql
create table if not exists public.aida_interest (
  id bigint generated always as identity primary key,
  email text not null unique,
  name text,
  company text,
  role_title text,
  interest text,
  source text not null default 'signup' check (source in ('signup', 'login', 'onboarding', 'workspace')),
  consent boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- No policies are added: the public anon key cannot read or write this table.
-- The server-side secret key used by /api/interest bypasses row-level security.
alter table public.aida_interest enable row level security;
```

Then set `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in Vercel and redeploy. View or export submissions from the Supabase Table Editor, and delete a row when someone asks to be removed.

On the free plan Supabase allows 500 MB of database storage, and pauses a project after 7 days without database activity. A paused project must be restored from the dashboard before new submissions can be saved.

## 3. Can the backend run on Vercel?

**Not as it is today.** Vercel can run FastAPI, but AIDA's backend keeps state on local disk and in memory, which Vercel functions do not preserve.

What Vercel documents:

- Python ASGI and WSGI apps, including FastAPI, run as Vercel Functions on Python 3.12, 3.13 or 3.14.
- Hobby limits:
  - 300 seconds maximum duration;
  - 2 GB memory and 1 vCPU;
  - request and response bodies up to 4.5 MB;
  - Python bundles up to 500 MB uncompressed;
  - functions run in one region, Washington, D.C. (`iad1`), by default.
- "Vercel containers are stateless by design, with persistence delegated to databases and caches from the Marketplace."
- Time spent waiting on I/O, such as model calls, does not count as active CPU time.

How that meets the current backend:

| AIDA backend today | On Vercel | Change needed |
| --- | --- | --- |
| Accounts, sessions, onboarding and security events in a local SQLite file | Not kept between function instances | Move to hosted Postgres (for example Supabase or Neon) |
| SQLite uploads up to 20 MB stored on disk | Requests over 4.5 MB are rejected; no persistent disk | Upload directly to object storage, or disable uploads |
| Demo databases generated into the data directory at startup | Regenerated on every cold start | Build them at deploy time and ship them read-only |
| Rate limits, misuse pauses and caches in memory | Not shared between instances | A shared store such as Redis or Postgres |
| A question takes about 1.5–2.5 s, plus Groq rate-limit waits capped at 20 s | Fits within 300 s | None |
| Host check accepts loopback names only; client IP trusted only from a loopback proxy | Rejects the Vercel hostname; every visitor looks the same | Configurable allowed hosts and a signed proxy header |

**Verdict:**

- **A read-only demo backend is feasible on Vercel.** Built-in sample sources with no accounts or uploads, after the host-check change and with the demo databases shipped read-only.
- **The full product needs the state migration above first.** Accounts, uploads and shared rate limits must move off local disk and memory.
- **Recommended today:** keep the frontend on Vercel, and run the backend on one always-on server with a persistent disk. A small Hostinger VPS in Mumbai works, or a container host with a volume such as Render, Railway or Fly.io. Set `BACKEND_URL` in Vercel to point at it.

## 4. Before hosting the backend publicly

These gaps from the production review still apply:

- **Allowed hosts:** add a setting for them; the backend and the Next.js proxy currently accept loopback host names only.
- **Client IP:** trust forwarded client IP only from the proxy, using a shared secret. Otherwise rate limits can be bypassed or shared by every visitor.
- **HTTPS cookies:** set `AIDA_COOKIE_SECURE=1` behind HTTPS.
- **Sign-up policy:** choose invite-only or open.
- **Single worker:** run one backend worker, or move rate limits to a shared store.
- **Backups:** back up `backend/data` daily.
- **API key:** rotate the Groq API key.
- **Docker:** update `docker-compose.yml`, which still describes the earlier local-model setup.

## Sources

- [Vercel Functions limits](https://vercel.com/docs/functions/limitations)
- [Using the Python runtime with Vercel Functions](https://vercel.com/docs/functions/runtimes/python)
- [Deploy a FastAPI app on Vercel](https://vercel.com/docs/frameworks/backend/fastapi)
- [Running Docker on Vercel vs Render](https://vercel.com/kb/guide/docker-on-vercel-vs-render)
- [Vercel Hobby plan](https://vercel.com/docs/plans/hobby)
- [Supabase pricing and free plan limits](https://uibakery.io/blog/supabase-pricing)
