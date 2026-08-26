# SinoTrust Level 100 Operational — Railway activation

This bundle is intentionally provider-aware but fail-honest: a capability is not marked
CONFIGURED until a live probe passes, and VALIDATED requires evidence from a real drill/test.

Recommended Railway services:
1. API service from this GitHub repository, start command `python app.py`, role `api`.
2. Worker service from the same repository, start command `python app.py`, role `worker`,
   no public domain required.
3. Railway PostgreSQL.
4. Railway Redis.
5. Railway Storage Bucket (S3-compatible).

Key application variables are documented in the ChatGPT handoff. Keep secrets in Railway
Variables / variable references, never commit them to GitHub.

After deployment:
- confirm logs contain `INFO: Application startup complete.`
- call `/healthz`
- use an authenticated admin/reviewer request to
  `/api/platform/level100/operational-readiness`
- use `/api/admin/level100/probe-now` after wiring providers.
