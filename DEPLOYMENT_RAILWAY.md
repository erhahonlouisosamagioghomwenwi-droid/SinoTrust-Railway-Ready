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

## Real Railway infrastructure wiring

The application natively accepts Railway service references for `DATABASE_URL` and `REDIS_URL`.
For a Railway Bucket, use **Add to Service -> AWS SDK (Generic)** so Railway injects `AWS_ENDPOINT_URL`, `AWS_S3_BUCKET_NAME`, `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.

When those Bucket variables are present, the application automatically enables the S3-compatible object-storage backend. Local development remains on local filesystem storage unless S3 variables are present or `SINOTRUST_OBJECT_STORAGE=s3` is explicitly set.

Recommended production validation after deploy: PostgreSQL `SELECT 1` plus read/write probe, Redis `PING` plus expiring read/write probe, and S3 upload/read/delete probe. Never print secret values in deployment logs.
