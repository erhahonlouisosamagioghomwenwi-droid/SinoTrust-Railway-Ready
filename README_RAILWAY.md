# SinoTrust Europe — Railway Ready

Production-oriented deployment package for the SinoTrust Europe B2B pilot/demo.

## Railway
- Builder: Railpack
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT --workers 1`
- Health check: `/healthz`
- Python: 3.12

## Recommended Railway variables
- `SINOTRUST_ENV=production`
- `SINOTRUST_TRUST_PROXY=1`
- `SINOTRUST_REGION=eu-central`
- `SINOTRUST_DATA_RESIDENCY=EU`
- `SINOTRUST_RELOAD=0`
- `OPENAI_API_KEY=<secret>` if live AI responses are required
- `OPENAI_MODEL=gpt-5.6` if desired

For persistent commercial leads and workspace data, add Railway PostgreSQL. The app accepts either `SINOTRUST_DATABASE_URL` or Railway's standard `DATABASE_URL`.

## Video assets
The three original videos are included under `static/videos/` with the exact filenames expected by the application. Originals were remuxed with MP4 fast-start without re-encoding, preserving source quality while improving web startup behavior. VIDEO 3 also includes existing 360p/480p variants.
