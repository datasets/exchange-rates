# Update Script Maintenance Report

Date: 2026-03-03

- Added GitHub Actions automation at `.github/workflows/actions.yml` (daily schedule + manual trigger).
- Updated `exchange_rates_flow.py` source retrieval to use FRED CSV endpoint (`fredgraph.csv?id=...`) instead of legacy `.txt` files.
- Added HTTP fail-fast handling with request timeouts.
- Updated `scripts/main.py` to use FRED CSV endpoint and stable path handling based on script location.
- Regenerated `data/daily.csv`, `data/monthly.csv`, and `data/yearly.csv` using current FRED CSV responses.
- This addresses stale updates caused by changes in FRED legacy endpoint behavior.
