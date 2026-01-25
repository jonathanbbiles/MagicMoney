# Bullish or Bust! Backend

This Node.js backend handles Alpaca API trades via a `/buy` endpoint.

## Setup

1. `npm install`
2. Create a `.env` file with your Alpaca API keys and API token.
3. `npm start`

## Node Version

- Local: `nvm use` (uses `.nvmrc` with Node 22)
- Render: set Node version to 22 in the service settings

## Environment Variables

Required:
- `ALPACA_API_KEY`
- `ALPACA_SECRET_KEY`
- `TRADE_BASE` (defaults to `https://api.alpaca.markets` if omitted)

Recommended:
- `API_TOKEN` (shared token used by the frontend; include it as `Authorization: Bearer <token>` or `x-api-key`.)

Optional:
- `CORS_ALLOWED_ORIGINS` (comma-separated list; leave empty to allow all origins during development)
- `CORS_ALLOWED_ORIGIN_REGEX` (regex pattern string for allowed origins)
- `CORS_ALLOW_LAN` (`true` to allow localhost + RFC1918 LAN origins)
- `RATE_LIMIT_WINDOW_MS` (default `60000`)
- `RATE_LIMIT_MAX` (default `120`)
- `HTTP_TIMEOUT_MS` (default `10000`, configurable request timeout)
- `DATA_BASE` (defaults to `https://data.alpaca.markets` if omitted)
- `DESIRED_NET_PROFIT_BASIS_POINTS` (default `100`, target net profit per trade after fees)
- `MAX_GROSS_TAKE_PROFIT_BASIS_POINTS` (default `220`, cap on gross take-profit distance above entry)
- `MAX_HOLD_SECONDS` (default `180`, soft max hold time before exiting when profitable)
- `FORCE_EXIT_SECONDS` (default `300`, hard max hold time before forced exit)
- `CRYPTO_QUOTE_MAX_AGE_MS` (default `600000`, overrides quote/trade staleness checks for crypto only; stock quotes remain strict)
- `DATASET_DIR` (path for persisted dataset/output files, if used)

## Trading Gates

Entries are filtered by multiple hard gates before any order is placed:
- Spread gate (skip if spread is wider than the configured maximum)
- Orderbook gate (skip if depth/impact fails configured thresholds)
- Probability + EV gates (skip if `pUp` is below `PUP_MIN` or if expected value is below `EV_MIN_BPS` when enabled)
- Required gross exit cap (skip if the modeled required gross take-profit exceeds `MAX_REQUIRED_GROSS_EXIT_BPS`)

## Exit Policy

After a buy fills, the bot immediately attaches a limit sell with a target equal to estimated round-trip fees plus `EXIT_FIXED_NET_PROFIT_BPS` (default 5 bps). Optional refresh logic can cancel and reprice stale exit orders when `EXIT_REFRESH_ENABLED` is on.

## Notes

- `GET /health` remains public for uptime checks.
- `GET /debug/auth` returns auth requirements without needing a token.
- All other routes require a valid API token.

## Persistent Storage (DATASET_DIR)

If your hosting platform uses an ephemeral filesystem (e.g., Render), mount a persistent disk or
set `DATASET_DIR` to a persistent path so any recorded data survives restarts.
