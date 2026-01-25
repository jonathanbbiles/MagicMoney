# MagicMoney (LIVE)

## Make it run (LIVE)

### Backend (Live trading)
1. `cd backend`
2. `npm install`
3. Copy `.env.example.live` to `.env` and fill in **live** Alpaca keys.
4. `npm start`
5. Health check: `curl http://localhost:3000/health`
6. Auth check: `curl http://localhost:3000/debug/auth`

### Frontend (Expo)
1. `cd frontend`
2. `npm install`
3. Copy `.env.example` to `.env`
4. `npx expo start`

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| 401 Unauthorized | Token mismatch | Check `API_TOKEN` on backend and Expo; hit `/debug/auth` to confirm auth requirements. |
| CORS blocked | Device/LAN origin not allowed | Set `CORS_ALLOW_LAN=true`, add device origin to `CORS_ALLOWED_ORIGINS`, or use `CORS_ALLOWED_ORIGIN_REGEX`. |
| 429 rate limited | Backend limit too low or UI polling too frequent | Raise `RATE_LIMIT_MAX`, or reduce polling interval in `frontend/src/config/polling.js`. |
| Timeouts / slow data | Network or Alpaca latency | Raise `HTTP_TIMEOUT_MS` for the backend. |
