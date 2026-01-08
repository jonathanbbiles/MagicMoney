Bullish or Bust! Frontend

This React Native app displays crypto tokens with entry signals and lets you place buy orders.

 

Entry Logic

Tokens are flagged ENTRY READY when the MACD line is above the signal line. If the MACD is rising but has not crossed, the token appears on the WATCHLIST. Other indicators are ignored for entry decisions.

Current UI

- Shows backend health and open orders.
- Lets you place buy orders.
- Uses /trade when available, otherwise falls back to legacy /orders.

Device setup

- Set BACKEND_BASE_URL to https://magicmoney.onrender.com (do not use localhost on device/Expo).
- If backend API_TOKEN is enabled, set API_TOKEN in Expo extra to the same value.

 

Setup

npm install

Copy .env.example to .env.local

Start backend (Node.js Express server)

Run: npm start (Expo)
Start Expo from the frontend folder.
If you see module resolution errors, restart Metro with cache clear: npx expo start -c

The app shows temporary trade messages using a built-in overlay notification.

To enable the commit guard, run:

git config core.hooksPath .git-hooks
