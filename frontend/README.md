Bullish or Bust! Frontend

This React Native app displays crypto tokens with entry signals and lets you place buy orders.

 

Entry Logic

Tokens are flagged ENTRY READY when the MACD line is above the signal line. If the MACD is rising but has not crossed, the token appears on the WATCHLIST. Other indicators are ignored for entry decisions.

 

Setup

npm install

Copy .env.example to .env.local

Start backend (Node.js Express server)

Run: npm start (Expo)

The app shows temporary trade messages using a built-in overlay notification.

To enable the commit guard, run:

git config core.hooksPath .git-hooks
