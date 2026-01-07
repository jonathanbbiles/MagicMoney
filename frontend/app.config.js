// app.config.js
// Bridge env → Expo extra (build-time). Never commit real secrets to the repo.
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const envPath = fs.existsSync(path.join(__dirname, '.env.local'))
  ? path.join(__dirname, '.env.local')
  : path.join(__dirname, '.env');
dotenv.config({ path: envPath });

module.exports = {
  expo: {
    name: "Bullish or Bust",
    slug: "bullish-or-bust",
    plugins: ["react-native-svg"],
    extra: {
      APCA_API_BASE: process.env.APCA_API_BASE || "https://api.alpaca.markets/v2",
      BACKEND_BASE_URL: process.env.BACKEND_BASE_URL || "https://magicmoney.onrender.com"
    }
  }
};
