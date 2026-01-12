const { getDefaultConfig } = require('expo/metro-config');
const path = require('path');

const config = getDefaultConfig(__dirname);

// Allow imports like "src/..." to resolve to ./src/...
config.resolver.extraNodeModules = {
  ...(config.resolver.extraNodeModules || {}),
  src: path.resolve(__dirname, 'src'),
};

module.exports = config;
