const path = require('path');
const { getDefaultConfig } = require('expo/metro-config');

/** @type {import('expo/metro-config').MetroConfig} */
const config = getDefaultConfig(__dirname);

const repoRoot = path.resolve(__dirname, '..');
const sharedRoot = path.join(repoRoot, 'shared');
const srcRoot = path.join(__dirname, 'src');

config.watchFolders = Array.from(new Set([repoRoot, sharedRoot]));

// Alias helper: map "src/..." and "shared/..." to real paths
const ALIASES = {
  src: srcRoot,
  shared: sharedRoot,
};

config.resolver.resolveRequest = (context, moduleName, platform) => {
  for (const key of Object.keys(ALIASES)) {
    if (moduleName === key) {
      return context.resolveRequest(context, ALIASES[key], platform);
    }
    if (moduleName.startsWith(key + '/')) {
      const rest = moduleName.slice(key.length + 1);
      const mapped = path.join(ALIASES[key], rest);
      return context.resolveRequest(context, mapped, platform);
    }
  }
  return context.resolveRequest(context, moduleName, platform);
};

module.exports = config;
