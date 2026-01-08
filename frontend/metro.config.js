const path = require('path');
const { getDefaultConfig } = require('expo/metro-config');

/** @type {import('expo/metro-config').MetroConfig} */
const config = getDefaultConfig(__dirname);

const repoRoot = path.resolve(__dirname, '..');
const sharedRoot = path.join(repoRoot, 'shared');
const workspaceNodeModules = path.join(repoRoot, 'node_modules');
const appNodeModules = path.join(__dirname, 'node_modules');

config.watchFolders = Array.from(new Set([repoRoot, sharedRoot]));
config.resolver.nodeModulesPaths = Array.from(
  new Set([appNodeModules, workspaceNodeModules])
);

module.exports = config;
