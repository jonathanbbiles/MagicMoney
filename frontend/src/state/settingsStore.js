import { DEFAULT_SETTINGS, SIMPLE_SETTINGS_ONLY } from '../config/alpaca';

let settings = { ...DEFAULT_SETTINGS };
let overrides = {};

const settingsListeners = new Set();
const overrideListeners = new Set();

function notifySettings() {
  for (const listener of settingsListeners) {
    listener(settings);
  }
}

function notifyOverrides() {
  for (const listener of overrideListeners) {
    listener(overrides);
  }
}

export function getSettings() {
  return settings;
}

export function getOverrides() {
  return overrides;
}

export function setSettingsSnapshot(next) {
  settings = { ...DEFAULT_SETTINGS, ...next };
  notifySettings();
}

export function updateSettings(updater) {
  const base = typeof updater === 'function' ? updater(settings) : updater;
  setSettingsSnapshot({ ...settings, ...base });
}

export function resetSettings() {
  setSettingsSnapshot(DEFAULT_SETTINGS);
}

export function setOverridesSnapshot(next) {
  overrides = { ...(next || {}) };
  notifyOverrides();
}

export function effectiveSetting(symbol, key) {
  const o = overrides?.[symbol];
  if (o && Object.prototype.hasOwnProperty.call(o, key)) {
    return o[key];
  }
  return settings[key];
}

export function subscribeSettings(listener) {
  if (typeof listener !== 'function') return () => {};
  settingsListeners.add(listener);
  return () => settingsListeners.delete(listener);
}

export function subscribeOverrides(listener) {
  if (typeof listener !== 'function') return () => {};
  overrideListeners.add(listener);
  return () => overrideListeners.delete(listener);
}

export { DEFAULT_SETTINGS, SIMPLE_SETTINGS_ONLY };
