const SYMBOL_COOLDOWN_MS = 60000;
const FAILURE_THRESHOLD = 3;

const symbolFailures = new Map();

function getState(symbol) {
  if (!symbolFailures.has(symbol)) {
    symbolFailures.set(symbol, {
      consecutiveFailures: 0,
      cooldownUntil: 0,
      lastErrorCode: null,
      lastAt: null,
    });
  }
  return symbolFailures.get(symbol);
}

function isCooling(symbol) {
  const state = getState(symbol);
  return Number.isFinite(state.cooldownUntil) && state.cooldownUntil > Date.now();
}

function recordFailure(symbol, info = {}) {
  if (!symbol) return;
  const state = getState(symbol);
  state.consecutiveFailures += 1;
  state.lastErrorCode = info.errorCode || info.statusCode || null;
  state.lastAt = new Date().toISOString();

  // Cooldown a symbol after consecutive transient failures to avoid hammering.
  if (state.consecutiveFailures >= FAILURE_THRESHOLD) {
    state.consecutiveFailures = 0;
    state.cooldownUntil = Date.now() + SYMBOL_COOLDOWN_MS;
    console.warn(`${symbol} — Cooldown 60s after 3 consecutive network failures`);
  }
}

function recordSuccess(symbol) {
  if (!symbol) return;
  const state = getState(symbol);
  state.consecutiveFailures = 0;
  state.cooldownUntil = 0;
}

function getFailureSnapshot() {
  const snapshot = {};
  for (const [symbol, state] of symbolFailures.entries()) {
    snapshot[symbol] = {
      consecutiveFailures: state.consecutiveFailures,
      cooldownUntil: state.cooldownUntil
        ? new Date(state.cooldownUntil).toISOString()
        : null,
      lastErrorCode: state.lastErrorCode,
      lastAt: state.lastAt,
    };
  }
  return snapshot;
}

module.exports = {
  isCooling,
  recordFailure,
  recordSuccess,
  getFailureSnapshot,
};
