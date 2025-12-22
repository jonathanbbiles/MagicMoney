function createLimiter(name, maxConcurrent) {
  let activeCount = 0;
  const queue = [];

  const runNext = () => {
    if (activeCount >= maxConcurrent || queue.length === 0) {
      return;
    }
    const { task, resolve, reject } = queue.shift();
    activeCount += 1;
    Promise.resolve()
      .then(task)
      .then(resolve)
      .catch(reject)
      .finally(() => {
        activeCount -= 1;
        runNext();
      });
  };

  const schedule = (task) =>
    new Promise((resolve, reject) => {
      queue.push({ task, resolve, reject });
      process.nextTick(runNext);
    });

  const status = () => ({
    name,
    maxConcurrent,
    active: activeCount,
    queued: queue.length,
  });

  return { schedule, status };
}

const alpacaLimiter = createLimiter('alpaca', 4);
const quoteLimiter = createLimiter('quotes', 4);

function getLimiterStatus() {
  return {
    alpaca: alpacaLimiter.status(),
    quotes: quoteLimiter.status(),
  };
}

module.exports = {
  alpacaLimiter,
  quoteLimiter,
  getLimiterStatus,
};
