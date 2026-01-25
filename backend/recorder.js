const path = require('path');

const getDatasetPath = () => {
  const raw = String(process.env.DATASET_DIR || '').trim();
  if (raw) {
    return path.resolve(raw);
  }
  return path.resolve('data');
};

module.exports = { getDatasetPath };
