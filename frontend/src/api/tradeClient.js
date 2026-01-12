import { getApiToken, getBackendBaseUrl } from '../config/alpaca';

const buildAuthHeaders = () => {
  const token = getApiToken();
  if (!token) return {};
  return {
    Authorization: `Bearer ${token}`,
    'x-api-key': token,
  };
};

const normalizePath = (path) => {
  const cleaned = String(path || '').trim();
  if (!cleaned) return '/';
  return cleaned.startsWith('/') ? cleaned : `/${cleaned}`;
};

const buildErrorMessage = async ({ baseUrl, path, res }) => {
  const status = res?.status ?? 'unknown';
  const responseText = res ? await res.text().catch(() => '') : '';
  const snippet = responseText ? responseText.slice(0, 300) : '';
  const details = [`baseUrl=${baseUrl}`, `path=${path}`, `status=${status}`];
  if (snippet) {
    details.push(`response=${snippet}`);
  }
  return `request_failed ${details.join(' ')}`;
};

export const request = async (path, options = {}) => {
  const baseUrl = getBackendBaseUrl();
  const normalizedPath = normalizePath(path);
  const url = `${baseUrl}${normalizedPath}`;
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(options.headers || {}),
    ...buildAuthHeaders(),
  };

  let res;
  try {
    res = await fetch(url, { ...options, headers });
  } catch (error) {
    const message = await buildErrorMessage({ baseUrl, path: normalizedPath, res: null });
    throw new Error(message);
  }

  if (!res.ok) {
    const message = await buildErrorMessage({ baseUrl, path: normalizedPath, res });
    throw new Error(message);
  }

  return res;
};
