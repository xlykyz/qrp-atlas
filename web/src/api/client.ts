type QueryValue = string | number | boolean | null | undefined;
type QueryParams = URLSearchParams | Record<string, QueryValue | QueryValue[]>;
type RequestOptions = Omit<RequestInit, 'body'> & {
  query?: QueryParams;
  body?: unknown;
};

const fallbackHost = typeof window === 'undefined' ? 'localhost' : window.location.hostname;
const BASE_URL = import.meta.env.VITE_API_BASE_URL || `http://${fallbackHost}:8000`;

class ApiError extends Error {
  status: number;
  body: unknown;

  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.body = body;
  }
}

function appendQuery(url: URL, query?: QueryParams): void {
  if (!query) return;

  if (query instanceof URLSearchParams) {
    query.forEach((value, key) => url.searchParams.append(key, value));
    return;
  }

  Object.entries(query).forEach(([key, value]) => {
    const values = Array.isArray(value) ? value : [value];
    values.forEach((item) => {
      if (item == null) return;
      url.searchParams.append(key, String(item));
    });
  });
}

function isBodyInit(body: unknown): body is BodyInit {
  return typeof body === 'string'
    || body instanceof Blob
    || body instanceof FormData
    || body instanceof URLSearchParams
    || body instanceof ArrayBuffer
    || ArrayBuffer.isView(body)
    || body instanceof ReadableStream;
}

function isJsonBody(body: unknown): boolean {
  return body != null && typeof body === 'object' && !isBodyInit(body);
}

function buildErrorMessage(status: number, statusText: string, body: unknown): string {
  if (body && typeof body === 'object') {
    const record = body as Record<string, unknown>;
    const detail = record.detail ?? record.message ?? record.error;
    if (typeof detail === 'string' && detail) return detail;
  }

  if (typeof body === 'string' && body) return body;
  return `API error: ${status} ${statusText}`;
}

async function parseResponseBody(res: Response): Promise<unknown> {
  const text = await res.text();
  if (!text) return null;

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T>;
async function request<T>(path: string, options?: RequestOptions): Promise<T>;
async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { query, headers, body, ...init } = options;
  const base = BASE_URL.endsWith('/') ? BASE_URL : `${BASE_URL}/`;
  const url = new URL(path, base);
  appendQuery(url, query);

  const requestHeaders = new Headers(headers);
  const shouldSerializeJson = isJsonBody(body);
  const requestBody = shouldSerializeJson ? JSON.stringify(body) : body;
  if (shouldSerializeJson && !requestHeaders.has('Content-Type')) {
    requestHeaders.set('Content-Type', 'application/json');
  }

  const res = await fetch(url.toString(), {
    ...init,
    body: requestBody as BodyInit | null | undefined,
    headers: requestHeaders,
  });

  if (!res.ok) {
    const errorBody = await parseResponseBody(res);
    throw new ApiError(
      res.status,
      buildErrorMessage(res.status, res.statusText, errorBody),
      errorBody,
    );
  }

  if (res.status === 204) return undefined as T;
  return parseResponseBody(res) as Promise<T>;
}

export type { RequestOptions };
export { request, ApiError, BASE_URL };
