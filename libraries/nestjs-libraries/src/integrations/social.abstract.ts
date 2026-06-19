import { timer } from '@gitroom/helpers/utils/timer';
import { Integration } from '@prisma/client';
import { ApplicationFailure } from '@temporalio/activity';
import { readOrFetch } from '@gitroom/helpers/utils/read.or.fetch';
import sharp from 'sharp';

export type ValidityMedia = {
  path: string;
  thumbnail?: string;
};

export class RefreshToken extends ApplicationFailure {
  constructor(identifier: string, json: string, body: BodyInit, message = '') {
    super(message, 'refresh_token', true, [
      {
        identifier,
        json,
        body,
      },
    ]);
  }
}

export class BadBody extends ApplicationFailure {
  constructor(identifier: string, json: string, body: BodyInit, message = '') {
    super(message, 'bad_body', true, [
      {
        identifier,
        json,
        body,
      },
    ]);
  }
}

export class NotEnoughScopes {
  constructor(
    public message = 'Not enough scopes, when choosing a provider, please add all the scopes'
  ) {}
}

function safeStringify(obj: any) {
  const seen = new WeakSet();

  return JSON.stringify(obj, (key, value) => {
    if (typeof value === 'object' && value !== null) {
      if (seen.has(value)) {
        return '[Circular]';
      }
      seen.add(value);
    }
    return value;
  });
}

export abstract class SocialAbstract {
  abstract identifier: string;
  maxConcurrentJob = 1;

  public handleErrors(
    body: string,
    status: number
  ):
    | { type: 'refresh-token' | 'bad-body' | 'retry'; value: string }
    | undefined {
    return undefined;
  }

  /**
   * Server-side replacement for the old client-side `checkValidity`.
   * Validates the media attached to a post (and its comments) against the
   * provider rules. Returns `true` when valid, or an error message string.
   *
   * `posts` mirrors the client shape: the outer array is the main post followed
   * by each comment, the inner array is the media items for that entry.
   *
   * Note: video-duration validations that used to run in the browser are not
   * re-implemented here (no ffmpeg dependency). Image-dimension checks use sharp.
   */
  async checkValidity(
    posts: Array<ValidityMedia[]>,
    settings: any,
    additionalSettings: any[]
  ): Promise<string | true> {
    return true;
  }

  /** Reads the pixel dimensions of an image via sharp (works for http or local paths). */
  protected async getImageDimensions(
    path: string
  ): Promise<{ width: number; height: number }> {
    // Stored media paths are relative (e.g. "uploads/x.png"); resolve them to a
    // fetchable URL the same way posts.service.updateMedia does.
    const url =
      path?.indexOf('http') === -1
        ? `${process.env.FRONTEND_URL}/${path}`
        : path;
    const { width = 0, height = 0 } = await sharp(
      await readOrFetch(url)
    ).metadata();
    return { width, height };
  }

  public async mention(
    token: string,
    d: { query: string },
    id: string,
    integration: Integration
  ): Promise<
    | { id: string; label: string; image: string; doNotCache?: boolean }[]
    | { none: true }
  > {
    return { none: true };
  }

  async runInConcurrent<T>(
    func: (...args: any[]) => Promise<T>,
    ignoreConcurrency?: boolean
  ) {
    let globalErr = {};
    let value: any;
    try {
      value = await func();
    } catch (err) {
      const handle = this.handleErrors(safeStringify(err), 200);
      value = { err: true, value: 'Unknown Error', ...(handle || {}) };
      globalErr = err;
    }

    if (value && value?.err && value?.value) {
      if (value.type === 'refresh-token') {
        throw new RefreshToken(
          '',
          safeStringify({}),
          {} as any,
          value.value || ''
        );
      }
      throw new BadBody('', safeStringify(globalErr), {} as any, value.value || '');
    }

    return value;
  }

  async fetch(
    url: string,
    options: RequestInit = {},
    identifier = '',
    totalRetries = 0,
    ignoreConcurrency = false
  ): Promise<Response> {
    // GoodMG patch: networks behind a flaky obfuscated VPN tunnel (RU -> Meta)
    // drop large TLS flows in waves. A raw `await fetch` throws ConnectTimeout
    // and the whole publish fails. Retry transient *connection* errors (the
    // request never reached the server, so retries can't duplicate a post)
    // long enough to ride out a tunnel flap window.
    const netRetries = Number(process.env.POSTIZ_NET_RETRY ?? 8);
    const netRetryDelay = Number(process.env.POSTIZ_NET_RETRY_DELAY_MS ?? 8000);
    const request = await this.fetchWithNetworkRetry(
      url,
      options,
      netRetries,
      netRetryDelay
    );

    if (request.status === 200 || request.status === 201) {
      return request;
    }

    if (totalRetries > 2) {
      throw new BadBody(identifier, '{}', options.body || '{}');
    }

    let json = '{}';
    try {
      json = await request.text();
    } catch (err) {
      json = '{}';
    }

    const handleError = this.handleErrors(json || '{}', request.status);

    if (
      request.status === 429 ||
      (request.status === 500 && !handleError) ||
      json.includes('rate_limit_exceeded') ||
      json.includes('Rate limit')
    ) {
      await timer(5000);
      return this.fetch(
        url,
        options,
        identifier,
        totalRetries + 1,
        ignoreConcurrency
      );
    }

    if (handleError?.type === 'retry') {
      await timer(5000);
      return this.fetch(
        url,
        options,
        identifier,
        totalRetries + 1,
        ignoreConcurrency
      );
    }

    if (
      (request.status === 401 &&
        (handleError?.type === 'refresh-token' || !handleError)) ||
      handleError?.type === 'refresh-token'
    ) {
      throw new RefreshToken(
        identifier,
        json,
        options.body!,
        handleError?.value
      );
    }

    throw new BadBody(
      identifier,
      json,
      options.body!,
      handleError?.value || 'Unknown Error'
    );
  }

  /**
   * GoodMG patch: wrap the raw network call so that transient connection
   * failures (ConnectTimeout / "fetch failed" / ECONNRESET / ETIMEDOUT) are
   * retried. Only connection-level errors are retried — an established
   * response (any HTTP status) is returned untouched so the normal status
   * handling above still runs. Connection errors mean the request never
   * reached the server, so retrying is safe for non-idempotent POSTs too.
   */
  private async fetchWithNetworkRetry(
    url: string,
    options: RequestInit,
    retriesLeft: number,
    delayMs: number,
    attempt = 0
  ): Promise<Response> {
    try {
      return await fetch(url, options);
    } catch (err: any) {
      const code =
        err?.cause?.code || err?.code || err?.name || String(err?.message || '');
      const isTransient =
        /ConnectTimeout|UND_ERR_CONNECT_TIMEOUT|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN|ENETUNREACH|fetch failed|aborted|terminated/i.test(
          String(code) + ' ' + String(err?.message || '')
        );

      if (!isTransient || retriesLeft <= 0) {
        throw err;
      }

      // eslint-disable-next-line no-console
      console.log(
        `[net-retry] ${url.split('?')[0]} failed (${code}); retry ${
          attempt + 1
        }, ${retriesLeft - 1} left, waiting ${delayMs}ms`
      );
      await timer(delayMs);
      return this.fetchWithNetworkRetry(
        url,
        options,
        retriesLeft - 1,
        delayMs,
        attempt + 1
      );
    }
  }

  checkScopes(required: string[], got: string | string[]) {
    if (Array.isArray(got)) {
      if (!required.every((scope) => got.includes(scope))) {
        throw new NotEnoughScopes();
      }

      return true;
    }

    const newGot = decodeURIComponent(got);

    const splitType = newGot.indexOf(',') > -1 ? ',' : ' ';
    const gotArray = newGot.split(splitType);
    if (!required.every((scope) => gotArray.includes(scope))) {
      throw new NotEnoughScopes();
    }

    return true;
  }
}
