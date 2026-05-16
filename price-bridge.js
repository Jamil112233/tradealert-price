/**
 * Trade Alert — Price Bridge
 * Runs 24/7 on Render as a Background Worker.
 *
 * Flow:
 *  1. Creates a Capital.com session (auto-renews every 9 min via ping)
 *  2. Subscribes to WebSocket for live GOLD + SILVER price ticks
 *  3. Subscribes to OHLC WebSocket for M1/M5/M15/H1 candles
 *  4. Every 5 sec  → writes current price to Firebase Realtime DB
 *  5. Every minute → POSTs latest price + OHLC to Cloudflare Worker
 */

const https  = require('https');
const http   = require('http');
const WebSocket = require('ws');

// ── Config (set these as Render environment variables) ─────────────────────
const CAP_EMAIL    = process.env.CAP_EMAIL;       // Capital.com account email
const CAP_PASSWORD = process.env.CAP_PASSWORD;    // API key custom password
const CAP_API_KEY  = process.env.CAP_API_KEY;     // Capital.com API key

const FIREBASE_URL = process.env.FIREBASE_URL;    // e.g. https://YOUR-PROJECT-default-rtdb.firebaseio.com
const FIREBASE_SECRET = process.env.FIREBASE_SECRET; // Firebase DB secret (for server-side write)

const WORKER_URL   = process.env.WORKER_URL;      // e.g. https://your-worker.workers.dev/update-price
const WORKER_SECRET = process.env.WORKER_SECRET;  // Secret key to authenticate with Worker

const CAP_DEMO_REST = 'https://demo-api-capital.backend-capital.com';
const CAP_WS_URL    = 'wss://api-streaming-capital.backend-capital.com/connect';

// ── State ───────────────────────────────────────────────────────────────────
let cst = null;
let securityToken = null;
let ws = null;

// Latest prices
const prices = {
  GOLD:   { current: 0, open: 0, high: 0, low: 0, close: 0 },
  SILVER: { current: 0, open: 0, high: 0, low: 0, close: 0 },
};

// Current minute candle being built from ticks
const tickCandle = {
  GOLD:   { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
  SILVER: { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
};

// ── Helpers ─────────────────────────────────────────────────────────────────
function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function post(url, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const urlObj = new URL(url);
    const isHttps = urlObj.protocol === 'https:';
    const lib = isHttps ? https : http;
    const req = lib.request({
      hostname: urlObj.hostname,
      port: urlObj.port || (isHttps ? 443 : 80),
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        ...headers,
      },
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(body), headers: res.headers }); }
        catch { resolve({ status: res.statusCode, body, headers: res.headers }); }
      });
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function get(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const req = https.request({
      hostname: urlObj.hostname,
      port: 443,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers,
    }, res => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(body), headers: res.headers }); }
        catch { resolve({ status: res.statusCode, body, headers: res.headers }); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// ── Step 1: Create Capital.com session ──────────────────────────────────────
async function createSession() {
  log('Creating Capital.com session...');
  log(`Using email: ${CAP_EMAIL}`);
  log(`Using API key: ${CAP_API_KEY?.substring(0,4)}...`);
  log(`Password length: ${CAP_PASSWORD?.length}`);

  const res = await post(`${CAP_DEMO_REST}/api/v1/session`, {
    identifier: CAP_EMAIL,
    password: CAP_PASSWORD,
    encryptedPassword: false,
  }, {
    'X-CAP-API-KEY': CAP_API_KEY,
  });

  log(`Session response status: ${res.status}`);
  log(`Session response body: ${JSON.stringify(res.body)}`);
  log(`Session response headers CST: ${res.headers['cst']?.substring(0,8)}`);

  if (res.status !== 200) {
    throw new Error(`Session failed: ${res.status} ${JSON.stringify(res.body)}`);
  }

  cst           = res.headers['cst'];
  securityToken = res.headers['x-security-token'];
  log(`Session created. CST=${cst?.substring(0,8)}...`);
}

// ── Step 2: Ping session every 9 minutes to keep it alive ───────────────────
async function pingSession() {
  try {
    const res = await get(`${CAP_DEMO_REST}/api/v1/ping`, {
      'CST': cst,
      'X-SECURITY-TOKEN': securityToken,
    });
    if (res.status === 200) log('Session ping OK');
    else {
      log(`Session ping failed (${res.status}) — recreating session`);
      await createSession();
      reconnectWebSocket();
    }
  } catch (e) {
    log(`Ping error: ${e.message} — recreating session`);
    await createSession();
    reconnectWebSocket();
  }
}

// ── Step 3: Connect WebSocket and subscribe ─────────────────────────────────
function connectWebSocket() {
  log('Connecting WebSocket...');
  ws = new WebSocket(CAP_WS_URL);

  ws.on('open', () => {
    log('WebSocket connected');

    // Subscribe to live tick prices (bid/ask)
    ws.send(JSON.stringify({
      destination: 'marketData.subscribe',
      correlationId: '1',
      cst,
      securityToken,
      payload: { epics: ['GOLD', 'SILVER'] },
    }));

    // Subscribe to OHLC candles — M1, M5, M15, H1
    ws.send(JSON.stringify({
      destination: 'OHLCMarketData.subscribe',
      correlationId: '2',
      cst,
      securityToken,
      payload: {
        epics: ['GOLD', 'SILVER'],
        resolutions: ['MINUTE', 'MINUTE_5', 'MINUTE_15', 'HOUR'],
        type: 'classic',
      },
    }));

    log('Subscribed to GOLD + SILVER tick + OHLC');
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Live tick price update
      if (msg.destination === 'quote' && msg.payload) {
        const epic = msg.payload.epic;
        if (prices[epic] !== undefined) {
          // Use mid price (bid + ask) / 2 for display
          const bid = msg.payload.bid || 0;
          const ask = msg.payload.ofr || 0;
          const mid = bid > 0 && ask > 0 ? (bid + ask) / 2 : (bid || ask);
          if (mid > 0) {
            const prev = prices[epic].current;
            prices[epic].current = mid;

            // Build tick-based candle for current minute
            const tc = tickCandle[epic];
            const nowMin = Math.floor(Date.now() / 60000);
            if (tc.startedAt !== nowMin) {
              // New minute started — save last close
              if (tc.startedAt > 0) {
                prices[epic].close = prev; // previous tick was the close
              }
              tc.startedAt = nowMin;
              tc.open = mid;
              tc.high = mid;
              tc.low  = mid;
            } else {
              if (mid > tc.high) tc.high = mid;
              if (mid < tc.low)  tc.low  = mid;
            }
          }
        }
      }

      // OHLC candle update — use these for accurate candle close data
      if (msg.destination === 'ohlc.event' && msg.payload) {
        const epic       = msg.payload.epic;
        const resolution = msg.payload.resolution;
        const o = msg.payload.o;
        const h = msg.payload.h;
        const l = msg.payload.l;
        const c = msg.payload.c;

        if (prices[epic] !== undefined && o && h && l && c) {
          // Update OHLC for the appropriate timeframe
          // For worker alert checking, we primarily need M1
          // Store all timeframes separately
          if (!prices[epic].ohlc) prices[epic].ohlc = {};
          prices[epic].ohlc[resolution] = { o, h, l, c, t: msg.payload.t };

          // Use M1 close as the primary candle close
          if (resolution === 'MINUTE') {
            prices[epic].open  = o;
            prices[epic].high  = h;
            prices[epic].low   = l;
            prices[epic].close = c;
          }
          log(`OHLC ${epic} ${resolution}: O=${o} H=${h} L=${l} C=${c}`);
        }
      }

    } catch (e) {
      log(`WS message parse error: ${e.message}`);
    }
  });

  ws.on('close', (code, reason) => {
    log(`WebSocket closed: ${code} ${reason} — reconnecting in 5s`);
    setTimeout(reconnectWebSocket, 5000);
  });

  ws.on('error', (e) => {
    log(`WebSocket error: ${e.message}`);
  });
}

function reconnectWebSocket() {
  if (ws) { try { ws.terminate(); } catch {} ws = null; }
  connectWebSocket();
}

// ── Step 4: Write current price to Firebase Realtime DB every 5 seconds ─────
async function updateFirebase() {
  if (!prices.GOLD.current && !prices.SILVER.current) return;

  const data = {
    xau: { current: prices.GOLD.current,   updatedAt: Date.now() },
    xag: { current: prices.SILVER.current, updatedAt: Date.now() },
  };

  const url = `${FIREBASE_URL}/prices.json?auth=${FIREBASE_SECRET}`;

  return new Promise((resolve) => {
    const body = JSON.stringify(data);
    const urlObj = new URL(url);
    const req = https.request({
      hostname: urlObj.hostname,
      port: 443,
      path: urlObj.pathname + urlObj.search,
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, res => {
      res.on('data', () => {});
      res.on('end', () => resolve());
    });
    req.on('error', (e) => { log(`Firebase write error: ${e.message}`); resolve(); });
    req.write(body);
    req.end();
  });
}

// ── Step 5: Send OHLC + current price to Cloudflare Worker every minute ──────
async function updateWorker() {
  if (!prices.GOLD.current && !prices.SILVER.current) return;

  const body = {
    xau: {
      current: prices.GOLD.current,
      open:    prices.GOLD.open    || prices.GOLD.current,
      high:    prices.GOLD.high    || prices.GOLD.current,
      low:     prices.GOLD.low     || prices.GOLD.current,
      close:   prices.GOLD.close   || prices.GOLD.current,
      ohlc:    prices.GOLD.ohlc    || {},
    },
    xag: {
      current: prices.SILVER.current,
      open:    prices.SILVER.open    || prices.SILVER.current,
      high:    prices.SILVER.high    || prices.SILVER.current,
      low:     prices.SILVER.low     || prices.SILVER.current,
      close:   prices.SILVER.close   || prices.SILVER.current,
      ohlc:    prices.SILVER.ohlc    || {},
    },
    timestamp: Date.now(),
  };

  try {
    const res = await post(WORKER_URL, body, {
      'X-Secret-Key': WORKER_SECRET,
    });
    if (res.status === 200) log(`Worker updated: GOLD=${body.xau.current} SILVER=${body.xag.current}`);
    else log(`Worker update failed: ${res.status}`);
  } catch (e) {
    log(`Worker POST error: ${e.message}`);
  }
}

// ── Keep Render alive — it kills services with no HTTP port ──────────────────
// Background Worker type on Render doesn't need this, but Web Service does
function startHealthServer() {
  http.createServer((req, res) => {
    res.writeHead(200);
    res.end('OK');
  }).listen(process.env.PORT || 3000, () => {
    log('Health server listening on port ' + (process.env.PORT || 3000));
  });
}

// ── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  log('=== Trade Alert Price Bridge Starting ===');

  if (!CAP_EMAIL || !CAP_PASSWORD || !CAP_API_KEY) {
    throw new Error('Missing Capital.com credentials in environment variables');
  }
  if (!FIREBASE_URL || !FIREBASE_SECRET) {
    throw new Error('Missing Firebase config in environment variables');
  }
  if (!WORKER_URL || !WORKER_SECRET) {
    throw new Error('Missing Worker config in environment variables');
  }

  startHealthServer();

  // Create session
  await createSession();

  // Connect WebSocket
  connectWebSocket();

  // Ping session every 9 minutes to keep alive (session expires at 10 min)
  setInterval(pingSession, 9 * 60 * 1000);

  // Update Firebase every 5 seconds
  setInterval(updateFirebase, 5000);

  // Update Worker every 60 seconds
  setInterval(updateWorker, 60 * 1000);

  log('=== Price Bridge running ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
