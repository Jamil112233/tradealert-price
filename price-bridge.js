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
const FIREBASE_SECRET     = process.env.FIREBASE_SECRET;     // Firebase RTDB secret
const FIREBASE_WEB_API_KEY = process.env.FIREBASE_WEB_API_KEY; // Firebase Web API Key (for Firestore REST)
const FIREBASE_PROJECT_ID  = process.env.FIREBASE_PROJECT_ID;  // Firebase project ID e.g. tradealert-2602c

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
  BITCOIN:{ current: 0, open: 0, high: 0, low: 0, close: 0 }, // Capital.com epic
  BTC:    { current: 0, open: 0, high: 0, low: 0, close: 0 }, // alternate epic name
};

// Current minute candle being built from ticks
const tickCandle = {
  GOLD:   { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
  SILVER: { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
  BITCOIN:{ open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
  BTC:    { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 },
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

  const res = await post(`${CAP_DEMO_REST}/api/v1/session`, {
    identifier: CAP_EMAIL,
    password: CAP_PASSWORD,
    encryptedPassword: false,
  }, {
    'X-CAP-API-KEY': CAP_API_KEY,
  });

  if (res.status !== 200) {
    throw new Error(`Session failed: ${res.status} ${JSON.stringify(res.body)}`);
  }

  cst           = res.headers['cst'];
  securityToken = res.headers['x-security-token'];
  log(`Session created successfully`);
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

// ── Step 2.5: Find correct epic name for Bitcoin ────────────────────────────
async function findBitcoinEpic() {
  try {
    const res = await get(
      `${CAP_DEMO_REST}/api/v1/markets?searchTerm=Bitcoin&limit=5`,
      { 'CST': cst, 'X-SECURITY-TOKEN': securityToken }
    );
    if (res.status === 200 && res.body?.markets) {
      log(`Bitcoin markets found: ${JSON.stringify(res.body.markets.map(m => ({ epic: m.epic, name: m.instrumentName })))}`);
      // Return first BTC/USD result
      const btcMarket = res.body.markets.find(m =>
        m.instrumentName?.includes('Bitcoin') && m.epic
      );
      return btcMarket?.epic || null;
    }
  } catch(e) {
    log(`findBitcoinEpic error: ${e.message}`);
  }
  return null;
}

// ── Step 3: Connect WebSocket and subscribe ─────────────────────────────────
function connectWebSocket(btcEpic) {
  currentBtcEpic = btcEpic; // store for reconnect
  log('Connecting WebSocket...');
  ws = new WebSocket(CAP_WS_URL);

  const tickEpics = ['GOLD', 'SILVER'];
  const ohlcEpics = ['GOLD', 'SILVER'];
  if (btcEpic) {
    tickEpics.push(btcEpic);
    ohlcEpics.push(btcEpic);
    // Store BTC epic for price tracking
    if (!prices[btcEpic]) prices[btcEpic] = { current: 0, open: 0, high: 0, low: 0, close: 0 };
    if (!tickCandle[btcEpic]) tickCandle[btcEpic] = { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 };
  }

  ws.on('open', () => {
    log('WebSocket connected');

    ws.send(JSON.stringify({
      destination: 'marketData.subscribe',
      correlationId: '1',
      cst,
      securityToken,
      payload: { epics: tickEpics },
    }));

    ws.send(JSON.stringify({
      destination: 'OHLCMarketData.subscribe',
      correlationId: '2',
      cst,
      securityToken,
      payload: {
        epics: ohlcEpics,
        resolutions: ['MINUTE', 'MINUTE_5', 'MINUTE_15', 'HOUR'],
        type: 'classic',
      },
    }));

    log(`Subscribed to: ${tickEpics.join(', ')} tick + OHLC`);

    // Ping WebSocket every 30 seconds to keep connection alive
    // Capital.com closes after ~60s of no activity
    if (ws._pingInterval) clearInterval(ws._pingInterval);
    ws._pingInterval = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          destination: 'ping',
          correlationId: 'ping-' + Date.now(),
          cst,
          securityToken,
        }));
      }
    }, 30000);
  });

  ws.on('message', (data) => {
    try {
      const raw = data.toString();
      const msg = JSON.parse(raw);

      // Log every message destination for debugging
      log(`WS msg: ${msg.destination || 'unknown'} | status: ${msg.status || ''}`);

      // Log full message if it's not a ping (to see subscription responses)
      if (msg.destination !== 'ping' && msg.destination !== 'quote') {
        log(`WS full: ${raw.substring(0, 300)}`);
      }

      // Live tick price update
      if (msg.destination === 'quote' && msg.payload) {
        // Log first few quotes to see exact structure
        if (!ws._quotesLogged) ws._quotesLogged = 0;
        if (ws._quotesLogged < 3) {
          log(`QUOTE payload: ${JSON.stringify(msg.payload)}`);
          ws._quotesLogged++;
        }
        const epic = msg.payload.epic;
        // Ensure prices entry exists for any epic we receive (handles dynamic epics)
        if (epic && !prices[epic]) {
          prices[epic] = { current: 0, open: 0, high: 0, low: 0, close: 0, ohlc: {} };
        }
        if (epic && prices[epic] !== undefined) {
          // Use mid price (bid + ask) / 2 for display
          const bid = msg.payload.bid || 0;
          const ask = msg.payload.ofr || 0;
          const mid = bid > 0 && ask > 0 ? (bid + ask) / 2 : (bid || ask);
          if (mid > 0) {
            const prev = prices[epic].current;
            prices[epic].current = mid;
            if (!ws._priceSetLogged) ws._priceSetLogged = 0;
            if (ws._priceSetLogged < 5) {
              log(`Price set: ${epic} = ${mid}`);
              ws._priceSetLogged++;
            }

            // Build tick-based candle for current minute
            // Ensure tickCandle entry exists for dynamic epics (e.g. BTCUSD)
            if (!tickCandle[epic]) {
              tickCandle[epic] = { open: 0, high: 0, low: 0, lastMinuteClose: 0, startedAt: 0 };
            }
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
    if (ws._pingInterval) { clearInterval(ws._pingInterval); ws._pingInterval = null; }
    log(`WebSocket closed: ${code} ${reason} — reconnecting in 5s`);
    setTimeout(reconnectWebSocket, 5000);
  });

  ws.on('error', (e) => {
    log(`WebSocket error: ${e.message}`);
  });
}

let currentBtcEpic = null;

function reconnectWebSocket() {
  if (ws) { try { ws.terminate(); } catch {} ws = null; }
  connectWebSocket(currentBtcEpic);
}

// ── Step 4: Write current price to Firebase Realtime DB every 5 seconds ─────
async function updateFirebase() {
  const hasAnyPrice = Object.values(prices).some(p => p.current > 0);
  if (!hasAnyPrice) {
    log('No prices yet — skipping Firebase write');
    return;
  }
  if (!FIREBASE_URL || !FIREBASE_SECRET) {
    log('Firebase not configured — skipping');
    return;
  }

  // RTDB stores ONLY current price — keeps bandwidth minimal for mobile listeners
  const btcPrice = currentBtcEpic ? (prices[currentBtcEpic]?.current || 0) : 0;
  const data = {
    xau: { current: prices.GOLD?.current   || 0, updatedAt: Date.now() },
    xag: { current: prices.SILVER?.current || 0, updatedAt: Date.now() },
    btc: { current: btcPrice,                     updatedAt: Date.now() },
  };

  const url    = `${FIREBASE_URL}/prices.json?auth=${FIREBASE_SECRET}`;
  const body   = JSON.stringify(data);
  const urlObj = new URL(url);

  return new Promise((resolve) => {
    const req = https.request({
      hostname: urlObj.hostname,
      port:     443,
      path:     urlObj.pathname + urlObj.search,
      method:   'PUT',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, res => {
      let rb = '';
      res.on('data', c => rb += c);
      res.on('end', () => { log(`RTDB write: ${res.statusCode}`); resolve(); });
    });
    req.on('error', e => { log(`RTDB error: ${e.message}`); resolve(); });
    req.write(body);
    req.end();
  });
}

// ── Step 4b: Write OHLC to Firestore every minute ───────────────────────────
// Stores 1 document per symbol: prices/xau and prices/xag (and prices/btc for testing)
// Worker reads these for alert checking — 1 read per symbol per cron run
// Format: { m1:{h,l,c}, m5:{h,l,c}, m15:{h,l,c}, h1:{h,l,c}, updatedAt }
// M1 also has high/low for instant-hit touch detection
// M5/M15/H1 only need close for candle-close alerts

async function updateFirestoreOHLC() {
  if (!FIREBASE_URL || !FIREBASE_SECRET) return;

  const symbols = [
    { key: 'xau',  epic: 'GOLD' },
    { key: 'xag',  epic: 'SILVER' },
    { key: 'btc',  epic: currentBtcEpic },
  ];

  for (const { key, epic } of symbols) {
    if (!epic) continue;
    const p = prices[epic];
    if (!p || !p.current) continue;

    const ohlc = p.ohlc || {};
    // Use bid priceType for cleaner prices (closer to spot)
    const m1  = ohlc['MINUTE']    || {};
    const m5  = ohlc['MINUTE_5']  || {};
    const m15 = ohlc['MINUTE_15'] || {};
    const h1  = ohlc['HOUR']      || {};

    const doc = {
      current: p.current, // also stored here so Worker can read current price
      // M1: high + low needed for instant-hit touch detection
      m1:  { h: m1.h  || 0, l: m1.l  || 0, c: m1.c  || p.current },
      // M5/M15/H1: only close needed for candle-close alerts
      m5:  { h: m5.h  || 0, l: m5.l  || 0, c: m5.c  || p.current },
      m15: { h: m15.h || 0, l: m15.l || 0, c: m15.c || p.current },
      h1:  { h: h1.h  || 0, l: h1.l  || 0, c: h1.c  || p.current },
      updatedAt: Date.now(),
    };

    // Write to Firestore: prices/{key}
    // Using REST API with Firebase Auth (Database Secret works for Firestore too via legacy)
    // Actually use Firestore REST endpoint
    try {
      const fsUrl = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT_ID}/databases/(default)/documents/prices/${key}?key=${FIREBASE_WEB_API_KEY}`;
      log(`Firestore URL (${key}): projects/${FIREBASE_PROJECT_ID}/prices/${key}`);

      // Build Firestore document format
      function toFsValue(val) {
        if (typeof val === 'number') return { doubleValue: val };
        if (typeof val === 'string') return { stringValue: val };
        if (typeof val === 'object') {
          const fields = {};
          for (const [k, v] of Object.entries(val)) fields[k] = toFsValue(v);
          return { mapValue: { fields } };
        }
        return { nullValue: null };
      }

      const fsDoc = { fields: {} };
      for (const [k, v] of Object.entries(doc)) {
        fsDoc.fields[k] = toFsValue(v);
      }

      const resp = await fetch(fsUrl, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fsDoc),
      });
      if (resp.ok) {
        log(`Firestore OHLC updated: ${key} M1 close=${doc.m1.c}`);
      } else {
        const err = await resp.text();
        log(`Firestore OHLC error (${key}): ${resp.status} ${err.substring(0,100)}`);
      }
    } catch(e) {
      log(`Firestore OHLC exception (${key}): ${e.message}`);
    }
  }
}

// ── Step 5: Send current price to Cloudflare Worker every minute ──────────────
async function updateWorker() {
  if (!Object.values(prices).some(p => p.current > 0)) return;

  const btcPrice = currentBtcEpic ? (prices[currentBtcEpic]?.current || 0) : 0;
  const body = {
    xau: { current: prices.GOLD?.current   || 0 },
    xag: { current: prices.SILVER?.current || 0 },
    btc: { current: btcPrice },
    timestamp: Date.now(),
  };

  try {
    const res = await post(WORKER_URL, body, { 'X-Secret-Key': WORKER_SECRET });
    if (res.status === 200) log(`Worker updated: GOLD=${body.xau.current?.toFixed(2)} SILVER=${body.xag.current?.toFixed(3)} BTC=${body.btc.current?.toFixed(2)}`);
    else log(`Worker update failed: ${res.status}`);
  } catch(e) { log(`Worker POST error: ${e.message}`); }
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
  const btcEpic = await findBitcoinEpic();
  log(`Bitcoin epic name: ${btcEpic || 'not found — skipping BTC'}`);
  connectWebSocket(btcEpic);

  // Ping session every 9 minutes to keep alive (session expires at 10 min)
  setInterval(pingSession, 9 * 60 * 1000);

  // Update Firebase RTDB every 5 seconds (current price only — minimal bandwidth)
  setInterval(updateFirebase, 5000);

  // Update Firestore OHLC + Worker every 60 seconds
  setInterval(async () => {
    await updateFirestoreOHLC();
    await updateWorker();
  }, 60 * 1000);

  log('=== Price Bridge running ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
