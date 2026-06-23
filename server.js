'use strict';

const express      = require('express');
const cors         = require('cors');
const { chromium } = require('playwright');

const PORT         = process.env.PORT || 3000;
const CACHE_TTL_MS = 4 * 60 * 60 * 1000; // 4 horas

const memCache = new Map();
function cacheGet(key) {
  const e = memCache.get(key);
  if (!e) return null;
  if (Date.now() > e.exp) { memCache.delete(key); return null; }
  return e.val;
}
function cacheSet(key, val) {
  memCache.set(key, { val, exp: Date.now() + CACHE_TTL_MS });
}

const inFlight = new Map();
let activeCount = 0;
const MAX_CONCURRENT = parseInt(process.env.BROWSER_LIMIT) || 15;
const waitQueue = [];

function acquireSlot() {
  return new Promise((resolve, reject) => {
    if (activeCount < MAX_CONCURRENT) { activeCount++; resolve(); return; }
    const timer = setTimeout(() => {
      const idx = waitQueue.findIndex(x => x.resolve === resolve);
      if (idx !== -1) waitQueue.splice(idx, 1);
      reject(new Error('Servidor ocupado, intenta en unos segundos'));
    }, 40_000);
    waitQueue.push({ resolve: () => { clearTimeout(timer); resolve(); }, reject });
  });
}
function releaseSlot() {
  if (waitQueue.length > 0) waitQueue.shift().resolve();
  else activeCount--;
}

// Detecta si una URL es un stream de video válido
function isVideoUrl(url) {
  const u = url.toLowerCase().split('?')[0];
  return u.includes('.m3u8') || u.includes('.mp4') || u.includes('.mkv') ||
         u.includes('.webm') || u.includes('.avi') || u.includes('.mov') ||
         u.includes('.txt');  // algunos servidores disfrazan HLS como .txt
}

// Devuelve el tipo de stream
function getStreamType(url) {
  const u = url.toLowerCase().split('?')[0];
  if (u.includes('.m3u8')) return 'hls';
  if (u.includes('.txt'))  return 'hls'; // HLS disfrazado como .txt
  if (u.includes('.mp4'))  return 'mp4';
  if (u.includes('.mkv'))  return 'mp4'; // JWPlayer lee mkv como mp4
  if (u.includes('.webm')) return 'mp4';
  return 'mp4';
}

async function extractStream(embedUrl) {
  await acquireSlot();
  console.log(`[BROWSER] Abriendo (activos: ${activeCount}): ${embedUrl}`);

  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--disable-dev-shm-usage', '--disable-gpu',
      '--mute-audio', '--autoplay-policy=no-user-gesture-required',
    ],
  });

  try {
    const ctx = await browser.newContext({
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/124.0.0.0 Safari/537.36',
      serviceWorkers: 'block',
    });

    await ctx.route('**/*', (route) => {
      const type = route.request().resourceType();
      if (['image', 'font'].includes(type)) return route.abort();
      return route.continue();
    });

    const page = await ctx.newPage();

    // ── Listener ANTES de navegar ─────────────────────────────────────────
    let resolved = false;
    const streamPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('Timeout: no se encontró stream en 55s')),
        55_000
      );

      const found = (url, headers) => {
        if (resolved) return;
        if (isVideoUrl(url)) {
          resolved = true;
          clearTimeout(timer);
          resolve({ url, headers: headers || {} });
        }
      };

      page.on('request',  req  => found(req.url(), req.headers()));
      page.on('response', resp => found(resp.url(), {}));
    });

    // ── Navega al embed ───────────────────────────────────────────────────
    console.log('[NAV] Cargando embed...');
    await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 })
              .catch(() => {});

    await page.waitForTimeout(2500);

    // ── Simula click en play ──────────────────────────────────────────────
    const playSelectors = [
      'button[class*="play"]', 'div[class*="play"]', '[class*="play-btn"]',
      '[class*="playbtn"]', '[id*="play"]', '.jw-icon-display',
      '.jw-display-icon-container', '.vjs-big-play-button',
      '.plyr__control--overlaid', '[class*="overlay"]', '[class*="poster"]', 'video',
    ];

    let clicked = false;
    for (const sel of playSelectors) {
      try {
        const el = await page.$(sel);
        if (el) {
          await el.click({ timeout: 2000 }).catch(() => {});
          console.log(`[CLICK] ${sel}`);
          clicked = true;
          break;
        }
      } catch (_) {}
    }

    if (!clicked) {
      console.log('[CLICK] Fallback centro');
      await page.mouse.click(640, 360).catch(() => {});
    }

    await page.evaluate(() => {
      document.querySelectorAll('video').forEach(v => { try { v.play(); } catch(_){} });
      document.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    }).catch(() => {});

    // ── Espera el stream ──────────────────────────────────────────────────
    const found = await streamPromise;
    const tipo  = getStreamType(found.url);
    console.log(`[FOUND] ${tipo.toUpperCase()} → ${found.url.slice(0, 100)}`);

    const cookies   = await ctx.cookies();
    const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

    return {
      url:       found.url,
      type:      tipo,
      referer:   embedUrl,
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      cookie:    cookieStr,
      origin:    new URL(embedUrl).origin,
    };

  } finally {
    await browser.close();
    releaseSlot();
    console.log(`[BROWSER] Cerrado (activos: ${activeCount})`);
  }
}

async function getStream(url) {
  const cached = cacheGet(url);
  if (cached) { console.log(`[CACHE HIT] ${url}`); return cached; }
  if (inFlight.has(url)) { console.log(`[DEDUP] ${url}`); return inFlight.get(url); }
  const promise = extractStream(url);
  inFlight.set(url, promise);
  promise.finally(() => inFlight.delete(url));
  return promise;
}

const app = express();
app.use(cors({ origin: '*' }));

// ── /get-stream ───────────────────────────────────────────────────────────────
app.get('/get-stream', async (req, res) => {
  const embedUrl = req.query.url;
  if (!embedUrl) return res.status(400).json({ success: false, error: 'Falta url' });
  try { new URL(embedUrl); } catch { return res.status(400).json({ success: false, error: 'URL inválida' }); }

  if (req.query.nocache) {
    memCache.delete(embedUrl);
    console.log(`[NOCACHE] ${embedUrl}`);
  }

  try {
    const data = await getStream(embedUrl);
    cacheSet(embedUrl, data);

    const token = Buffer.from(JSON.stringify({
      url:       data.url,
      type:      data.type,
      referer:   data.referer,
      userAgent: data.userAgent,
      cookie:    data.cookie,
      origin:    data.origin,
    })).toString('base64url');

    res.json({
      success: true,
      m3u8:    `/proxy-stream?t=${token}`,
      type:    data.type,
    });
  } catch (err) {
    console.error(`[ERROR] ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── /proxy-stream ─────────────────────────────────────────────────────────────
app.get('/proxy-stream', async (req, res) => {
  const token     = req.query.t;
  const targetUrl = req.query.url;

  let streamUrl, referer, userAgent, cookie, origin, base, tipo;

  if (token) {
    try {
      const data = JSON.parse(Buffer.from(token, 'base64url').toString());
      streamUrl = data.url;
      tipo      = data.type;
      referer   = data.referer;
      userAgent = data.userAgent;
      cookie    = data.cookie;
      origin    = data.origin;
      base      = streamUrl.substring(0, streamUrl.lastIndexOf('/') + 1);
    } catch {
      return res.status(400).send('Token inválido');
    }
  } else if (targetUrl) {
    streamUrl = targetUrl;
    tipo      = getStreamType(targetUrl);
    referer   = req.query.referer || '';
    userAgent = req.query.ua      || '';
    cookie    = req.query.cookie  || '';
    origin    = req.query.origin  || '';
    base      = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
  } else {
    return res.status(400).send('Falta t o url');
  }

  try {
    const upstream = await fetch(streamUrl, {
      headers: {
        'User-Agent': userAgent,
        'Referer':    referer,
        'Origin':     origin,
        ...(cookie ? { 'Cookie': cookie } : {}),
      },
    });

    if (!upstream.ok) {
      console.error(`[PROXY] ${upstream.status} para ${streamUrl}`);
      return res.status(upstream.status).send('CDN devolvió ' + upstream.status);
    }

    const contentType = upstream.headers.get('content-type') || 'application/octet-stream';
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'no-cache');

    // Si es HLS reescribe las URLs internas
    const isHLS = streamUrl.includes('.m3u8') ||
                  streamUrl.includes('.txt') ||
                  contentType.includes('mpegurl') ||
                  contentType.includes('x-mpegurl') ||
                  contentType.includes('text/plain'); // .txt disfrazado de HLS

    if (isHLS) {
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
      let text = await upstream.text();
      console.log(`[PROXY] HLS OK (${text.length} bytes)`);

      const params = new URLSearchParams({ referer, ua: userAgent, cookie, origin });
      text = text.split('\n').map(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return line;
        const absUrl = trimmed.startsWith('http') ? trimmed : base + trimmed;
        return `/proxy-stream?url=${encodeURIComponent(absUrl)}&${params.toString()}`;
      }).join('\n');

      return res.send(text);
    }

    // Para MP4/MKV/otros — stream binario directo con soporte de Range
    const rangeHeader = req.headers['range'];
    if (rangeHeader) {
      // Reenvía el header Range al CDN para permitir seek en MP4
      const upstreamRange = await fetch(streamUrl, {
        headers: {
          'User-Agent': userAgent,
          'Referer':    referer,
          'Origin':     origin,
          'Range':      rangeHeader,
          ...(cookie ? { 'Cookie': cookie } : {}),
        },
      });
      res.setHeader('Content-Type', upstreamRange.headers.get('content-type') || contentType);
      res.setHeader('Content-Range',  upstreamRange.headers.get('content-range') || '');
      res.setHeader('Accept-Ranges',  'bytes');
      res.setHeader('Content-Length', upstreamRange.headers.get('content-length') || '');
      res.status(upstreamRange.status);
      const buf = await upstreamRange.arrayBuffer();
      return res.send(Buffer.from(buf));
    }

    // Sin Range — stream completo
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    const contentLength = upstream.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);

    const buffer = await upstream.arrayBuffer();
    res.send(Buffer.from(buffer));

  } catch (err) {
    console.error(`[PROXY ERROR] ${err.message}`);
    res.status(500).send('Error: ' + err.message);
  }
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', active: activeCount, cached: memCache.size });
});

app.listen(PORT, () => {
  console.log(`✅ API lista en puerto ${PORT} | Pool: ${MAX_CONCURRENT} navegadores`);
});
