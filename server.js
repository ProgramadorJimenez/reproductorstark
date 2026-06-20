'use strict';

const express      = require('express');
const cors         = require('cors');
const { chromium } = require('playwright');

const PORT         = process.env.PORT || 3000;
const CACHE_TTL_MS = 8 * 60 * 1000;

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
const MAX_CONCURRENT = parseInt(process.env.BROWSER_LIMIT) || 2;
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

// ── Extractor: devuelve { m3u8, headers } ────────────────────────────────────
async function extractM3U8(embedUrl) {
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

    // Captura la URL del m3u8 Y los headers exactos que usó el player
    let resolved = false;
    const result = await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('Timeout: no se encontró .m3u8 en 55s')),
        55_000
      );

      page.on('request', req => {
        if (resolved) return;
        const url = req.url();
        if (url.includes('.m3u8')) {
          resolved = true;
          clearTimeout(timer);
          // Captura los headers que el player usó para pedir el m3u8
          const reqHeaders = req.headers();
          resolve({ m3u8: url, reqHeaders });
        }
      });
    });

    // Navega y simula play
    await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 }).catch(() => {});
    await page.waitForTimeout(2500);

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
        if (el) { await el.click({ timeout: 2000 }).catch(() => {}); clicked = true; break; }
      } catch (_) {}
    }
    if (!clicked) await page.mouse.click(640, 360).catch(() => {});
    await page.evaluate(() => {
      document.querySelectorAll('video').forEach(v => { try { v.play(); } catch(_){} });
      document.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    }).catch(() => {});

    const final = await result;

    // Obtiene las cookies del contexto para pasarlas al proxy
    const cookies = await ctx.cookies();
    const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

    return {
      m3u8:      final.m3u8,
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
  const promise = extractM3U8(url);
  inFlight.set(url, promise);
  promise.finally(() => inFlight.delete(url));
  return promise;
}

const app = express();
app.use(cors({ origin: '*' }));

// ── /get-stream — extrae m3u8 y devuelve URL del proxy con token ──────────────
app.get('/get-stream', async (req, res) => {
  const embedUrl = req.query.url;
  if (!embedUrl) return res.status(400).json({ success: false, error: 'Falta url' });
  try { new URL(embedUrl); } catch { return res.status(400).json({ success: false, error: 'URL inválida' }); }

  try {
    const data = await getStream(embedUrl);
    cacheSet(embedUrl, data);

    // Codifica los datos del stream en base64 para pasarlos como token al proxy
    const token = Buffer.from(JSON.stringify({
      m3u8:      data.m3u8,
      referer:   data.referer,
      userAgent: data.userAgent,
      cookie:    data.cookie,
      origin:    data.origin,
    })).toString('base64url');

    res.json({ success: true, m3u8: `/proxy-stream?t=${token}` });
  } catch (err) {
    console.error(`[ERROR] ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── /proxy-stream — proxy que usa los headers/cookies exactos del browser ─────
app.get('/proxy-stream', async (req, res) => {
  const token     = req.query.t;
  const targetUrl = req.query.url; // para segmentos internos reescritos

  let m3u8, referer, userAgent, cookie, origin, base;

  if (token) {
    // Primera petición: decodifica el token con todos los datos
    try {
      const data = JSON.parse(Buffer.from(token, 'base64url').toString());
      m3u8      = data.m3u8;
      referer   = data.referer;
      userAgent = data.userAgent;
      cookie    = data.cookie;
      origin    = data.origin;
      base      = m3u8.substring(0, m3u8.lastIndexOf('/') + 1);
    } catch {
      return res.status(400).send('Token inválido');
    }
  } else if (targetUrl) {
    // Segmentos internos: los parámetros vienen en query string
    m3u8      = targetUrl;
    referer   = req.query.referer   || '';
    userAgent = req.query.ua        || '';
    cookie    = req.query.cookie    || '';
    origin    = req.query.origin    || '';
    base      = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
  } else {
    return res.status(400).send('Falta t o url');
  }

  try {
    const upstream = await fetch(m3u8, {
      headers: {
        'User-Agent': userAgent,
        'Referer':    referer,
        'Origin':     origin,
        ...(cookie ? { 'Cookie': cookie } : {}),
      },
    });

    if (!upstream.ok) {
      console.error(`[PROXY] ${upstream.status} para ${m3u8}`);
      return res.status(upstream.status).send('CDN devolvió ' + upstream.status);
    }

    const contentType = upstream.headers.get('content-type') || 'application/octet-stream';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'no-cache');

    // Si es un m3u8, reescribe las URLs internas para que también pasen por el proxy
    if (m3u8.includes('.m3u8') || contentType.includes('mpegurl')) {
      let text = await upstream.text();
      console.log(`[PROXY] m3u8 obtenido (${text.length} bytes)`);

      const params = new URLSearchParams({
        referer, ua: userAgent, cookie, origin
      });

      text = text.split('\n').map(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return line;
        const absUrl = trimmed.startsWith('http') ? trimmed : base + trimmed;
        return `/proxy-stream?url=${encodeURIComponent(absUrl)}&${params.toString()}`;
      }).join('\n');

      return res.send(text);
    }

    // Segmentos .ts — stream binario directo
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
