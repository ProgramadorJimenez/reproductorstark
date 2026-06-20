'use strict';

const express      = require('express');
const cors         = require('cors');
const { chromium } = require('playwright');

const PORT         = process.env.PORT || 3000;
const CACHE_TTL_MS = 8 * 60 * 1000; // 8 minutos

// ── Caché en memoria ──────────────────────────────────────────────────────────
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

// ── Deduplicación de requests en vuelo ───────────────────────────────────────
// Si 100 usuarios piden el mismo embed al mismo tiempo,
// solo se lanza UN navegador. Los demás esperan el mismo resultado.
const inFlight = new Map();

// ── Pool simple: máximo 3 navegadores simultáneos ────────────────────────────
let activeCount = 0;
const MAX_CONCURRENT = parseInt(process.env.BROWSER_LIMIT) || 3;
const waitQueue = [];

function acquireSlot() {
  return new Promise((resolve, reject) => {
    if (activeCount < MAX_CONCURRENT) {
      activeCount++;
      resolve();
    } else {
      const timer = setTimeout(() => {
        const idx = waitQueue.findIndex(x => x.resolve === resolve);
        if (idx !== -1) waitQueue.splice(idx, 1);
        reject(new Error('Servidor ocupado, intenta en unos segundos'));
      }, 40_000);

      waitQueue.push({
        resolve: () => { clearTimeout(timer); resolve(); },
        reject,
      });
    }
  });
}

function releaseSlot() {
  if (waitQueue.length > 0) {
    waitQueue.shift().resolve();
  } else {
    activeCount--;
  }
}

// ── Extractor principal ───────────────────────────────────────────────────────
async function extractM3U8(embedUrl) {
  await acquireSlot();
  console.log(`[BROWSER] Abriendo (activos: ${activeCount}): ${embedUrl}`);

  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--mute-audio',
      '--autoplay-policy=no-user-gesture-required',
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

    // Solo bloquea imágenes y fuentes
    // NO bloquees scripts ni media: el player los necesita para generar el m3u8
    await ctx.route('**/*', (route) => {
      const type = route.request().resourceType();
      if (['image', 'font'].includes(type)) return route.abort();
      return route.continue();
    });

    const page = await ctx.newPage();

    // ── Promesa que se resuelve cuando detectamos el .m3u8 ──────────────────
    let resolved = false;
    const m3u8Promise = new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error('Timeout: no se encontró .m3u8 en 55s')),
        55_000
      );

      const found = (url) => {
        if (resolved) return;
        if (url.includes('.m3u8')) {
          resolved = true;
          clearTimeout(timer);
          resolve(url);
        }
      };

      page.on('request',  req  => found(req.url()));
      page.on('response', resp => found(resp.url()));
    });

    // ── 1. Navega al embed ───────────────────────────────────────────────────
    console.log(`[NAV] Visitando embed...`);
    await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 })
              .catch(() => {});

    // ── 2. Espera a que el player JS inicialice ──────────────────────────────
    await page.waitForTimeout(2500);

    // ── 3. Intenta clicks en selectores comunes de play ──────────────────────
    const playSelectors = [
      'button[class*="play"]',
      'div[class*="play"]',
      '[class*="play-btn"]',
      '[class*="playbtn"]',
      '[id*="play"]',
      '.jw-icon-display',
      '.jw-display-icon-container',
      '.vjs-big-play-button',
      '.plyr__control--overlaid',
      '[class*="overlay"]',
      '[class*="poster"]',
      'video',
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

    // ── 4. Fallback: click en el centro de pantalla ──────────────────────────
    if (!clicked) {
      console.log('[CLICK] Fallback centro');
      await page.mouse.click(640, 360).catch(() => {});
    }

    // ── 5. Fuerza play por JS en todos los <video> de la página ─────────────
    await page.evaluate(() => {
      document.querySelectorAll('video').forEach(v => { try { v.play(); } catch(_){} });
      document.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    }).catch(() => {});

    // ── 6. Espera el .m3u8 en el tráfico ────────────────────────────────────
    const result = await m3u8Promise;
    cacheSet(embedUrl, result);
    console.log(`[FOUND] ${result.slice(0, 100)}`);
    return result;

  } finally {
    await browser.close();
    releaseSlot();
    console.log(`[BROWSER] Cerrado (activos: ${activeCount})`);
  }
}

// ── Función pública: caché + deduplicación + extractor ───────────────────────
async function getStream(url) {
  const cached = cacheGet(url);
  if (cached) { console.log(`[CACHE HIT] ${url}`); return cached; }

  if (inFlight.has(url)) {
    console.log(`[DEDUP] Reutilizando promesa: ${url}`);
    return inFlight.get(url);
  }

  const promise = extractM3U8(url);
  inFlight.set(url, promise);
  promise.finally(() => inFlight.delete(url));
  return promise;
}

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(cors({ origin: '*' }));

// ── /get-stream — extrae el m3u8 y devuelve la URL del proxy ─────────────────
app.get('/get-stream', async (req, res) => {
  const embedUrl = req.query.url;
  if (!embedUrl) return res.status(400).json({ success: false, error: 'Falta parámetro url' });
  try { new URL(embedUrl); } catch { return res.status(400).json({ success: false, error: 'URL inválida' }); }

  try {
    const m3u8 = await getStream(embedUrl);

    // En lugar de devolver la URL directa del CDN (que está atada a la IP de Render),
    // devolvemos una URL de nuestro propio proxy para que el navegador del usuario
    // pida el stream a través del servidor, manteniendo siempre la IP de Render.
    const proxyUrl = `/proxy-stream?url=${encodeURIComponent(m3u8)}`;
    res.json({ success: true, m3u8: proxyUrl });
  } catch (err) {
    console.error(`[ERROR] ${err.message}`);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ── /proxy-stream — hace de intermediario entre el usuario y el CDN ───────────
// El navegador pide los segmentos a este endpoint.
// El servidor los descarga del CDN (con la IP de Render) y los reenvía.
app.get('/proxy-stream', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).send('Falta url');

  try {
    const upstream = await fetch(targetUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer':    'https://vimeos.net/',
        'Origin':     'https://vimeos.net',
      },
    });

    if (!upstream.ok) {
      return res.status(upstream.status).send('Error del CDN: ' + upstream.status);
    }

    // Reenvía los headers de contenido relevantes
    const contentType = upstream.headers.get('content-type') || 'application/octet-stream';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Access-Control-Allow-Origin', '*');

    // Si es un archivo .m3u8 (playlist), reescribe las URLs internas
    // para que también pasen por el proxy
    if (targetUrl.includes('.m3u8') || contentType.includes('mpegurl')) {
      let text = await upstream.text();

      // La base URL del m3u8 (para resolver rutas relativas)
      const base = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);

      // Reescribe cada línea que sea una URL de segmento o sub-playlist
      text = text.split('\n').map(line => {
        line = line.trim();
        if (!line || line.startsWith('#')) return line;

        // Construye la URL absoluta del segmento
        const absUrl = line.startsWith('http') ? line : base + line;
        return `/proxy-stream?url=${encodeURIComponent(absUrl)}`;
      }).join('\n');

      return res.send(text);
    }

    // Para segmentos .ts y otros binarios: stream directo
    const buffer = await upstream.arrayBuffer();
    res.send(Buffer.from(buffer));

  } catch (err) {
    console.error(`[PROXY ERROR] ${err.message}`);
    res.status(500).send('Error de proxy: ' + err.message);
  }
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', active: activeCount, cached: memCache.size });
});

app.listen(PORT, () => {
  console.log(`✅ API lista en puerto ${PORT} | Pool: ${MAX_CONCURRENT} navegadores`);
});
