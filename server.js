/**
 * ╔══════════════════════════════════════════════════════════════════╗
 *  STREAM EXTRACTOR API — Producción (1000+ usuarios concurrentes)
 *  Stack: Express + Playwright + Bull Queue + Redis + Node Cluster
 * ╚══════════════════════════════════════════════════════════════════╝
 *
 *  ARQUITECTURA:
 *  ┌─────────────────────────────────────────────────────────┐
 *  │  Usuario → Express (master) → Bull Queue → Workers      │
 *  │                                    ↕                    │
 *  │                              Redis Cache                │
 *  │                         (TTL 8 min por URL)             │
 *  └─────────────────────────────────────────────────────────┘
 *
 *  Sin Redis disponible, cae automáticamente a caché en memoria.
 *
 *  INSTALACIÓN:
 *    npm install
 *    npx playwright install chromium
 *    node server.js
 *
 *  ENDPOINT:
 *    GET /get-stream?url=<embed_url_encoded>
 *
 *  RESPUESTA OK:   { "success": true,  "m3u8": "https://..." }
 *  RESPUESTA ERR:  { "success": false, "error": "..." }
 */

'use strict';

const cluster  = require('cluster');
const os       = require('os');
const express  = require('express');
const cors     = require('cors');
const Queue    = require('bull');
const { chromium } = require('playwright');

// ─── Configuración ──────────────────────────────────────────────────────────
const PORT          = process.env.PORT        || 3000;
const REDIS_URL     = process.env.REDIS_URL   || null;   // ej: redis://localhost:6379
const CACHE_TTL_MS  = 8 * 60 * 1000;   // 8 min (tokens de Rumble duran ~10-15 min)
const BROWSER_LIMIT = parseInt(process.env.BROWSER_LIMIT) || 3; // por worker
const WORKERS       = parseInt(process.env.WEB_CONCURRENCY) || Math.min(os.cpus().length, 2);
const JOB_TIMEOUT   = 70_000;          // 70s por job (55s extractor + margen)
const JOB_CONCURRENCY = BROWSER_LIMIT; // jobs paralelos por worker

// ─── Redis / caché en memoria como fallback ──────────────────────────────────
let redisClient = null;
const memCache  = new Map(); // fallback sin Redis

async function cacheGet(key) {
  if (redisClient) {
    const val = await redisClient.get(`stream:${key}`);
    return val || null;
  }
  const e = memCache.get(key);
  if (!e) return null;
  if (Date.now() > e.exp) { memCache.delete(key); return null; }
  return e.val;
}

async function cacheSet(key, val) {
  if (redisClient) {
    await redisClient.setEx(`stream:${key}`, Math.floor(CACHE_TTL_MS / 1000), val);
  } else {
    memCache.set(key, { val, exp: Date.now() + CACHE_TTL_MS });
    // limpia entradas expiradas cada 100 inserciones
    if (memCache.size % 100 === 0) {
      const now = Date.now();
      for (const [k, v] of memCache) if (now > v.exp) memCache.delete(k);
    }
  }
}

// ─── Extractor con Playwright ─────────────────────────────────────────────────
async function extractM3U8(embedUrl) {
  // 1. Intenta caché primero
  const cached = await cacheGet(embedUrl);
  if (cached) { console.log(`[CACHE HIT] ${embedUrl}`); return cached; }

  // 2. Lanza navegador
  const browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox', '--disable-setuid-sandbox',
      '--disable-dev-shm-usage', '--disable-gpu',
      '--disable-extensions', '--mute-audio',
      '--autoplay-policy=no-user-gesture-required', // permite autoplay
    ],
  });

  try {
    const ctx = await browser.newContext({
      userAgent:
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
        'Chrome/124.0.0.0 Safari/537.36',
      serviceWorkers: 'block',
      // Permisos necesarios para que el player arranque
      permissions: ['autoplay'],
    });

    // Solo bloquea imágenes y fuentes — NO bloquea media ni scripts
    // porque el player necesita sus JS para generar la URL del stream
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

    // ── 1. Navega al embed y espera a que el DOM esté listo ─────────────────
    console.log(`[BROWSER] Navegando a: ${embedUrl}`);
    await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 })
              .catch(() => {});

    // ── 2. Espera un momento a que el player JS inicialice ──────────────────
    await page.waitForTimeout(2000);

    // ── 3. Intenta clicks en el orden más común de players de video ─────────
    // Lista de selectores típicos de botones de play en players embebidos
    const playSelectors = [
      // Genéricos
      'button[class*="play"]',
      'div[class*="play"]',
      '[class*="play-btn"]',
      '[class*="playbtn"]',
      '[class*="play_btn"]',
      '[id*="play"]',
      // JWPlayer
      '.jw-icon-display',
      '.jw-display-icon-container',
      // VideoJS
      '.vjs-big-play-button',
      // Plyr
      '.plyr__control--overlaid',
      // HTML5 nativo
      'video',
      // Overlay de click general (muchos players usan un div encima del video)
      '[class*="overlay"]',
      '[class*="poster"]',
      '[class*="thumbnail"]',
      // Iframes anidados: intenta también dentro del primer iframe
    ];

    let clicked = false;
    for (const selector of playSelectors) {
      try {
        const el = await page.$(selector);
        if (el) {
          await el.click({ timeout: 2000 }).catch(() => {});
          console.log(`[CLICK] Selector: ${selector}`);
          clicked = true;
          break;
        }
      } catch (_) {}
    }

    // ── 4. Si ningún selector funcionó, intenta click en el centro de la página
    //       (muchos players de embed detectan cualquier click para iniciar)
    if (!clicked) {
      console.log('[CLICK] Fallback: click en el centro de la página');
      await page.mouse.click(640, 360).catch(() => {});
    }

    // ── 5. Si hay un <video> en la página, fuerza play por JS ───────────────
    await page.evaluate(() => {
      const videos = document.querySelectorAll('video');
      videos.forEach(v => { try { v.play(); } catch(_) {} });
      // Algunos players escuchan eventos de click en el documento
      document.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    }).catch(() => {});

    // ── 6. Espera a que el .m3u8 aparezca en el tráfico de red ──────────────
    const result = await m3u8Promise;
    await cacheSet(embedUrl, result);
    console.log(`[FOUND] ${result.slice(0, 100)}…`);
    return result;

  } finally {
    await browser.close();
  }
}

// ══════════════════════════════════════════════════════════════════════════════
//  MODO CLUSTER
//  El proceso master levanta N workers. Cada worker maneja su propia cola Bull.
//  Esto distribuye la carga CPU/RAM y evita bloquear el event loop.
// ══════════════════════════════════════════════════════════════════════════════
if (cluster.isMaster) {
  console.log(`🚀 Master PID ${process.pid} — iniciando ${WORKERS} workers`);

  for (let i = 0; i < WORKERS; i++) cluster.fork();

  cluster.on('exit', (worker, code) => {
    console.warn(`⚠️  Worker ${worker.process.pid} caído (code ${code}) — reiniciando…`);
    cluster.fork();
  });

} else {
  // ─── WORKER ────────────────────────────────────────────────────────────────
  const app = express();
  app.use(cors({ origin: '*' }));

  // Inicializa Redis si está disponible
  if (REDIS_URL) {
    const redis = require('ioredis');
    redisClient = new redis(REDIS_URL, { lazyConnect: true, enableOfflineQueue: false });
    redisClient.connect().catch(err => {
      console.warn('[REDIS] No disponible, usando caché en memoria:', err.message);
      redisClient = null;
    });
  }

  // ─── Cola Bull (usa Redis si está disponible, sino modo "in-memory" local) ──
  const queueOpts = REDIS_URL
    ? { redis: REDIS_URL }
    : { createClient: () => require('ioredis')({ enableOfflineQueue: false }) };

  let streamQueue;
  try {
    streamQueue = new Queue('stream-extract', REDIS_URL || 'redis://127.0.0.1:6379', {
      defaultJobOptions: { timeout: JOB_TIMEOUT, removeOnComplete: 50, removeOnFail: 20 },
    });

    // Procesa hasta JOB_CONCURRENCY jobs simultáneos en este worker
    streamQueue.process(JOB_CONCURRENCY, async (job) => {
      return extractM3U8(job.data.url);
    });

  } catch (_) {
    // Sin Redis: procesa directamente (modo simple, válido para Render free tier)
    streamQueue = null;
    console.warn('[QUEUE] Bull no disponible, procesando directamente');
  }

  // ─── Mapa de peticiones en vuelo (deduplica requests para el mismo embed) ──
  const inFlight = new Map(); // Map<url, Promise<string>>

  async function getStream(url) {
    // Caché: responde instantáneo sin tocar el pool
    const cached = await cacheGet(url);
    if (cached) return cached;

    // Deduplicación: si ya hay una promesa en vuelo para esta URL, la comparte
    if (inFlight.has(url)) {
      console.log(`[DEDUP] Reutilizando promesa en vuelo para: ${url}`);
      return inFlight.get(url);
    }

    let promise;
    if (streamQueue) {
      // Modo Bull Queue: encola el job y espera su resolución
      promise = streamQueue
        .add({ url }, { attempts: 2, backoff: { type: 'fixed', delay: 2000 } })
        .then(job => job.finished());
    } else {
      // Modo directo (sin Redis)
      promise = extractM3U8(url);
    }

    inFlight.set(url, promise);
    promise.finally(() => inFlight.delete(url));
    return promise;
  }

  // ─── Endpoint principal ──────────────────────────────────────────────────
  app.get('/get-stream', async (req, res) => {
    const embedUrl = req.query.url;

    if (!embedUrl) {
      return res.status(400).json({ success: false, error: 'Parámetro "url" requerido' });
    }
    try { new URL(embedUrl); }
    catch { return res.status(400).json({ success: false, error: 'URL inválida' }); }

    try {
      const m3u8 = await getStream(embedUrl);
      res.json({ success: true, m3u8 });
    } catch (err) {
      console.error(`[ERROR] ${err.message}`);
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // Health-check (Render / Railway lo usan para verificar que el servicio vive)
  app.get('/health', (_req, res) => {
    res.json({
      status: 'ok',
      worker: process.pid,
      cacheSize: memCache.size,
      redis: !!redisClient,
    });
  });

  app.listen(PORT, () => {
    console.log(`✅  Worker ${process.pid} escuchando en puerto ${PORT}`);
  });
}
