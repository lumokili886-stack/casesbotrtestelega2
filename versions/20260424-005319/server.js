const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const express = require('express');
const dotenv = require('dotenv');
const { RelyingParty } = require('openid');

dotenv.config();

const app = express();
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_BASE_URL_ENV = process.env.PUBLIC_BASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '8703390473:AAEzMS_cnHWF5I_L2T0Hjd4q0adAs1tmjL0';
const WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
const STEAM_REALM_ENV = process.env.STEAM_REALM || '';
const STEAM_LINK_TOKEN_SECRET = process.env.STEAM_LINK_TOKEN_SECRET || WEBHOOK_SECRET || BOT_TOKEN || 'vault-steam-link-secret';
const DEFAULT_DATA_PATH = process.env.VERCEL ? '/tmp/db.json' : './data/db.json';
const DATA_PATH = path.resolve(__dirname, process.env.DATA_PATH || DEFAULT_DATA_PATH);
const TG_INITDATA_MAX_AGE_SECONDS = Number(process.env.TG_INITDATA_MAX_AGE_SECONDS || 86400);
const STEAM_OPENID_ENDPOINT = 'https://steamcommunity.com/openid';

const casePrices = {
  premier: 850,
  gamma: 620,
  danger: 1200,
  clutch: 430,
  free: 0,
};

const skinPool = [
  { emoji: '🔫', image: 'assets/skins/usp-cortex.png', name: 'USP-S | Cortex', priceUsd: 6, rarity: 'common', r: 'c' },
  { emoji: '🎯', image: 'assets/skins/p2000-pulse.png', name: 'P2000 | Pulse', priceUsd: 5, rarity: 'common', r: 'c' },
  { emoji: '💣', image: 'assets/skins/p250-asiimov.png', name: 'P250 | Asiimov', priceUsd: 18, rarity: 'uncommon', r: 'u' },
  { emoji: '⚡', image: 'assets/skins/awp-atheris.png', name: 'AWP | Atheris', priceUsd: 22, rarity: 'uncommon', r: 'u' },
  { emoji: '🌊', image: 'assets/skins/m4a4-spider-lily.png', name: 'M4A4 | Spider Lily', priceUsd: 45, rarity: 'rare', r: 'r' },
  { emoji: '🔧', image: 'assets/skins/m4a1s-hyper-beast.png', name: 'M4A1-S | Hyper Beast', priceUsd: 85, rarity: 'rare', r: 'r' },
  { emoji: '🔥', image: 'assets/skins/ak-fire-serpent.png', name: 'AK-47 | Fire Serpent', priceUsd: 420, rarity: 'legendary', r: 'l' },
  { emoji: '🔪', image: 'assets/skins/karambit-tiger-tooth.png', name: 'Karambit | Tiger Tooth', priceUsd: 890, rarity: 'legendary', r: 'l' },
];

const rarityWeights = [48, 22, 14, 9, 4, 2, 0.7, 0.3];

if (!BOT_TOKEN) {
  console.warn('[warn] BOT_TOKEN is empty. Telegram auth/payments will fail until configured.');
}

function nowIso() {
  return new Date().toISOString();
}

function b64urlEncode(input) {
  return Buffer.from(input).toString('base64url');
}

function b64urlDecode(input) {
  return Buffer.from(String(input || ''), 'base64url').toString('utf8');
}

function signSteamLinkPayload(payloadB64) {
  return crypto.createHmac('sha256', STEAM_LINK_TOKEN_SECRET).update(payloadB64).digest('base64url');
}

function createSteamLinkToken(tgUserId) {
  const payload = {
    tgUserId: Number(tgUserId),
    exp: Date.now() + 10 * 60 * 1000,
    n: randomToken(8),
  };
  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const sig = signSteamLinkPayload(payloadB64);
  return `${payloadB64}.${sig}`;
}

function parseSteamLinkToken(token) {
  const raw = String(token || '');
  const dot = raw.lastIndexOf('.');
  if (dot <= 0) return { ok: false, error: 'bad token format' };

  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expected = signSteamLinkPayload(payloadB64);
  const sigBuf = Buffer.from(sig);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length || !crypto.timingSafeEqual(sigBuf, expBuf)) {
    return { ok: false, error: 'bad token signature' };
  }

  let payload = null;
  try {
    payload = JSON.parse(b64urlDecode(payloadB64));
  } catch {
    return { ok: false, error: 'bad token payload' };
  }

  if (!payload?.tgUserId) return { ok: false, error: 'tgUserId missing' };
  if (!payload?.exp || Date.now() > Number(payload.exp)) return { ok: false, error: 'token expired' };

  return { ok: true, tgUserId: Number(payload.tgUserId) };
}

function trimTrailingSlash(value) {
  return String(value || '').replace(/\/+$/, '');
}

function resolvePublicBaseUrl(req) {
  if (PUBLIC_BASE_URL_ENV) return trimTrailingSlash(PUBLIC_BASE_URL_ENV);
  const protoRaw = req?.get?.('x-forwarded-proto') || req?.protocol || 'https';
  const hostRaw = req?.get?.('x-forwarded-host') || req?.get?.('host') || `localhost:${PORT}`;
  const proto = String(protoRaw).split(',')[0].trim();
  const host = String(hostRaw).split(',')[0].trim();
  return `${proto}://${host}`;
}

function resolveSteamRealm(req) {
  if (STEAM_REALM_ENV) return `${trimTrailingSlash(STEAM_REALM_ENV)}/`;
  return `${resolvePublicBaseUrl(req)}/`;
}

function randomToken(bytes = 24) {
  return crypto.randomBytes(bytes).toString('hex');
}

function ensureDb() {
  const dir = path.dirname(DATA_PATH);
  fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(DATA_PATH)) {
    fs.writeFileSync(
      DATA_PATH,
      JSON.stringify({ users: {}, steamLinkRequests: {}, orders: {} }, null, 2),
      'utf8'
    );
  }
}

function readDb() {
  ensureDb();
  return JSON.parse(fs.readFileSync(DATA_PATH, 'utf8'));
}

function writeDb(db) {
  fs.writeFileSync(DATA_PATH, JSON.stringify(db, null, 2), 'utf8');
}

function cleanup(db) {
  const nowMs = Date.now();

  for (const [nonce, reqData] of Object.entries(db.steamLinkRequests || {})) {
    if (!reqData?.expiresAt || nowMs > new Date(reqData.expiresAt).getTime()) {
      delete db.steamLinkRequests[nonce];
    }
  }

  for (const [id, order] of Object.entries(db.orders || {})) {
    if (order?.status === 'pending' && order?.expiresAt && nowMs > new Date(order.expiresAt).getTime()) {
      db.orders[id].status = 'expired';
      db.orders[id].updatedAt = nowIso();
    }
  }
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function parseInitData(initDataRaw) {
  const params = new URLSearchParams(initDataRaw);
  const hash = params.get('hash');
  if (!hash) return { ok: false, error: 'hash missing' };

  params.delete('hash');
  const pairs = [];
  for (const [k, v] of params.entries()) pairs.push(`${k}=${v}`);
  pairs.sort();
  const dataCheckString = pairs.join('\n');

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const calcHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  if (!crypto.timingSafeEqual(Buffer.from(calcHash), Buffer.from(hash))) {
    return { ok: false, error: 'hash mismatch' };
  }

  const authDate = Number(params.get('auth_date') || '0');
  if (!Number.isFinite(authDate) || authDate <= 0) {
    return { ok: false, error: 'bad auth_date' };
  }

  const age = Math.floor(Date.now() / 1000) - authDate;
  if (age > TG_INITDATA_MAX_AGE_SECONDS) {
    return { ok: false, error: 'initData expired' };
  }

  const userRaw = params.get('user');
  const user = userRaw ? safeJsonParse(userRaw) : null;
  if (!user?.id) return { ok: false, error: 'user missing' };

  return { ok: true, user, authDate };
}

function getInitData(req) {
  const fromHeader = req.get('x-telegram-init-data');
  const fromBody = typeof req.body?.initData === 'string' ? req.body.initData : null;
  const fromQuery = typeof req.query?.initData === 'string' ? req.query.initData : null;
  return fromHeader || fromBody || fromQuery || '';
}

function makeDefaultUser(parsedUser) {
  return {
    tgUserId: parsedUser?.id || null,
    username: parsedUser?.username || null,
    firstName: parsedUser?.first_name || null,
    lastName: parsedUser?.last_name || null,
    photoUrl: parsedUser?.photo_url || null,
    balance: 0,
    steamId: null,
    steamLinkedAt: null,
    inventory: [],
    stats: {
      casesOpened: 0,
      totalWonUsd: 0,
      legendaryCount: 0,
    },
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
}

function ensureUserShape(user) {
  if (!Array.isArray(user.inventory)) user.inventory = [];
  if (!user.stats || typeof user.stats !== 'object') {
    user.stats = { casesOpened: 0, totalWonUsd: 0, legendaryCount: 0 };
  }
  user.stats.casesOpened = Number(user.stats.casesOpened || 0);
  user.stats.totalWonUsd = Number(user.stats.totalWonUsd || 0);
  user.stats.legendaryCount = Number(user.stats.legendaryCount || 0);
  user.balance = Number(user.balance || 0);
}

function ensureAuthed(req, res) {
  if (!BOT_TOKEN) {
    res.status(500).json({ ok: false, error: 'BOT_TOKEN is not configured' });
    return null;
  }

  const initData = getInitData(req);
  if (!initData) {
    res.status(401).json({ ok: false, error: 'Missing Telegram initData' });
    return null;
  }

  const parsed = parseInitData(initData);
  if (!parsed.ok) {
    res.status(401).json({ ok: false, error: `Telegram auth failed: ${parsed.error}` });
    return null;
  }

  const db = readDb();
  cleanup(db);

  const uid = String(parsed.user.id);
  if (!db.users[uid]) {
    db.users[uid] = makeDefaultUser(parsed.user);
  } else {
    db.users[uid].username = parsed.user.username || db.users[uid].username || null;
    db.users[uid].firstName = parsed.user.first_name || db.users[uid].firstName || null;
    db.users[uid].lastName = parsed.user.last_name || db.users[uid].lastName || null;
    db.users[uid].photoUrl = parsed.user.photo_url || db.users[uid].photoUrl || null;
    ensureUserShape(db.users[uid]);
    db.users[uid].updatedAt = nowIso();
  }

  writeDb(db);
  return { db, userKey: uid, user: db.users[uid], initData, tgUser: parsed.user };
}

function weightedRandomSkin() {
  const total = rarityWeights.reduce((a, b) => a + b, 0);
  let r = Math.random() * total;
  for (let i = 0; i < rarityWeights.length; i++) {
    r -= rarityWeights[i];
    if (r <= 0) return skinPool[i];
  }
  return skinPool[0];
}

function makeInventoryItem(template) {
  return {
    id: randomToken(10),
    emoji: template.emoji,
    image: template.image || null,
    name: template.name,
    rarity: template.rarity,
    r: template.r,
    priceUsd: template.priceUsd,
    priceText: `$${template.priceUsd}`,
    sellStars: Math.round(template.priceUsd * 10),
    acquiredAt: nowIso(),
  };
}

function presentUser(user) {
  return {
    tgUserId: user.tgUserId,
    username: user.username,
    firstName: user.firstName,
    lastName: user.lastName,
    photoUrl: user.photoUrl,
    balance: user.balance,
    steamId: user.steamId,
    steamLinkedAt: user.steamLinkedAt,
    steamProfileUrl: user.steamId ? `https://steamcommunity.com/profiles/${user.steamId}` : null,
    stats: user.stats,
  };
}

function extractSteamId(claimedIdentifier) {
  const m = String(claimedIdentifier || '').match(/\/(\d+)\/?$/);
  return m ? m[1] : null;
}

function buildRp(returnUrl, realm) {
  return new RelyingParty(returnUrl, realm, true, false, []);
}

async function telegramApi(method, payload) {
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await resp.json();
  if (!data.ok) throw new Error(`${method} failed: ${data.description || 'unknown error'}`);
  return data.result;
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, now: nowIso() });
});

app.post('/api/auth/telegram', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;
  res.json({ ok: true, user: presentUser(auth.user) });
});

app.get('/api/state', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const inv = [...auth.user.inventory]
    .sort((a, b) => new Date(b.acquiredAt).getTime() - new Date(a.acquiredAt).getTime());

  res.json({
    ok: true,
    user: presentUser(auth.user),
    inventory: inv,
    inventoryCount: inv.length,
    inventoryUsdValue: inv.reduce((s, x) => s + Number(x.priceUsd || 0), 0),
    casePrices,
  });
});

app.post('/api/cases/open', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const caseName = String(req.body?.caseName || '');
  const count = Number(req.body?.count || 1);

  if (!Object.prototype.hasOwnProperty.call(casePrices, caseName)) {
    return res.status(400).json({ ok: false, error: 'unknown case' });
  }
  if (!Number.isInteger(count) || count < 1 || count > 10) {
    return res.status(400).json({ ok: false, error: 'count must be 1..10' });
  }

  const totalPrice = casePrices[caseName] * count;
  if (caseName !== 'free' && auth.user.balance < totalPrice) {
    return res.status(400).json({ ok: false, error: 'not enough balance' });
  }

  if (caseName !== 'free') auth.user.balance -= totalPrice;

  const winners = [];
  for (let i = 0; i < count; i++) {
    const item = makeInventoryItem(weightedRandomSkin());
    auth.user.inventory.push(item);
    auth.user.stats.casesOpened += 1;
    auth.user.stats.totalWonUsd += Number(item.priceUsd || 0);
    if (item.rarity === 'legendary') auth.user.stats.legendaryCount += 1;
    winners.push(item);
  }

  auth.user.updatedAt = nowIso();
  writeDb(auth.db);

  res.json({
    ok: true,
    winners,
    winner: winners[0],
    balance: auth.user.balance,
    inventoryCount: auth.user.inventory.length,
    user: presentUser(auth.user),
  });
});

app.post('/api/inventory/sell', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const itemId = String(req.body?.itemId || '');
  if (!itemId) return res.status(400).json({ ok: false, error: 'itemId required' });

  const idx = auth.user.inventory.findIndex(x => x.id === itemId);
  if (idx === -1) return res.status(404).json({ ok: false, error: 'item not found' });

  const [item] = auth.user.inventory.splice(idx, 1);
  const stars = Number(item.sellStars || Math.round(Number(item.priceUsd || 0) * 10));
  auth.user.balance += stars;
  auth.user.updatedAt = nowIso();
  writeDb(auth.db);

  res.json({ ok: true, sold: item, stars, balance: auth.user.balance, inventoryCount: auth.user.inventory.length });
});

app.post('/api/inventory/sell-all', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const totalStars = auth.user.inventory.reduce(
    (s, item) => s + Number(item.sellStars || Math.round(Number(item.priceUsd || 0) * 10)),
    0
  );
  const count = auth.user.inventory.length;
  auth.user.inventory = [];
  auth.user.balance += totalStars;
  auth.user.updatedAt = nowIso();
  writeDb(auth.db);

  res.json({ ok: true, soldCount: count, totalStars, balance: auth.user.balance, inventoryCount: 0 });
});

app.post('/api/steam/link/start', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const nonce = createSteamLinkToken(auth.user.tgUserId);
  const baseUrl = resolvePublicBaseUrl(req);
  res.json({ ok: true, steamUrl: `${baseUrl}/auth/steam/start?nonce=${encodeURIComponent(nonce)}` });
});

app.get('/auth/steam/start', (req, res) => {
  const nonce = String(req.query.nonce || '');
  if (!nonce) return res.status(400).send('Missing nonce');

  const parsed = parseSteamLinkToken(nonce);
  if (!parsed.ok) return res.status(400).send('Link request not found or expired');

  const baseUrl = resolvePublicBaseUrl(req);
  const returnUrl = `${baseUrl}/auth/steam/return?nonce=${encodeURIComponent(nonce)}`;
  const rp = buildRp(returnUrl, resolveSteamRealm(req));
  rp.authenticate(STEAM_OPENID_ENDPOINT, false, (err, authUrl) => {
    if (err || !authUrl) return res.status(500).send('Steam auth init failed');
    return res.redirect(authUrl);
  });
});

app.get('/auth/steam/return', (req, res) => {
  const nonce = String(req.query.nonce || '');
  if (!nonce) return res.status(400).send('Missing nonce');

  const parsedNonce = parseSteamLinkToken(nonce);
  if (!parsedNonce.ok) return res.status(400).send('Link request not found or expired');

  const baseUrl = resolvePublicBaseUrl(req);
  const returnUrl = `${baseUrl}/auth/steam/return?nonce=${encodeURIComponent(nonce)}`;
  const rp = buildRp(returnUrl, resolveSteamRealm(req));

  rp.verifyAssertion(req, (err, result) => {
    if (err || !result?.authenticated) return res.status(401).send('Steam authentication failed');

    const steamId = extractSteamId(result.claimedIdentifier);
    if (!steamId) return res.status(400).send('Failed to parse Steam ID');

    const db = readDb();
    cleanup(db);
    const userKey = String(parsedNonce.tgUserId);
    if (!db.users[userKey]) db.users[userKey] = makeDefaultUser({ id: parsedNonce.tgUserId });

    db.users[userKey].steamId = steamId;
    db.users[userKey].steamLinkedAt = nowIso();
    db.users[userKey].updatedAt = nowIso();
    writeDb(db);

    const botUsername = String(process.env.BOT_USERNAME || '').replace('@', '');

    res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Steam linked</title><style>body{font-family:Arial,sans-serif;background:#0a0b0f;color:#f0f2f8;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:24px}.card{max-width:460px;background:#161922;border:1px solid #2a2f3d;border-radius:12px;padding:22px;text-align:center}a{color:#4e9eff}</style></head><body><div class="card"><h2>Steam аккаунт привязан</h2><p>SteamID: <b>${steamId}</b></p><p><a href="tg://resolve?domain=${encodeURIComponent(botUsername)}">Вернуться в Telegram</a></p><p>Можно просто закрыть это окно.</p></div></body></html>`);
  });
});

app.post('/api/payments/stars/create', async (req, res) => {
  try {
    const auth = ensureAuthed(req, res);
    if (!auth) return;

    const amount = Number(req.body?.amount || 0);
    if (!Number.isInteger(amount) || amount < 1 || amount > 250000) {
      return res.status(400).json({ ok: false, error: 'amount must be integer 1..250000' });
    }

    const orderId = randomToken(12);
    const payload = `vault:deposit:${auth.user.tgUserId}:${orderId}:${amount}`;

    const invoiceLink = await telegramApi('createInvoiceLink', {
      title: 'VAULT Balance Top Up',
      description: `Пополнение баланса VAULT на ${amount} Stars`,
      payload,
      provider_token: '',
      currency: 'XTR',
      prices: [{ label: `VAULT ${amount} Stars`, amount }],
    });

    auth.db.orders[orderId] = {
      id: orderId,
      tgUserId: auth.user.tgUserId,
      amount,
      payload,
      status: 'pending',
      createdAt: nowIso(),
      updatedAt: nowIso(),
      expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
      source: 'telegram-stars',
    };
    writeDb(auth.db);

    res.json({ ok: true, orderId, invoiceLink });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'create invoice failed' });
  }
});

app.get('/api/payments/order/:orderId', (req, res) => {
  const auth = ensureAuthed(req, res);
  if (!auth) return;

  const order = auth.db.orders[req.params.orderId];
  if (!order || String(order.tgUserId) !== String(auth.user.tgUserId)) {
    return res.status(404).json({ ok: false, error: 'order not found' });
  }

  res.json({ ok: true, order: { id: order.id, status: order.status, amount: order.amount, updatedAt: order.updatedAt } });
});

app.post('/telegram/webhook', async (req, res) => {
  try {
    if (WEBHOOK_SECRET) {
      const got = req.get('x-telegram-bot-api-secret-token');
      if (got !== WEBHOOK_SECRET) return res.status(403).json({ ok: false, error: 'bad webhook secret' });
    }

    const update = req.body || {};

    if (update.pre_checkout_query) {
      await telegramApi('answerPreCheckoutQuery', {
        pre_checkout_query_id: update.pre_checkout_query.id,
        ok: true,
      });
      return res.json({ ok: true });
    }

    const sp = update.message?.successful_payment;
    if (sp && sp.currency === 'XTR') {
      const payload = String(sp.invoice_payload || '');
      const match = payload.match(/^vault:deposit:(\d+):([a-f0-9]+):(\d+)$/i);
      if (match) {
        const tgUserId = Number(match[1]);
        const orderId = match[2];
        const amount = Number(match[3]);

        const db = readDb();
        cleanup(db);

        const userKey = String(tgUserId);
        if (!db.users[userKey]) db.users[userKey] = makeDefaultUser({ id: tgUserId });
        ensureUserShape(db.users[userKey]);

        const order = db.orders[orderId];
        if (order && order.status !== 'paid') {
          order.status = 'paid';
          order.telegramChargeId = sp.telegram_payment_charge_id || null;
          order.updatedAt = nowIso();
          db.users[userKey].balance = Number(db.users[userKey].balance || 0) + amount;
          db.users[userKey].updatedAt = nowIso();
          writeDb(db);
        }
      }
      return res.json({ ok: true });
    }

    return res.json({ ok: true, ignored: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message || 'webhook error' });
  }
});

app.use(express.static(__dirname));

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'cs2-miniapp.html'));
});

if (!process.env.VERCEL) {
  app.listen(PORT, () => {
    console.log(`[vault] listening on ${PORT}`);
    console.log(`[vault] public base: ${PUBLIC_BASE_URL_ENV || '(auto from request host)'}`);
  });
}

module.exports = app;
