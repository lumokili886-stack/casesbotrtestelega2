const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const express = require('express');
const dotenv = require('dotenv');
const { RelyingParty } = require('openid');
const { MongoClient } = require('mongodb');

dotenv.config();

const app = express();
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 3000);
const PUBLIC_BASE_URL_ENV = process.env.PUBLIC_BASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || '';
const STEAM_REALM_ENV = process.env.STEAM_REALM || '';
const STEAM_LINK_TOKEN_SECRET = process.env.STEAM_LINK_TOKEN_SECRET || WEBHOOK_SECRET || BOT_TOKEN || 'vault-steam-link-secret';
const DEFAULT_DATA_PATH = process.env.VERCEL ? '/tmp/db.json' : './data/db.json';
const DATA_PATH = path.resolve(__dirname, process.env.DATA_PATH || DEFAULT_DATA_PATH);
const KV_REST_API_URL = (process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '').trim();
const KV_REST_API_TOKEN = (process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
const MONGODB_URI = (process.env.MONGODB_URI || process.env.MONGO_URL || '').trim();
const MONGODB_DB_NAME = (process.env.MONGODB_DB || process.env.MONGODB_DATABASE || process.env.MONGO_DB || '').trim();
const DB_KEY = process.env.DB_KEY || 'vault:db:v1';
const USE_KV = Boolean(KV_REST_API_URL && KV_REST_API_TOKEN);
const USE_MONGO = Boolean(MONGODB_URI);
const TG_INITDATA_MAX_AGE_SECONDS = Number(process.env.TG_INITDATA_MAX_AGE_SECONDS || 86400);
const STEAM_OPENID_ENDPOINT = 'https://steamcommunity.com/openid';

const casePrices = {
  premier: 850,
  gamma: 620,
  danger: 1200,
  clutch: 430,
  mirage: 990,
  anubis: 760,
  inferno: 1350,
  overpass: 510,
  free: 0,
};

const skinPool = [
  { key: 'usp-cortex', emoji: '🔫', image: '/assets/skins/usp-cortex.png', name: 'USP-S | Cortex', priceUsd: 6, rarity: 'common', r: 'c' },
  { key: 'p2000-pulse', emoji: '🎯', image: '/assets/skins/p2000-pulse.png', name: 'P2000 | Pulse', priceUsd: 5, rarity: 'common', r: 'c' },
  { key: 'p250-asiimov', emoji: '💣', image: '/assets/skins/p250-asiimov.png', name: 'P250 | Asiimov', priceUsd: 18, rarity: 'uncommon', r: 'u' },
  { key: 'awp-atheris', emoji: '⚡', image: '/assets/skins/awp-atheris.png', name: 'AWP | Atheris', priceUsd: 22, rarity: 'uncommon', r: 'u' },
  { key: 'm4a4-spider-lily', emoji: '🌊', image: '/assets/skins/m4a4-spider-lily.png', name: 'M4A4 | Spider Lily', priceUsd: 45, rarity: 'rare', r: 'r' },
  { key: 'm4a1s-hyper-beast', emoji: '🔧', image: '/assets/skins/m4a1s-hyper-beast.png', name: 'M4A1-S | Hyper Beast', priceUsd: 85, rarity: 'rare', r: 'r' },
  { key: 'ak-fire-serpent', emoji: '🔥', image: '/assets/skins/ak-fire-serpent.png', name: 'AK-47 | Fire Serpent', priceUsd: 420, rarity: 'legendary', r: 'l' },
  { key: 'karambit-tiger-tooth', emoji: '🔪', image: '/assets/skins/karambit-tiger-tooth.png', name: 'Karambit | Tiger Tooth', priceUsd: 890, rarity: 'legendary', r: 'l' },
];

const rarityWeights = [48, 22, 14, 9, 4, 2, 0.7, 0.3];

if (!BOT_TOKEN) {
  console.warn('[warn] BOT_TOKEN is empty. Telegram auth/payments will fail until configured.');
}
if (process.env.VERCEL && !USE_KV && !USE_MONGO) {
  console.warn('[warn] Persistent storage is not configured. User data and balances can reset after deploy/restart.');
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

function createEmptyDb() {
  return { users: {}, steamLinkRequests: {}, orders: {} };
}

function sanitizeDb(db) {
  const base = db && typeof db === 'object' ? db : createEmptyDb();
  if (!base.users || typeof base.users !== 'object') base.users = {};
  if (!base.steamLinkRequests || typeof base.steamLinkRequests !== 'object') base.steamLinkRequests = {};
  if (!base.orders || typeof base.orders !== 'object') base.orders = {};
  return base;
}

let mongoClientPromise = null;

function resolveMongoDbName() {
  if (MONGODB_DB_NAME) return MONGODB_DB_NAME;
  try {
    const parsed = new URL(MONGODB_URI);
    const pathName = String(parsed.pathname || '').replace(/^\/+/, '');
    if (pathName) return decodeURIComponent(pathName);
  } catch {}
  return 'vault';
}

async function getMongoCollection() {
  if (!USE_MONGO) return null;
  if (!mongoClientPromise) {
    const client = new MongoClient(MONGODB_URI, {
      maxPoolSize: 10,
      minPoolSize: 0,
      serverSelectionTimeoutMS: 5000,
    });
    mongoClientPromise = client.connect();
  }
  const client = await mongoClientPromise;
  const db = client.db(resolveMongoDbName());
  return db.collection('vault_state');
}

function ensureDbFile() {
  const dir = path.dirname(DATA_PATH);
  fs.mkdirSync(dir, { recursive: true });
  if (!fs.existsSync(DATA_PATH)) {
    fs.writeFileSync(
      DATA_PATH,
      JSON.stringify(createEmptyDb(), null, 2),
      'utf8'
    );
  }
}

function readDbFromFile() {
  ensureDbFile();
  return sanitizeDb(JSON.parse(fs.readFileSync(DATA_PATH, 'utf8')));
}

function writeDbToFile(db) {
  fs.writeFileSync(DATA_PATH, JSON.stringify(db, null, 2), 'utf8');
}

async function kvCall(pathSuffix) {
  const trimmed = pathSuffix.replace(/^\/+/, '');
  const url = `${KV_REST_API_URL.replace(/\/+$/, '')}/${trimmed}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${KV_REST_API_TOKEN}`,
      'content-type': 'application/json',
    },
    body: '[]',
  });
  if (!resp.ok) throw new Error(`KV HTTP ${resp.status}`);
  return resp.json();
}

async function readDb() {
  if (USE_MONGO) {
    const col = await getMongoCollection();
    const doc = await col.findOne({ _id: DB_KEY });
    return sanitizeDb(doc?.data || createEmptyDb());
  }
  if (!USE_KV) return readDbFromFile();
  const getResp = await kvCall(`get/${encodeURIComponent(DB_KEY)}`);
  const raw = getResp?.result;
  if (!raw) return createEmptyDb();
  try {
    return sanitizeDb(JSON.parse(raw));
  } catch {
    return createEmptyDb();
  }
}

async function writeDb(db) {
  if (USE_MONGO) {
    const col = await getMongoCollection();
    const payload = sanitizeDb(db);
    await col.updateOne(
      { _id: DB_KEY },
      { $set: { data: payload, updatedAt: new Date() }, $setOnInsert: { createdAt: new Date() } },
      { upsert: true }
    );
    return;
  }
  if (!USE_KV) {
    writeDbToFile(db);
    return;
  }
  const payload = encodeURIComponent(JSON.stringify(sanitizeDb(db)));
  await kvCall(`set/${encodeURIComponent(DB_KEY)}/${payload}`);
}

async function persistDbOr503(res, db) {
  try {
    await writeDb(db);
    return true;
  } catch (e) {
    console.error('[db] write failed:', e);
    res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
    return false;
  }
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
    steamAvatarUrl: null,
    steamProfileName: null,
    inventory: [],
    openHistory: [],
    stats: {
      casesOpened: 0,
      totalWonUsd: 0,
      legendaryCount: 0,
      topDropUsd: 0,
      topDropName: null,
    },
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
}

function ensureUserShape(user) {
  if (!Array.isArray(user.inventory)) user.inventory = [];
  if (!Array.isArray(user.openHistory)) user.openHistory = [];
  if (!user.stats || typeof user.stats !== 'object') {
    user.stats = { casesOpened: 0, totalWonUsd: 0, legendaryCount: 0, topDropUsd: 0, topDropName: null };
  }
  user.stats.casesOpened = Number(user.stats.casesOpened || 0);
  user.stats.totalWonUsd = Number(user.stats.totalWonUsd || 0);
  user.stats.legendaryCount = Number(user.stats.legendaryCount || 0);
  user.stats.topDropUsd = Number(user.stats.topDropUsd || 0);
  user.stats.topDropName = user.stats.topDropName || null;
  user.balance = Number(user.balance || 0);
  user.steamAvatarUrl = user.steamAvatarUrl || null;
  user.steamProfileName = user.steamProfileName || null;

  if (!user.stats.topDropUsd && user.openHistory.length) {
    let best = null;
    for (const entry of user.openHistory) {
      const item = entry?.item || entry;
      if (!item) continue;
      if (!best || Number(item.priceUsd || 0) > Number(best.priceUsd || 0)) best = item;
    }
    if (best) {
      user.stats.topDropUsd = Number(best.priceUsd || 0);
      user.stats.topDropName = best.name || null;
    }
  }
}

async function ensureAuthed(req, res) {
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

  let db;
  try {
    db = await readDb();
    cleanup(db);
  } catch (e) {
    console.error('[db] read failed:', e);
    res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
    return null;
  }

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

  try {
    await writeDb(db);
  } catch (e) {
    console.error('[db] write failed:', e);
    res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
    return null;
  }
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

function getSkinByKey(key) {
  return skinPool.find((item) => item.key === key) || null;
}

function makeInventoryItem(template) {
  return {
    id: randomToken(10),
    key: template.key || null,
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
    steamAvatarUrl: user.steamAvatarUrl || null,
    steamProfileName: user.steamProfileName || null,
    steamProfileUrl: user.steamId ? `https://steamcommunity.com/profiles/${user.steamId}` : null,
    stats: user.stats,
  };
}

function updateTopDropStats(user, item) {
  const usd = Number(item?.priceUsd || 0);
  if (!usd) return;
  if (usd >= Number(user.stats.topDropUsd || 0)) {
    user.stats.topDropUsd = usd;
    user.stats.topDropName = item?.name || null;
  }
}

async function fetchSteamProfileMeta(steamId) {
  try {
    const url = `https://steamcommunity.com/profiles/${encodeURIComponent(steamId)}?xml=1`;
    const resp = await fetch(url, { headers: { 'user-agent': 'vault-miniapp/1.0' } });
    if (!resp.ok) return { avatarUrl: null, profileName: null };
    const raw = await resp.text();
    const avatar = raw.match(/<avatarFull><!\[CDATA\[(.*?)\]\]><\/avatarFull>/i)?.[1] || null;
    const name = raw.match(/<steamID><!\[CDATA\[(.*?)\]\]><\/steamID>/i)?.[1] || null;
    return { avatarUrl: avatar, profileName: name };
  } catch {
    return { avatarUrl: null, profileName: null };
  }
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
  const storage = USE_MONGO ? 'mongo' : (USE_KV ? 'kv' : 'file-ephemeral');
  res.json({ ok: true, now: nowIso(), storage });
});

app.post('/api/auth/telegram', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;
  res.json({ ok: true, user: presentUser(auth.user) });
});

app.get('/api/state', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const inv = [...auth.user.inventory]
    .sort((a, b) => new Date(b.acquiredAt).getTime() - new Date(a.acquiredAt).getTime());
  const openHistory = [...(auth.user.openHistory || [])]
    .sort((a, b) => new Date(b.openedAt || 0).getTime() - new Date(a.openedAt || 0).getTime())
    .slice(0, 50);

  res.json({
    ok: true,
    user: presentUser(auth.user),
    inventory: inv,
    openHistory,
    inventoryCount: inv.length,
    inventoryUsdValue: inv.reduce((s, x) => s + Number(x.priceUsd || 0), 0),
    casePrices,
  });
});

app.post('/api/cases/open', async (req, res) => {
  const auth = await ensureAuthed(req, res);
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
    auth.user.openHistory.unshift({
      id: randomToken(10),
      caseName,
      openedAt: nowIso(),
      item,
    });
    auth.user.stats.casesOpened += 1;
    auth.user.stats.totalWonUsd += Number(item.priceUsd || 0);
    if (item.rarity === 'legendary') auth.user.stats.legendaryCount += 1;
    updateTopDropStats(auth.user, item);
    winners.push(item);
  }
  auth.user.openHistory = auth.user.openHistory.slice(0, 100);

  auth.user.updatedAt = nowIso();
  if (!await persistDbOr503(res, auth.db)) return;

  res.json({
    ok: true,
    winners,
    winner: winners[0],
    balance: auth.user.balance,
    inventoryCount: auth.user.inventory.length,
    user: presentUser(auth.user),
  });
});

app.post('/api/inventory/sell', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const itemId = String(req.body?.itemId || '');
  if (!itemId) return res.status(400).json({ ok: false, error: 'itemId required' });

  const idx = auth.user.inventory.findIndex(x => x.id === itemId);
  if (idx === -1) return res.status(404).json({ ok: false, error: 'item not found' });

  const [item] = auth.user.inventory.splice(idx, 1);
  const stars = Number(item.sellStars || Math.round(Number(item.priceUsd || 0) * 10));
  auth.user.balance += stars;
  auth.user.updatedAt = nowIso();
  if (!await persistDbOr503(res, auth.db)) return;

  res.json({ ok: true, sold: item, stars, balance: auth.user.balance, inventoryCount: auth.user.inventory.length });
});

app.post('/api/inventory/sell-all', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const totalStars = auth.user.inventory.reduce(
    (s, item) => s + Number(item.sellStars || Math.round(Number(item.priceUsd || 0) * 10)),
    0
  );
  const count = auth.user.inventory.length;
  auth.user.inventory = [];
  auth.user.balance += totalStars;
  auth.user.updatedAt = nowIso();
  if (!await persistDbOr503(res, auth.db)) return;

  res.json({ ok: true, soldCount: count, totalStars, balance: auth.user.balance, inventoryCount: 0 });
});

app.post('/api/upgrades/try', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const sourceItemId = String(req.body?.sourceItemId || '');
  const targetKey = String(req.body?.targetKey || '');
  if (!sourceItemId || !targetKey) {
    return res.status(400).json({ ok: false, error: 'sourceItemId and targetKey are required' });
  }

  const sourceIndex = auth.user.inventory.findIndex((x) => x.id === sourceItemId);
  if (sourceIndex === -1) return res.status(404).json({ ok: false, error: 'source item not found' });

  const sourceItem = auth.user.inventory[sourceIndex];
  const targetTemplate = getSkinByKey(targetKey);
  if (!targetTemplate) return res.status(400).json({ ok: false, error: 'target skin not found' });

  const sourcePrice = Number(sourceItem.priceUsd || 0);
  const targetPrice = Number(targetTemplate.priceUsd || 0);
  if (targetPrice <= sourcePrice) {
    return res.status(400).json({ ok: false, error: 'target must be more expensive than source' });
  }

  const chance = Math.max(3, Math.min(95, Math.round((sourcePrice / targetPrice) * 100)));
  const roll = Math.random() * 100;
  const success = roll <= chance;

  auth.user.inventory.splice(sourceIndex, 1);
  let wonItem = null;
  if (success) {
    wonItem = makeInventoryItem(targetTemplate);
    auth.user.inventory.push(wonItem);
    auth.user.stats.totalWonUsd += Number(wonItem.priceUsd || 0);
    updateTopDropStats(auth.user, wonItem);
    auth.user.openHistory.unshift({
      id: randomToken(10),
      caseName: 'upgrade',
      openedAt: nowIso(),
      item: wonItem,
    });
    auth.user.openHistory = auth.user.openHistory.slice(0, 100);
  }

  auth.user.updatedAt = nowIso();
  if (!await persistDbOr503(res, auth.db)) return;

  return res.json({
    ok: true,
    success,
    chance,
    sourceItem,
    target: { ...targetTemplate, priceText: `$${targetTemplate.priceUsd}` },
    wonItem,
    inventoryCount: auth.user.inventory.length,
    user: presentUser(auth.user),
  });
});

app.post('/api/steam/link/start', async (req, res) => {
  const auth = await ensureAuthed(req, res);
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

  rp.verifyAssertion(req, async (err, result) => {
    if (err || !result?.authenticated) return res.status(401).send('Steam authentication failed');

    const steamId = extractSteamId(result.claimedIdentifier);
    if (!steamId) return res.status(400).send('Failed to parse Steam ID');
    try {
      const db = await readDb();
      cleanup(db);
      const userKey = String(parsedNonce.tgUserId);
      if (!db.users[userKey]) db.users[userKey] = makeDefaultUser({ id: parsedNonce.tgUserId });

      db.users[userKey].steamId = steamId;
      db.users[userKey].steamLinkedAt = nowIso();
      const steamMeta = await fetchSteamProfileMeta(steamId);
      db.users[userKey].steamAvatarUrl = steamMeta.avatarUrl || db.users[userKey].steamAvatarUrl || null;
      db.users[userKey].steamProfileName = steamMeta.profileName || db.users[userKey].steamProfileName || null;
      db.users[userKey].updatedAt = nowIso();
      if (!await persistDbOr503(res, db)) return;

      const botUsername = String(process.env.BOT_USERNAME || '').replace('@', '');
      res.send(`<!doctype html><html><head><meta charset="utf-8"><title>Steam linked</title><style>body{font-family:Arial,sans-serif;background:#0a0b0f;color:#f0f2f8;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:24px}.card{max-width:460px;background:#161922;border:1px solid #2a2f3d;border-radius:12px;padding:22px;text-align:center}a{color:#4e9eff}</style></head><body><div class="card"><h2>Steam аккаунт привязан</h2><p>SteamID: <b>${steamId}</b></p><p><a href="tg://resolve?domain=${encodeURIComponent(botUsername)}">Вернуться в Telegram</a></p><p>Можно просто закрыть это окно.</p></div></body></html>`);
    } catch (e) {
      console.error('[steam] return handler failed:', e);
      return res.status(500).send('Storage temporarily unavailable');
    }
  });
});

app.post('/api/payments/stars/create', async (req, res) => {
  try {
    const auth = await ensureAuthed(req, res);
    if (!auth) return;

    const amount = Number(req.body?.amount || 0);
    if (!Number.isInteger(amount) || amount < 1 || amount > 5000) {
      return res.status(400).json({ ok: false, error: 'amount must be integer 1..5000' });
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
    if (!await persistDbOr503(res, auth.db)) return;

    res.json({ ok: true, orderId, invoiceLink });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message || 'create invoice failed' });
  }
});

app.get('/api/payments/order/:orderId', async (req, res) => {
  const auth = await ensureAuthed(req, res);
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

        const db = await readDb();
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
          if (!await persistDbOr503(res, db)) return;
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
