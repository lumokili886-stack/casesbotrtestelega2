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
app.use(express.urlencoded({ extended: false }));

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
const ADMIN_PATH = (() => {
  const raw = String(process.env.ADMIN_PATH || '/vault-admin').trim();
  if (!raw || raw === '/') return '/vault-admin';
  return raw.startsWith('/') ? raw.replace(/\/+$/, '') : `/${raw.replace(/\/+$/, '')}`;
})();
const ADMIN_LOGIN = String(process.env.ADMIN_LOGIN || '').trim();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || '').trim();
const ADMIN_SESSION_SECRET = String(process.env.ADMIN_SESSION_SECRET || WEBHOOK_SECRET || BOT_TOKEN || 'vault-admin-session');
const ADMIN_SESSION_TTL_MS = Number(process.env.ADMIN_SESSION_TTL_HOURS || 12) * 60 * 60 * 1000;
const ADMIN_COOKIE = 'vault_admin_session';
const ADMIN_2FA_CHALLENGE_TTL_MS = 5 * 60 * 1000;
const ADMIN_2FA_ISSUER = String(process.env.ADMIN_2FA_ISSUER || 'VAULT Admin');
const ADMIN_AUTH_WINDOW_MS = Number(process.env.ADMIN_AUTH_WINDOW_MS || 10 * 60 * 1000);
const ADMIN_AUTH_BLOCK_MS = Number(process.env.ADMIN_AUTH_BLOCK_MS || 15 * 60 * 1000);
const ADMIN_AUTH_MAX_ATTEMPTS = Number(process.env.ADMIN_AUTH_MAX_ATTEMPTS || 6);
const adminAuthRateState = new Map();

const defaultCasePrices = {
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

function buildDefaultCaseConfig() {
  const out = {};
  for (const [name, price] of Object.entries(defaultCasePrices)) {
    out[name] = {
      price: Number(price || 0),
      enabled: true,
      updatedAt: null,
      updatedBy: null,
    };
  }
  if (out.free) out.free.price = 0;
  return out;
}

const defaultPaymentMethods = {
  telegram_stars: {
    title: 'Telegram Stars',
    enabled: true,
    minAmount: 1,
    maxAmount: 5000,
  },
};

function sanitizePaymentMethodConfig(raw, fallback = {}) {
  const fallbackTitle = String(fallback.title || 'Payment Method').trim() || 'Payment Method';
  const title = String(raw?.title || fallbackTitle).trim() || fallbackTitle;
  const enabled = Boolean(raw?.enabled !== false);
  let minAmount = Math.floor(Number(raw?.minAmount ?? fallback.minAmount ?? 1));
  let maxAmount = Math.floor(Number(raw?.maxAmount ?? fallback.maxAmount ?? 5000));
  if (!Number.isFinite(minAmount) || minAmount < 1) minAmount = 1;
  if (!Number.isFinite(maxAmount) || maxAmount < 1) maxAmount = Math.max(minAmount, 1);
  if (maxAmount < minAmount) maxAmount = minAmount;
  return {
    title,
    enabled,
    minAmount,
    maxAmount,
    updatedAt: raw?.updatedAt || null,
    updatedBy: raw?.updatedBy || null,
  };
}

function buildDefaultPaymentConfig() {
  const methods = {};
  for (const [methodKey, cfg] of Object.entries(defaultPaymentMethods)) {
    methods[methodKey] = sanitizePaymentMethodConfig(cfg, cfg);
  }
  return { methods };
}

function sanitizePromoCodeKey(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '')
    .replace(/[^A-Z0-9_-]/g, '')
    .slice(0, 32);
}

function buildDefaultPromoConfig() {
  return { codes: {} };
}

function buildDefaultCaseDraftStore() {
  return { cases: {}, updatedAt: null, updatedBy: null };
}

function buildDefaultConfigHistoryStore() {
  return { entries: [] };
}

function parsePromoExpiresAt(raw) {
  const d = new Date(raw || '');
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function normalizePromoConfig(db) {
  if (!db.promoConfig || typeof db.promoConfig !== 'object') db.promoConfig = buildDefaultPromoConfig();
  if (!db.promoConfig.codes || typeof db.promoConfig.codes !== 'object') db.promoConfig.codes = {};
  let changed = false;
  for (const [rawKey, rawCfg] of Object.entries(db.promoConfig.codes)) {
    const code = sanitizePromoCodeKey(rawCfg?.code || rawKey);
    const expiresAt = parsePromoExpiresAt(rawCfg?.expiresAt);
    if (!code || !expiresAt || !rawCfg || typeof rawCfg !== 'object') {
      delete db.promoConfig.codes[rawKey];
      changed = true;
      continue;
    }
    const next = {
      code,
      expiresAt,
      enabled: Boolean(rawCfg.enabled !== false),
      createdAt: rawCfg.createdAt || nowIso(),
      createdBy: rawCfg.createdBy || null,
      updatedAt: rawCfg.updatedAt || rawCfg.createdAt || nowIso(),
      updatedBy: rawCfg.updatedBy || rawCfg.createdBy || null,
    };
    if (rawKey !== code) {
      delete db.promoConfig.codes[rawKey];
      db.promoConfig.codes[code] = next;
      changed = true;
      continue;
    }
    if (JSON.stringify(rawCfg) !== JSON.stringify(next)) {
      db.promoConfig.codes[code] = next;
      changed = true;
    }
  }
  return changed;
}

function normalizeAllConfig(db) {
  let changed = false;
  changed = normalizeCaseConfig(db) || changed;
  changed = normalizeCaseDraftStore(db) || changed;
  changed = normalizePaymentConfig(db) || changed;
  changed = normalizePromoConfig(db) || changed;
  changed = normalizeConfigHistoryStore(db) || changed;
  changed = ensureAdminStore(db) || changed;
  return changed;
}

function normalizeCaseDraftStore(db) {
  if (!db.caseDrafts || typeof db.caseDrafts !== 'object') {
    db.caseDrafts = buildDefaultCaseDraftStore();
    return true;
  }
  let changed = false;
  if (!db.caseDrafts.cases || typeof db.caseDrafts.cases !== 'object') {
    db.caseDrafts.cases = {};
    changed = true;
  }
  for (const [caseName, draft] of Object.entries(db.caseDrafts.cases)) {
    if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName) || !draft || typeof draft !== 'object') {
      delete db.caseDrafts.cases[caseName];
      changed = true;
      continue;
    }
    const next = {
      caseName,
      price: Math.max(0, Math.floor(Number(draft.price || 0))),
      enabled: Boolean(draft.enabled !== false),
      dropTable: normalizeCaseDropTable(draft.dropTable).table,
      updatedAt: draft.updatedAt || nowIso(),
      updatedBy: draft.updatedBy || null,
    };
    if (caseName === 'free') next.price = 0;
    if (JSON.stringify(next) !== JSON.stringify(draft)) {
      db.caseDrafts.cases[caseName] = next;
      changed = true;
    }
  }
  if (!Object.prototype.hasOwnProperty.call(db.caseDrafts, 'updatedAt')) {
    db.caseDrafts.updatedAt = null;
    changed = true;
  }
  if (!Object.prototype.hasOwnProperty.call(db.caseDrafts, 'updatedBy')) {
    db.caseDrafts.updatedBy = null;
    changed = true;
  }
  return changed;
}

function normalizeConfigHistoryStore(db) {
  if (!db.configHistory || typeof db.configHistory !== 'object') {
    db.configHistory = buildDefaultConfigHistoryStore();
    return true;
  }
  if (!Array.isArray(db.configHistory.entries)) {
    db.configHistory.entries = [];
    return true;
  }
  const next = [];
  for (const entry of db.configHistory.entries) {
    if (!entry || typeof entry !== 'object') continue;
    if (!entry.id || !entry.entityType || !entry.entityKey || !entry.createdAt) continue;
    next.push({
      id: String(entry.id),
      entityType: String(entry.entityType),
      entityKey: String(entry.entityKey),
      action: String(entry.action || 'update'),
      before: entry.before ?? null,
      after: entry.after ?? null,
      createdAt: String(entry.createdAt),
      createdBy: String(entry.createdBy || 'admin'),
      rollbackOf: entry.rollbackOf ? String(entry.rollbackOf) : null,
    });
  }
  const changed = JSON.stringify(next) !== JSON.stringify(db.configHistory.entries);
  db.configHistory.entries = next.slice(-1500);
  return changed;
}

function getActivePromo(db) {
  const nowMs = Date.now();
  const active = Object.values(db?.promoConfig?.codes || {})
    .filter((x) => x?.enabled !== false && x?.expiresAt && new Date(x.expiresAt).getTime() > nowMs)
    .sort((a, b) => new Date(a.expiresAt).getTime() - new Date(b.expiresAt).getTime());
  if (!active.length) return null;
  return {
    code: active[0].code,
    expiresAt: active[0].expiresAt,
  };
}

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

function isAdminConfigured() {
  return Boolean(ADMIN_LOGIN && ADMIN_PASSWORD);
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a || ''));
  const right = Buffer.from(String(b || ''));
  if (left.length !== right.length) return false;
  return crypto.timingSafeEqual(left, right);
}

function parseCookies(req) {
  const raw = String(req.get('cookie') || '');
  const out = {};
  if (!raw) return out;
  for (const part of raw.split(';')) {
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    out[key] = decodeURIComponent(value || '');
  }
  return out;
}

function signAdminPayload(payloadB64) {
  return crypto.createHmac('sha256', ADMIN_SESSION_SECRET).update(payloadB64).digest('base64url');
}

function makeAdminSessionToken(login, role) {
  const payload = {
    login: String(login || ''),
    role: String(role || 'admin'),
    exp: Date.now() + ADMIN_SESSION_TTL_MS,
    nonce: randomToken(8),
  };
  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  const sig = signAdminPayload(payloadB64);
  return `${payloadB64}.${sig}`;
}

function parseAdminSession(token) {
  const raw = String(token || '');
  const dot = raw.lastIndexOf('.');
  if (dot <= 0) return { ok: false, error: 'bad format' };
  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expectedSig = signAdminPayload(payloadB64);
  if (!safeEqual(sig, expectedSig)) return { ok: false, error: 'bad signature' };
  let payload = null;
  try {
    payload = JSON.parse(b64urlDecode(payloadB64));
  } catch {
    return { ok: false, error: 'bad payload' };
  }
  if (!payload?.exp || Date.now() > Number(payload.exp)) return { ok: false, error: 'expired' };
  if (!payload?.login) return { ok: false, error: 'bad login' };
  return { ok: true, login: String(payload.login), role: String(payload.role || 'admin') };
}

function buildAdminCookie(token, req) {
  const secure = Boolean(process.env.VERCEL || String(req.protocol || '').toLowerCase() === 'https' || String(req.get('x-forwarded-proto') || '').includes('https'));
  const parts = [
    `${ADMIN_COOKIE}=${encodeURIComponent(token)}`,
    'Path=/',
    `Max-Age=${Math.max(60, Math.floor(ADMIN_SESSION_TTL_MS / 1000))}`,
    'HttpOnly',
    'SameSite=Lax',
  ];
  if (secure) parts.push('Secure');
  return parts.join('; ');
}

function buildAdminCookieClear(req) {
  const secure = Boolean(process.env.VERCEL || String(req.protocol || '').toLowerCase() === 'https' || String(req.get('x-forwarded-proto') || '').includes('https'));
  const parts = [
    `${ADMIN_COOKIE}=`,
    'Path=/',
    'Max-Age=0',
    'HttpOnly',
    'SameSite=Lax',
  ];
  if (secure) parts.push('Secure');
  return parts.join('; ');
}

function hashAdminPassword(password, salt = randomToken(12)) {
  const pwd = String(password || '');
  const digest = crypto.pbkdf2Sync(pwd, `${salt}:${ADMIN_SESSION_SECRET}`, 120000, 32, 'sha256').toString('base64url');
  return { salt, hash: digest };
}

function verifyAdminPassword(password, account) {
  if (!account?.passwordSalt || !account?.passwordHash) return false;
  const probe = hashAdminPassword(password, account.passwordSalt);
  return safeEqual(probe.hash, account.passwordHash);
}

function getAdminIp(req) {
  const xf = String(req.get('x-forwarded-for') || '').split(',')[0].trim();
  return xf || String(req.ip || req.socket?.remoteAddress || '');
}

function getAdminAuthRateKey(login, req) {
  return `${String(login || '').toLowerCase()}|${getAdminIp(req)}`;
}

function pruneAdminAuthRateState(nowMs = Date.now()) {
  for (const [key, state] of adminAuthRateState.entries()) {
    if (!state || typeof state !== 'object') {
      adminAuthRateState.delete(key);
      continue;
    }
    const blockedUntil = Number(state.blockedUntil || 0);
    const attempts = Array.isArray(state.attempts) ? state.attempts : [];
    const active = attempts.filter((x) => nowMs - Number(x || 0) <= ADMIN_AUTH_WINDOW_MS);
    if (active.length > 0 || blockedUntil > nowMs) {
      adminAuthRateState.set(key, { attempts: active, blockedUntil });
      continue;
    }
    adminAuthRateState.delete(key);
  }
}

function getAdminAuthRateStatus(login, req) {
  const nowMs = Date.now();
  pruneAdminAuthRateState(nowMs);
  const state = adminAuthRateState.get(getAdminAuthRateKey(login, req));
  const blockedUntil = Number(state?.blockedUntil || 0);
  if (blockedUntil > nowMs) {
    return { blocked: true, retryAfterSec: Math.max(1, Math.ceil((blockedUntil - nowMs) / 1000)) };
  }
  return { blocked: false, retryAfterSec: 0 };
}

function recordAdminAuthFailure(login, req) {
  const nowMs = Date.now();
  pruneAdminAuthRateState(nowMs);
  const key = getAdminAuthRateKey(login, req);
  const curr = adminAuthRateState.get(key) || { attempts: [], blockedUntil: 0 };
  const attempts = Array.isArray(curr.attempts)
    ? curr.attempts.filter((x) => nowMs - Number(x || 0) <= ADMIN_AUTH_WINDOW_MS)
    : [];
  attempts.push(nowMs);
  const shouldBlock = attempts.length >= Math.max(1, ADMIN_AUTH_MAX_ATTEMPTS);
  const blockedUntil = shouldBlock ? nowMs + Math.max(5000, ADMIN_AUTH_BLOCK_MS) : Number(curr.blockedUntil || 0);
  adminAuthRateState.set(key, { attempts, blockedUntil });
  return {
    blocked: blockedUntil > nowMs,
    retryAfterSec: blockedUntil > nowMs ? Math.max(1, Math.ceil((blockedUntil - nowMs) / 1000)) : 0,
    attempts: attempts.length,
  };
}

function clearAdminAuthFailures(login, req) {
  adminAuthRateState.delete(getAdminAuthRateKey(login, req));
}

function getAdmin2faKey() {
  return crypto.createHash('sha256').update(`${ADMIN_SESSION_SECRET}:2fa`).digest();
}

function encryptSecret(plain) {
  const iv = crypto.randomBytes(12);
  const key = getAdmin2faKey();
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const enc = Buffer.concat([cipher.update(String(plain || ''), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64url')}.${tag.toString('base64url')}.${enc.toString('base64url')}`;
}

function decryptSecret(payload) {
  const raw = String(payload || '');
  const parts = raw.split('.');
  if (parts.length !== 3) return '';
  try {
    const [ivB64, tagB64, encB64] = parts;
    const iv = Buffer.from(ivB64, 'base64url');
    const tag = Buffer.from(tagB64, 'base64url');
    const enc = Buffer.from(encB64, 'base64url');
    const key = getAdmin2faKey();
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
    decipher.setAuthTag(tag);
    const dec = Buffer.concat([decipher.update(enc), decipher.final()]);
    return dec.toString('utf8');
  } catch {
    return '';
  }
}

function base32Encode(buf) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const bytes = Buffer.isBuffer(buf) ? buf : Buffer.from(buf || '');
  let out = '';
  let bits = 0;
  let value = 0;
  for (const b of bytes) {
    value = (value << 8) | b;
    bits += 8;
    while (bits >= 5) {
      out += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) out += alphabet[(value << (5 - bits)) & 31];
  return out;
}

function base32Decode(raw) {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
  const clean = String(raw || '').toUpperCase().replace(/[^A-Z2-7]/g, '');
  let bits = 0;
  let value = 0;
  const out = [];
  for (const ch of clean) {
    const idx = alphabet.indexOf(ch);
    if (idx < 0) continue;
    value = (value << 5) | idx;
    bits += 5;
    if (bits >= 8) {
      out.push((value >>> (bits - 8)) & 255);
      bits -= 8;
    }
  }
  return Buffer.from(out);
}

function generateTotpSecret() {
  return base32Encode(crypto.randomBytes(20));
}

function generateTotpCode(secret, atMs = Date.now(), periodSec = 30) {
  const key = base32Decode(secret);
  if (!key.length) return '';
  const counter = Math.floor(atMs / 1000 / periodSec);
  const msg = Buffer.alloc(8);
  msg.writeUInt32BE(Math.floor(counter / 0x100000000), 0);
  msg.writeUInt32BE(counter >>> 0, 4);
  const hmac = crypto.createHmac('sha1', key).update(msg).digest();
  const off = hmac[hmac.length - 1] & 0x0f;
  const bin = ((hmac[off] & 0x7f) << 24) | ((hmac[off + 1] & 0xff) << 16) | ((hmac[off + 2] & 0xff) << 8) | (hmac[off + 3] & 0xff);
  return String(bin % 1000000).padStart(6, '0');
}

function verifyTotpCode(secret, codeRaw, windowSteps = 1) {
  const code = String(codeRaw || '').replace(/\D/g, '');
  if (code.length !== 6) return false;
  const now = Date.now();
  for (let step = -windowSteps; step <= windowSteps; step += 1) {
    const probe = generateTotpCode(secret, now + step * 30000);
    if (probe && safeEqual(probe, code)) return true;
  }
  return false;
}

function makeOtpAuthUri(login, secret) {
  const label = encodeURIComponent(`${ADMIN_2FA_ISSUER}:${login}`);
  const issuer = encodeURIComponent(ADMIN_2FA_ISSUER);
  return `otpauth://totp/${label}?secret=${encodeURIComponent(secret)}&issuer=${issuer}&algorithm=SHA1&digits=6&period=30`;
}

function signAdmin2faChallenge(payloadB64) {
  return crypto.createHmac('sha256', `${ADMIN_SESSION_SECRET}:2fa:challenge`).update(payloadB64).digest('base64url');
}

function makeAdmin2faChallenge(login, req) {
  const payload = {
    login: String(login || ''),
    ip: getAdminIp(req),
    ua: String(req.get('user-agent') || '').slice(0, 256),
    exp: Date.now() + ADMIN_2FA_CHALLENGE_TTL_MS,
    nonce: randomToken(10),
  };
  const payloadB64 = b64urlEncode(JSON.stringify(payload));
  return `${payloadB64}.${signAdmin2faChallenge(payloadB64)}`;
}

function parseAdmin2faChallenge(token, req) {
  const raw = String(token || '');
  const dot = raw.lastIndexOf('.');
  if (dot <= 0) return { ok: false, error: 'bad challenge format' };
  const payloadB64 = raw.slice(0, dot);
  const sig = raw.slice(dot + 1);
  const expected = signAdmin2faChallenge(payloadB64);
  if (!safeEqual(sig, expected)) return { ok: false, error: 'bad challenge signature' };
  let payload = null;
  try {
    payload = JSON.parse(b64urlDecode(payloadB64));
  } catch {
    return { ok: false, error: 'bad challenge payload' };
  }
  if (!payload?.login || !payload?.exp) return { ok: false, error: 'bad challenge data' };
  if (Date.now() > Number(payload.exp)) return { ok: false, error: 'challenge expired' };
  if (payload.ip && payload.ip !== getAdminIp(req)) return { ok: false, error: 'challenge ip mismatch' };
  return { ok: true, login: String(payload.login) };
}

function normalizeCaseConfig(db) {
  if (!db.caseConfig || typeof db.caseConfig !== 'object') db.caseConfig = {};
  let changed = false;
  for (const [name, defaultPrice] of Object.entries(defaultCasePrices)) {
    if (!db.caseConfig[name] || typeof db.caseConfig[name] !== 'object') {
      db.caseConfig[name] = { price: Number(defaultPrice || 0), enabled: true, updatedAt: null, updatedBy: null };
      changed = true;
      continue;
    }
    const cfg = db.caseConfig[name];
    const priceNum = Math.max(0, Math.floor(Number(cfg.price ?? defaultPrice)));
    const enabled = Boolean(cfg.enabled !== false);
    if (!Number.isFinite(priceNum) || cfg.price !== priceNum) {
      cfg.price = Number.isFinite(priceNum) ? priceNum : Number(defaultPrice || 0);
      changed = true;
    }
    if (name === 'free' && cfg.price !== 0) {
      cfg.price = 0;
      changed = true;
    }
    if (cfg.enabled !== enabled) {
      cfg.enabled = enabled;
      changed = true;
    }
    const nextDropTable = normalizeCaseDropTable(cfg.dropTable).table;
    const prevDropTableRaw = cfg.dropTable && typeof cfg.dropTable === 'object' ? cfg.dropTable : null;
    const prevNorm = normalizeCaseDropTable(prevDropTableRaw).table;
    if (JSON.stringify(prevNorm) !== JSON.stringify(nextDropTable)) {
      cfg.dropTable = nextDropTable;
      changed = true;
    } else if (!Object.prototype.hasOwnProperty.call(cfg, 'dropTable')) {
      cfg.dropTable = nextDropTable;
      changed = true;
    }
    if (!Object.prototype.hasOwnProperty.call(cfg, 'updatedAt')) cfg.updatedAt = null;
    if (!Object.prototype.hasOwnProperty.call(cfg, 'updatedBy')) cfg.updatedBy = null;
  }
  return changed;
}

function normalizeCaseDropTable(raw) {
  if (!raw || typeof raw !== 'object') return { table: null, totalWeight: 0 };
  const table = {};
  let totalWeight = 0;
  for (const skin of skinPool) {
    const weight = Number(raw?.[skin.key] ?? 0);
    if (!Number.isFinite(weight) || weight <= 0) continue;
    const normalizedWeight = Math.round(weight * 1000) / 1000;
    if (normalizedWeight <= 0) continue;
    table[skin.key] = normalizedWeight;
    totalWeight += normalizedWeight;
  }
  if (totalWeight <= 0) return { table: null, totalWeight: 0 };
  return { table, totalWeight };
}

function getCaseDropTableRows(caseCfg = null) {
  const normCustom = normalizeCaseDropTable(caseCfg?.dropTable);
  const customTable = normCustom.table;
  const useCustom = Boolean(customTable);
  const fallbackTable = {};
  let fallbackTotal = 0;
  for (let i = 0; i < skinPool.length; i++) {
    const skin = skinPool[i];
    const weight = Math.max(0, Number(rarityWeights[i] || 0));
    if (weight <= 0) continue;
    fallbackTable[skin.key] = weight;
    fallbackTotal += weight;
  }
  const activeTable = useCustom ? customTable : fallbackTable;
  const activeTotal = useCustom ? normCustom.totalWeight : fallbackTotal;
  const rows = skinPool.map((skin, idx) => {
    const rawWeight = Number(activeTable?.[skin.key] || 0);
    const chance = activeTotal > 0 ? Number(((rawWeight / activeTotal) * 100).toFixed(2)) : 0;
    return {
      key: skin.key,
      name: skin.name,
      rarity: skin.rarity,
      priceUsd: Number(skin.priceUsd || 0),
      fallbackWeight: Number(rarityWeights[idx] || 0),
      weight: rawWeight,
      chance,
      enabled: rawWeight > 0,
    };
  });
  return { useCustom, rows, totalWeight: activeTotal };
}

function normalizePaymentConfig(db) {
  if (!db.paymentConfig || typeof db.paymentConfig !== 'object') db.paymentConfig = { methods: {} };
  if (!db.paymentConfig.methods || typeof db.paymentConfig.methods !== 'object') db.paymentConfig.methods = {};
  let changed = false;

  for (const [methodKey, fallback] of Object.entries(defaultPaymentMethods)) {
    if (!db.paymentConfig.methods[methodKey] || typeof db.paymentConfig.methods[methodKey] !== 'object') {
      db.paymentConfig.methods[methodKey] = sanitizePaymentMethodConfig({}, fallback);
      changed = true;
      continue;
    }
    const prev = db.paymentConfig.methods[methodKey];
    const next = sanitizePaymentMethodConfig(prev, fallback);
    if (JSON.stringify(prev) !== JSON.stringify(next)) {
      db.paymentConfig.methods[methodKey] = next;
      changed = true;
    }
  }

  for (const [methodKey, rawCfg] of Object.entries(db.paymentConfig.methods)) {
    if (!rawCfg || typeof rawCfg !== 'object') {
      delete db.paymentConfig.methods[methodKey];
      changed = true;
      continue;
    }
    const fallback = defaultPaymentMethods[methodKey] || { title: methodKey.replace(/_/g, ' ') || 'Payment Method', minAmount: 1, maxAmount: 5000 };
    const next = sanitizePaymentMethodConfig(rawCfg, fallback);
    if (JSON.stringify(rawCfg) !== JSON.stringify(next)) {
      db.paymentConfig.methods[methodKey] = next;
      changed = true;
    }
  }
  return changed;
}

function ensureAdminStore(db) {
  if (!db.admins || typeof db.admins !== 'object') db.admins = {};
  let changed = false;
  if (isAdminConfigured() && !db.admins[ADMIN_LOGIN]) {
    const hp = hashAdminPassword(ADMIN_PASSWORD);
    db.admins[ADMIN_LOGIN] = {
      login: ADMIN_LOGIN,
      role: 'owner',
      passwordSalt: hp.salt,
      passwordHash: hp.hash,
      createdAt: nowIso(),
      updatedAt: nowIso(),
      createdBy: 'bootstrap',
    };
    changed = true;
  }
  for (const [login, account] of Object.entries(db.admins)) {
    if (!account || typeof account !== 'object') {
      delete db.admins[login];
      changed = true;
      continue;
    }
    if (!account.login) {
      account.login = login;
      changed = true;
    }
    if (!account.role || !['owner', 'admin'].includes(account.role)) {
      account.role = login === ADMIN_LOGIN ? 'owner' : 'admin';
      changed = true;
    }
    if (!account.createdAt) {
      account.createdAt = nowIso();
      changed = true;
    }
    if (!account.updatedAt) {
      account.updatedAt = nowIso();
      changed = true;
    }
    if (!Object.prototype.hasOwnProperty.call(account, 'twoFactorEnabled')) {
      account.twoFactorEnabled = false;
      changed = true;
    }
    if (!Object.prototype.hasOwnProperty.call(account, 'twoFactorSecretEncrypted')) {
      account.twoFactorSecretEncrypted = null;
      changed = true;
    }
    if (!Object.prototype.hasOwnProperty.call(account, 'twoFactorEnabledAt')) {
      account.twoFactorEnabledAt = null;
      changed = true;
    }
    if (!Object.prototype.hasOwnProperty.call(account, 'twoFactorPending')) {
      account.twoFactorPending = null;
      changed = true;
    }
  }
  return changed;
}

function getCasePriceMap(db) {
  const out = {};
  for (const [name, cfg] of Object.entries(db.caseConfig || {})) out[name] = Number(cfg?.price || 0);
  return out;
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
  return {
    users: {},
    steamLinkRequests: {},
    orders: {},
    adminAudit: [],
    admins: {},
    caseConfig: buildDefaultCaseConfig(),
    caseDrafts: buildDefaultCaseDraftStore(),
    paymentConfig: buildDefaultPaymentConfig(),
    promoConfig: buildDefaultPromoConfig(),
    configHistory: buildDefaultConfigHistoryStore(),
  };
}

function sanitizeDb(db) {
  const base = db && typeof db === 'object' ? db : createEmptyDb();
  if (!base.users || typeof base.users !== 'object') base.users = {};
  if (!base.steamLinkRequests || typeof base.steamLinkRequests !== 'object') base.steamLinkRequests = {};
  if (!base.orders || typeof base.orders !== 'object') base.orders = {};
  if (!Array.isArray(base.adminAudit)) base.adminAudit = [];
  if (!base.admins || typeof base.admins !== 'object') base.admins = {};
  if (!base.caseConfig || typeof base.caseConfig !== 'object') base.caseConfig = buildDefaultCaseConfig();
  if (!base.caseDrafts || typeof base.caseDrafts !== 'object') base.caseDrafts = buildDefaultCaseDraftStore();
  if (!base.paymentConfig || typeof base.paymentConfig !== 'object') base.paymentConfig = buildDefaultPaymentConfig();
  if (!base.promoConfig || typeof base.promoConfig !== 'object') base.promoConfig = buildDefaultPromoConfig();
  if (!base.configHistory || typeof base.configHistory !== 'object') base.configHistory = buildDefaultConfigHistoryStore();
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

  if (Array.isArray(db.adminAudit) && db.adminAudit.length > 2000) {
    db.adminAudit = db.adminAudit.slice(-2000);
  }
  if (Array.isArray(db.configHistory?.entries) && db.configHistory.entries.length > 1500) {
    db.configHistory.entries = db.configHistory.entries.slice(-1500);
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
    settings: {
      language: 'ru',
    },
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
  if (!user.settings || typeof user.settings !== 'object') user.settings = { language: 'ru' };
  const allowedLanguages = new Set(['ru', 'uz', 'kk', 'en']);
  if (!allowedLanguages.has(String(user.settings.language || 'ru'))) user.settings.language = 'ru';

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
    normalizeAllConfig(db);
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

function pickSkinByCaseConfig(caseCfg) {
  const custom = normalizeCaseDropTable(caseCfg?.dropTable);
  if (!custom.table || custom.totalWeight <= 0) return weightedRandomSkin();
  let r = Math.random() * custom.totalWeight;
  for (const skin of skinPool) {
    const weight = Number(custom.table?.[skin.key] || 0);
    if (weight <= 0) continue;
    r -= weight;
    if (r <= 0) return skin;
  }
  for (const skin of skinPool) {
    if (Number(custom.table?.[skin.key] || 0) > 0) return skin;
  }
  return weightedRandomSkin();
}

function runCaseSimulation(caseCfg, spins = 5000) {
  const totalSpins = Math.max(100, Math.min(50000, Math.floor(Number(spins || 0))));
  const rarityDist = { common: 0, uncommon: 0, rare: 0, legendary: 0 };
  const topDrops = {};
  let totalUsd = 0;
  for (let i = 0; i < totalSpins; i++) {
    const skin = pickSkinByCaseConfig(caseCfg);
    const usd = Number(skin?.priceUsd || 0);
    totalUsd += usd;
    const rarity = String(skin?.rarity || 'common');
    rarityDist[rarity] = Number(rarityDist[rarity] || 0) + 1;
    const key = String(skin?.key || 'unknown');
    if (!topDrops[key]) {
      topDrops[key] = { key, name: skin?.name || key, rarity, priceUsd: usd, count: 0 };
    }
    topDrops[key].count += 1;
  }
  const avgUsd = totalSpins > 0 ? totalUsd / totalSpins : 0;
  const casePrice = Math.max(0, Number(caseCfg?.price || 0));
  const priceUsdEquivalent = casePrice / 10;
  const rtpPct = priceUsdEquivalent > 0 ? (avgUsd / priceUsdEquivalent) * 100 : 0;
  const top = Object.values(topDrops)
    .sort((a, b) => b.count - a.count)
    .slice(0, 6)
    .map((x) => ({
      key: x.key,
      name: x.name,
      rarity: x.rarity,
      priceUsd: x.priceUsd,
      count: x.count,
      chancePct: Number(((x.count / totalSpins) * 100).toFixed(2)),
    }));
  return {
    spins: totalSpins,
    averageUsd: Number(avgUsd.toFixed(3)),
    averageStars: Number((avgUsd * 10).toFixed(2)),
    priceStars: casePrice,
    priceUsdEquivalent: Number(priceUsdEquivalent.toFixed(3)),
    rtpPct: Number(rtpPct.toFixed(2)),
    rarityDist: {
      commonPct: Number(((rarityDist.common / totalSpins) * 100).toFixed(2)),
      uncommonPct: Number(((rarityDist.uncommon / totalSpins) * 100).toFixed(2)),
      rarePct: Number(((rarityDist.rare / totalSpins) * 100).toFixed(2)),
      legendaryPct: Number(((rarityDist.legendary / totalSpins) * 100).toFixed(2)),
    },
    topDrops: top,
  };
}

const CASE_ECON_PRESETS = {
  safe: {
    key: 'safe',
    title: 'Safe',
    description: 'Ниже риск для экономики, выше доля бюджетных дропов',
    alpha: 1.35,
    rarityMult: { common: 1.4, uncommon: 1.0, rare: 0.62, legendary: 0.28 },
  },
  balanced: {
    key: 'balanced',
    title: 'Balanced',
    description: 'Сбалансированный профиль, нейтральная выдача',
    alpha: 1.0,
    rarityMult: { common: 1.0, uncommon: 1.0, rare: 1.0, legendary: 1.0 },
  },
  aggressive: {
    key: 'aggressive',
    title: 'Aggressive',
    description: 'Более “щедрый” профиль, выше шанс дорогих дропов',
    alpha: 0.72,
    rarityMult: { common: 0.78, uncommon: 1.08, rare: 1.48, legendary: 2.15 },
  },
  high_rtp: {
    key: 'high_rtp',
    title: 'High-RTP',
    description: 'A/B preset: повышенный RTP, заметно больше дорогих дропов',
    alpha: 0.58,
    rarityMult: { common: 0.62, uncommon: 1.18, rare: 1.9, legendary: 3.1 },
  },
  low_rtp: {
    key: 'low_rtp',
    title: 'Low-RTP',
    description: 'A/B preset: пониженный RTP, больше бюджетных дропов',
    alpha: 1.62,
    rarityMult: { common: 1.78, uncommon: 0.94, rare: 0.46, legendary: 0.18 },
  },
};

function buildPresetDropTable(presetKey, casePriceStars) {
  const preset = CASE_ECON_PRESETS[presetKey] || CASE_ECON_PRESETS.balanced;
  const caseUsd = Math.max(1, Number(casePriceStars || 0) / 10);
  const table = {};
  for (const skin of skinPool) {
    const rarity = String(skin?.rarity || 'common');
    const rarityMul = Number(preset.rarityMult?.[rarity] || 1);
    const price = Math.max(0.5, Number(skin.priceUsd || 0));
    const ratio = (caseUsd + 2) / (price + 2);
    const priceFactor = Math.pow(Math.max(0.05, ratio), Number(preset.alpha || 1));
    const weight = Math.max(0.001, rarityMul * priceFactor * 100);
    table[skin.key] = Math.round(weight * 1000) / 1000;
  }
  return normalizeCaseDropTable(table).table;
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
    settings: user.settings || { language: 'ru' },
  };
}

function userDisplayName(user) {
  const full = `${user?.firstName || ''} ${user?.lastName || ''}`.trim();
  if (full) return full;
  if (user?.username) return `@${user.username}`;
  return `ID ${user?.tgUserId || '?'}`;
}

function buildAdminActivity(db, limit = 200) {
  const rows = [];
  for (const [userKey, user] of Object.entries(db.users || {})) {
    const history = Array.isArray(user.openHistory) ? user.openHistory : [];
    for (const entry of history) {
      const item = entry?.item || {};
      const ts = entry?.openedAt || user.updatedAt || user.createdAt || nowIso();
      const type = entry.caseName === 'upgrade' ? 'upgrade' : 'case_open';
      rows.push({
        ts,
        type,
        tgUserId: Number(user.tgUserId || userKey),
        userLabel: userDisplayName(user),
        amount: Number(item.priceUsd || 0),
        details: `${entry.caseName || 'case'} · ${item.name || 'item'}`,
      });
    }
  }
  for (const order of Object.values(db.orders || {})) {
    if (!order) continue;
    const ts = order.updatedAt || order.createdAt || nowIso();
    rows.push({
      ts,
      type: `payment_${order.status || 'unknown'}`,
      tgUserId: Number(order.tgUserId || 0),
      userLabel: `ID ${order.tgUserId || '?'}`,
      amount: Number(order.amount || 0),
      details: `order ${order.id || ''}`,
    });
  }
  for (const event of db.adminAudit || []) {
    if (!event) continue;
    rows.push({
      ts: event.ts || nowIso(),
      type: `admin_${event.action || 'action'}`,
      tgUserId: Number(event.tgUserId || 0),
      userLabel: `ID ${event.tgUserId || '?'}`,
      amount: Number(event.delta || 0),
      details: `${event.adminLogin || 'admin'} · ${event.reason || ''}`.trim(),
    });
  }
  rows.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());
  return rows.slice(0, Math.max(1, Math.min(5000, Number(limit || 200))));
}

function appendAdminAudit(db, payload = {}) {
  if (!db || typeof db !== 'object') return;
  if (!Array.isArray(db.adminAudit)) db.adminAudit = [];
  db.adminAudit.push({
    id: randomToken(8),
    ts: nowIso(),
    action: String(payload.action || 'event'),
    tgUserId: Number(payload.tgUserId || 0),
    delta: Number(payload.delta || 0),
    reason: String(payload.reason || ''),
    adminLogin: String(payload.adminLogin || 'admin'),
  });
  if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
}

function cloneJson(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value));
}

function appendConfigHistory(db, payload = {}) {
  if (!db || typeof db !== 'object') return null;
  if (!db.configHistory || typeof db.configHistory !== 'object') db.configHistory = buildDefaultConfigHistoryStore();
  if (!Array.isArray(db.configHistory.entries)) db.configHistory.entries = [];
  const entry = {
    id: randomToken(8),
    entityType: String(payload.entityType || 'unknown'),
    entityKey: String(payload.entityKey || 'unknown'),
    action: String(payload.action || 'update'),
    before: cloneJson(payload.before ?? null),
    after: cloneJson(payload.after ?? null),
    createdAt: nowIso(),
    createdBy: String(payload.createdBy || 'admin'),
    rollbackOf: payload.rollbackOf ? String(payload.rollbackOf) : null,
  };
  db.configHistory.entries.push(entry);
  if (db.configHistory.entries.length > 1500) db.configHistory.entries = db.configHistory.entries.slice(-1500);
  return entry;
}

function getOrderStatus(order) {
  if (!order) return 'unknown';
  const status = String(order.status || 'unknown');
  if (status === 'pending' && order.expiresAt && Date.now() > new Date(order.expiresAt).getTime()) return 'expired';
  return status;
}

function buildFunnelAnalytics(db) {
  const users = db.users || {};
  const paidOrdersByUser = new Map();
  for (const order of Object.values(db.orders || {})) {
    if (!order || order.status !== 'paid') continue;
    const uid = String(order.tgUserId || '');
    if (!uid) continue;
    paidOrdersByUser.set(uid, (paidOrdersByUser.get(uid) || 0) + 1);
  }

  const stage1 = new Set();
  const stage2 = new Set();
  const stage3 = new Set();
  const stage4 = new Set();

  for (const [userKey, user] of Object.entries(users)) {
    const uid = String(user?.tgUserId || userKey || '');
    if (!uid) continue;
    stage1.add(uid);
    const paidCount = Number(paidOrdersByUser.get(uid) || 0);
    if (paidCount >= 1) stage2.add(uid);
    const openedCases = Number(user?.stats?.casesOpened || 0) > 0;
    if (openedCases && stage2.has(uid)) stage3.add(uid);
    if (paidCount >= 2 && stage3.has(uid)) stage4.add(uid);
  }

  const steps = [
    { key: 'started', label: 'Старт Mini App', users: stage1.size },
    { key: 'deposited', label: 'Сделали депозит', users: stage2.size },
    { key: 'opened_case', label: 'Открыли кейс', users: stage3.size },
    { key: 'repeat_deposit', label: 'Повторный депозит', users: stage4.size },
  ];
  const startBase = Math.max(1, steps[0].users);
  for (let i = 0; i < steps.length; i++) {
    const prevBase = i === 0 ? steps[0].users : steps[i - 1].users;
    const fromPrev = prevBase > 0 ? (steps[i].users / prevBase) * 100 : 0;
    const fromStart = (steps[i].users / startBase) * 100;
    steps[i].fromPrevPct = Number(fromPrev.toFixed(1));
    steps[i].fromStartPct = Number(fromStart.toFixed(1));
  }

  return {
    steps,
    totals: {
      started: steps[0].users,
      deposited: steps[1].users,
      openedCase: steps[2].users,
      repeatDeposit: steps[3].users,
    },
  };
}

function matchesActivityKind(row, kind) {
  const k = String(kind || '').trim().toLowerCase();
  if (!k || k === 'all') return true;
  const type = String(row?.type || '').toLowerCase();
  if (!type) return false;
  const groups = {
    deposits: ['payment_paid'],
    charges: ['admin_balance_adjust'],
    case_open: ['case_open'],
    upgrades: ['upgrade'],
    payments: ['payment_'],
    admin: ['admin_'],
    security: ['admin_login_', 'admin_2fa_', 'admin_logout'],
  };
  const exact = groups[k] || null;
  if (!exact) return type.includes(k);
  return exact.some((prefix) => type.startsWith(prefix) || type === prefix);
}

function presentAdminUser(user, key) {
  const inv = Array.isArray(user.inventory) ? user.inventory : [];
  const openHistory = Array.isArray(user.openHistory) ? user.openHistory : [];
  return {
    tgUserId: Number(user.tgUserId || key || 0),
    username: user.username || null,
    name: userDisplayName(user),
    language: user?.settings?.language || 'ru',
    balance: Number(user.balance || 0),
    steamLinked: Boolean(user.steamId),
    steamId: user.steamId || null,
    casesOpened: Number(user?.stats?.casesOpened || 0),
    totalWonUsd: Number(user?.stats?.totalWonUsd || 0),
    topDropUsd: Number(user?.stats?.topDropUsd || 0),
    topDropName: user?.stats?.topDropName || null,
    inventoryCount: inv.length,
    openHistoryCount: openHistory.length,
    createdAt: user.createdAt || null,
    updatedAt: user.updatedAt || null,
  };
}

function ensureAdminAuth(options = {}) {
  const ownerOnly = Boolean(options.ownerOnly);
  return async (req, res, next) => {
    if (!isAdminConfigured()) return res.status(404).json({ ok: false, error: 'Not Found' });
    const token = parseCookies(req)[ADMIN_COOKIE] || '';
    const parsed = parseAdminSession(token);
    if (!parsed.ok) return res.status(401).json({ ok: false, error: 'Admin session required' });
    try {
      const db = await readDb();
      cleanup(db);
      const changed = normalizeAllConfig(db);
      const account = db.admins?.[parsed.login];
      if (!account) return res.status(401).json({ ok: false, error: 'Admin session is invalid' });
      req.admin = { login: account.login, role: account.role || 'admin', twoFactorEnabled: Boolean(account.twoFactorEnabled) };
      if (ownerOnly && req.admin.role !== 'owner') {
        return res.status(403).json({ ok: false, error: 'Owner access required' });
      }
      req.adminDb = db;
      if (changed) await writeDb(db);
      return next();
    } catch (e) {
      console.error('[admin] auth failed:', e);
      return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
    }
  };
}

function normalizeAdminLoginError(raw) {
  const text = String(raw || '').trim();
  if (!text) return '';
  return text.replace(/[^a-zA-Z0-9а-яА-ЯёЁ\s:().,_+-]/g, '').slice(0, 180);
}

function renderAdminPage(options = {}) {
  const loginErrorMessage = normalizeAdminLoginError(options.loginErrorMessage || '');
  return `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VAULT Admin</title>
  <link rel="icon" href="/assets/icons/favicon.ico?v=20260425-3" sizes="any">
  <link rel="icon" type="image/png" sizes="32x32" href="/assets/icons/favicon-32x32.png?v=20260425-3">
  <link rel="icon" type="image/png" sizes="16x16" href="/assets/icons/favicon-16x16.png?v=20260425-3">
  <link rel="apple-touch-icon" sizes="180x180" href="/assets/icons/apple-touch-icon.png?v=20260425-3">
  <style>
    :root { --bg:#0b0f16; --panel:#121826; --panel2:#1a2234; --line:#2a3550; --text:#e7ecf5; --muted:#96a1b8; --acc:#e8a630; --good:#43d48a; --bad:#ff7272; --blue:#5da6ff; }
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 20% 10%, #1f2740 0%, #0b0f16 45%);color:var(--text);font:14px/1.4 Inter, system-ui, -apple-system, Segoe UI, sans-serif}
    .wrap{max-width:1200px;margin:0 auto;padding:20px}
    .head{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px}
    .admin-layout{display:grid;grid-template-columns:260px 1fr;gap:14px}
    .side-nav{background:linear-gradient(180deg,var(--panel),#0f1522);border:1px solid var(--line);border-radius:14px;padding:10px;height:max-content;position:sticky;top:12px}
    .tabs{display:flex;flex-direction:column;gap:8px}
    .tab-btn{border:1px solid var(--line);background:#11192a;color:#c8d3eb;padding:8px 12px;border-radius:10px;cursor:pointer;font-weight:700}
    .tab-btn.active{background:linear-gradient(90deg,#2a3f6d,#2b5da4);border-color:#4f77bf;color:#fff}
    .mobile-section{display:none;margin-bottom:10px}
    .title{font-size:26px;font-weight:800}
    .sub{color:var(--muted);font-size:13px}
    .btn{border:1px solid var(--line);background:var(--panel2);color:var(--text);padding:10px 14px;border-radius:10px;cursor:pointer;font-weight:700}
    .btn:hover{border-color:#3b4b72}
    .btn.small{padding:5px 9px;font-size:12px;border-radius:8px}
    .btn.primary{background:linear-gradient(90deg,#2a3f6d,#2b5da4);border-color:#4f77bf}
    .btn.warn{background:linear-gradient(90deg,#4a2f2f,#6f3737);border-color:#8d4848}
    .btn,.tab-btn{position:relative}
    .btn.tap,.tab-btn.tap{animation:tap-feedback .18s ease}
    @keyframes tap-feedback{0%{transform:scale(1)}50%{transform:scale(.975)}100%{transform:scale(1)}}
    .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:14px}
    .card{background:linear-gradient(180deg,var(--panel),#0f1522);border:1px solid var(--line);border-radius:14px;padding:14px}
    .k{font-size:12px;color:var(--muted)} .v{font-size:24px;font-weight:800;margin-top:5px}
    .cols{display:grid;grid-template-columns:1.2fr .8fr;gap:14px}
    .panel{background:linear-gradient(180deg,var(--panel),#0f1522);border:1px solid var(--line);border-radius:14px;padding:14px}
    .panel h3{margin:0 0 12px;font-size:16px}
    .row{display:flex;gap:8px;flex-wrap:wrap}
    input,select{background:#0f1522;border:1px solid var(--line);color:var(--text);padding:10px 12px;border-radius:10px;outline:none}
    input:focus,select:focus{border-color:#4f77bf}
    .table-wrap{max-height:500px;overflow:auto;border:1px solid var(--line);border-radius:10px}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{padding:9px 10px;border-bottom:1px solid #202a3f;text-align:left;white-space:nowrap}
    th{position:sticky;top:0;background:#11192a;color:#b8c2d8;z-index:1}
    #activity-table{table-layout:fixed}
    #activity-table th:nth-child(1), #activity-table td:nth-child(1){width:145px}
    #activity-table th:nth-child(2), #activity-table td:nth-child(2){width:165px}
    #activity-table th:nth-child(3), #activity-table td:nth-child(3){width:85px}
    #activity-table th:nth-child(4), #activity-table td:nth-child(4){width:130px}
    #activity-table th:nth-child(5), #activity-table td:nth-child(5){width:95px}
    #activity-table th:nth-child(6), #activity-table td:nth-child(6){width:auto}
    .activity-details{
      display:block;
      max-width:100%;
      overflow:hidden;
      text-overflow:ellipsis;
      white-space:nowrap;
    }
    .tag{font-size:11px;padding:2px 7px;border-radius:999px;border:1px solid #3a4c72;color:#b7c6e8}
    .tag.ok{border-color:#2d7a56;color:#85e6b7}
    .tag.no{border-color:#7a3a3a;color:#ffadad}
    .hint{font-size:12px;color:var(--muted)}
    .ok{color:var(--good)} .bad{color:var(--bad)}
    .login{max-width:420px;margin:60px auto;background:linear-gradient(180deg,var(--panel),#0f1522);border:1px solid var(--line);border-radius:14px;padding:16px}
    .hidden{display:none}
    .section-block{display:block}
    .section-block.hidden{display:none}
    .funnel{margin-top:14px}
    .funnel-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
    .funnel-step{background:#11192a;border:1px solid var(--line);border-radius:12px;padding:12px}
    .funnel-step .name{font-size:12px;color:#9eb0cf}
    .funnel-step .num{font-size:24px;font-weight:800;margin-top:4px}
    .funnel-step .rate{font-size:12px;color:#8fb6ff;margin-top:6px}
    .case-lab-layout{display:grid;grid-template-columns:1.1fr .9fr;gap:12px}
    .case-lab-metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
    .case-lab-metric{background:#101827;border:1px solid var(--line);border-radius:10px;padding:10px}
    .case-lab-metric .k{font-size:12px;color:var(--muted)}
    .case-lab-metric .v{font-size:20px;font-weight:800;margin-top:3px}
    .drop-rarity{font-size:11px;padding:2px 6px;border-radius:999px;border:1px solid #2a3757}
    .drop-rarity.common{color:#b9c8e7}
    .drop-rarity.uncommon{color:#7cb6ff}
    .drop-rarity.rare{color:#b691ff}
    .drop-rarity.legendary{color:#ffcd6b}
    .notices{position:fixed;top:14px;right:14px;z-index:3000;display:flex;flex-direction:column;gap:8px;max-width:min(360px,calc(100vw - 24px))}
    .notice{background:#111a2a;border:1px solid #2f3f62;border-left:4px solid #4f77bf;border-radius:10px;padding:10px 12px;font-size:13px;box-shadow:0 10px 24px rgba(0,0,0,.28);animation:notice-in .2s ease}
    .notice.ok{border-left-color:#2f9b67}
    .notice.bad{border-left-color:#b44a4a}
    .notice.loading{border-left-color:#e8a630}
    @keyframes notice-in{from{opacity:0;transform:translateY(-6px) translateX(4px)}to{opacity:1;transform:translateY(0) translateX(0)}}
    @media (max-width:1020px){.grid{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}.admin-layout{grid-template-columns:1fr}.side-nav{position:static}}
    @media (max-width:760px){.mobile-section{display:block}.side-nav .tabs{display:none}}
    @media (max-width:900px){.funnel-grid{grid-template-columns:repeat(2,1fr)}}
    @media (max-width:1100px){.case-lab-layout{grid-template-columns:1fr}.case-lab-metrics{grid-template-columns:1fr 1fr}}
  </style>
</head>
<body>
  <div id="login-box" class="login">
    <div class="title" style="font-size:22px">VAULT Admin Login</div>
    <div class="sub" style="margin:4px 0 12px">Закрытая панель. Доступ только по админ-учетке.</div>
    <form id="login-form" method="POST" action="/admin/login">
    <div id="login-stage-password">
      <div class="row" style="margin-bottom:8px"><input id="login" name="login" placeholder="login" style="flex:1"></div>
      <div class="row" style="margin-bottom:12px"><input id="password" name="password" placeholder="password" type="password" style="flex:1"></div>
      <button id="btn-login" type="submit" class="btn primary" style="width:100%">Войти</button>
    </div>
    </form>
    <div id="login-stage-2fa" class="hidden">
      <div class="hint" style="margin-bottom:8px">Введите 6-значный код из Authenticator.</div>
      <div class="row" style="margin-bottom:12px"><input id="login-2fa-code" placeholder="123456" inputmode="numeric" maxlength="6" style="flex:1"></div>
      <div class="row" style="gap:6px">
        <button id="btn-login-2fa" class="btn primary" style="flex:1">Подтвердить 2FA</button>
        <button id="btn-login-2fa-back" class="btn" style="width:110px">Назад</button>
      </div>
    </div>
    <div id="login-msg" class="hint" style="margin-top:10px">${escapeHtml(loginErrorMessage)}</div>
  </div>

  <div id="admin-app" class="hidden">
    <div class="wrap">
      <div class="head">
        <div>
          <div class="title">VAULT Admin</div>
          <div class="sub" id="meta-line">Loading...</div>
        </div>
        <div class="row">
          <button class="btn" id="btn-refresh">Обновить</button>
          <button class="btn warn" id="btn-logout">Выйти</button>
        </div>
      </div>

      <div class="mobile-section">
        <select id="section-select" style="width:100%">
          <option value="overview">Главная</option>
          <option value="security">Безопасность</option>
          <option value="funnel">Воронка</option>
          <option value="users">Пользователи</option>
          <option value="balance">Корректировки Баланса</option>
          <option value="cases">Управление Кейсами</option>
          <option value="case-lab">Конструктор Кейса</option>
          <option value="promocodes">Промокоды</option>
          <option value="payments">Платежные Методы</option>
          <option value="payment-center">Платежный Центр</option>
          <option value="audit">Аудит и Откат</option>
          <option value="admins" id="section-select-admins">Управление Админами</option>
        </select>
      </div>

      <div class="admin-layout">
        <aside class="side-nav">
          <div class="tabs">
            <button class="tab-btn active" data-section="overview">Главная</button>
            <button class="tab-btn" data-section="security">Безопасность</button>
            <button class="tab-btn" data-section="funnel">Воронка</button>
            <button class="tab-btn" data-section="users">Пользователи</button>
            <button class="tab-btn" data-section="balance">Корректировки Баланса</button>
            <button class="tab-btn" data-section="cases">Управление Кейсами</button>
            <button class="tab-btn" data-section="case-lab">Конструктор Кейса</button>
            <button class="tab-btn" data-section="promocodes">Управление Промокодами</button>
            <button class="tab-btn" data-section="payments">Управление Платежными Методами</button>
            <button class="tab-btn" data-section="payment-center">Платежный Центр</button>
            <button class="tab-btn" data-section="audit">Аудит и Откат</button>
            <button class="tab-btn" data-section="admins" id="admins-tab-btn">Управление Админами</button>
          </div>
        </aside>
        <div>
      <div class="section-block" id="section-overview">
        <div class="grid" id="kpi-grid"></div>
      </div>

      <div class="section-block hidden" id="section-security">
        <div class="panel">
          <h3>Безопасность</h3>
          <div class="hint" style="margin-bottom:10px">Подключите двухфакторную авторизацию для дополнительной защиты входа в админку.</div>
          <div class="row" style="margin-bottom:8px">
            <span id="security-2fa-status" class="tag">2FA: loading...</span>
          </div>
          <div id="security-2fa-disabled">
            <div class="row" style="margin-bottom:8px">
              <button class="btn primary" id="btn-2fa-setup-start">Подключить 2FA</button>
            </div>
            <div id="security-2fa-setup-box" class="hidden">
              <div class="hint" style="margin-bottom:8px">1) Отсканируйте QR-код в Authenticator. 2) Введите код и подтвердите.</div>
              <div style="background:#0f1522;border:1px solid var(--line);border-radius:12px;padding:12px;display:inline-block;margin-bottom:8px">
                <img id="security-2fa-qr" alt="2FA QR" style="width:180px;height:180px;border-radius:10px;display:block">
              </div>
              <div class="hint" style="margin-bottom:8px">Секрет: <code id="security-2fa-secret"></code></div>
              <div class="row" style="margin-bottom:8px">
                <input id="security-2fa-enable-code" placeholder="Код из Authenticator" maxlength="6" inputmode="numeric" style="width:220px">
                <button class="btn primary" id="btn-2fa-enable">Подтвердить и включить</button>
              </div>
            </div>
          </div>
          <div id="security-2fa-enabled" class="hidden">
            <div class="hint" style="margin-bottom:8px">2FA уже включен. Для отключения введите текущий код.</div>
            <div class="row" style="margin-bottom:8px">
              <input id="security-2fa-disable-code" placeholder="Код из Authenticator" maxlength="6" inputmode="numeric" style="width:220px">
              <button class="btn warn" id="btn-2fa-disable">Отключить 2FA</button>
            </div>
            <div class="hint">Включено: <span id="security-2fa-enabled-at">-</span></div>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-funnel">
        <div class="panel funnel">
          <h3>Воронка (All Time)</h3>
          <div class="funnel-grid" id="funnel-grid"></div>
        </div>
      </div>

      <div class="section-block hidden" id="section-users">
        <div class="panel">
          <h3>Пользователи</h3>
          <div class="row" style="margin-bottom:10px">
            <input id="search-q" placeholder="Поиск: tg id / username / имя" style="flex:1;min-width:260px">
            <select id="search-limit">
              <option value="50">50</option><option value="100" selected>100</option><option value="200">200</option>
            </select>
            <button class="btn" id="btn-search">Найти</button>
          </div>
          <div class="table-wrap">
            <table id="users-table">
              <thead><tr><th>ID</th><th>User</th><th>Баланс ⭐</th><th>Steam</th><th>Язык</th><th>Кейсы</th><th>Инв.</th><th>Обновлен</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
          <div class="panel" style="margin-top:10px">
            <h3>Карточка Пользователя</h3>
            <div class="row" style="margin-bottom:8px">
              <input id="user-card-id" placeholder="tg user id" style="width:200px">
              <button class="btn" id="btn-user-card-load">Открыть карточку</button>
            </div>
            <div id="user-card-body" class="hint">Выберите пользователя из таблицы или введите ID.</div>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-balance">
        <div class="panel">
          <h3>Ручная Коррекция Баланса</h3>
          <div class="hint" style="margin-bottom:10px">Положительное число начисляет, отрицательное списывает. Все операции пишутся в аудит.</div>
          <div class="row" style="margin-bottom:8px"><input id="adj-user" placeholder="tg user id" style="flex:1"></div>
          <div class="row" style="margin-bottom:8px"><input id="adj-delta" placeholder="delta stars (например 150 или -70)" style="flex:1"></div>
          <div class="row" style="margin-bottom:8px"><input id="adj-reason" placeholder="reason" style="flex:1"></div>
          <button class="btn primary" id="btn-adjust" style="width:100%">Применить</button>
          <div id="adj-msg" class="hint" style="margin-top:10px"></div>
        </div>
      </div>

      <div class="section-block hidden" id="section-cases">
        <div class="panel">
          <h3>Управление Кейсами (цена / вкл-выкл)</h3>
          <div class="table-wrap" style="max-height:320px">
            <table id="cases-table">
              <thead><tr><th>Case</th><th>Price ⭐</th><th>Enabled</th><th>Updated</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
          <div class="row" style="margin-top:10px;justify-content:flex-end">
            <button class="btn primary" id="btn-cases-save-all">Сохранить все изменения</button>
            <button class="btn" id="btn-cases-publish-all">Опубликовать черновики</button>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-case-lab">
        <div class="panel">
          <h3>Конструктор Кейса + Симулятор</h3>
          <div class="hint" style="margin-bottom:10px">Настройте дроп-таблицу конкретного кейса и проверьте экономику через симуляцию до сохранения.</div>
          <div class="row" style="margin-bottom:10px">
            <select id="case-lab-case" style="min-width:220px;flex:1"></select>
            <input id="case-lab-price" type="number" min="0" step="1" placeholder="Цена ⭐" style="width:130px">
            <label class="hint" style="display:flex;align-items:center;gap:6px"><input id="case-lab-enabled" type="checkbox"> Включен</label>
            <label class="hint" style="display:flex;align-items:center;gap:6px"><input id="case-lab-use-custom" type="checkbox"> Кастомный дроп</label>
            <select id="case-lab-preset" style="min-width:180px">
              <option value="safe">Preset: Safe</option>
              <option value="balanced" selected>Preset: Balanced</option>
              <option value="aggressive">Preset: Aggressive</option>
              <option value="high_rtp">Preset: High-RTP</option>
              <option value="low_rtp">Preset: Low-RTP</option>
            </select>
            <button class="btn" id="btn-case-lab-apply-preset">Применить preset</button>
            <input id="case-lab-spins" type="number" min="100" max="50000" step="100" value="1000" style="width:120px" title="Количество симуляций">
            <button class="btn" id="btn-case-lab-sim-1000">Тест 1000</button>
            <button class="btn" id="btn-case-lab-simulate">Симулировать</button>
            <button class="btn primary" id="btn-case-lab-save">Сохранить кейс</button>
            <button class="btn primary" id="btn-case-lab-publish">Опубликовать кейс</button>
          </div>
          <div class="hint" id="case-lab-chance-sum" style="margin-bottom:8px">Сумма шансов: 0%</div>
          <div class="case-lab-layout">
            <div class="table-wrap" style="max-height:420px">
              <table id="case-lab-table">
                <thead><tr><th>Скин</th><th>Редкость</th><th>Цена $</th><th>Шанс %</th><th>Вес</th></tr></thead>
                <tbody></tbody>
              </table>
            </div>
            <div>
              <div class="case-lab-metrics" id="case-lab-metrics"></div>
              <div class="panel" style="margin-top:10px">
                <h3 style="margin-bottom:8px">Топ результатов симуляции</h3>
                <div id="case-lab-top-drops" class="hint">Сначала запустите симуляцию</div>
              </div>
              <div class="hint" id="case-lab-meta" style="margin-top:8px"></div>
            </div>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-promocodes">
        <div class="panel">
          <h3>Управление Промокодами</h3>
          <div class="hint" style="margin-bottom:10px">Добавляйте промокоды и срок действия. В мини-аппе автоматически показывается активный промокод с таймером.</div>
          <div class="row" style="margin-bottom:10px">
            <input id="promo-code" placeholder="PROMO2026" style="min-width:200px;flex:1">
            <input id="promo-expires" type="datetime-local" style="min-width:220px">
            <label class="hint" style="display:flex;align-items:center;gap:6px"><input id="promo-enabled" type="checkbox" checked> Активен</label>
            <button class="btn primary" id="btn-promo-save">Сохранить промокод</button>
          </div>
          <div class="table-wrap" style="max-height:320px">
            <table id="promocodes-table">
              <thead><tr><th>Промокод</th><th>Активен</th><th>Срок действия</th><th>Обновлён</th><th>Действие</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-payments">
        <div class="panel">
          <h3>Управление Платежными Методами</h3>
          <div class="hint" style="margin-bottom:10px">Для каждого метода можно включать/выключать его и задавать лимиты пополнения.</div>
          <div class="table-wrap" style="max-height:320px">
            <table id="payments-table">
              <thead><tr><th>Метод</th><th>Статус</th><th>Min ⭐</th><th>Max ⭐</th><th>Updated</th><th>Action</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-payment-center">
        <div class="panel">
          <h3>Платежный Центр</h3>
          <div class="row" style="margin-bottom:10px">
            <select id="pc-status" style="min-width:170px">
              <option value="all">all</option>
              <option value="pending">pending</option>
              <option value="paid">paid</option>
              <option value="failed">failed</option>
              <option value="expired">expired</option>
            </select>
            <input id="pc-q" placeholder="order id / user id / payload" style="flex:1;min-width:220px">
            <button class="btn" id="btn-pc-load">Найти</button>
          </div>
          <div class="table-wrap" style="max-height:360px">
            <table id="pc-table">
              <thead><tr><th>Order</th><th>User</th><th>Amount ⭐</th><th>Status</th><th>Updated</th><th>Action</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-audit">
        <div class="panel">
          <h3>Аудит + Откат</h3>
          <div class="row" style="margin-bottom:10px">
            <select id="audit-entity-type" style="min-width:180px">
              <option value="">Все сущности</option>
              <option value="case_config">case_config</option>
              <option value="case_draft">case_draft</option>
              <option value="payment_method">payment_method</option>
              <option value="promo_code">promo_code</option>
            </select>
            <input id="audit-entity-key" placeholder="entity key (optional)" style="flex:1;min-width:220px">
            <button class="btn" id="btn-audit-load">Найти</button>
          </div>
          <div class="table-wrap" style="max-height:360px">
            <table id="audit-table">
              <thead><tr><th>Время</th><th>Entity</th><th>Key</th><th>Action</th><th>By</th><th>Rollback</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-block hidden" id="section-admins">
        <div class="panel" id="admins-panel">
          <h3>Управление Админами</h3>
          <div class="hint" style="margin-bottom:10px">Только owner может создавать/удалять админов. Админ не может создавать других админов.</div>
          <div class="row" style="margin-bottom:8px"><input id="new-admin-login" placeholder="login (4..32)" style="flex:1"></div>
          <div class="row" style="margin-bottom:8px"><input id="new-admin-password" placeholder="password (min 8)" style="flex:1"></div>
          <button class="btn primary" id="btn-create-admin" style="width:100%">Создать Админа</button>
          <div id="admins-msg" class="hint" style="margin:10px 0 8px"></div>
          <div class="table-wrap" style="max-height:210px">
            <table id="admins-table">
              <thead><tr><th>Login</th><th>Role</th><th>Created</th><th>Action</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="section-block" id="section-activity">
      <div class="panel" style="margin-top:14px">
        <h3>Лента Операций</h3>
        <div class="row" style="margin-bottom:10px">
          <input id="act-q" placeholder="Поиск по деталям/user" style="flex:1;min-width:220px">
          <input id="act-user-id" placeholder="user id" style="width:130px">
          <select id="act-kind" style="min-width:210px">
            <option value="all">Все события</option>
            <option value="deposits">Пополнения</option>
            <option value="charges">Списания/корректировки</option>
            <option value="case_open">Открытия кейсов</option>
            <option value="upgrades">Апгрейды</option>
            <option value="payments">Все платежные события</option>
            <option value="admin">Все админские действия</option>
            <option value="security">События безопасности</option>
          </select>
          <input id="act-type" placeholder="Доп. type фильтр (опционально)" style="min-width:220px;flex:1">
          <input id="act-from" type="date">
          <input id="act-to" type="date">
          <button class="btn" id="btn-activity-filter">Поиск</button>
          <button class="btn" id="btn-activity-export">Экспорт XLSX</button>
        </div>
        <div class="table-wrap" style="max-height:360px">
          <table id="activity-table">
            <thead><tr><th>Время</th><th>Тип</th><th>ID</th><th>User</th><th>Сумма</th><th>Детали</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      </div>
      </div>
      </div>
    </div>
  </div>
  <div id="admin-notices" class="notices"></div>

  <script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>
  <script>
    async function jfetch(url, opt = {}) {
      const r = await fetch(url, { credentials: 'include', ...opt });
      let data = null;
      try { data = await r.json(); } catch {}
      if (!r.ok) throw new Error(data?.error || ('HTTP ' + r.status));
      return data;
    }
    function fmtTs(v){ if(!v) return '-'; const d=new Date(v); if(Number.isNaN(d.getTime())) return '-'; return d.toLocaleString(); }
    function esc(v){ return String(v ?? '').replace(/[&<>"]/g, s => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[s])); }
    function cut(v, max = 80) {
      const s = String(v ?? '');
      if (s.length <= max) return s;
      return s.slice(0, Math.max(0, max - 1)) + '…';
    }
    function showNotice(message, type = 'info', duration = 2300){
      const root = document.getElementById('admin-notices');
      if (!root) return null;
      const el = document.createElement('div');
      el.className = 'notice ' + (type || 'info');
      el.textContent = String(message || '');
      root.appendChild(el);
      if (duration > 0) {
        setTimeout(() => {
          if (el.parentNode) el.parentNode.removeChild(el);
        }, duration);
      }
      return el;
    }
    function createProgressNotice(message){
      const el = showNotice(message, 'loading', 0);
      return {
        done(nextMessage, type = 'ok', duration = 2200){
          if (!el) return;
          el.className = 'notice ' + type;
          el.textContent = String(nextMessage || '');
          setTimeout(() => { if (el.parentNode) el.parentNode.removeChild(el); }, duration);
        }
      };
    }
    function addTapFeedback(ev){
      const btn = ev.target.closest('.btn, .tab-btn, button');
      if (!btn) return;
      btn.classList.remove('tap');
      void btn.offsetWidth;
      btn.classList.add('tap');
      setTimeout(() => btn.classList.remove('tap'), 220);
    }
    document.addEventListener('click', addTapFeedback, true);
    let currentAdmin = null;
    let activeSection = 'overview';
    let login2faChallenge = '';
    let securityState = null;
    let caseLabState = null;
    let paymentCenterRows = [];
    let configHistoryRows = [];

    function renderKpi(data){
      const grid = document.getElementById('kpi-grid');
      const items = [
        ['Users', data.kpi.totalUsers],
        ['Total Balance ⭐', data.kpi.totalBalance],
        ['Cases Opened', data.kpi.totalCasesOpened],
        ['Orders Paid', data.kpi.paidOrders],
        ['Active Cases', data.kpi.activeCases],
      ];
      grid.innerHTML = items.map(([k,v]) => '<div class="card"><div class="k">'+esc(k)+'</div><div class="v">'+esc(v)+'</div></div>').join('');
      document.getElementById('meta-line').textContent = 'Private Control Center';
      renderFunnel(data.funnel || null);
    }

    function renderFunnel(funnel){
      const root = document.getElementById('funnel-grid');
      if (!root) return;
      const steps = Array.isArray(funnel?.steps) ? funnel.steps : [];
      if (!steps.length) {
        root.innerHTML = '<div class="hint">Нет данных для воронки</div>';
        return;
      }
      root.innerHTML = steps.map((s, idx) => {
        const fromPrev = idx === 0 ? '100.0%' : (Number(s.fromPrevPct || 0).toFixed(1) + '%');
        const fromStart = Number(s.fromStartPct || 0).toFixed(1) + '%';
        return '' +
          '<div class="funnel-step">' +
            '<div class="name">'+esc(s.label || s.key)+'</div>' +
            '<div class="num">'+esc(s.users || 0)+'</div>' +
            '<div class="rate">Конверсия от прошлого шага: '+esc(fromPrev)+'</div>' +
            '<div class="rate">Конверсия от старта: '+esc(fromStart)+'</div>' +
          '</div>';
      }).join('');
    }

    function setSection(section){
      const map = ['overview','security','funnel','users','balance','cases','case-lab','promocodes','payments','payment-center','audit','admins'];
      activeSection = map.includes(section) ? section : 'overview';
      map.forEach((key) => {
        const el = document.getElementById('section-' + key);
        if (!el) return;
        el.classList.toggle('hidden', key !== activeSection);
      });
      const activitySection = document.getElementById('section-activity');
      if (activitySection) activitySection.classList.toggle('hidden', activeSection !== 'overview');
      document.querySelectorAll('.tab-btn[data-section]').forEach((btn) => {
        btn.classList.toggle('active', btn.getAttribute('data-section') === activeSection);
      });
      const sectionSelect = document.getElementById('section-select');
      if (sectionSelect && sectionSelect.value !== activeSection) sectionSelect.value = activeSection;
    }

    function renderSecurityBlock() {
      const status = document.getElementById('security-2fa-status');
      const disabledBox = document.getElementById('security-2fa-disabled');
      const enabledBox = document.getElementById('security-2fa-enabled');
      const setupBox = document.getElementById('security-2fa-setup-box');
      const enabledAt = document.getElementById('security-2fa-enabled-at');
      if (!status || !disabledBox || !enabledBox || !setupBox || !enabledAt) return;
      const enabled = Boolean(securityState?.twoFactorEnabled);
      status.textContent = enabled ? '2FA: включен' : '2FA: выключен';
      status.classList.toggle('ok', enabled);
      status.classList.toggle('no', !enabled);
      disabledBox.classList.toggle('hidden', enabled);
      enabledBox.classList.toggle('hidden', !enabled);
      if (enabled) {
        enabledAt.textContent = fmtTs(securityState?.twoFactorEnabledAt);
      } else {
        enabledAt.textContent = '-';
      }
      if (!securityState?.pendingSetup) setupBox.classList.add('hidden');
      if (securityState?.pendingSetup) {
        setupBox.classList.remove('hidden');
        const secretEl = document.getElementById('security-2fa-secret');
        const qrEl = document.getElementById('security-2fa-qr');
        if (secretEl) secretEl.textContent = securityState.pendingSetup.secret || '';
        if (qrEl) qrEl.src = 'https://api.qrserver.com/v1/create-qr-code/?size=256x256&data=' + encodeURIComponent(securityState.pendingSetup.otpauth || '');
      }
    }

    function renderUsers(users){
      const body = document.querySelector('#users-table tbody');
      body.innerHTML = users.map(u => {
        const steamTag = u.steamLinked ? '<span class="tag ok">linked</span>' : '<span class="tag no">no</span>';
        return '<tr data-user-row="'+esc(u.tgUserId)+'">' +
          '<td>'+esc(u.tgUserId)+'</td>' +
          '<td>'+esc(u.name)+'<div class="hint">@'+esc(u.username || '-')+'</div></td>' +
          '<td>'+esc(u.balance)+'</td>' +
          '<td>'+steamTag+'</td>' +
          '<td>'+esc(u.language)+'</td>' +
          '<td>'+esc(u.casesOpened)+'</td>' +
          '<td>'+esc(u.inventoryCount)+'</td>' +
          '<td>'+esc(fmtTs(u.updatedAt))+'</td>' +
          '</tr>';
      }).join('');
      document.querySelectorAll('[data-user-row]').forEach((tr) => {
        tr.onclick = async () => {
          const id = tr.getAttribute('data-user-row');
          const input = document.getElementById('user-card-id');
          if (input) input.value = String(id || '');
          if (id) await loadUserCard(id).catch(()=>{});
        };
      });
    }

    function renderActivity(list){
      const body = document.querySelector('#activity-table tbody');
      body.innerHTML = list.map(x => '<tr>' +
        '<td>'+esc(fmtTs(x.ts))+'</td>' +
        '<td>'+esc(x.type)+'</td>' +
        '<td>'+esc(x.tgUserId)+'</td>' +
        '<td>'+esc(x.userLabel)+'</td>' +
        '<td>'+esc(x.amount)+'</td>' +
        '<td><span class="activity-details" title="'+esc(x.details)+'">'+esc(cut(x.details, 96))+'</span></td>' +
      '</tr>').join('');
    }

    function renderCases(rows){
      const body = document.querySelector('#cases-table tbody');
      body.innerHTML = rows.map(c => {
        const disPrice = c.caseName === 'free' ? 'disabled' : '';
        const draftHint = c.hasDraft ? ('<div class="hint">draft: ' + esc(c.draftPrice) + ' / ' + esc(c.draftEnabled ? 'on' : 'off') + '</div>') : '<div class="hint">draft: -</div>';
        return '<tr>' +
          '<td>'+esc(c.caseName)+'</td>' +
          '<td><input data-case-price="'+esc(c.caseName)+'" type="number" min="0" step="1" value="'+esc(c.hasDraft ? c.draftPrice : c.price)+'" '+disPrice+' style="width:110px"></td>' +
          '<td><input data-case-enabled="'+esc(c.caseName)+'" type="checkbox" '+((c.hasDraft ? c.draftEnabled : c.enabled) ? 'checked' : '')+'></td>' +
          '<td>'+esc(fmtTs(c.updatedAt))+'<div class="hint">'+esc(c.updatedBy || '-')+' · '+(c.hasCustomDropTable ? 'custom drop' : 'default drop')+'</div>'+draftHint+'</td>' +
        '</tr>';
      }).join('');
    }

    function getRarityBadge(rarity){
      const r = String(rarity || 'common');
      return '<span class="drop-rarity ' + esc(r) + '">' + esc(r) + '</span>';
    }

    function renderCaseLabSummary(sim){
      const metrics = document.getElementById('case-lab-metrics');
      const top = document.getElementById('case-lab-top-drops');
      if (!metrics || !top) return;
      if (!sim) {
        metrics.innerHTML = '<div class="case-lab-metric"><div class="k">Симуляция</div><div class="v">-</div></div>';
        top.innerHTML = '<div class="hint">Сначала запустите симуляцию</div>';
        return;
      }
      const cards = [
        ['RTP', String(sim.rtpPct ?? 0) + '%'],
        ['AVG $', String(sim.averageUsd ?? 0)],
        ['AVG ⭐', String(sim.averageStars ?? 0)],
        ['Прогонов', String(sim.spins ?? 0)],
        ['Common', String(sim?.rarityDist?.commonPct ?? 0) + '%'],
        ['Legendary', String(sim?.rarityDist?.legendaryPct ?? 0) + '%'],
      ];
      metrics.innerHTML = cards.map((x) => '' +
        '<div class="case-lab-metric">' +
          '<div class="k">'+esc(x[0])+'</div>' +
          '<div class="v">'+esc(x[1])+'</div>' +
        '</div>'
      ).join('');
      const topRows = Array.isArray(sim.topDrops) ? sim.topDrops : [];
      if (!topRows.length) {
        top.innerHTML = '<div class="hint">Нет данных</div>';
      } else {
        top.innerHTML = topRows.map((x) => '' +
          '<div class="row" style="justify-content:space-between;padding:6px 0;border-bottom:1px solid #1f2a42">' +
            '<div>' + esc(x.name) + '<div class="hint">' + getRarityBadge(x.rarity) + ' · $' + esc(x.priceUsd) + '</div></div>' +
            '<div><b>' + esc(x.chancePct) + '%</b></div>' +
          '</div>'
        ).join('');
      }
    }

    function updateCaseLabChanceSum() {
      const sumEl = document.getElementById('case-lab-chance-sum');
      if (!sumEl) return { sum: 0, valid: false };
      const values = Array.from(document.querySelectorAll('input[data-drop-chance]')).map((el) => Number(el.value || 0));
      const sum = values.reduce((s, v) => s + (Number.isFinite(v) ? v : 0), 0);
      const rounded = Math.round(sum * 100) / 100;
      const valid = Math.abs(rounded - 100) <= 0.05;
      sumEl.textContent = 'Сумма шансов: ' + rounded.toFixed(2) + '%';
      sumEl.className = 'hint ' + (valid ? 'ok' : 'bad');
      return { sum: rounded, valid };
    }

    function collectCaseLabDraft(){
      const caseName = String(document.getElementById('case-lab-case')?.value || '');
      const priceInput = document.getElementById('case-lab-price');
      const enabledInput = document.getElementById('case-lab-enabled');
      const useCustomInput = document.getElementById('case-lab-use-custom');
      const rows = Array.from(document.querySelectorAll('#case-lab-table tbody tr'));
      const dropTable = {};
      for (const tr of rows) {
        const key = tr.getAttribute('data-skin-key') || '';
        const chanceInput = tr.querySelector('input[data-drop-chance]');
        const chance = Number(chanceInput ? chanceInput.value : 0);
        const weight = chance * 100;
        if (key && Number.isFinite(weight) && weight > 0) dropTable[key] = weight;
      }
      return {
        caseName,
        price: Number(priceInput?.value || 0),
        enabled: Boolean(enabledInput?.checked),
        dropTable: useCustomInput?.checked ? dropTable : null,
      };
    }

    function renderCaseLab(data){
      caseLabState = data || null;
      const select = document.getElementById('case-lab-case');
      const body = document.querySelector('#case-lab-table tbody');
      const meta = document.getElementById('case-lab-meta');
      const priceInput = document.getElementById('case-lab-price');
      const enabledInput = document.getElementById('case-lab-enabled');
      const useCustomInput = document.getElementById('case-lab-use-custom');
      const sumEl = document.getElementById('case-lab-chance-sum');
      if (!select || !body || !meta || !priceInput || !enabledInput || !useCustomInput) return;
      if (!select.options.length) {
        const known = Array.from(document.querySelectorAll('#cases-table [data-case-price]'))
          .map((el) => el.getAttribute('data-case-price'))
          .filter(Boolean);
        const uniq = Array.from(new Set(known));
        select.innerHTML = uniq.map((x) => '<option value="'+esc(x)+'">'+esc(x)+'</option>').join('');
      }
      if (data?.case?.caseName) select.value = data.case.caseName;
      priceInput.value = String(data?.case?.price ?? 0);
      enabledInput.checked = Boolean(data?.case?.enabled);
      useCustomInput.checked = Boolean(data?.case?.useCustomDropTable);
      const rows = Array.isArray(data?.drops?.rows) ? data.drops.rows : [];
      body.innerHTML = rows.map((r) => '' +
        '<tr data-skin-key="'+esc(r.key)+'">' +
          '<td>'+esc(r.name)+'</td>' +
          '<td>'+getRarityBadge(r.rarity)+'</td>' +
          '<td>$'+esc(r.priceUsd)+'</td>' +
          '<td><input data-drop-chance="'+esc(r.key)+'" type="number" min="0" max="100" step="0.01" value="'+esc(r.chance)+'" style="width:100px"></td>' +
          '<td>'+esc(r.weight)+'</td>' +
        '</tr>'
      ).join('');
      const lockRows = () => {
        const disabled = !useCustomInput.checked;
        document.querySelectorAll('input[data-drop-chance]').forEach((el) => {
          el.disabled = disabled;
        });
        if (!disabled) updateCaseLabChanceSum();
        if (disabled && sumEl) { sumEl.className = 'hint'; sumEl.textContent = 'Сумма шансов: auto (default)'; }
      };
      document.querySelectorAll('input[data-drop-chance]').forEach((el) => {
        el.oninput = () => updateCaseLabChanceSum();
      });
      lockRows();
      useCustomInput.onchange = lockRows;
      meta.textContent = 'Total weight: ' + String(data?.drops?.totalWeight ?? 0) + ' · updated: ' + fmtTs(data?.case?.updatedAt) + ' · by: ' + String(data?.case?.updatedBy || '-');
      renderCaseLabSummary(data?.simulation || null);
    }

    async function loadCaseLab(caseName, withSimulation = false){
      const key = String(caseName || document.getElementById('case-lab-case')?.value || '');
      if (!key) return;
      const data = await jfetch('/admin/api/case-lab/' + encodeURIComponent(key));
      renderCaseLab(data);
      if (withSimulation) {
        const draft = collectCaseLabDraft();
        const sim = await jfetch('/admin/api/case-lab/' + encodeURIComponent(key) + '/simulate', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ spins: Number(document.getElementById('case-lab-spins')?.value || 1000), draft }),
        });
        renderCaseLab({ ...data, simulation: sim.simulation });
      }
    }

    async function applyCasePreset(){
      const presetKey = String(document.getElementById('case-lab-preset')?.value || 'balanced');
      const priceStars = Number(document.getElementById('case-lab-price')?.value || 0);
      const result = await jfetch('/admin/api/case-lab-preset/build', {
        method:'POST',
        headers:{'content-type':'application/json'},
        body: JSON.stringify({ presetKey, priceStars }),
      });
      document.getElementById('case-lab-use-custom').checked = true;
      const byKey = {};
      for (const row of (result?.drops?.rows || [])) byKey[row.key] = row;
      document.querySelectorAll('input[data-drop-chance]').forEach((el) => {
        const key = el.getAttribute('data-drop-chance');
        const row = byKey[key] || null;
        el.value = String(row?.chance ?? 0);
        el.disabled = false;
      });
      updateCaseLabChanceSum();
      const meta = document.getElementById('case-lab-meta');
      if (meta) {
        meta.textContent = 'Preset: ' + String(result?.preset?.title || presetKey) + ' · ' + String(result?.preset?.description || '');
      }
      return result;
    }

    async function loadUserCard(tgUserId){
      const body = document.getElementById('user-card-body');
      if (!body) return;
      body.textContent = 'Загрузка...';
      const data = await jfetch('/admin/api/users/' + encodeURIComponent(tgUserId) + '/card');
      const actions = Array.isArray(data.adminActions) ? data.adminActions.slice(0, 8) : [];
      body.innerHTML = '' +
        '<div class="row" style="gap:14px;margin-bottom:8px">' +
          '<div><b>' + esc(data.profile.name) + '</b><div class="hint">ID ' + esc(data.profile.tgUserId) + '</div></div>' +
          '<div><b>Баланс: ' + esc(data.profile.balance) + ' ⭐</b></div>' +
          '<div><b>Депозиты: ' + esc(data.metrics.depositsStars) + ' ⭐</b></div>' +
          '<div><b>Открытия: ' + esc(data.metrics.opensCount) + '</b></div>' +
          '<div><b>Апгрейды: ' + esc(data.metrics.upgradesCount) + '</b></div>' +
        '</div>' +
        '<div class="hint">Топ дропы: ' + esc((data.topDrops || []).map((x) => x.name + ' ($' + x.priceUsd + ')').join(', ') || '-') + '</div>' +
        '<div class="hint" style="margin-top:8px">Админ действия: ' + esc(actions.map((x) => (x.action + ' [' + (x.reason || '') + ']')).join(' | ') || '-')
        + '</div>';
    }

    function renderPaymentCenter(rows){
      paymentCenterRows = Array.isArray(rows) ? rows : [];
      const body = document.querySelector('#pc-table tbody');
      if (!body) return;
      body.innerHTML = paymentCenterRows.map((o) => {
        return '<tr>' +
          '<td>'+esc(o.id)+'</td>' +
          '<td>'+esc(o.tgUserId)+'</td>' +
          '<td>'+esc(o.amount)+'</td>' +
          '<td>'+esc(o.status)+'</td>' +
          '<td>'+esc(fmtTs(o.updatedAt || o.createdAt))+'</td>' +
          '<td><button class="btn small" data-pc-recheck="'+esc(o.id)+'">Recheck</button></td>' +
        '</tr>';
      }).join('');
      document.querySelectorAll('[data-pc-recheck]').forEach((btn) => {
        btn.onclick = async () => {
          const id = btn.getAttribute('data-pc-recheck');
          btn.disabled = true;
          const progress = createProgressNotice('Перепроверяем платеж...');
          try {
            await jfetch('/admin/api/payments/orders/' + encodeURIComponent(id) + '/recheck', { method:'POST' });
            await loadPaymentCenter();
            progress.done('Платеж перепроверен');
          } catch (e) {
            progress.done('Ошибка: ' + e.message, 'bad', 3400);
          } finally {
            btn.disabled = false;
          }
        };
      });
    }

    async function loadPaymentCenter(){
      const status = document.getElementById('pc-status')?.value || 'all';
      const q = document.getElementById('pc-q')?.value || '';
      const params = new URLSearchParams({ status, q, limit:'400' });
      const data = await jfetch('/admin/api/payments/orders?' + params.toString());
      renderPaymentCenter(data.orders || []);
    }

    function renderConfigHistory(rows){
      configHistoryRows = Array.isArray(rows) ? rows : [];
      const body = document.querySelector('#audit-table tbody');
      if (!body) return;
      body.innerHTML = configHistoryRows.map((x) => '' +
        '<tr>' +
          '<td>'+esc(fmtTs(x.createdAt))+'</td>' +
          '<td>'+esc(x.entityType)+'</td>' +
          '<td>'+esc(x.entityKey)+'</td>' +
          '<td>'+esc(x.action)+'</td>' +
          '<td>'+esc(x.createdBy)+'</td>' +
          '<td><button class="btn small warn" data-audit-rollback="'+esc(x.id)+'">Откат</button></td>' +
        '</tr>'
      ).join('');
      document.querySelectorAll('[data-audit-rollback]').forEach((btn) => {
        btn.onclick = async () => {
          const id = btn.getAttribute('data-audit-rollback');
          if (!confirm('Откатить изменение ' + id + '?')) return;
          btn.disabled = true;
          const progress = createProgressNotice('Выполняем откат...');
          try {
            await jfetch('/admin/api/config-history/' + encodeURIComponent(id) + '/rollback', { method:'POST' });
            await loadConfigHistory();
            await loadDashboard({ section: 'audit' });
            progress.done('Откат выполнен');
          } catch (e) {
            progress.done('Ошибка: ' + e.message, 'bad', 3400);
          } finally {
            btn.disabled = false;
          }
        };
      });
    }

    async function loadConfigHistory(){
      const entityType = document.getElementById('audit-entity-type')?.value || '';
      const entityKey = document.getElementById('audit-entity-key')?.value || '';
      const params = new URLSearchParams({ limit: '300' });
      if (entityType) params.set('entityType', entityType);
      if (entityKey) params.set('entityKey', entityKey);
      const data = await jfetch('/admin/api/config-history?' + params.toString());
      renderConfigHistory(data.entries || []);
    }

    function renderPayments(rows){
      const body = document.querySelector('#payments-table tbody');
      body.innerHTML = rows.map(p => {
        return '<tr>' +
          '<td>'+esc(p.title || p.methodKey)+'<div class="hint">'+esc(p.methodKey)+'</div></td>' +
          '<td><input data-pay-enabled="'+esc(p.methodKey)+'" type="checkbox" '+(p.enabled ? 'checked' : '')+'></td>' +
          '<td><input data-pay-min="'+esc(p.methodKey)+'" type="number" min="1" step="1" value="'+esc(p.minAmount)+'" style="width:120px"></td>' +
          '<td><input data-pay-max="'+esc(p.methodKey)+'" type="number" min="1" step="1" value="'+esc(p.maxAmount)+'" style="width:120px"></td>' +
          '<td>'+esc(fmtTs(p.updatedAt))+'<div class="hint">'+esc(p.updatedBy || '-')+'</div></td>' +
          '<td><button class="btn small" data-pay-save="'+esc(p.methodKey)+'">Save</button></td>' +
        '</tr>';
      }).join('');
      document.querySelectorAll('[data-pay-save]').forEach((btn) => {
        btn.onclick = async () => {
          const methodKey = btn.getAttribute('data-pay-save');
          const enabledEl = document.querySelector('[data-pay-enabled="'+methodKey+'"]');
          const minEl = document.querySelector('[data-pay-min="'+methodKey+'"]');
          const maxEl = document.querySelector('[data-pay-max="'+methodKey+'"]');
          const enabled = Boolean(enabledEl && enabledEl.checked);
          const minAmount = Number(minEl ? minEl.value : 1);
          const maxAmount = Number(maxEl ? maxEl.value : 5000);
          btn.disabled = true;
          const progress = createProgressNotice('Сохраняем платежный метод...');
          try {
            await jfetch('/admin/api/payments/' + encodeURIComponent(methodKey), {
              method:'POST',
              headers:{'content-type':'application/json'},
              body: JSON.stringify({ enabled, minAmount, maxAmount }),
            });
            await loadDashboard();
            progress.done('Платежный метод обновлен');
          } catch (e) {
            progress.done('Ошибка: ' + e.message, 'bad', 3400);
          } finally {
            btn.disabled = false;
          }
        };
      });
    }

    function renderPromoCodes(rows){
      const body = document.querySelector('#promocodes-table tbody');
      body.innerHTML = rows.map((p) => {
        const isExpired = new Date(p.expiresAt || 0).getTime() <= Date.now();
        const status = p.enabled && !isExpired ? '<span class="tag ok">active</span>' : '<span class="tag no">' + (isExpired ? 'expired' : 'disabled') + '</span>';
        return '<tr>' +
          '<td><b>'+esc(p.code)+'</b></td>' +
          '<td>'+status+'</td>' +
          '<td>'+esc(fmtTs(p.expiresAt))+'</td>' +
          '<td>'+esc(fmtTs(p.updatedAt))+'<div class="hint">'+esc(p.updatedBy || '-')+'</div></td>' +
          '<td><button class="btn small warn" data-promo-del="'+esc(p.code)+'">Delete</button></td>' +
        '</tr>';
      }).join('');
      document.querySelectorAll('[data-promo-del]').forEach((btn) => {
        btn.onclick = async () => {
          const code = btn.getAttribute('data-promo-del');
          if (!confirm('Delete promo ' + code + '?')) return;
          btn.disabled = true;
          const progress = createProgressNotice('Удаляем промокод...');
          try {
            await jfetch('/admin/api/promocodes/' + encodeURIComponent(code), { method:'DELETE' });
            await loadDashboard();
            progress.done('Промокод удален');
          } catch (e) {
            progress.done('Ошибка: ' + e.message, 'bad', 3400);
          } finally {
            btn.disabled = false;
          }
        };
      });
    }

    function renderAdmins(rows){
      const panel = document.getElementById('admins-panel');
      const tab = document.getElementById('admins-tab-btn');
      const selectAdmins = document.getElementById('section-select-admins');
      if (!currentAdmin || currentAdmin.role !== 'owner') {
        panel.classList.add('hidden');
        if (tab) tab.classList.add('hidden');
        if (selectAdmins) { selectAdmins.disabled = true; selectAdmins.hidden = true; }
        const sectionSelect = document.getElementById('section-select');
        if (sectionSelect && sectionSelect.value === 'admins') sectionSelect.value = 'overview';
        return;
      }
      panel.classList.remove('hidden');
      if (tab) tab.classList.remove('hidden');
      if (selectAdmins) { selectAdmins.disabled = false; selectAdmins.hidden = false; }
      const body = document.querySelector('#admins-table tbody');
      body.innerHTML = rows.map(a => {
        const canDelete = a.role !== 'owner';
        return '<tr>' +
          '<td>'+esc(a.login)+'</td>' +
          '<td>'+esc(a.role)+'</td>' +
          '<td>'+esc(fmtTs(a.createdAt))+'</td>' +
          '<td>' + (canDelete ? '<button class="btn small warn" data-admin-del="'+esc(a.login)+'">Delete</button>' : '<span class="hint">locked</span>') + '</td>' +
        '</tr>';
      }).join('');
      document.querySelectorAll('[data-admin-del]').forEach((btn) => {
        btn.onclick = async () => {
          const login = btn.getAttribute('data-admin-del');
          if (!confirm('Delete admin ' + login + '?')) return;
          btn.disabled = true;
          const progress = createProgressNotice('Удаляем админа...');
          try {
            await jfetch('/admin/api/admins/' + encodeURIComponent(login), { method:'DELETE' });
            await loadDashboard();
            progress.done('Админ удален');
          } catch (e) {
            progress.done('Ошибка: ' + e.message, 'bad', 3400);
          } finally {
            btn.disabled = false;
          }
        };
      });
    }

    function collectActivityFilters(limit = 300){
      const q = document.getElementById('act-q').value || '';
      const userId = document.getElementById('act-user-id').value || '';
      const kind = document.getElementById('act-kind').value || 'all';
      const type = document.getElementById('act-type').value || '';
      const from = document.getElementById('act-from').value || '';
      const to = document.getElementById('act-to').value || '';
      const params = new URLSearchParams();
      params.set('limit', String(limit));
      if (q) params.set('q', q);
      if (userId) params.set('userId', userId);
      if (kind && kind !== 'all') params.set('kind', kind);
      if (type) params.set('type', type);
      if (from) params.set('from', from);
      if (to) params.set('to', to);
      return params;
    }

    async function loadActivityOnly(){
      const params = collectActivityFilters(300);
      const progress = createProgressNotice('Ищем операции...');
      const activity = await jfetch('/admin/api/activity?' + params.toString());
      renderActivity(activity.activity || []);
      progress.done('Фильтры применены');
    }

    async function exportActivityXlsx(){
      const params = collectActivityFilters(5000);
      const activity = await jfetch('/admin/api/activity?' + params.toString());
      const rows = Array.isArray(activity.activity) ? activity.activity : [];
      if (!rows.length) {
        throw new Error('Нет данных для выгрузки по выбранным фильтрам');
      }
      if (typeof XLSX === 'undefined') {
        throw new Error('XLSX библиотека не загружена');
      }
      const normalized = rows.map((x) => ({
        time: fmtTs(x.ts),
        type: x.type || '',
        userId: x.tgUserId || '',
        user: x.userLabel || '',
        amount: x.amount || 0,
        details: x.details || '',
      }));
      const ws = XLSX.utils.json_to_sheet(normalized);
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, 'Report');
      const stamp = new Date().toISOString().replace(/[:T]/g, '-').slice(0, 19);
      XLSX.writeFile(wb, 'vault-activity-' + stamp + '.xlsx');
      return rows.length;
    }

    async function loadDashboard(options = {}){
      const targetSection = options.section || (options.preserveSection === false ? 'overview' : activeSection || 'overview');
      const q = document.getElementById('search-q').value || '';
      const limit = document.getElementById('search-limit').value || '100';
      const [me, overview, users, activity, cases, promoCodes, payments, security, paymentCenter, configHistory] = await Promise.all([
        jfetch('/admin/api/me'),
        jfetch('/admin/api/overview'),
        jfetch('/admin/api/users?q=' + encodeURIComponent(q) + '&limit=' + encodeURIComponent(limit)),
        jfetch('/admin/api/activity?limit=300'),
        jfetch('/admin/api/cases'),
        jfetch('/admin/api/promocodes'),
        jfetch('/admin/api/payments'),
        jfetch('/admin/api/security'),
        jfetch('/admin/api/payments/orders?status=all&limit=200'),
        jfetch('/admin/api/config-history?limit=200'),
      ]);
      currentAdmin = me.admin || null;
      securityState = security || null;
      renderKpi(overview);
      renderUsers(users.users || []);
      renderActivity(activity.activity || []);
      renderCases(cases.cases || []);
      const caseSelect = document.getElementById('case-lab-case');
      const caseRows = Array.isArray(cases.cases) ? cases.cases : [];
      if (caseSelect) {
        const prev = String(caseSelect.value || '');
        caseSelect.innerHTML = caseRows.map((c) => '<option value="'+esc(c.caseName)+'">'+esc(c.caseName)+'</option>').join('');
        const nextValue = caseRows.some((c) => c.caseName === prev) ? prev : (caseRows[0]?.caseName || '');
        if (nextValue) {
          caseSelect.value = nextValue;
          const caseLab = await jfetch('/admin/api/case-lab/' + encodeURIComponent(nextValue));
          renderCaseLab(caseLab);
        }
      }
      renderPromoCodes(promoCodes.codes || []);
      renderPayments(payments.methods || []);
      renderPaymentCenter(paymentCenter.orders || []);
      renderConfigHistory(configHistory.entries || []);
      renderSecurityBlock();
      if (currentAdmin && currentAdmin.role === 'owner') {
        const admins = await jfetch('/admin/api/admins');
        renderAdmins(admins.admins || []);
      } else {
        renderAdmins([]);
      }
      setSection(targetSection);
      document.getElementById('admin-app').classList.remove('hidden');
      document.getElementById('login-box').classList.add('hidden');
    }

    async function checkSession(){
      try {
        await jfetch('/admin/api/overview');
        await loadDashboard({ preserveSection: false, section: 'overview' });
      } catch {
        document.getElementById('admin-app').classList.add('hidden');
        document.getElementById('login-box').classList.remove('hidden');
      }
    }

    async function handlePasswordLogin() {
      const login = document.getElementById('login').value.trim();
      const password = document.getElementById('password').value;
      const msg = document.getElementById('login-msg');
      msg.textContent = 'Вход...';
      try {
        const result = await jfetch('/admin/api/login', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ login, password }),
        });
        if (result?.requires2fa) {
          login2faChallenge = String(result.challengeToken || '');
          document.getElementById('login-stage-password').classList.add('hidden');
          document.getElementById('login-stage-2fa').classList.remove('hidden');
          document.getElementById('login-2fa-code').value = '';
          msg.textContent = 'Введите код 2FA';
          return;
        }
        msg.textContent = '';
        await loadDashboard({ preserveSection: false, section: 'overview' });
      } catch (e) {
        msg.textContent = 'Ошибка: ' + e.message;
      }
    }
    const loginForm = document.getElementById('login-form');
    if (loginForm) {
      loginForm.onsubmit = async (ev) => {
        ev.preventDefault();
        await handlePasswordLogin();
      };
    }
    document.getElementById('btn-login').onclick = async (ev) => {
      if (ev) ev.preventDefault();
      await handlePasswordLogin();
    };

    document.getElementById('btn-login-2fa-back').onclick = () => {
      login2faChallenge = '';
      document.getElementById('login-stage-2fa').classList.add('hidden');
      document.getElementById('login-stage-password').classList.remove('hidden');
      document.getElementById('login-msg').textContent = '';
    };

    document.getElementById('btn-login-2fa').onclick = async () => {
      const code = (document.getElementById('login-2fa-code').value || '').trim();
      const msg = document.getElementById('login-msg');
      msg.textContent = 'Проверяем код...';
      try {
        await jfetch('/admin/api/login/2fa', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ challengeToken: login2faChallenge, code }),
        });
        msg.textContent = '';
        await loadDashboard({ preserveSection: false, section: 'overview' });
      } catch (e) {
        msg.textContent = 'Ошибка: ' + e.message;
      }
    };

    document.getElementById('btn-refresh').onclick = () => {
      const progress = createProgressNotice('Обновляем данные...');
      loadDashboard().then(() => progress.done('Данные обновлены')).catch((e) => {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      });
    };
    document.getElementById('btn-search').onclick = () => {
      const progress = createProgressNotice('Выполняем поиск...');
      loadDashboard().then(() => progress.done('Поиск завершён')).catch((e) => {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      });
    };
    document.getElementById('btn-activity-filter').onclick = () => loadActivityOnly().catch((e)=>{
      showNotice('Ошибка: ' + e.message, 'bad', 3400);
    });
    document.getElementById('btn-activity-export').onclick = () => {
      const progress = createProgressNotice('Ваш отчет загружается...');
      exportActivityXlsx().then(() => {
        progress.done('Отчет сформирован и скачан');
      }).catch((e)=>{
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      });
    };
    document.querySelectorAll('.tab-btn[data-section]').forEach((btn) => {
      btn.onclick = () => setSection(btn.getAttribute('data-section') || 'overview');
    });
    const sectionSelect = document.getElementById('section-select');
    if (sectionSelect) {
      sectionSelect.onchange = () => setSection(sectionSelect.value || 'overview');
    }
    document.getElementById('btn-cases-save-all').onclick = async () => {
      const btn = document.getElementById('btn-cases-save-all');
      btn.disabled = true;
      const progress = createProgressNotice('Сохраняем черновики кейсов...');
      try {
        const rows = Array.from(document.querySelectorAll('#cases-table tbody tr'));
        for (const tr of rows) {
          const priceInput = tr.querySelector('input[data-case-price]');
          const enabledInput = tr.querySelector('input[data-case-enabled]');
          if (!enabledInput) continue;
          const key = enabledInput.getAttribute('data-case-enabled');
          const price = Number(priceInput ? priceInput.value : 0);
          const enabled = Boolean(enabledInput.checked);
          await jfetch('/admin/api/cases/' + encodeURIComponent(key), {
            method:'POST',
            headers:{'content-type':'application/json'},
            body: JSON.stringify({ price, enabled }),
          });
        }
        await loadDashboard();
        progress.done('Черновики кейсов сохранены');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    document.getElementById('btn-cases-publish-all').onclick = async () => {
      const btn = document.getElementById('btn-cases-publish-all');
      btn.disabled = true;
      const progress = createProgressNotice('Публикуем черновики...');
      try {
        await jfetch('/admin/api/case-drafts/publish', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({}) });
        await loadDashboard({ section: 'cases' });
        progress.done('Черновики опубликованы');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    const caseLabSelect = document.getElementById('case-lab-case');
    if (caseLabSelect) {
      caseLabSelect.onchange = () => {
        const progress = createProgressNotice('Загружаем кейс...');
        loadCaseLab(caseLabSelect.value, false).then(() => {
          progress.done('Кейс загружен');
        }).catch((e) => {
          progress.done('Ошибка: ' + e.message, 'bad', 3400);
        });
      };
    }
    document.getElementById('btn-case-lab-simulate').onclick = async () => {
      const btn = document.getElementById('btn-case-lab-simulate');
      const draft = collectCaseLabDraft();
      if (!draft.caseName) {
        showNotice('Выберите кейс', 'bad', 2600);
        return;
      }
      if (draft.dropTable && !updateCaseLabChanceSum().valid) {
        showNotice('Сумма шансов должна быть 100%', 'bad', 2800);
        return;
      }
      btn.disabled = true;
      const progress = createProgressNotice('Считаем симуляцию...');
      try {
        const data = await jfetch('/admin/api/case-lab/' + encodeURIComponent(draft.caseName) + '/simulate', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({
            spins: Number(document.getElementById('case-lab-spins')?.value || 1000),
            draft,
          }),
        });
        renderCaseLab({
          case: data.case,
          drops: data.drops,
          simulation: data.simulation,
        });
        progress.done('Симуляция готова');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    document.getElementById('btn-case-lab-sim-1000').onclick = async () => {
      const spinsInput = document.getElementById('case-lab-spins');
      if (spinsInput) spinsInput.value = '1000';
      document.getElementById('btn-case-lab-simulate').click();
    };
    document.getElementById('btn-case-lab-apply-preset').onclick = async () => {
      const btn = document.getElementById('btn-case-lab-apply-preset');
      btn.disabled = true;
      const progress = createProgressNotice('Применяем preset...');
      try {
        await applyCasePreset();
        const draft = collectCaseLabDraft();
        const sim = await jfetch('/admin/api/case-lab/' + encodeURIComponent(draft.caseName) + '/simulate', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ spins: Number(document.getElementById('case-lab-spins')?.value || 1000), draft }),
        });
        renderCaseLab({
          case: sim.case,
          drops: sim.drops,
          simulation: sim.simulation,
        });
        progress.done('Preset применен и пересчитан');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    document.getElementById('btn-case-lab-save').onclick = async () => {
      const btn = document.getElementById('btn-case-lab-save');
      const draft = collectCaseLabDraft();
      if (!draft.caseName) {
        showNotice('Выберите кейс', 'bad', 2600);
        return;
      }
      if (draft.dropTable && !updateCaseLabChanceSum().valid) {
        showNotice('Сумма шансов должна быть 100%', 'bad', 2800);
        return;
      }
      btn.disabled = true;
      const progress = createProgressNotice('Сохраняем кейс...');
      try {
        await jfetch('/admin/api/case-lab/' + encodeURIComponent(draft.caseName), {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify(draft),
        });
        await loadDashboard({ section: 'case-lab' });
        progress.done('Кейс и дроп-таблица сохранены');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    document.getElementById('btn-case-lab-publish').onclick = async () => {
      const btn = document.getElementById('btn-case-lab-publish');
      const draft = collectCaseLabDraft();
      if (!draft.caseName) {
        showNotice('Выберите кейс', 'bad', 2600);
        return;
      }
      btn.disabled = true;
      const progress = createProgressNotice('Публикуем кейс...');
      try {
        await jfetch('/admin/api/case-drafts/publish', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ caseName: draft.caseName }),
        });
        await loadDashboard({ section: 'case-lab' });
        progress.done('Кейс опубликован');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };
    document.getElementById('btn-user-card-load').onclick = async () => {
      const id = String(document.getElementById('user-card-id').value || '').trim();
      if (!id) return showNotice('Введите tg user id', 'bad', 2400);
      try {
        await loadUserCard(id);
      } catch (e) {
        showNotice('Ошибка: ' + e.message, 'bad', 3400);
      }
    };
    document.getElementById('btn-pc-load').onclick = () => {
      const progress = createProgressNotice('Загружаем платежи...');
      loadPaymentCenter().then(() => progress.done('Платежи загружены')).catch((e) => {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      });
    };
    document.getElementById('btn-audit-load').onclick = () => {
      const progress = createProgressNotice('Загружаем аудит...');
      loadConfigHistory().then(() => progress.done('Аудит загружен')).catch((e) => {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      });
    };
    document.getElementById('btn-logout').onclick = async () => {
      await jfetch('/admin/api/logout', { method:'POST' }).catch(()=>{});
      location.reload();
    };
    document.getElementById('btn-create-admin').onclick = async () => {
      const login = document.getElementById('new-admin-login').value.trim();
      const password = document.getElementById('new-admin-password').value;
      const msg = document.getElementById('admins-msg');
      msg.textContent = 'Создание...';
      try {
        await jfetch('/admin/api/admins', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ login, password }),
        });
        msg.innerHTML = '<span class="ok">OK:</span> admin created';
        showNotice('Админ успешно создан', 'ok');
        document.getElementById('new-admin-password').value = '';
        await loadDashboard();
      } catch (e) {
        msg.innerHTML = '<span class="bad">Ошибка:</span> ' + e.message;
        showNotice('Ошибка: ' + e.message, 'bad', 3400);
      }
    };
    document.getElementById('btn-adjust').onclick = async () => {
      const tgUserId = document.getElementById('adj-user').value.trim();
      const delta = Number(document.getElementById('adj-delta').value.trim());
      const reason = document.getElementById('adj-reason').value.trim();
      const msg = document.getElementById('adj-msg');
      msg.textContent = 'Сохраняем...';
      try {
        const data = await jfetch('/admin/api/users/' + encodeURIComponent(tgUserId) + '/balance', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ delta, reason }),
        });
        msg.innerHTML = '<span class="ok">OK:</span> balance = ' + data.user.balance;
        showNotice('Корректировка баланса применена', 'ok');
        await loadDashboard();
      } catch (e) {
        msg.innerHTML = '<span class="bad">Ошибка:</span> ' + e.message;
        showNotice('Ошибка: ' + e.message, 'bad', 3400);
      }
    };
    document.getElementById('btn-promo-save').onclick = async () => {
      const code = document.getElementById('promo-code').value.trim();
      const expiresLocal = document.getElementById('promo-expires').value;
      const enabled = Boolean(document.getElementById('promo-enabled').checked);
      if (!code) {
        showNotice('Введите промокод', 'bad', 2600);
        return;
      }
      if (!expiresLocal) {
        showNotice('Укажите срок действия', 'bad', 2600);
        return;
      }
      const expiresAt = new Date(expiresLocal).toISOString();
      const btn = document.getElementById('btn-promo-save');
      btn.disabled = true;
      const progress = createProgressNotice('Сохраняем промокод...');
      try {
        await jfetch('/admin/api/promocodes', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ code, expiresAt, enabled }),
        });
        document.getElementById('promo-code').value = '';
        await loadDashboard();
        progress.done('Промокод сохранен');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      } finally {
        btn.disabled = false;
      }
    };

    document.getElementById('btn-2fa-setup-start').onclick = async () => {
      const progress = createProgressNotice('Готовим 2FA...');
      try {
        const data = await jfetch('/admin/api/security/2fa/setup', { method:'POST' });
        securityState = data;
        renderSecurityBlock();
        progress.done('2FA секрет создан. Подтвердите код');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      }
    };

    document.getElementById('btn-2fa-enable').onclick = async () => {
      const code = String(document.getElementById('security-2fa-enable-code').value || '').trim();
      const progress = createProgressNotice('Включаем 2FA...');
      try {
        const data = await jfetch('/admin/api/security/2fa/enable', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ code }),
        });
        securityState = data;
        document.getElementById('security-2fa-enable-code').value = '';
        renderSecurityBlock();
        progress.done('2FA успешно включен');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      }
    };

    document.getElementById('btn-2fa-disable').onclick = async () => {
      const code = String(document.getElementById('security-2fa-disable-code').value || '').trim();
      const progress = createProgressNotice('Отключаем 2FA...');
      try {
        const data = await jfetch('/admin/api/security/2fa/disable', {
          method:'POST',
          headers:{'content-type':'application/json'},
          body: JSON.stringify({ code }),
        });
        securityState = data;
        document.getElementById('security-2fa-disable-code').value = '';
        renderSecurityBlock();
        progress.done('2FA отключен');
      } catch (e) {
        progress.done('Ошибка: ' + e.message, 'bad', 3400);
      }
    };

    checkSession();
  </script>
</body>
</html>`;
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

app.post('/api/user/settings', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const nextLang = String(req.body?.language || '').trim().toLowerCase();
  const allowedLanguages = new Set(['ru', 'uz', 'kk', 'en']);
  if (!allowedLanguages.has(nextLang)) {
    return res.status(400).json({ ok: false, error: 'language must be one of: ru, uz, kk, en' });
  }

  if (!auth.user.settings || typeof auth.user.settings !== 'object') auth.user.settings = { language: 'ru' };
  auth.user.settings.language = nextLang;
  auth.user.updatedAt = nowIso();
  if (!await persistDbOr503(res, auth.db)) return;

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

  const casePriceMap = getCasePriceMap(auth.db);
  const caseEnabledMap = {};
  for (const [name, cfg] of Object.entries(auth.db.caseConfig || {})) caseEnabledMap[name] = Boolean(cfg?.enabled !== false);
  const paymentMethods = {};
  for (const [methodKey, cfg] of Object.entries(auth.db.paymentConfig?.methods || {})) {
    paymentMethods[methodKey] = {
      title: String(cfg?.title || methodKey),
      enabled: Boolean(cfg?.enabled !== false),
      minAmount: Math.floor(Number(cfg?.minAmount || 1)),
      maxAmount: Math.floor(Number(cfg?.maxAmount || 5000)),
    };
  }
  const activePromo = getActivePromo(auth.db);

  res.json({
    ok: true,
    user: presentUser(auth.user),
    inventory: inv,
    openHistory,
    inventoryCount: inv.length,
    inventoryUsdValue: inv.reduce((s, x) => s + Number(x.priceUsd || 0), 0),
    casePrices: casePriceMap,
    caseEnabled: caseEnabledMap,
    payments: { methods: paymentMethods },
    promo: {
      serverNowMs: Date.now(),
      active: activePromo,
    },
  });
});

app.post('/api/cases/open', async (req, res) => {
  const auth = await ensureAuthed(req, res);
  if (!auth) return;

  const caseName = String(req.body?.caseName || '');
  const count = Number(req.body?.count || 1);

  if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName)) {
    return res.status(400).json({ ok: false, error: 'unknown case' });
  }
  const caseCfg = auth.db.caseConfig?.[caseName] || null;
  if (!caseCfg) return res.status(400).json({ ok: false, error: 'case config missing' });
  if (caseCfg.enabled === false) return res.status(400).json({ ok: false, error: 'case is disabled' });
  if (!Number.isInteger(count) || count < 1 || count > 10) {
    return res.status(400).json({ ok: false, error: 'count must be 1..10' });
  }

  const casePrice = Number(caseCfg.price || 0);
  const totalPrice = casePrice * count;
  if (caseName !== 'free' && auth.user.balance < totalPrice) {
    return res.status(400).json({ ok: false, error: 'not enough balance' });
  }

  if (caseName !== 'free') auth.user.balance -= totalPrice;

  const winners = [];
  for (let i = 0; i < count; i++) {
    const item = makeInventoryItem(pickSkinByCaseConfig(caseCfg));
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

    normalizePaymentConfig(auth.db);
    const starsCfg = auth.db.paymentConfig?.methods?.telegram_stars || sanitizePaymentMethodConfig({}, defaultPaymentMethods.telegram_stars);
    if (starsCfg.enabled === false) {
      return res.status(400).json({ ok: false, error: 'telegram stars is disabled' });
    }
    const minAmount = Math.max(1, Math.floor(Number(starsCfg.minAmount || 1)));
    const maxAmount = Math.max(minAmount, Math.floor(Number(starsCfg.maxAmount || 5000)));
    const amount = Number(req.body?.amount || 0);
    if (!Number.isInteger(amount) || amount < minAmount || amount > maxAmount) {
      return res.status(400).json({ ok: false, error: `amount must be integer ${minAmount}..${maxAmount}` });
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

const ADMIN_PAGE_PATHS = Array.from(new Set([ADMIN_PATH, '/admin', '/vault-admin']));
app.get(ADMIN_PAGE_PATHS, (req, res) => {
  if (!isAdminConfigured()) return res.status(404).send('Not Found');
  const cookies = parseCookies(req);
  const parsed = parseAdminSession(cookies[ADMIN_COOKIE] || '');
  try {
    const html = renderAdminPage({
      loginErrorMessage: normalizeAdminLoginError(req.query?.login_error || ''),
    });
    if (!parsed.ok) {
      return res.status(200).type('html').send(html);
    }
    return res.status(200).type('html').send(html);
  } catch (e) {
    console.error('[admin] render page failed:', e);
    const fallbackError = normalizeAdminLoginError(req.query?.login_error || '');
    const fallbackHtml = `<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VAULT Admin Login</title>
  <style>
    body{margin:0;background:#0b0f16;color:#e7ecf5;font:14px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif}
    .box{max-width:420px;margin:64px auto;padding:16px;border:1px solid #2a3550;border-radius:12px;background:#121826}
    input{width:100%;height:40px;background:#101827;color:#e7ecf5;border:1px solid #2a3550;border-radius:10px;padding:0 12px;margin-bottom:10px}
    button{width:100%;height:42px;border-radius:10px;border:1px solid #4f77bf;background:#2b5da4;color:#fff;font-weight:700;cursor:pointer}
    .hint{margin-bottom:10px;color:#96a1b8}
  </style>
</head>
<body>
  <div class="box">
    <h2 style="margin:0 0 6px">VAULT Admin Login</h2>
    <div class="hint">Аварийный вход (fallback). После входа откроется админка.</div>
    <form method="POST" action="/admin/login">
      <input name="login" placeholder="login" autocomplete="username">
      <input name="password" placeholder="password" type="password" autocomplete="current-password">
      <button type="submit">Войти</button>
    </form>
    <div style="margin-top:10px;color:#ff9f9f">${escapeHtml(fallbackError)}</div>
  </div>
</body>
</html>`;
    return res.status(200).type('html').send(fallbackHtml);
  }
});

const ADMIN_FORM_LOGIN_PATHS = Array.from(new Set([
  `${ADMIN_PATH}/login`,
  '/admin/login',
  '/vault-admin/login',
]));
app.post(ADMIN_FORM_LOGIN_PATHS, async (req, res) => {
  if (!isAdminConfigured()) return res.status(404).send('Not Found');
  const sourcePath = String(req.path || '/admin/login');
  const adminPagePath = sourcePath.endsWith('/login') ? sourcePath.slice(0, -6) : '/admin';
  const safeTarget = ADMIN_PAGE_PATHS.includes(adminPagePath) ? adminPagePath : '/admin';
  const login = String(req.body?.login || '').trim();
  const password = String(req.body?.password || '');
  const failRedirect = (message) => {
    const query = message ? `?login_error=${encodeURIComponent(String(message || '').slice(0, 120))}` : '';
    return res.redirect(302, `${safeTarget}${query}`);
  };
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[login] || null;
    if (!account || !verifyAdminPassword(password, account)) {
      return failRedirect('Invalid credentials');
    }
    if (account.twoFactorEnabled) {
      return failRedirect('2FA для этого аккаунта: войдите через JS-форму');
    }
    const token = makeAdminSessionToken(account.login, account.role);
    res.setHeader('Set-Cookie', buildAdminCookie(token, req));
    return res.redirect(302, safeTarget);
  } catch (e) {
    console.error('[admin] form login failed:', e);
    return failRedirect('Storage temporarily unavailable');
  }
});

app.post('/admin/api/login', async (req, res) => {
  if (!isAdminConfigured()) return res.status(404).json({ ok: false, error: 'not found' });
  const login = String(req.body?.login || '').trim();
  const password = String(req.body?.password || '');
  const ip = getAdminIp(req);
  const ua = String(req.get('user-agent') || '').slice(0, 180);
  try {
    const rate = getAdminAuthRateStatus(login, req);
    if (rate.blocked) {
      res.setHeader('Retry-After', String(rate.retryAfterSec));
      return res.status(429).json({ ok: false, error: `Too many attempts. Retry in ${rate.retryAfterSec}s` });
    }
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[login] || null;
    if (!account || !verifyAdminPassword(password, account)) {
      const fail = recordAdminAuthFailure(login, req);
      appendAdminAudit(db, {
        action: fail.blocked ? 'login_blocked' : 'login_failed',
        reason: `bad_password ip=${ip} ua=${ua}`.slice(0, 350),
        adminLogin: login || 'unknown',
      });
      await writeDb(db);
      if (fail.blocked) {
        res.setHeader('Retry-After', String(fail.retryAfterSec));
        return res.status(429).json({ ok: false, error: `Too many attempts. Retry in ${fail.retryAfterSec}s` });
      }
      return res.status(401).json({ ok: false, error: 'Invalid credentials' });
    }
    if (account.twoFactorEnabled) {
      const challengeToken = makeAdmin2faChallenge(account.login, req);
      appendAdminAudit(db, {
        action: 'login_2fa_required',
        reason: `ip=${ip} ua=${ua}`.slice(0, 350),
        adminLogin: account.login,
      });
      await writeDb(db);
      return res.json({ ok: true, requires2fa: true, challengeToken });
    }
    clearAdminAuthFailures(account.login, req);
    appendAdminAudit(db, {
      action: 'login_success',
      reason: `password_ok ip=${ip} ua=${ua}`.slice(0, 350),
      adminLogin: account.login,
    });
    await writeDb(db);
    const token = makeAdminSessionToken(account.login, account.role);
    res.setHeader('Set-Cookie', buildAdminCookie(token, req));
    return res.json({ ok: true, admin: { login: account.login, role: account.role } });
  } catch (e) {
    console.error('[admin] login failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/login/2fa', async (req, res) => {
  if (!isAdminConfigured()) return res.status(404).json({ ok: false, error: 'not found' });
  const challengeToken = String(req.body?.challengeToken || '');
  const code = String(req.body?.code || '');
  const parsed = parseAdmin2faChallenge(challengeToken, req);
  if (!parsed.ok) return res.status(401).json({ ok: false, error: '2FA challenge expired. Login again.' });
  const ip = getAdminIp(req);
  const ua = String(req.get('user-agent') || '').slice(0, 180);
  try {
    const rate = getAdminAuthRateStatus(parsed.login, req);
    if (rate.blocked) {
      res.setHeader('Retry-After', String(rate.retryAfterSec));
      return res.status(429).json({ ok: false, error: `Too many attempts. Retry in ${rate.retryAfterSec}s` });
    }
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[parsed.login] || null;
    if (!account || !account.twoFactorEnabled) {
      appendAdminAudit(db, {
        action: 'login_2fa_failed',
        reason: `account_invalid ip=${ip} ua=${ua}`.slice(0, 350),
        adminLogin: parsed.login || 'unknown',
      });
      await writeDb(db);
      return res.status(401).json({ ok: false, error: '2FA is not enabled for this account' });
    }
    const secret = decryptSecret(account.twoFactorSecretEncrypted);
    if (!secret || !verifyTotpCode(secret, code)) {
      const fail = recordAdminAuthFailure(parsed.login, req);
      appendAdminAudit(db, {
        action: fail.blocked ? 'login_blocked' : 'login_2fa_failed',
        reason: `bad_2fa ip=${ip} ua=${ua}`.slice(0, 350),
        adminLogin: account.login,
      });
      await writeDb(db);
      if (fail.blocked) {
        res.setHeader('Retry-After', String(fail.retryAfterSec));
        return res.status(429).json({ ok: false, error: `Too many attempts. Retry in ${fail.retryAfterSec}s` });
      }
      return res.status(401).json({ ok: false, error: 'Invalid 2FA code' });
    }
    clearAdminAuthFailures(account.login, req);
    appendAdminAudit(db, {
      action: 'login_2fa_success',
      reason: `2fa_ok ip=${ip} ua=${ua}`.slice(0, 350),
      adminLogin: account.login,
    });
    await writeDb(db);
    const token = makeAdminSessionToken(account.login, account.role);
    res.setHeader('Set-Cookie', buildAdminCookie(token, req));
    return res.json({ ok: true, admin: { login: account.login, role: account.role } });
  } catch (e) {
    console.error('[admin] login 2fa failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/logout', async (req, res) => {
  if (!isAdminConfigured()) return res.status(404).json({ ok: false, error: 'not found' });
  try {
    const token = parseCookies(req)[ADMIN_COOKIE] || '';
    const parsed = parseAdminSession(token);
    if (parsed.ok) {
      const db = await readDb();
      cleanup(db);
      normalizeAllConfig(db);
      appendAdminAudit(db, {
        action: 'logout',
        reason: `ip=${getAdminIp(req)} ua=${String(req.get('user-agent') || '').slice(0, 180)}`.slice(0, 350),
        adminLogin: parsed.login,
      });
      await writeDb(db);
    }
  } catch (e) {
    console.error('[admin] logout audit failed:', e);
  }
  res.setHeader('Set-Cookie', buildAdminCookieClear(req));
  return res.json({ ok: true });
});

app.get('/admin/api/me', ensureAdminAuth(), async (req, res) => {
  return res.json({
    ok: true,
    admin: {
      login: req.admin.login,
      role: req.admin.role,
      twoFactorEnabled: Boolean(req.admin.twoFactorEnabled),
      canManageAdmins: req.admin.role === 'owner',
    },
  });
});

app.get('/admin/api/security', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[req.admin.login] || null;
    if (!account) return res.status(404).json({ ok: false, error: 'admin not found' });
    const pendingSetup = account.twoFactorPending ? {
      secret: String(account.twoFactorPending.secret || ''),
      otpauth: String(account.twoFactorPending.otpauth || ''),
      createdAt: account.twoFactorPending.createdAt || null,
    } : null;
    return res.json({
      ok: true,
      login: account.login,
      twoFactorEnabled: Boolean(account.twoFactorEnabled),
      twoFactorEnabledAt: account.twoFactorEnabledAt || null,
      pendingSetup,
    });
  } catch (e) {
    console.error('[admin] security state failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/security/2fa/setup', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[req.admin.login] || null;
    if (!account) return res.status(404).json({ ok: false, error: 'admin not found' });
    if (account.twoFactorEnabled) {
      return res.status(400).json({ ok: false, error: '2FA is already enabled' });
    }
    const secret = generateTotpSecret();
    const otpauth = makeOtpAuthUri(account.login, secret);
    account.twoFactorPending = { secret, otpauth, createdAt: nowIso() };
    account.updatedAt = nowIso();
    appendAdminAudit(db, {
      action: '2fa_setup_start',
      reason: `ip=${getAdminIp(req)} ua=${String(req.get('user-agent') || '').slice(0, 180)}`.slice(0, 350),
      adminLogin: account.login,
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({
      ok: true,
      login: account.login,
      twoFactorEnabled: false,
      twoFactorEnabledAt: null,
      pendingSetup: account.twoFactorPending,
    });
  } catch (e) {
    console.error('[admin] 2fa setup failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/security/2fa/enable', ensureAdminAuth(), async (req, res) => {
  try {
    const code = String(req.body?.code || '');
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[req.admin.login] || null;
    if (!account) return res.status(404).json({ ok: false, error: 'admin not found' });
    const pending = account.twoFactorPending || null;
    if (!pending?.secret) return res.status(400).json({ ok: false, error: 'Start 2FA setup first' });
    if (!verifyTotpCode(String(pending.secret), code)) {
      return res.status(400).json({ ok: false, error: 'Invalid authenticator code' });
    }
    account.twoFactorEnabled = true;
    account.twoFactorEnabledAt = nowIso();
    account.twoFactorSecretEncrypted = encryptSecret(String(pending.secret));
    account.twoFactorPending = null;
    account.updatedAt = nowIso();
    appendAdminAudit(db, {
      action: '2fa_enabled',
      reason: `ip=${getAdminIp(req)} ua=${String(req.get('user-agent') || '').slice(0, 180)}`.slice(0, 350),
      adminLogin: account.login,
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({
      ok: true,
      login: account.login,
      twoFactorEnabled: true,
      twoFactorEnabledAt: account.twoFactorEnabledAt,
      pendingSetup: null,
    });
  } catch (e) {
    console.error('[admin] 2fa enable failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/security/2fa/disable', ensureAdminAuth(), async (req, res) => {
  try {
    const code = String(req.body?.code || '');
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const account = db.admins?.[req.admin.login] || null;
    if (!account) return res.status(404).json({ ok: false, error: 'admin not found' });
    if (!account.twoFactorEnabled) return res.status(400).json({ ok: false, error: '2FA is already disabled' });
    const secret = decryptSecret(account.twoFactorSecretEncrypted);
    if (!secret || !verifyTotpCode(secret, code)) {
      return res.status(400).json({ ok: false, error: 'Invalid authenticator code' });
    }
    account.twoFactorEnabled = false;
    account.twoFactorEnabledAt = null;
    account.twoFactorSecretEncrypted = null;
    account.twoFactorPending = null;
    account.updatedAt = nowIso();
    appendAdminAudit(db, {
      action: '2fa_disabled',
      reason: `ip=${getAdminIp(req)} ua=${String(req.get('user-agent') || '').slice(0, 180)}`.slice(0, 350),
      adminLogin: account.login,
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({
      ok: true,
      login: account.login,
      twoFactorEnabled: false,
      twoFactorEnabledAt: null,
      pendingSetup: null,
    });
  } catch (e) {
    console.error('[admin] 2fa disable failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/overview', ensureAdminAuth(), async (_req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const users = Object.values(db.users || {});
    const totalUsers = users.length;
    const totalBalance = users.reduce((s, u) => s + Number(u.balance || 0), 0);
    const totalCasesOpened = users.reduce((s, u) => s + Number(u?.stats?.casesOpened || 0), 0);
    const paidOrders = Object.values(db.orders || {}).filter((x) => x?.status === 'paid').length;
    const activeCases = Object.values(db.caseConfig || {}).filter((x) => x?.enabled !== false).length;
    const funnel = buildFunnelAnalytics(db);
    const storage = USE_MONGO ? 'mongo' : (USE_KV ? 'kv' : 'file-ephemeral');
    return res.json({
      ok: true,
      storage,
      adminPath: ADMIN_PATH,
      kpi: { totalUsers, totalBalance, totalCasesOpened, paidOrders, activeCases },
      funnel,
    });
  } catch (e) {
    console.error('[admin] overview failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/users', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const q = String(req.query.q || '').trim().toLowerCase();
    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 100)));
    let rows = Object.entries(db.users || {}).map(([k, user]) => presentAdminUser(user, k));
    if (q) {
      rows = rows.filter((u) =>
        String(u.tgUserId).includes(q) ||
        String(u.username || '').toLowerCase().includes(q) ||
        String(u.name || '').toLowerCase().includes(q)
      );
    }
    rows.sort((a, b) => new Date(b.updatedAt || 0).getTime() - new Date(a.updatedAt || 0).getTime());
    return res.json({ ok: true, users: rows.slice(0, limit), total: rows.length });
  } catch (e) {
    console.error('[admin] users failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/users/:tgUserId/card', ensureAdminAuth(), async (req, res) => {
  try {
    const tgUserId = String(req.params.tgUserId || '').trim();
    if (!/^\d+$/.test(tgUserId)) return res.status(400).json({ ok: false, error: 'tgUserId must be numeric' });
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const user = db.users?.[tgUserId] || null;
    if (!user) return res.status(404).json({ ok: false, error: 'user not found' });
    ensureUserShape(user);

    const orders = Object.values(db.orders || {}).filter((x) => String(x?.tgUserId || '') === tgUserId);
    const paidOrders = orders.filter((x) => getOrderStatus(x) === 'paid');
    const profile = presentAdminUser(user, tgUserId);
    const history = Array.isArray(user.openHistory) ? user.openHistory : [];
    const caseOpens = history.filter((x) => String(x?.caseName || '') !== 'upgrade');
    const upgrades = history.filter((x) => String(x?.caseName || '') === 'upgrade');
    const topDrops = [...history]
      .map((x) => x?.item || null)
      .filter(Boolean)
      .sort((a, b) => Number(b.priceUsd || 0) - Number(a.priceUsd || 0))
      .slice(0, 5)
      .map((x) => ({ name: x.name, rarity: x.rarity, priceUsd: Number(x.priceUsd || 0), openedAt: x.openedAt || null }));
    const adminActions = (db.adminAudit || [])
      .filter((x) => String(x?.tgUserId || '') === tgUserId)
      .sort((a, b) => new Date(b.ts || 0).getTime() - new Date(a.ts || 0).getTime())
      .slice(0, 60);

    return res.json({
      ok: true,
      profile,
      metrics: {
        depositsCount: paidOrders.length,
        depositsStars: paidOrders.reduce((s, x) => s + Number(x.amount || 0), 0),
        ordersTotal: orders.length,
        opensCount: caseOpens.length,
        upgradesCount: upgrades.length,
        inventoryCount: Array.isArray(user.inventory) ? user.inventory.length : 0,
      },
      topDrops,
      adminActions,
    });
  } catch (e) {
    console.error('[admin] user card failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/activity', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const limit = Math.max(1, Math.min(5000, Number(req.query.limit || 200)));
    const kind = String(req.query.kind || '').trim().toLowerCase();
    const type = String(req.query.type || '').trim().toLowerCase();
    const userId = String(req.query.userId || '').trim();
    const q = String(req.query.q || '').trim().toLowerCase();
    const from = String(req.query.from || '').trim();
    const to = String(req.query.to || '').trim();
    let activity = buildAdminActivity(db, 6000);
    if (kind && kind !== 'all') activity = activity.filter((x) => matchesActivityKind(x, kind));
    if (type) activity = activity.filter((x) => String(x.type || '').toLowerCase().includes(type));
    if (userId) activity = activity.filter((x) => String(x.tgUserId || '') === userId);
    if (q) {
      activity = activity.filter((x) =>
        String(x.type || '').toLowerCase().includes(q) ||
        String(x.userLabel || '').toLowerCase().includes(q) ||
        String(x.details || '').toLowerCase().includes(q)
      );
    }
    if (from) {
      const fromTs = new Date(from).getTime();
      if (!Number.isNaN(fromTs)) activity = activity.filter((x) => new Date(x.ts || 0).getTime() >= fromTs);
    }
    if (to) {
      const toTs = new Date(to).getTime();
      if (!Number.isNaN(toTs)) activity = activity.filter((x) => new Date(x.ts || 0).getTime() <= toTs);
    }
    return res.json({ ok: true, activity: activity.slice(0, limit) });
  } catch (e) {
    console.error('[admin] activity failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/users/:tgUserId/balance', ensureAdminAuth(), async (req, res) => {
  try {
    const tgUserId = String(req.params.tgUserId || '').trim();
    const delta = Number(req.body?.delta || 0);
    const reason = String(req.body?.reason || '').trim();
    if (!/^\d+$/.test(tgUserId)) return res.status(400).json({ ok: false, error: 'tgUserId must be numeric' });
    if (!Number.isFinite(delta) || !Number.isInteger(delta)) return res.status(400).json({ ok: false, error: 'delta must be integer' });
    if (!reason || reason.length < 3) return res.status(400).json({ ok: false, error: 'reason is required' });

    const db = await readDb();
    cleanup(db);
    if (!db.users[tgUserId]) db.users[tgUserId] = makeDefaultUser({ id: Number(tgUserId) });
    ensureUserShape(db.users[tgUserId]);
    const prevBalance = Number(db.users[tgUserId].balance || 0);
    const nextBalance = prevBalance + delta;
    if (nextBalance < 0) return res.status(400).json({ ok: false, error: 'resulting balance cannot be negative' });
    db.users[tgUserId].balance = nextBalance;
    db.users[tgUserId].updatedAt = nowIso();
    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'balance_adjust',
      tgUserId: Number(tgUserId),
      delta,
      prevBalance,
      nextBalance,
      reason,
      adminLogin: req.admin?.login || 'admin',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    if (!await persistDbOr503(res, db)) return;

    return res.json({ ok: true, user: presentAdminUser(db.users[tgUserId], tgUserId) });
  } catch (e) {
    console.error('[admin] balance adjust failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/cases', ensureAdminAuth(), async (_req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    const changed = normalizeAllConfig(db);
    const items = Object.entries(db.caseConfig || {}).map(([caseName, cfg]) => ({
      caseName,
      price: Number(cfg?.price || 0),
      enabled: Boolean(cfg?.enabled !== false),
      hasCustomDropTable: Boolean(normalizeCaseDropTable(cfg?.dropTable).table),
      hasDraft: Boolean(db.caseDrafts?.cases?.[caseName]),
      draftPrice: Number(db.caseDrafts?.cases?.[caseName]?.price || 0),
      draftEnabled: Boolean(db.caseDrafts?.cases?.[caseName]?.enabled !== false),
      updatedAt: cfg?.updatedAt || null,
      updatedBy: cfg?.updatedBy || null,
      draftUpdatedAt: db.caseDrafts?.cases?.[caseName]?.updatedAt || null,
      draftUpdatedBy: db.caseDrafts?.cases?.[caseName]?.updatedBy || null,
    })).sort((a, b) => a.caseName.localeCompare(b.caseName));
    if (changed) await writeDb(db);
    return res.json({ ok: true, cases: items });
  } catch (e) {
    console.error('[admin] cases list failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/cases/:caseName', ensureAdminAuth(), async (req, res) => {
  try {
    const caseName = String(req.params.caseName || '').trim();
    if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName)) {
      return res.status(400).json({ ok: false, error: 'unknown case' });
    }
    const nextPriceRaw = Number(req.body?.price);
    const nextEnabled = req.body?.enabled;
    if (!Number.isInteger(nextPriceRaw) || nextPriceRaw < 0) {
      return res.status(400).json({ ok: false, error: 'price must be integer >= 0' });
    }
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const beforeDraft = cloneJson(db.caseDrafts?.cases?.[caseName] || null);
    const currentPublished = db.caseConfig?.[caseName] || null;
    const draft = {
      caseName,
      price: caseName === 'free' ? 0 : Number(nextPriceRaw),
      enabled: Boolean(nextEnabled),
      dropTable: normalizeCaseDropTable(beforeDraft?.dropTable || currentPublished?.dropTable).table,
      updatedAt: nowIso(),
      updatedBy: req.admin?.login || 'admin',
    };
    db.caseDrafts.cases[caseName] = draft;
    db.caseDrafts.updatedAt = nowIso();
    db.caseDrafts.updatedBy = req.admin?.login || 'admin';
    appendAdminAudit(db, {
      action: 'case_draft_update',
      reason: `${caseName} draft price=${draft.price} enabled=${draft.enabled}`,
      adminLogin: req.admin?.login || 'admin',
    });
    appendConfigHistory(db, {
      entityType: 'case_draft',
      entityKey: caseName,
      action: 'draft_update',
      before: beforeDraft,
      after: draft,
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({
      ok: true,
      draft: {
        caseName,
        price: draft.price,
        enabled: draft.enabled,
        updatedAt: draft.updatedAt,
        updatedBy: draft.updatedBy,
      },
    });
  } catch (e) {
    console.error('[admin] case update failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/case-drafts/publish', ensureAdminAuth(), async (req, res) => {
  try {
    const onlyCaseName = req.body?.caseName ? String(req.body.caseName).trim() : '';
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const entries = Object.entries(db.caseDrafts?.cases || {})
      .filter(([name]) => !onlyCaseName || name === onlyCaseName);
    if (!entries.length) return res.status(400).json({ ok: false, error: 'no drafts to publish' });

    const published = [];
    for (const [caseName, draft] of entries) {
      if (!db.caseConfig?.[caseName]) continue;
      const before = cloneJson(db.caseConfig[caseName]);
      db.caseConfig[caseName].price = caseName === 'free' ? 0 : Math.max(0, Math.floor(Number(draft.price || 0)));
      db.caseConfig[caseName].enabled = Boolean(draft.enabled !== false);
      db.caseConfig[caseName].dropTable = normalizeCaseDropTable(draft.dropTable).table;
      db.caseConfig[caseName].updatedAt = nowIso();
      db.caseConfig[caseName].updatedBy = req.admin?.login || 'admin';
      const after = cloneJson(db.caseConfig[caseName]);
      appendConfigHistory(db, {
        entityType: 'case_config',
        entityKey: caseName,
        action: 'publish',
        before,
        after,
        createdBy: req.admin?.login || 'admin',
      });
      appendAdminAudit(db, {
        action: 'case_publish',
        reason: `${caseName} price=${after.price} enabled=${after.enabled} custom=${Boolean(after.dropTable)}`,
        adminLogin: req.admin?.login || 'admin',
      });
      delete db.caseDrafts.cases[caseName];
      published.push(caseName);
    }
    db.caseDrafts.updatedAt = nowIso();
    db.caseDrafts.updatedBy = req.admin?.login || 'admin';
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, published });
  } catch (e) {
    console.error('[admin] case draft publish failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/case-lab/:caseName', ensureAdminAuth(), async (req, res) => {
  try {
    const caseName = String(req.params.caseName || '').trim();
    if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName)) {
      return res.status(400).json({ ok: false, error: 'unknown case' });
    }
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const publishedCfg = db.caseConfig?.[caseName] || null;
    if (!publishedCfg) return res.status(404).json({ ok: false, error: 'case config missing' });
    const draftCfg = db.caseDrafts?.cases?.[caseName] || null;
    const effectiveCfg = draftCfg || publishedCfg;
    const dropRows = getCaseDropTableRows(effectiveCfg);
    return res.json({
      ok: true,
      case: {
        caseName,
        price: Number(effectiveCfg.price || 0),
        enabled: Boolean(effectiveCfg.enabled !== false),
        useCustomDropTable: dropRows.useCustom,
        updatedAt: effectiveCfg.updatedAt || null,
        updatedBy: effectiveCfg.updatedBy || null,
        draftExists: Boolean(draftCfg),
      },
      publishedCase: cloneJson(publishedCfg),
      draftCase: cloneJson(draftCfg),
      drops: {
        totalWeight: Number(dropRows.totalWeight || 0),
        rows: dropRows.rows,
      },
      skins: skinPool.map((s) => ({
        key: s.key,
        name: s.name,
        rarity: s.rarity,
        priceUsd: Number(s.priceUsd || 0),
        image: s.image || null,
      })),
    });
  } catch (e) {
    console.error('[admin] case lab get failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/case-lab/:caseName', ensureAdminAuth(), async (req, res) => {
  try {
    const caseName = String(req.params.caseName || '').trim();
    if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName)) {
      return res.status(400).json({ ok: false, error: 'unknown case' });
    }
    const nextPriceRaw = Number(req.body?.price);
    const nextEnabled = req.body?.enabled;
    if (!Number.isInteger(nextPriceRaw) || nextPriceRaw < 0) {
      return res.status(400).json({ ok: false, error: 'price must be integer >= 0' });
    }
    if (typeof nextEnabled !== 'boolean') {
      return res.status(400).json({ ok: false, error: 'enabled must be boolean' });
    }
    const parsedDropTable = normalizeCaseDropTable(req.body?.dropTable);

    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    if (!db.caseConfig?.[caseName]) return res.status(404).json({ ok: false, error: 'case config missing' });
    const beforeDraft = cloneJson(db.caseDrafts?.cases?.[caseName] || null);
    db.caseDrafts.cases[caseName] = {
      caseName,
      price: caseName === 'free' ? 0 : Number(nextPriceRaw),
      enabled: Boolean(nextEnabled),
      dropTable: parsedDropTable.table,
      updatedAt: nowIso(),
      updatedBy: req.admin?.login || 'admin',
    };
    db.caseDrafts.updatedAt = nowIso();
    db.caseDrafts.updatedBy = req.admin?.login || 'admin';
    const sim = runCaseSimulation(db.caseDrafts.cases[caseName], 1000);
    appendAdminAudit(db, {
      action: 'case_lab_draft_update',
      reason: `${caseName} draft price=${db.caseDrafts.cases[caseName].price} enabled=${db.caseDrafts.cases[caseName].enabled} custom=${Boolean(parsedDropTable.table)} rtp=${sim.rtpPct}%`,
      adminLogin: req.admin?.login || 'admin',
    });
    appendConfigHistory(db, {
      entityType: 'case_draft',
      entityKey: caseName,
      action: 'draft_update',
      before: beforeDraft,
      after: db.caseDrafts.cases[caseName],
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    const dropRows = getCaseDropTableRows(db.caseDrafts.cases[caseName]);
    return res.json({
      ok: true,
      case: {
        caseName,
        price: Number(db.caseDrafts.cases[caseName].price || 0),
        enabled: Boolean(db.caseDrafts.cases[caseName].enabled !== false),
        useCustomDropTable: dropRows.useCustom,
        updatedAt: db.caseDrafts.cases[caseName].updatedAt || null,
        updatedBy: db.caseDrafts.cases[caseName].updatedBy || null,
        draftExists: true,
      },
      drops: {
        totalWeight: Number(dropRows.totalWeight || 0),
        rows: dropRows.rows,
      },
      simulation: sim,
    });
  } catch (e) {
    console.error('[admin] case lab save failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/case-lab/:caseName/simulate', ensureAdminAuth(), async (req, res) => {
  try {
    const caseName = String(req.params.caseName || '').trim();
    if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, caseName)) {
      return res.status(400).json({ ok: false, error: 'unknown case' });
    }
    const spins = Number(req.body?.spins || 1000);
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const baseCfg = db.caseDrafts?.cases?.[caseName] || db.caseConfig?.[caseName] || null;
    if (!baseCfg) return res.status(404).json({ ok: false, error: 'case config missing' });
    const draft = req.body?.draft && typeof req.body.draft === 'object' ? req.body.draft : {};
    const price = Number.isInteger(Number(draft.price)) && Number(draft.price) >= 0
      ? Number(draft.price)
      : Number(baseCfg.price || 0);
    const enabled = typeof draft.enabled === 'boolean' ? draft.enabled : Boolean(baseCfg.enabled !== false);
    const dropTable = normalizeCaseDropTable(draft.dropTable ?? baseCfg.dropTable).table;
    const cfg = { ...baseCfg, price: caseName === 'free' ? 0 : price, enabled, dropTable };
    const dropRows = getCaseDropTableRows(cfg);
    const simulation = runCaseSimulation(cfg, spins);
    return res.json({
      ok: true,
      case: {
        caseName,
        price: Number(cfg.price || 0),
        enabled: Boolean(cfg.enabled !== false),
        useCustomDropTable: dropRows.useCustom,
      },
      drops: {
        totalWeight: Number(dropRows.totalWeight || 0),
        rows: dropRows.rows,
      },
      simulation,
    });
  } catch (e) {
    console.error('[admin] case lab simulate failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/case-lab-presets', ensureAdminAuth(), async (_req, res) => {
  const presets = Object.values(CASE_ECON_PRESETS).map((x) => ({
    key: x.key,
    title: x.title,
    description: x.description,
  }));
  return res.json({ ok: true, presets });
});

app.post('/admin/api/case-lab-preset/build', ensureAdminAuth(), async (req, res) => {
  try {
    const presetKey = String(req.body?.presetKey || 'balanced').trim().toLowerCase();
    const priceStars = Number(req.body?.priceStars || 0);
    if (!Number.isFinite(priceStars) || priceStars < 0) {
      return res.status(400).json({ ok: false, error: 'priceStars must be >= 0' });
    }
    if (!CASE_ECON_PRESETS[presetKey]) {
      return res.status(400).json({ ok: false, error: 'unknown presetKey' });
    }
    const dropTable = buildPresetDropTable(presetKey, priceStars);
    const dropRows = getCaseDropTableRows({ dropTable });
    return res.json({
      ok: true,
      preset: {
        key: presetKey,
        title: CASE_ECON_PRESETS[presetKey].title,
        description: CASE_ECON_PRESETS[presetKey].description,
      },
      dropTable,
      drops: {
        totalWeight: Number(dropRows.totalWeight || 0),
        rows: dropRows.rows,
      },
    });
  } catch (e) {
    console.error('[admin] case lab preset build failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/promocodes', ensureAdminAuth(), async (_req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    const changed = normalizeAllConfig(db);
    const codes = Object.values(db.promoConfig?.codes || {})
      .map((cfg) => ({
        code: cfg.code,
        enabled: Boolean(cfg.enabled !== false),
        expiresAt: cfg.expiresAt,
        createdAt: cfg.createdAt || null,
        createdBy: cfg.createdBy || null,
        updatedAt: cfg.updatedAt || null,
        updatedBy: cfg.updatedBy || null,
      }))
      .sort((a, b) => new Date(b.updatedAt || 0).getTime() - new Date(a.updatedAt || 0).getTime());
    if (changed) await writeDb(db);
    return res.json({ ok: true, codes });
  } catch (e) {
    console.error('[admin] promocodes list failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/promocodes', ensureAdminAuth(), async (req, res) => {
  try {
    const code = sanitizePromoCodeKey(req.body?.code);
    const expiresAt = parsePromoExpiresAt(req.body?.expiresAt);
    const enabled = req.body?.enabled;
    if (!code || code.length < 3) return res.status(400).json({ ok: false, error: 'code must be 3..32 chars [A-Z0-9_-]' });
    if (!expiresAt) return res.status(400).json({ ok: false, error: 'expiresAt must be valid ISO date' });
    if (typeof enabled !== 'boolean') return res.status(400).json({ ok: false, error: 'enabled must be boolean' });

    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const before = cloneJson(db.promoConfig?.codes?.[code] || null);
    const current = db.promoConfig.codes[code] || null;
    const createdAt = current?.createdAt || nowIso();
    const createdBy = current?.createdBy || (req.admin?.login || 'admin');
    db.promoConfig.codes[code] = {
      code,
      expiresAt,
      enabled,
      createdAt,
      createdBy,
      updatedAt: nowIso(),
      updatedBy: req.admin?.login || 'admin',
    };
    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'promo_update',
      tgUserId: 0,
      delta: 0,
      reason: `${code} enabled=${enabled} expiresAt=${expiresAt}`,
      adminLogin: req.admin?.login || 'admin',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    appendConfigHistory(db, {
      entityType: 'promo_code',
      entityKey: code,
      action: 'update',
      before,
      after: db.promoConfig.codes[code],
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, promo: db.promoConfig.codes[code] });
  } catch (e) {
    console.error('[admin] promocode save failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.delete('/admin/api/promocodes/:code', ensureAdminAuth(), async (req, res) => {
  try {
    const code = sanitizePromoCodeKey(req.params.code || '');
    if (!code) return res.status(400).json({ ok: false, error: 'invalid code' });
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    if (!db.promoConfig.codes[code]) return res.status(404).json({ ok: false, error: 'promo not found' });
    const before = cloneJson(db.promoConfig.codes[code]);
    delete db.promoConfig.codes[code];
    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'promo_delete',
      tgUserId: 0,
      delta: 0,
      reason: `deleted ${code}`,
      adminLogin: req.admin?.login || 'admin',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    appendConfigHistory(db, {
      entityType: 'promo_code',
      entityKey: code,
      action: 'delete',
      before,
      after: null,
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true });
  } catch (e) {
    console.error('[admin] promocode delete failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/payments', ensureAdminAuth(), async (_req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    const changed = normalizeAllConfig(db);
    const methods = Object.entries(db.paymentConfig?.methods || {})
      .map(([methodKey, cfg]) => ({
        methodKey,
        title: String(cfg?.title || methodKey),
        enabled: Boolean(cfg?.enabled !== false),
        minAmount: Math.floor(Number(cfg?.minAmount || 1)),
        maxAmount: Math.floor(Number(cfg?.maxAmount || 5000)),
        updatedAt: cfg?.updatedAt || null,
        updatedBy: cfg?.updatedBy || null,
      }))
      .sort((a, b) => a.methodKey.localeCompare(b.methodKey));
    if (changed) await writeDb(db);
    return res.json({ ok: true, methods });
  } catch (e) {
    console.error('[admin] payments list failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/payments/:methodKey', ensureAdminAuth(), async (req, res) => {
  try {
    const methodKey = String(req.params.methodKey || '').trim();
    if (!methodKey || !/^[a-z0-9_]{2,50}$/i.test(methodKey)) {
      return res.status(400).json({ ok: false, error: 'invalid method key' });
    }

    const enabled = req.body?.enabled;
    const minAmount = Number(req.body?.minAmount);
    const maxAmount = Number(req.body?.maxAmount);
    if (typeof enabled !== 'boolean') return res.status(400).json({ ok: false, error: 'enabled must be boolean' });
    if (!Number.isInteger(minAmount) || minAmount < 1) return res.status(400).json({ ok: false, error: 'minAmount must be integer >= 1' });
    if (!Number.isInteger(maxAmount) || maxAmount < 1) return res.status(400).json({ ok: false, error: 'maxAmount must be integer >= 1' });
    if (maxAmount < minAmount) return res.status(400).json({ ok: false, error: 'maxAmount must be >= minAmount' });

    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);

    const existing = db.paymentConfig.methods[methodKey] || {};
    const before = cloneJson(existing || null);
    const fallback = defaultPaymentMethods[methodKey] || { title: methodKey.replace(/_/g, ' ') || methodKey, minAmount: 1, maxAmount: 5000 };
    db.paymentConfig.methods[methodKey] = sanitizePaymentMethodConfig({
      ...existing,
      enabled,
      minAmount,
      maxAmount,
      updatedAt: nowIso(),
      updatedBy: req.admin?.login || 'admin',
    }, fallback);

    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'payment_config_update',
      tgUserId: 0,
      delta: 0,
      reason: `${methodKey} enabled=${db.paymentConfig.methods[methodKey].enabled} min=${db.paymentConfig.methods[methodKey].minAmount} max=${db.paymentConfig.methods[methodKey].maxAmount}`,
      adminLogin: req.admin?.login || 'admin',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    appendConfigHistory(db, {
      entityType: 'payment_method',
      entityKey: methodKey,
      action: 'update',
      before,
      after: db.paymentConfig.methods[methodKey],
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, method: { methodKey, ...db.paymentConfig.methods[methodKey] } });
  } catch (e) {
    console.error('[admin] payment update failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/payments/orders', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const status = String(req.query.status || 'all').trim().toLowerCase();
    const q = String(req.query.q || '').trim().toLowerCase();
    const limit = Math.max(1, Math.min(5000, Number(req.query.limit || 300)));
    let rows = Object.values(db.orders || {}).map((order) => {
      const currentStatus = getOrderStatus(order);
      return {
        id: order.id,
        tgUserId: Number(order.tgUserId || 0),
        amount: Number(order.amount || 0),
        status: currentStatus,
        source: String(order.source || ''),
        createdAt: order.createdAt || null,
        updatedAt: order.updatedAt || null,
        expiresAt: order.expiresAt || null,
        payload: String(order.payload || ''),
      };
    });
    if (status && status !== 'all') rows = rows.filter((x) => String(x.status || '') === status);
    if (q) {
      rows = rows.filter((x) =>
        String(x.id || '').toLowerCase().includes(q) ||
        String(x.tgUserId || '').includes(q) ||
        String(x.payload || '').toLowerCase().includes(q)
      );
    }
    rows.sort((a, b) => new Date(b.updatedAt || b.createdAt || 0).getTime() - new Date(a.updatedAt || a.createdAt || 0).getTime());
    return res.json({ ok: true, orders: rows.slice(0, limit), total: rows.length });
  } catch (e) {
    console.error('[admin] payment center list failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/payments/orders/:orderId/recheck', ensureAdminAuth(), async (req, res) => {
  try {
    const orderId = String(req.params.orderId || '').trim();
    if (!orderId) return res.status(400).json({ ok: false, error: 'orderId required' });
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const order = db.orders?.[orderId] || null;
    if (!order) return res.status(404).json({ ok: false, error: 'order not found' });
    const before = cloneJson(order);
    const beforeStatus = getOrderStatus(order);
    if (beforeStatus === 'pending' && order.expiresAt && Date.now() > new Date(order.expiresAt).getTime()) {
      order.status = 'expired';
      order.updatedAt = nowIso();
    }
    const after = cloneJson(order);
    const afterStatus = getOrderStatus(order);
    appendAdminAudit(db, {
      action: 'payment_recheck',
      reason: `${orderId} ${beforeStatus}->${afterStatus}`,
      adminLogin: req.admin?.login || 'admin',
    });
    appendConfigHistory(db, {
      entityType: 'payment_order',
      entityKey: orderId,
      action: 'recheck',
      before,
      after,
      createdBy: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, order: after, beforeStatus, afterStatus });
  } catch (e) {
    console.error('[admin] payment recheck failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/config-history', ensureAdminAuth(), async (req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const entityType = String(req.query.entityType || '').trim();
    const entityKey = String(req.query.entityKey || '').trim();
    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 150)));
    let entries = Array.isArray(db.configHistory?.entries) ? [...db.configHistory.entries] : [];
    if (entityType) entries = entries.filter((x) => String(x.entityType || '') === entityType);
    if (entityKey) entries = entries.filter((x) => String(x.entityKey || '') === entityKey);
    entries.sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime());
    return res.json({ ok: true, entries: entries.slice(0, limit) });
  } catch (e) {
    console.error('[admin] config history failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/config-history/:entryId/rollback', ensureAdminAuth(), async (req, res) => {
  try {
    const entryId = String(req.params.entryId || '').trim();
    if (!entryId) return res.status(400).json({ ok: false, error: 'entryId required' });
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    const entry = (db.configHistory?.entries || []).find((x) => String(x.id || '') === entryId);
    if (!entry) return res.status(404).json({ ok: false, error: 'history entry not found' });

    const beforeRollback = (() => {
      if (entry.entityType === 'case_config') return cloneJson(db.caseConfig?.[entry.entityKey] || null);
      if (entry.entityType === 'case_draft') return cloneJson(db.caseDrafts?.cases?.[entry.entityKey] || null);
      if (entry.entityType === 'payment_method') return cloneJson(db.paymentConfig?.methods?.[entry.entityKey] || null);
      if (entry.entityType === 'promo_code') return cloneJson(db.promoConfig?.codes?.[entry.entityKey] || null);
      return null;
    })();

    const target = cloneJson(entry.before);
    if (entry.entityType === 'case_config') {
      if (!Object.prototype.hasOwnProperty.call(defaultCasePrices, entry.entityKey)) {
        return res.status(400).json({ ok: false, error: 'unsupported case key for rollback' });
      }
      db.caseConfig[entry.entityKey] = target || {
        price: defaultCasePrices[entry.entityKey] || 0,
        enabled: true,
        dropTable: null,
        updatedAt: nowIso(),
        updatedBy: req.admin?.login || 'admin',
      };
      if (entry.entityKey === 'free') db.caseConfig[entry.entityKey].price = 0;
      db.caseConfig[entry.entityKey].updatedAt = nowIso();
      db.caseConfig[entry.entityKey].updatedBy = req.admin?.login || 'admin';
    } else if (entry.entityType === 'case_draft') {
      if (target) db.caseDrafts.cases[entry.entityKey] = target;
      else delete db.caseDrafts.cases[entry.entityKey];
      db.caseDrafts.updatedAt = nowIso();
      db.caseDrafts.updatedBy = req.admin?.login || 'admin';
    } else if (entry.entityType === 'payment_method') {
      if (target) db.paymentConfig.methods[entry.entityKey] = target;
      else delete db.paymentConfig.methods[entry.entityKey];
    } else if (entry.entityType === 'promo_code') {
      if (target) db.promoConfig.codes[entry.entityKey] = target;
      else delete db.promoConfig.codes[entry.entityKey];
    } else {
      return res.status(400).json({ ok: false, error: 'rollback is not supported for this entityType' });
    }

    appendConfigHistory(db, {
      entityType: entry.entityType,
      entityKey: entry.entityKey,
      action: 'rollback',
      before: beforeRollback,
      after: cloneJson(target),
      createdBy: req.admin?.login || 'admin',
      rollbackOf: entry.id,
    });
    appendAdminAudit(db, {
      action: 'config_rollback',
      reason: `${entry.entityType}:${entry.entityKey} rollback of ${entry.id}`,
      adminLogin: req.admin?.login || 'admin',
    });
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, rolledBack: entry.id, entityType: entry.entityType, entityKey: entry.entityKey });
  } catch (e) {
    console.error('[admin] config rollback failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/admin/api/admins', ensureAdminAuth({ ownerOnly: true }), async (_req, res) => {
  try {
    const db = await readDb();
    cleanup(db);
    const changed = normalizeAllConfig(db);
    const admins = Object.values(db.admins || {})
      .map((x) => ({ login: x.login, role: x.role, createdAt: x.createdAt, updatedAt: x.updatedAt, createdBy: x.createdBy || null }))
      .sort((a, b) => a.login.localeCompare(b.login));
    if (changed) await writeDb(db);
    return res.json({ ok: true, admins });
  } catch (e) {
    console.error('[admin] admins list failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.post('/admin/api/admins', ensureAdminAuth({ ownerOnly: true }), async (req, res) => {
  try {
    const login = String(req.body?.login || '').trim();
    const password = String(req.body?.password || '');
    if (!/^[a-zA-Z0-9_.-]{4,32}$/.test(login)) {
      return res.status(400).json({ ok: false, error: 'login must be 4..32 chars [a-zA-Z0-9_.-]' });
    }
    if (password.length < 8) {
      return res.status(400).json({ ok: false, error: 'password must be at least 8 chars' });
    }

    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    if (db.admins[login]) return res.status(409).json({ ok: false, error: 'admin already exists' });
    const hp = hashAdminPassword(password);
    db.admins[login] = {
      login,
      role: 'admin',
      passwordSalt: hp.salt,
      passwordHash: hp.hash,
      createdAt: nowIso(),
      updatedAt: nowIso(),
      createdBy: req.admin?.login || 'owner',
    };
    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'admin_create',
      tgUserId: 0,
      delta: 0,
      reason: `created admin ${login}`,
      adminLogin: req.admin?.login || 'owner',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true, admin: { login, role: 'admin' } });
  } catch (e) {
    console.error('[admin] admin create failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.delete('/admin/api/admins/:login', ensureAdminAuth({ ownerOnly: true }), async (req, res) => {
  try {
    const login = String(req.params.login || '').trim();
    if (!login) return res.status(400).json({ ok: false, error: 'login required' });
    if (login === ADMIN_LOGIN) return res.status(400).json({ ok: false, error: 'owner cannot be deleted' });
    const db = await readDb();
    cleanup(db);
    normalizeAllConfig(db);
    if (!db.admins[login]) return res.status(404).json({ ok: false, error: 'admin not found' });
    delete db.admins[login];
    db.adminAudit.push({
      id: randomToken(8),
      ts: nowIso(),
      action: 'admin_delete',
      tgUserId: 0,
      delta: 0,
      reason: `deleted admin ${login}`,
      adminLogin: req.admin?.login || 'owner',
    });
    if (db.adminAudit.length > 2000) db.adminAudit = db.adminAudit.slice(-2000);
    if (!await persistDbOr503(res, db)) return;
    return res.json({ ok: true });
  } catch (e) {
    console.error('[admin] admin delete failed:', e);
    return res.status(503).json({ ok: false, error: 'Storage temporarily unavailable' });
  }
});

app.get('/favicon.ico', (_req, res) => {
  res.sendFile(path.join(__dirname, 'assets/icons/favicon.ico'));
});
app.get('/favicon-16x16.png', (_req, res) => {
  res.sendFile(path.join(__dirname, 'assets/icons/favicon-16x16.png'));
});
app.get('/favicon-32x32.png', (_req, res) => {
  res.sendFile(path.join(__dirname, 'assets/icons/favicon-32x32.png'));
});
app.get('/apple-touch-icon.png', (_req, res) => {
  res.sendFile(path.join(__dirname, 'assets/icons/apple-touch-icon.png'));
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
