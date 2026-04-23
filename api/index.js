let cachedHandler = null;

export default async function handler(req, res) {
  try {
    if (!cachedHandler) {
      const mod = await import('../server.js');
      cachedHandler = mod.default;
    }
    return cachedHandler(req, res);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const stack = err instanceof Error ? err.stack : null;
    console.error('[api/index] bootstrap failed:', err);
    return res.status(500).json({
      ok: false,
      error: 'bootstrap_failed',
      message,
      stack,
    });
  }
}
