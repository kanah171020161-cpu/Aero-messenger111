'use strict';
// Aero Messenger — локальный сервер (Node.js 18+, без зависимостей)
const http = require('http'), fs = require('fs'), os = require('os'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 3000, DATA = process.env.DATA_DIR || __dirname, DBF = path.join(DATA, 'db.json');
try { fs.mkdirSync(DATA, { recursive: true }); } catch (e) {}
let db = { users: {}, tokens: {}, msgs: [] };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DBF, 'utf8'))); } catch (e) {}
let st = null;
const save = () => { clearTimeout(st); st = setTimeout(() => fs.writeFile(DBF, JSON.stringify(db), () => {}), 300); };
const codes = new Map(), lim = new Map(), sse = new Map();
const NICK = /^[A-Za-zА-Яа-яЁё0-9_]{3,20}$/, MAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

const reply = (res, c, o) => { res.writeHead(c, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); };
const rate = (k, n, ms) => { const now = Date.now(), e = lim.get(k); if (!e || now > e.t) { lim.set(k, { n: 1, t: now + ms }); return true; } return ++e.n <= n; };
const readBody = req => new Promise((ok, no) => {
  let b = ''; req.on('data', d => { b += d; if (b.length > 300000) { no(new Error('big')); req.destroy(); } });
  req.on('end', () => { try { ok(b ? JSON.parse(b) : {}); } catch (e) { no(e); } });
});
const hash = (pw, salt) => crypto.scryptSync(pw, salt, 64).toString('hex');
const pub = u => ({ nick: u.nick, email: u.email, av: u.av });
const key = s => String(s).toLowerCase();
function auth(req, url) {
  const h = req.headers.authorization || '', t = h.startsWith('Bearer ') ? h.slice(7) : url.searchParams.get('token');
  const k = t && db.tokens[t]; return k && db.users[k] ? db.users[k] : null;
}
function cleanAv(a) {
  if (a && typeof a === 'object') {
    if (typeof a.img === 'string' && /^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(a.img) && a.img.length < 80000) return { img: a.img };
    if (typeof a.f === 'string' && a.f.length <= 4 && /^#[0-9a-f]{6}$/i.test(a.c || '')) return { f: a.f, c: a.c };
  }
  return { f: '😀', c: '#ff7a59' };
}
function emit(ev, data, to) {
  const s = 'event: ' + ev + '\ndata: ' + JSON.stringify(data) + '\n\n';
  for (const [k, set] of sse) if (!to || to.includes(k)) set.forEach(r => r.write(s));
}
async function sendMail(to, code) {
  const k = process.env.RESEND_API_KEY;
  if (!k) { console.log(`[КОД] ${to}: ${code}  (RESEND_API_KEY не задан — письмо не отправлено)`); return false; }
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST', headers: { Authorization: 'Bearer ' + k, 'Content-Type': 'application/json' },
      body: JSON.stringify({ from: process.env.MAIL_FROM || 'Aero Messenger <onboarding@resend.dev>', to: [to],
        subject: 'Код подтверждения: ' + code, text: `Ваш код подтверждения: ${code}\nОн действует 10 минут.` })
    });
    if (!r.ok) throw new Error(await r.text());
    return true;
  } catch (e) { console.log('[ПОЧТА] ошибка:', e.message); console.log(`[КОД] ${to}: ${code}`); return false; }
}


// ───── ИИ-бот ─────
const BOT = { nick: 'AeroAI', name: 'Aero AI', av: { f: '😎', c: '#6b4cff' }, bot: true, online: true };
const AI_SYS = 'Ты — Aero AI, дружелюбный ИИ-помощник в мессенджере Aero. Отвечай по-русски, коротко и по делу (не больше 6 предложений), без Markdown-разметки.';
const AI_SETUP = 'ИИ пока не подключён. Два способа:\n1) Облако: запустите сервер с ключом ANTHROPIC_API_KEY=...\n2) Локально и бесплатно: установите Ollama (ollama.com), выполните «ollama pull llama3.2» и перезапустите сервер.\nПодробности в файле ИИ-БОТ.txt';
let aiBusy = 0;
async function askAI(history) {
  const ctl = new AbortController(), to = setTimeout(() => ctl.abort(), 60000);
  try {
    if (process.env.ANTHROPIC_API_KEY) {
      const r = await fetch((process.env.ANTHROPIC_BASE_URL || 'https://api.anthropic.com') + '/v1/messages', {
        method: 'POST', signal: ctl.signal,
        headers: { 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
        body: JSON.stringify({ model: process.env.AI_MODEL || 'claude-haiku-4-5-20251001', max_tokens: 500, system: AI_SYS, messages: history })
      });
      if (!r.ok) return 'Ошибка ИИ-сервиса (HTTP ' + r.status + '). Проверьте ключ и модель.';
      const d = await r.json();
      return (d.content || []).filter(c => c.type === 'text').map(c => c.text).join('').trim() || 'Пустой ответ.';
    }
    try {
      const r = await fetch((process.env.OLLAMA_URL || 'http://localhost:11434') + '/api/chat', {
        method: 'POST', signal: ctl.signal, headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ model: process.env.AI_MODEL || 'llama3.2', stream: false, messages: [{ role: 'system', content: AI_SYS }, ...history] })
      });
      if (!r.ok) return 'Ollama ответил ошибкой (HTTP ' + r.status + '). Проверьте, что модель скачана: ollama pull ' + (process.env.AI_MODEL || 'llama3.2');
      const d = await r.json();
      return String((d.message && d.message.content) || '').trim() || 'Пустой ответ.';
    } catch (e) { return e.name === 'AbortError' ? 'Слишком долго думаю, попробуйте ещё раз.' : AI_SETUP; }
  } catch (e) { return e.name === 'AbortError' ? 'Слишком долго думаю, попробуйте ещё раз.' : 'Не удалось связаться с ИИ.'; }
  finally { clearTimeout(to); }
}
async function aiReply(me, to, history) {
  const k = key(me.nick), targets = to === '#all' ? null : [k], typing = on => emit('typing', { nick: BOT.nick, on }, targets);
  let out;
  if (!rate('ai:' + k, 15, 60000)) out = 'Слишком много вопросов подряд, подождите минуту.';
  else if (aiBusy >= 4) out = 'Я сейчас занят, повторите через минуту.';
  else { aiBusy++; typing(true); try { out = await askAI(history); } finally { aiBusy--; typing(false); } }
  const m = { id: crypto.randomUUID(), from: BOT.nick, to, text: out.slice(0, 2000), t: Date.now(), cid: '' };
  db.msgs.push(m); save(); emit('msg', m, targets);
}
function dmHistory(me) {
  const k = key(me.nick), h = db.msgs.filter(m => m.to !== '#all' && ((key(m.from) === k && key(m.to) === key(BOT.nick)) || (key(m.from) === key(BOT.nick) && key(m.to) === k))).slice(-20)
    .map(m => ({ role: key(m.from) === key(BOT.nick) ? 'assistant' : 'user', content: m.text }));
  while (h.length && h[0].role !== 'user') h.shift();
  return h;
}

const server = http.createServer(async (req, res) => {
  let url; try { url = new URL(req.url, 'http://x'); } catch (e) { res.writeHead(400); return res.end(); }
  const ip = (process.env.TRUST_PROXY === '1' && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress, p = url.pathname;
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('X-Frame-Options', 'DENY'); res.setHeader('Referrer-Policy', 'no-referrer');
  if (req.headers['x-forwarded-proto'] === 'https') res.setHeader('Strict-Transport-Security', 'max-age=31536000');
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      return fs.readFile(path.join(__dirname, 'index.html'), (e, d) => {
        if (e) { res.writeHead(500); return res.end('index.html не найден'); }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(d);
      });
    }
    const STATIC = { '/manifest.webmanifest': 'application/manifest+json', '/sw.js': 'text/javascript', '/icon-192.png': 'image/png', '/icon-512.png': 'image/png', '/icon-maskable.png': 'image/png' };
    if (req.method === 'GET' && STATIC[p]) {
      return fs.readFile(path.join(__dirname, p.slice(1)), (e, d) => {
        if (e) { res.writeHead(404); return res.end(); }
        res.writeHead(200, { 'Content-Type': STATIC[p], 'Cache-Control': p === '/sw.js' ? 'no-cache' : 'public, max-age=86400' }); res.end(d);
      });
    }
    if (p === '/api/health') return reply(res, 200, { ok: true, app: 'aero-messenger' });

    if (p === '/api/code' && req.method === 'POST') {
      const b = await readBody(req), email = key(b.email || '').trim();
      if (!MAIL.test(email)) return reply(res, 400, { error: 'Некорректная почта' });
      if (!rate('code:' + email, 1, 30000)) return reply(res, 429, { error: 'Подождите 30 секунд перед повторной отправкой' });
      if (!rate('codeip:' + ip, 20, 3600000)) return reply(res, 429, { error: 'Слишком много запросов, попробуйте позже' });
      const code = String(crypto.randomInt(1000, 10000));
      codes.set(email, { code, exp: Date.now() + 600000, tries: 0 });
      return reply(res, 200, { ok: true, mail: await sendMail(email, code) });
    }
    if (p === '/api/register' && req.method === 'POST') {
      if (!rate('reg:' + ip, 10, 3600000)) return reply(res, 429, { error: 'Слишком много попыток, попробуйте позже' });
      const b = await readBody(req), nick = String(b.nick || ''), email = key(b.email || '').trim(), pw = String(b.password || '');
      if (!NICK.test(nick)) return reply(res, 400, { error: 'Никнейм: 3–20 символов, буквы, цифры и _' });
      if (!MAIL.test(email)) return reply(res, 400, { error: 'Некорректная почта' });
      if (pw.length < 8 || !/\d/.test(pw) || !/[A-Za-zА-Яа-яЁё]/.test(pw)) return reply(res, 400, { error: 'Пароль: минимум 8 символов, буквы и цифры' });
      const c = codes.get(email);
      if (!c || Date.now() > c.exp) return reply(res, 400, { error: 'Код устарел. Запросите новый' });
      if (++c.tries > 5) { codes.delete(email); return reply(res, 400, { error: 'Слишком много попыток ввода кода. Запросите новый' }); }
      if (String(b.code) !== c.code) return reply(res, 400, { error: 'Неверный код' });
      if (db.users[key(nick)] || key(nick) === key(BOT.nick) || key(nick) === 'aero_ai') return reply(res, 409, { error: 'Этот никнейм уже занят' });
      if (Object.values(db.users).some(u => u.email === email)) return reply(res, 409, { error: 'Эта почта уже зарегистрирована' });
      const salt = crypto.randomBytes(16).toString('hex');
      const u = { nick, email, av: cleanAv(b.av), salt, hash: hash(pw, salt), created: Date.now() };
      db.users[key(nick)] = u; codes.delete(email);
      const tok = crypto.randomBytes(24).toString('hex'); db.tokens[tok] = key(nick); save();
      emit('user', pub(u));
      return reply(res, 200, { token: tok, user: pub(u) });
    }
    if (p === '/api/login' && req.method === 'POST') {
      if (!rate('login:' + ip, 10, 900000)) return reply(res, 429, { error: 'Слишком много попыток входа. Подождите 15 минут' });
      const b = await readBody(req), u = db.users[key(b.nick || '')];
      const ok = u && crypto.timingSafeEqual(Buffer.from(hash(String(b.password || ''), u.salt), 'hex'), Buffer.from(u.hash, 'hex'));
      if (!ok) return reply(res, 401, { error: 'Неверный никнейм или пароль' });
      const tok = crypto.randomBytes(24).toString('hex'); db.tokens[tok] = key(u.nick); save();
      return reply(res, 200, { token: tok, user: pub(u) });
    }

    const me = auth(req, url);
    if (p.startsWith('/api/') && !me) return reply(res, 401, { error: 'Нужно войти' });

    if (p === '/api/users') return reply(res, 200, { users: [BOT, ...Object.values(db.users)].map(u => u.bot ? { nick: u.nick, name: u.name, av: u.av, online: true, bot: true } : ({ nick: u.nick, av: u.av, online: sse.has(key(u.nick)) })) });
    if (p === '/api/history') {
      const w = url.searchParams.get('with') || '', k = key(me.nick);
      const list = w === '#all' ? db.msgs.filter(m => m.to === '#all') : db.msgs.filter(m => m.to !== '#all' && ((key(m.from) === k && key(m.to) === key(w)) || (key(m.from) === key(w) && key(m.to) === k)));
      return reply(res, 200, { msgs: list.slice(-200) });
    }
    if (p === '/api/send' && req.method === 'POST') {
      if (!rate('send:' + key(me.nick), 60, 60000)) return reply(res, 429, { error: 'Слишком быстро, подождите немного' });
      const b = await readBody(req), text = String(b.text || '').trim().slice(0, 2000);
      let to = String(b.to || ''); const tu = db.users[key(to)];
      if (!text) return reply(res, 400, { error: 'Пустое сообщение' });
      const isBot = key(to) === key(BOT.nick);
      if (to !== '#all') { if (isBot) to = BOT.nick; else { if (!tu) return reply(res, 404, { error: 'Пользователь не найден' }); to = tu.nick; } }
      const m = { id: crypto.randomUUID(), from: me.nick, to, text, t: Date.now(), cid: String(b.cid || '').slice(0, 40) };
      db.msgs.push(m); if (db.msgs.length > 20000) db.msgs.splice(0, db.msgs.length - 20000); save();
      emit('msg', m, to === '#all' ? null : [key(me.nick), key(to)]);
      if (isBot) aiReply(me, me.nick, dmHistory(me));
      else if (to === '#all' && /^\s*(@ai|\/ai)\b/i.test(text)) aiReply(me, '#all', [{ role: 'user', content: text.replace(/^\s*(@ai|\/ai)\b[:,]?\s*/i, '') || 'Привет!' }]);
      return reply(res, 200, { ok: true, id: m.id });
    }
    if (p === '/api/stream') {
      const k = key(me.nick);
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
      res.write(':ok\n\n');
      const first = !sse.has(k); if (first) sse.set(k, new Set()); sse.get(k).add(res);
      if (first) emit('presence', { nick: me.nick, online: true });
      const ping = setInterval(() => res.write(':\n\n'), 25000);
      req.on('close', () => { clearInterval(ping); const s = sse.get(k); if (s) { s.delete(res); if (!s.size) { sse.delete(k); emit('presence', { nick: me.nick, online: false }); } } });
      return;
    }
    reply(res, 404, { error: 'Не найдено' });
  } catch (e) { if (!res.headersSent) reply(res, 400, { error: 'Некорректный запрос' }); }
});
setInterval(() => { const n = Date.now(); for (const [k, v] of codes) if (n > v.exp) codes.delete(k); for (const [k, v] of lim) if (n > v.t) lim.delete(k); }, 600000).unref();
function start(port, tries) {
  const onErr = e => {
    server.removeListener('listening', onListen);
    if (e.code === 'EADDRINUSE' && tries < 10) { console.log('Порт ' + port + ' занят, пробую ' + (port + 1)); start(port + 1, tries + 1); }
    else { console.log('Не удалось запустить сервер:', e.message); process.exit(1); }
  };
  const onListen = () => {
    server.removeListener('error', onErr);
    const p = server.address().port;
    console.log('Aero Messenger запущен.\n  На этом устройстве: http://localhost:' + p);
    try {
      for (const l of Object.values(os.networkInterfaces()).flat()) if (l && l.family === 'IPv4' && !l.internal) console.log('  В вашей Wi-Fi сети:  http://' + l.address + ':' + p);
    } catch (e) { console.log('  (адрес в Wi-Fi сети узнайте в настройках Wi-Fi телефона и добавьте :' + p + ')'); }
    console.log(process.env.RESEND_API_KEY ? '  Почта: Resend подключён' : '  Почта: RESEND_API_KEY не задан — коды будут печататься здесь, в консоли');
    console.log('  Остановить: Ctrl+C');
  };
  server.once('error', onErr); server.once('listening', onListen);
  server.listen(port, '0.0.0.0');
}
start(PORT, 0);
const flush = () => { try { fs.writeFileSync(DBF, JSON.stringify(db)); } catch (e) {} process.exit(0); };
process.on('SIGTERM', flush); process.on('SIGINT', flush);
process.on('uncaughtException', e => console.log('[ошибка]', e && e.message));
