// 水球大亂鬥・自架伺服器（不需要安裝任何套件，只要 Node.js 18 以上）
// 啟動：node server.js            （預設 port 3000）
//       PORT=8080 node server.js   （換 port）
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const os = require("os");

const PORT = +process.env.PORT || 3000;
const HTML = path.join(__dirname, "index.html");
const MAX_MSG = 64 * 1024;      // 單一訊息上限
const MAX_PRES = 8 * 1024;      // 單一房間的 presence 上限
const MAX_ROOMS_PER_CLIENT = 16;

// ---------- 網頁 ----------
const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/" || url === "/index.html") {
    fs.readFile(HTML, (err, buf) => {
      if (err) { res.writeHead(500); res.end("index.html not found"); return; }
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
      res.end(buf);
    });
    return;
  }
  if (url === "/health") { res.writeHead(200, { "Content-Type": "text/plain" }); res.end("ok"); return; }
  res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); res.end("找不到這個頁面");
});

// ---------- WebSocket（自己實作，免安裝套件）----------
const clients = new Set();          // 所有連線
const rooms = new Map();            // 房間名 -> Map(peerId -> client)

server.on("upgrade", (req, socket) => {
  if ((req.url || "").split("?")[0] !== "/ws" || (req.headers.upgrade || "").toLowerCase() !== "websocket") { socket.destroy(); return; }
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);
  const c = { socket, buf: Buffer.alloc(0), id: null, rooms: new Map(), alive: true, frag: null };
  clients.add(c);
  socket.on("data", d => { c.buf = Buffer.concat([c.buf, d]); try { parseFrames(c); } catch (e) { drop(c); } });
  socket.on("close", () => drop(c));
  socket.on("error", () => drop(c));
});

function parseFrames(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80; let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; const hi = b.readUInt32BE(2); if (hi) throw new Error("too big"); len = b.readUInt32BE(6); off = 10; }
    if (len > MAX_MSG) throw new Error("too big");
    if (!masked) throw new Error("unmasked");
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4); off += 4;
    const data = Buffer.alloc(len); for (let i = 0; i < len; i++) data[i] = b[off + i] ^ mask[i & 3];
    c.buf = b.slice(off + len);
    if (op === 0x8) { drop(c); return; }
    if (op === 0x9) { sendFrame(c, 0xA, data); continue; }
    if (op === 0xA) { c.alive = true; continue; }
    if (op === 0x1 || op === 0x0) {
      if (op === 0x1) c.frag = [data]; else if (c.frag) c.frag.push(data); else continue;
      if (fin) { const all = Buffer.concat(c.frag); c.frag = null; if (all.length <= MAX_MSG) onMessage(c, all.toString("utf8")); }
    }
  }
}
function sendFrame(c, op, payload) {
  if (c.socket.destroyed) return;
  const len = payload.length; let head;
  if (len < 126) { head = Buffer.from([0x80 | op, len]); }
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  try { c.socket.write(Buffer.concat([head, payload])); } catch (e) {}
}
const send = (c, obj) => sendFrame(c, 0x1, Buffer.from(JSON.stringify(obj)));

// ---------- 房間協定 ----------
// 用戶端 -> 伺服器：{t:"hello",id} {t:"join",r} {t:"leave",r} {t:"p",r,pr:{...patch}}
// 伺服器 -> 用戶端：{t:"welcome",id} {t:"joined",r,list:[[id,presence]...]} {t:"pr",r,id,pr} {t:"left",r,id}
const okName = r => typeof r === "string" && /^[a-z0-9][a-z0-9_.\-/]{0,60}$/.test(r);
function onMessage(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m !== "object") return;
  if (m.t === "hello") {
    if (c.id) return;
    let id = typeof m.id === "string" && /^[a-z0-9]{4,16}$/.test(m.id) ? m.id : crypto.randomBytes(6).toString("hex");
    for (const o of clients) if (o !== c && o.id === id) { id = crypto.randomBytes(6).toString("hex"); break; }
    c.id = id; send(c, { t: "welcome", id }); return;
  }
  if (!c.id) return;
  if (m.t === "join" && okName(m.r)) {
    if (c.rooms.has(m.r) || c.rooms.size >= MAX_ROOMS_PER_CLIENT) { if (c.rooms.has(m.r)) sendRoom(c, m.r); return; }
    let R = rooms.get(m.r); if (!R) { R = new Map(); rooms.set(m.r, R); }
    R.set(c.id, c); c.rooms.set(m.r, {});
    sendRoom(c, m.r);
    for (const o of R.values()) if (o !== c) send(o, { t: "pr", r: m.r, id: c.id, pr: {} });
    return;
  }
  if (m.t === "leave" && okName(m.r)) { leaveRoom(c, m.r); return; }
  if (m.t === "p" && okName(m.r) && c.rooms.has(m.r) && m.pr && typeof m.pr === "object" && !Array.isArray(m.pr)) {
    const cur = c.rooms.get(m.r);
    const next = Object.assign({}, cur);
    for (const k of Object.keys(m.pr)) { if (!/^[A-Za-z_][A-Za-z0-9_-]{0,63}$/.test(k)) continue; if (m.pr[k] === null) delete next[k]; else next[k] = m.pr[k]; }
    if (JSON.stringify(next).length > MAX_PRES) return;
    c.rooms.set(m.r, next);
    const R = rooms.get(m.r); if (!R) return;
    const out = JSON.stringify({ t: "pr", r: m.r, id: c.id, pr: next });
    const buf = Buffer.from(out);
    for (const o of R.values()) sendFrame(o, 0x1, buf);
  }
}
function sendRoom(c, r) { const R = rooms.get(r); if (!R) return; send(c, { t: "joined", r, list: [...R.values()].map(o => [o.id, o.rooms.get(r) || {}]) }); }
function leaveRoom(c, r) {
  if (!c.rooms.has(r)) return; c.rooms.delete(r);
  const R = rooms.get(r); if (!R) return; R.delete(c.id);
  if (!R.size) rooms.delete(r); else for (const o of R.values()) send(o, { t: "left", r, id: c.id });
}
function drop(c) {
  if (!clients.has(c)) return; clients.delete(c);
  for (const r of [...c.rooms.keys()]) leaveRoom(c, r);
  try { c.socket.destroy(); } catch (e) {}
}
// 心跳：30 秒沒回應就斷線
setInterval(() => { for (const c of clients) { if (!c.alive) { drop(c); continue; } c.alive = false; sendFrame(c, 0x9, Buffer.alloc(0)); } }, 15000);

server.listen(PORT, "0.0.0.0", () => {
  console.log("\n  水球大亂鬥 伺服器啟動了！\n");
  console.log("  自己玩：     http://localhost:" + PORT);
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) for (const n of nets[name] || []) {
    if (n.family === "IPv4" && !n.internal) console.log("  同網路朋友： http://" + n.address + ":" + PORT + "   (" + name + ")");
  }
  console.log("\n  不同地方的朋友：請看「使用說明.txt」的第 3 步。");
  console.log("  要關掉伺服器：按 Ctrl + C\n");
});
