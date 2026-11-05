const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");
const PUB = path.join(__dirname, "public"), FILE = path.join(__dirname, "users.json");
const ACCESS = process.env.ACCESS_CODE || "";           // código da empresa (opcional)
let ICE = [{ urls: "stun:stun.l.google.com:19302" }];  // pode definir TURN com ICE_JSON
try { if (process.env.ICE_JSON) ICE = JSON.parse(process.env.ICE_JSON); } catch (e) {}
let U = {}; try { U = JSON.parse(fs.readFileSync(FILE)); } catch (e) {}
for (const i of Object.keys(U)) if (!U[i].email) delete U[i];
const save = () => { try { fs.writeFileSync(FILE, JSON.stringify(U)); } catch (e) {} };
const dig = s => String(s || "").replace(/\D/g, "");
const hash = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString("hex");
const same = (a, b) => a.length === b.length && crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
const newId = () => { let i; do { i = "PT-" + Math.floor(10000 + Math.random() * 90000); } while (U[i]); return i; };
function find(q) {
  q = String(q || "").trim();
  if (/^pt-\d{5}$/i.test(q)) { q = q.toUpperCase(); return U[q] ? q : null; }
  if (q.includes("@")) { const e = q.toLowerCase(); return Object.keys(U).find(i => U[i].email === e) || null; }
  const d = dig(q); if (d.length < 7) return null;
  return Object.keys(U).find(i => { const p = U[i].phone; return p && (p.endsWith(d) || d.endsWith(p)); }) || null;
}
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".json": "application/json", ".webmanifest": "application/manifest+json", ".png": "image/png", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
const srv = http.createServer((req, res) => {
  let u = "/"; try { u = decodeURIComponent((req.url || "/").split("?")[0]); } catch (e) {}
  if (u === "/healthz") return res.end("ok");
  if (u === "/") u = "/index.html";
  const f = path.join(PUB, path.normalize(u));
  if (!f.startsWith(PUB)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, b) => {
    if (e) { res.writeHead(404); return res.end("Not found"); }
    res.writeHead(200, { "Content-Type": MIME[path.extname(f)] || "application/octet-stream", "Cache-Control": "no-cache" }); res.end(b);
  });
});
const wss = new WebSocketServer({ server: srv, maxPayload: 14 * 1024 * 1024 });
const on = new Map(), queue = {}, rooms = new Map();
const out = (w, o) => w && w.readyState === 1 && w.send(JSON.stringify(o));
const outId = (id, o) => (on.get(id) || []).forEach(w => out(w, o));
const online = id => on.has(id) && on.get(id).size > 0;
function leave(r, id, ws) {
  const m = rooms.get(r); if (!m || m.get(id) !== ws) return;
  m.delete(id); m.forEach(w => out(w, { t: "m-left", room: r, id })); if (!m.size) rooms.delete(r);
}
wss.on("connection", ws => {
  let me = null, fails = 0;
  const bad = x => { out(ws, { t: "autherr", x }); if (++fails > 8) ws.close(); };
  ws.on("message", raw => {
    let d; try { d = JSON.parse(raw); } catch (e) { return; }
    if (d.t === "ping") return;
    if (d.t === "hello") {
      const em = String(d.email || "").trim().toLowerCase(), name = String(d.name || "").trim().slice(0, 40), ph = dig(d.phone), pw = String(d.pw || "");
      if (!/^\S+@\S+\.\S+$/.test(em) || pw.length < 6) return bad("Email ou palavra-passe inválidos.");
      let id = find(em);
      if (id) { const u = U[id]; if (!same(hash(pw, u.salt), u.hash)) return bad("Palavra-passe incorreta para este email."); }
      else {
        if (ACCESS && String(d.code || "").trim() !== ACCESS) return bad("Código da empresa inválido.");
        if (!name || ph.length < 7) return bad("Indica o nome e um telefone válido.");
        if (find(ph)) return bad("Esse telefone já está associado a outra conta.");
        id = /^PT-\d{5}$/.test(d.id || "") && !U[d.id] ? d.id : newId();
        const salt = crypto.randomBytes(8).toString("hex");
        U[id] = { name, email: em, phone: ph, salt, hash: hash(pw, salt) }; save();
      }
      me = id; if (!on.has(id)) on.set(id, new Set()); on.get(id).add(ws);
      out(ws, { t: "ready", id, name: U[id].name, email: em, phone: U[id].phone, ice: ICE });
      (queue[id] || []).forEach(q => { out(ws, q.o); outId(q.o.from, { t: "ack", to: id, mid: q.o.mid }); }); delete queue[id];
      return;
    }
    if (!me) return;
    if (d.t === "lookup") { const i = find(d.q); return out(ws, i ? { t: "found", id: i, name: U[i].name, email: U[i].email } : { t: "notfound", q: String(d.q || "").slice(0, 60) }); }
    if (d.t === "dir") return out(ws, { t: "dir", list: Object.keys(U).filter(i => i !== me).slice(0, 1000).map(i => ({ id: i, name: U[i].name, email: U[i].email, on: online(i) })) });
    if (d.t === "presence") { const map = {}; (Array.isArray(d.ids) ? d.ids.slice(0, 200) : []).forEach(i => { map[i] = online(i); }); return out(ws, { t: "presence", map }); }
    if (d.t === "typing") return outId(d.to, { t: "typing", from: me });
    if (d.t === "read") return outId(d.to, { t: "read", from: me, mids: Array.isArray(d.mids) ? d.mids.slice(0, 200) : [] });
    if (d.t === "msg") {
      if (!U[d.to]) return out(ws, { t: "err", x: "Esse utilizador não existe." });
      const o = { t: "msg", from: me, name: U[me].name, mid: String(d.mid || "").slice(0, 40), ts: Date.now(), x: String(d.x || "").slice(0, 2000) };
      if (d.k === "meet") { o.k = "meet"; o.room = String(d.room || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12); }
      else if (d.d) {
        if (typeof d.d !== "string" || d.d.length > 11200000) return out(ws, { t: "err", x: "Ficheiro demasiado grande (máx. 8 MB)." });
        o.k = ["img", "aud"].includes(d.k) ? d.k : "file"; o.d = d.d; o.fn = String(d.fn || "ficheiro").slice(0, 120); o.mime = String(d.mime || "").slice(0, 80); o.size = +d.size || 0;
      }
      if (online(d.to)) { outId(d.to, o); out(ws, { t: "ack", to: d.to, mid: o.mid }); }
      else {
        const q = queue[d.to] = queue[d.to] || []; q.push({ o, n: (o.d || "").length + 300 });
        let tot = q.reduce((a, b) => a + b.n, 0); while (tot > 40e6 && q.length > 1) tot -= q.shift().n;
      }
      return;
    }
    if (d.t === "m-join") {
      const r = String(d.room || "").toUpperCase(); if (!/^[A-Z0-9]{6,12}$/.test(r)) return;
      let m = rooms.get(r); if (!m) { m = new Map(); rooms.set(r, m); }
      if (m.size >= 8 && !m.has(me)) return out(ws, { t: "err", x: "A reunião está cheia (máx. 8 pessoas)." });
      const peers = [...m.keys()].filter(i => i !== me).map(i => ({ id: i, name: U[i].name }));
      m.set(me, ws); out(ws, { t: "m-peers", room: r, peers });
      peers.forEach(p => out(m.get(p.id), { t: "m-joined", room: r, id: me, name: U[me].name }));
      return;
    }
    if (d.t === "m-sig") { const m = rooms.get(String(d.room || "").toUpperCase()); if (m && m.has(me)) out(m.get(d.to), { t: "m-sig", room: d.room, from: me, data: d.data }); return; }
    if (d.t === "m-leave") return leave(String(d.room || "").toUpperCase(), me, ws);
  });
  ws.on("close", () => {
    if (!me) return;
    const s = on.get(me); if (s) { s.delete(ws); if (!s.size) on.delete(me); }
    for (const r of [...rooms.keys()]) leave(r, me, ws);
  });
});
srv.listen(process.env.PORT || 8080, () => console.log("Servidor PinTalk a correr na porta", process.env.PORT || 8080));
