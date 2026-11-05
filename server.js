const { WebSocketServer } = require("ws");
const fs = require("fs");
const FILE = __dirname + "/users.json";
let U = {}; try { U = JSON.parse(fs.readFileSync(FILE)); } catch (e) {}
const save = () => { try { fs.writeFileSync(FILE, JSON.stringify(U)); } catch (e) {} };
const online = new Map(), queue = {};
const wss = new WebSocketServer({ port: process.env.PORT || 8080, maxPayload: 16 * 1024 });
const newId = () => { let i; do { i = "PT-" + Math.floor(10000 + Math.random() * 90000); } while (U[i]); return i; };
const out = (ws, o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));

wss.on("connection", ws => {
  let me = null;
  ws.on("message", raw => {
    let d; try { d = JSON.parse(raw); } catch (e) { return; }
    if (d.t === "ping") return;
    if (d.t === "hello") {
      const name = String(d.name || "Anónimo").slice(0, 24);
      let id = d.id, token = d.token;
      if (!/^PT-\d{5}$/.test(id || "") || !token) { id = newId(); token = require("crypto").randomBytes(16).toString("hex"); }
      if (U[id] && U[id].token !== token) return out(ws, { t: "err", x: "ID já em uso por outra conta." });
      U[id] = { token, name }; save();
      me = id; online.set(id, ws);
      out(ws, { t: "ready", id, token });
      (queue[id] || []).forEach(m => { out(ws, m); out(online.get(m.from), { t: "ack", to: id, mid: m.mid }); });
      delete queue[id];
      return;
    }
    if (!me) return;
    if (d.t === "lookup") {
      const u = U[d.id];
      return out(ws, u ? { t: "found", id: d.id, name: u.name } : { t: "notfound", id: d.id });
    }
    if (d.t === "presence") {
      const map = {}; (Array.isArray(d.ids) ? d.ids.slice(0, 200) : []).forEach(i => { map[i] = online.has(i); });
      return out(ws, { t: "presence", map });
    }
    if (d.t === "typing") return out(online.get(d.to), { t: "typing", from: me });
    if (d.t === "read") return out(online.get(d.to), { t: "read", from: me, mids: Array.isArray(d.mids) ? d.mids.slice(0, 200) : [] });
    if (d.t === "msg") {
      if (!U[d.to]) return out(ws, { t: "err", x: "Esse ID não existe." });
      const m = { t: "msg", from: me, name: U[me].name, x: String(d.x || "").slice(0, 2000), mid: String(d.mid || "").slice(0, 40), ts: Date.now() };
      const dest = online.get(d.to);
      if (dest) { out(dest, m); out(ws, { t: "ack", to: d.to, mid: m.mid }); }
      else (queue[d.to] = queue[d.to] || []).push(m);
    }
  });
  ws.on("close", () => { if (me && online.get(me) === ws) online.delete(me); });
});
console.log("Servidor Pintalk a correr na porta", process.env.PORT || 8080);
