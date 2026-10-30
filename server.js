// Candy Grab relay server — zero dependencies. Run:  node server.js   (then open http://<this machine>:8080)
// Serves candy-grab.html and relays game messages between the host browser and player browsers over WebSocket.
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 8080;
const HTML = path.join(__dirname, 'candy-grab.html');

const rooms = new Map(); // code -> {host:ws, clients:Map<id,ws>, nextId}
function makeCode(){const A='ABCDEFGHJKLMNPQRSTUVWXYZ';let s='';do{s='';for(let i=0;i<4;i++)s+=A[Math.floor(Math.random()*A.length)]}while(rooms.has(s));return s}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/candy-grab.html' || url === '/index.html') {
    fs.readFile(HTML, (err, data) => { if (err) { res.writeHead(500); return res.end('candy-grab.html not found next to server.js'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(data); });
  } else if (url === '/health') { res.writeHead(200, {'Content-Type':'text/plain'}); res.end('ok rooms=' + rooms.size); }
  else { res.writeHead(404); res.end('not found'); }
});

// ---- minimal RFC6455 WebSocket ----
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const ws = { socket, alive: true, role: null, room: null, id: 0,
    send(obj) { if (!this.alive) return; try { socket.write(frame(Buffer.from(JSON.stringify(obj)))); } catch (e) {} },
    close() { if (!this.alive) return; this.alive = false; try { socket.write(Buffer.from([0x88, 0x00])); socket.end(); } catch (e) {} } };
  let buf = Buffer.alloc(0), frags = [];
  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 2) return;
      const fin = buf[0] & 0x80, op = buf[0] & 0x0f, masked = buf[1] & 0x80; let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (masked) off += 4;
      if (buf.length < off + len) return;
      let payload = buf.slice(off, off + len);
      if (masked) { const m = buf.slice(off - 4, off); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      buf = buf.slice(off + len);
      if (op === 0x8) { ws.close(); onClose(ws); return; }
      if (op === 0x9) { try { socket.write(frame(payload, 0xA)); } catch (e) {} continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x0) { frags.push(payload); if (!fin) continue; const text = Buffer.concat(frags).toString('utf8'); frags = [];
        let msg; try { msg = JSON.parse(text); } catch (e) { continue; } onMessage(ws, msg); }
    }
  });
  socket.on('close', () => onClose(ws)); socket.on('error', () => onClose(ws));
  // keepalive ping
  const ping = setInterval(() => { if (!ws.alive) return clearInterval(ping); try { socket.write(Buffer.from([0x89, 0x00])); } catch (e) {} }, 25000);
});
function frame(payload, op = 0x1) {
  const len = payload.length; let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

// ---- relay logic ----
function onMessage(ws, m) {
  if (!m || typeof m !== 'object') return;
  if (m.t === 'host' && !ws.role) {
    const code = makeCode(); const room = { code, host: ws, clients: new Map(), nextId: 1 };
    rooms.set(code, room); ws.role = 'host'; ws.room = room; ws.send({ t: 'hosted', code });
    console.log(new Date().toISOString(), 'room', code, 'opened');
  } else if (m.t === 'join' && !ws.role) {
    const room = rooms.get(String(m.code || '').toUpperCase());
    if (!room) return ws.send({ t: 'err', msg: 'Room not found — check the code (the host must still have the game open).' });
    ws.role = 'client'; ws.room = room; ws.id = room.nextId++; room.clients.set(ws.id, ws); ws.send({ t: 'joined' });
  } else if (m.t === 'm' && ws.role === 'client') {            // client -> host
    ws.room.host.send({ t: 'c', id: ws.id, m: m.m });
  } else if (m.t === 'to' && ws.role === 'host') {             // host -> one client
    const c = ws.room.clients.get(m.id); if (c) c.send({ t: 'm', m: m.m });
  } else if (m.t === 'bc' && ws.role === 'host') {             // host -> all (optionally except one)
    for (const [id, c] of ws.room.clients) if (id !== m.ex) c.send({ t: 'm', m: m.m });
  }
}
function onClose(ws) {
  if (!ws.alive && !ws.room) return;
  ws.alive = false; const room = ws.room; if (!room) return; ws.room = null;
  if (ws.role === 'host') { for (const c of room.clients.values()) { c.send({ t: 'hostgone' }); c.close(); } rooms.delete(room.code); console.log(new Date().toISOString(), 'room', room.code, 'closed'); }
  else if (ws.role === 'client') { room.clients.delete(ws.id); room.host.send({ t: 'cx', id: ws.id }); }
}

server.listen(PORT, () => console.log(`Candy Grab server running.\n  Local:   http://localhost:${PORT}\n  Share the machine's address on port ${PORT} with players (e.g. http://<server-ip-or-name>:${PORT}).`));
