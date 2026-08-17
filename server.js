// =============================================
// TINY WEBRTC SIGNALING SERVER
// Uses: Node.js + WebSocket (ws package)
// Deploy FREE on: Render / Railway / Glitch
// =============================================

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
};

// HTTP server: serve static files + health check
const server = http.createServer((req, res) => {
  let reqPath = decodeURIComponent(req.url.split('?')[0]);
  if (reqPath === '/health') {
    res.writeHead(200, {'Content-Type': 'text/plain'});
    res.end('P2P Signaling Server Running ✅\n');
    return;
  }
  if (reqPath === '/') reqPath = '/index.html';
  const filePath = path.join(__dirname, reqPath);
  // Prevent directory traversal
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, {'Content-Type': 'text/plain'});
      res.end('Not Found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    res.writeHead(200, {'Content-Type': MIME[ext] || 'application/octet-stream'});
    res.end(data);
  });
});

// WebSocket server
const wss = new WebSocket.Server({ server });

// Store rooms: { roomCode: [ws1, ws2] }
const rooms = {};

// Store user info: { ws: { room, name, id } }
const users = new WeakMap();

console.log(`🚀 Signaling server starting on port ${PORT}`);

wss.on('connection', (ws) => {
  console.log('New connection');

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      handleMessage(ws, msg);
    } catch(e) {
      console.error('Bad message:', e);
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);
    if (user?.room && rooms[user.room]) {
      // Remove from room
      rooms[user.room] = rooms[user.room].filter(c => c !== ws);
      // Notify others
      broadcastToRoom(user.room, ws, {
        type: 'peer_left',
        name: user.name
      });
      // Clean empty rooms
      if (rooms[user.room].length === 0) {
        delete rooms[user.room];
        console.log(`Room ${user.room} deleted`);
      }
    }
    console.log('Connection closed');
  });

  ws.on('error', (e) => {
    console.error('WS Error:', e.message);
  });
});

function handleMessage(ws, msg) {
  switch(msg.type) {

    case 'join': {
      const { room, name } = msg;
      if (!room || !name) return;

      // Leave previous room
      const prev = users.get(ws);
      if (prev?.room) {
        rooms[prev.room] = (rooms[prev.room]||[]).filter(c => c !== ws);
      }

      // Join new room
      if (!rooms[room]) rooms[room] = [];

      // Max 2 users per room
      if (rooms[room].length >= 2) {
        send(ws, { type: 'error', msg: 'Room is full (max 2 users)' });
        return;
      }

      rooms[room].push(ws);
      users.set(ws, { room, name, id: Date.now() });

      const count = rooms[room].length;
      send(ws, {
        type: 'joined',
        room,
        count,
        message: count === 1
          ? 'Waiting for peer to join...'
          : 'Peer found! Connecting...'
      });

      // Notify existing user
      broadcastToRoom(room, ws, {
        type: 'peer_joined',
        name,
        count
      });

      console.log(`${name} joined room: ${room} (${count}/2)`);
      break;
    }

    case 'offer': {
      // Forward offer to peer
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'offer',
        sdp: msg.sdp,
        from: user.name
      });
      break;
    }

    case 'answer': {
      // Forward answer to peer
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'answer',
        sdp: msg.sdp,
        from: user.name
      });
      break;
    }

    case 'ice': {
      // Forward ICE candidate
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'ice',
        candidate: msg.candidate
      });
      break;
    }

    case 'chat': {
      // Forward chat message
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'chat',
        content: msg.content,
        name: user.name,
        time: new Date().toLocaleTimeString()
      });
      break;
    }

    case 'ping': {
      send(ws, { type: 'pong' });
      break;
    }

    default:
      console.log('Unknown message type:', msg.type);
  }
}

// Send to specific client
function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// Send to all in room except sender
function broadcastToRoom(room, senderWs, data) {
  (rooms[room] || []).forEach(client => {
    if (client !== senderWs) send(client, data);
  });
}

server.listen(PORT, () => {
  console.log(`✅ Server running on port ${PORT}`);
  console.log(`📡 WebSocket ready for P2P signaling`);
});