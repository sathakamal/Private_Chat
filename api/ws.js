// =============================================
// VERCEL WEBSOCKET FUNCTION — P2P signaling
// Deploys the same signaling logic as server.js
// but as a Vercel serverless WebSocket function.
// =============================================
import { WebSocketServer } from 'ws';

// Rooms: { roomCode: [ws1, ws2] }
const rooms = new Map();
// User info: { ws: { room, name, id } }
const users = new WeakMap();

// Privacy: never log names in production (Vercel = production)
const LOG_SENSITIVE = process.env.DISABLE_LOGGING === 'true';

const wss = new WebSocketServer({ noServer: true });

wss.on('connection', (ws) => {
  console.log('New connection');

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      handleMessage(ws, msg);
    } catch (e) {
      console.error('Bad message:', e);
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);
    if (user?.room && rooms.has(user.room)) {
      const list = rooms.get(user.room).filter((c) => c !== ws);
      broadcastToRoom(user.room, ws, {
        type: 'peer_left',
        name: LOG_SENSITIVE ? user.name : 'Peer',
      });
      if (list.length === 0) rooms.delete(user.room);
      else rooms.set(user.room, list);
    }
    console.log('Connection closed');
  });

  ws.on('error', (e) => console.error('WS Error:', e.message));
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'join': {
      const { room, name } = msg;
      if (!room || !name) return;

      const prev = users.get(ws);
      if (prev?.room && rooms.has(prev.room)) {
        rooms.set(prev.room, rooms.get(prev.room).filter((c) => c !== ws));
      }

      if (!rooms.has(room)) rooms.set(room, []);
      const list = rooms.get(room);

      if (list.length >= 2) {
        send(ws, { type: 'error', msg: 'Room is full (max 2 users)' });
        return;
      }

      list.push(ws);
      users.set(ws, { room, name, id: Date.now() });
      const count = list.length;

      send(ws, {
        type: 'joined',
        room,
        count,
        message: count === 1 ? 'Waiting for peer to join...' : 'Peer found! Connecting...',
      });

      broadcastToRoom(room, ws, {
        type: 'peer_joined',
        name: LOG_SENSITIVE ? name : 'Peer',
        count,
      });
      break;
    }

    case 'offer': {
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'offer',
        sdp: msg.sdp,
        from: LOG_SENSITIVE ? user.name : 'Peer',
      });
      break;
    }

    case 'answer': {
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'answer',
        sdp: msg.sdp,
        from: LOG_SENSITIVE ? user.name : 'Peer',
      });
      break;
    }

    case 'ice': {
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, { type: 'ice', candidate: msg.candidate });
      break;
    }

    case 'chat': {
      const user = users.get(ws);
      if (!user?.room) return;
      broadcastToRoom(user.room, ws, {
        type: 'chat',
        content: msg.content,
        name: LOG_SENSITIVE ? user.name : 'Peer',
        time: new Date().toLocaleTimeString(),
      });
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;

    default:
      console.log('Unknown message type:', msg.type);
  }
}

function send(ws, data) {
  if (ws.readyState === 1) ws.send(JSON.stringify(data));
}

function broadcastToRoom(room, senderWs, data) {
  (rooms.get(room) || []).forEach((client) => {
    if (client !== senderWs) send(client, data);
  });
}

// Vercel WebSocket handler — handles the HTTP upgrade
export default function handler(req, res) {
  if (req.headers.upgrade === 'websocket') {
    wss.handleUpgrade(req, req.socket, Buffer.alloc(0), (socket) => {
      wss.emit('connection', socket, req);
    });
  } else {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'text/plain');
    res.end('P2P Signaling WebSocket endpoint — connect with wss://<host>/api/ws\n');
  }
}
