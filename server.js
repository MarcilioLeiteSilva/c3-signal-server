/**
 * Construct 3 Compatible Signaling Server
 * Implements the exact C3 Multiplayer plugin protocol
 * Supports: multiple games, multiple instances, multiple rooms
 */

const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;

const SERVER_INFO = {
  protocolrev: 1,
  version: 1,
  name: 'Games Backend Signal Server',
  operator: 'InfinitGames',
  motd: 'Welcome!'
};

// Structure: games[gameName][instanceName][roomName] = { peers: Map, maxClients: N }
const games = new Map();

let nextClientId = 1;

function generateId() {
  return 'c' + (nextClientId++).toString(36) + Math.random().toString(36).substr(2, 4);
}

function getOrCreateRoom(gameName, instanceName, roomName, maxClients) {
  if (!games.has(gameName)) games.set(gameName, new Map());
  const gameInstances = games.get(gameName);

  if (!gameInstances.has(instanceName)) gameInstances.set(instanceName, new Map());
  const rooms = gameInstances.get(instanceName);

  if (!rooms.has(roomName)) {
    rooms.set(roomName, { peers: new Map(), maxClients: maxClients || 2 });
  }
  return rooms.get(roomName);
}

function getRoom(gameName, instanceName, roomName) {
  const gi = games.get(gameName);
  if (!gi) return null;
  const rooms = gi.get(instanceName);
  if (!rooms) return null;
  return rooms.get(roomName) || null;
}

function cleanEmptyRooms(gameName, instanceName, roomName) {
  const gi = games.get(gameName);
  if (!gi) return;
  const rooms = gi.get(instanceName);
  if (!rooms) return;
  const room = rooms.get(roomName);
  if (room && room.peers.size === 0) {
    rooms.delete(roomName);
    if (rooms.size === 0) gi.delete(instanceName);
    if (gi.size === 0) games.delete(gameName);
  }
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

const wss = new WebSocket.Server({ 
  port: PORT,
  handleProtocols: (protocols, request) => {
    // Construct 3 sometimes sends specific protocols, we must return one to accept
    return protocols.size > 0 ? [...protocols][0] : false;
  }
});

console.log(`[Signal] Server running on port ${PORT}`);

wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const clientId = generateId();
  ws.clientId = clientId;
  ws.alias = null;
  ws.game = null;
  ws.instance = null;
  ws.room = null;
  ws.isLoggedIn = false;

  console.log(`[Signal] Client connected: ${clientId} from IP: ${ip}`);

  // Send welcome
  send(ws, {
    message: 'welcome',
    myid: clientId,
    sigservinfo: SERVER_INFO
  });

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch (e) {
      console.warn(`[Signal] Invalid JSON from ${clientId}`);
      return;
    }

    const type = msg.message;

    // --- LOGIN ---
    if (type === 'login') {
      ws.alias = msg.alias || ('Player_' + clientId.substr(0, 4));
      ws.isLoggedIn = true;
      send(ws, { message: 'login-ok', myalias: ws.alias });
      console.log(`[Signal] ${clientId} logged in as "${ws.alias}"`);

    // --- JOIN ROOM ---
    } else if (type === 'join-room') {
      const { game, instance, room, maxClients } = msg;
      const roomObj = getOrCreateRoom(game, instance, room, maxClients);

      if (roomObj.peers.size >= roomObj.maxClients) {
        send(ws, { message: 'join-failed', reason: 'room-full' });
        return;
      }

      // Determine if this peer is host (first in room)
      const isHost = roomObj.peers.size === 0;
      const hostPeer = isHost ? ws : [...roomObj.peers.values()][0];

      roomObj.peers.set(clientId, ws);
      ws.game = game;
      ws.instance = instance;
      ws.room = room;

      send(ws, {
        message: 'join-ok',
        isHost,
        hostId: hostPeer.clientId,
        hostAlias: hostPeer.alias,
        game,
        gameInstance: instance,
        room
      });

      console.log(`[Signal] ${ws.alias} joined room "${game}/${instance}/${room}" (host: ${isHost})`);

    // --- AUTO JOIN ROOM ---
    } else if (type === 'auto-join-room') {
      const { game, instance, room: preferredRoom, maxClients, lock } = msg;
      const gi = games.get(game);
      let targetRoom = null;
      let targetRoomName = null;

      // Find available room
      if (gi && gi.has(instance)) {
        for (const [rName, rObj] of gi.get(instance)) {
          if (rObj.peers.size < rObj.maxClients) {
            targetRoom = rObj;
            targetRoomName = rName;
            break;
          }
        }
      }

      // Create new room if none available
      if (!targetRoom) {
        targetRoomName = preferredRoom + '_' + Date.now();
        targetRoom = getOrCreateRoom(game, instance, targetRoomName, maxClients);
      }

      const isHost = targetRoom.peers.size === 0;
      const hostPeer = isHost ? ws : [...targetRoom.peers.values()][0];

      targetRoom.peers.set(clientId, ws);
      ws.game = game;
      ws.instance = instance;
      ws.room = targetRoomName;

      send(ws, {
        message: 'join-ok',
        isHost,
        hostId: hostPeer.clientId,
        hostAlias: hostPeer.alias,
        game,
        gameInstance: instance,
        room: targetRoomName
      });

      console.log(`[Signal] ${ws.alias} auto-joined "${game}/${instance}/${targetRoomName}"`);

    // --- LEAVE ROOM ---
    } else if (type === 'leave-room') {
      _leaveRoom(ws);
      send(ws, { message: 'leave-ok' });

    // --- ICE CANDIDATE relay ---
    } else if (type === 'icecandidate') {
      const target = findPeerById(msg.toclientid);
      if (target) {
        send(target, {
          message: 'icecandidate',
          fromclientid: clientId,
          icecandidate: msg.icecandidate
        });
      }

    // --- OFFER relay ---
    } else if (type === 'offer') {
      const target = findPeerById(msg.toclientid);
      if (target) {
        send(target, {
          message: 'offer',
          fromclientid: clientId,
          offer: msg.offer
        });
      }

    // --- ANSWER relay ---
    } else if (type === 'answer') {
      const target = findPeerById(msg.toclientid);
      if (target) {
        send(target, {
          message: 'answer',
          fromclientid: clientId,
          answer: msg.answer
        });
      }

    // --- CONFIRM PEER ---
    } else if (type === 'confirm-peer') {
      const target = findPeerById(msg.peerid);
      if (target) {
        send(target, { message: 'peer-confirmed', peerid: clientId });
      }

    // --- LIST GAME INSTANCES ---
    } else if (type === 'list-game-instances') {
      const { game } = msg;
      const list = [];
      const gi = games.get(game);
      if (gi) {
        for (const [instName, rooms] of gi) {
          let peerCount = 0;
          for (const r of rooms.values()) peerCount += r.peers.size;
          list.push({ name: instName, peercount: peerCount });
        }
      }
      send(ws, { message: 'instance-list', list });

    // --- LIST ROOMS ---
    } else if (type === 'list-rooms') {
      const { game, instance } = msg;
      const list = [];
      const gi = games.get(game);
      if (gi && gi.has(instance)) {
        for (const [rName, rObj] of gi.get(instance)) {
          list.push({
            name: rName,
            peercount: rObj.peers.size,
            maxpeercount: rObj.maxClients,
            state: rObj.peers.size < rObj.maxClients ? 'available' : 'locked'
          });
        }
      }
      send(ws, { message: 'room-list', list });
    }
  });

  ws.on('close', () => {
    console.log(`[Signal] Client disconnected: ${ws.alias || clientId}`);
    _leaveRoom(ws);
  });

  ws.on('error', (err) => {
    console.error(`[Signal] Error from ${clientId}:`, err.message);
  });
});

function findPeerById(id) {
  for (const client of wss.clients) {
    if (client.clientId === id) return client;
  }
  return null;
}

function _leaveRoom(ws) {
  if (!ws.room) return;
  const room = getRoom(ws.game, ws.instance, ws.room);
  if (room) {
    room.peers.delete(ws.clientId);
    cleanEmptyRooms(ws.game, ws.instance, ws.room);
  }
  ws.game = null;
  ws.instance = null;
  ws.room = null;
}
