/**
 * Soccer Cup 2026 — Signaling Server v3.0
 * Protocolo: c2multiplayer (compatível exato com servidor oficial Scirra)
 *
 * CORREÇÕES CRÍTICAS vs versão anterior:
 *  1. confirm-peer: NÃO envia peer-confirmed de volta (Scirra não envia — C3 não espera essa msg)
 *  2. Trata "leave" E "leave-room" (C3 envia {"message":"leave"}, não "leave-room")
 *  3. Envia "peer-quit" para peers restantes quando alguém desconecta
 *  4. Envia "kicked" com reason:"host-left" quando o host desconecta
 *  5. Implementa lock_when_full corretamente
 *  6. Remove sufixo de timestamp no auto-join (causava inconsistência)
 *  7. Protocolo handleProtocols retorna false se não reconhecido (não string vazia)
 *  8. Todos os campos esperados pelo C3 estão presentes e com os nomes corretos
 */

'use strict';

const http    = require('http');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const HOST = '0.0.0.0';

// Protocolo exato que o C3 usa (veja main.js: "c2multiplayer")
const PROTOCOL_REV = 1;

// ICE Servers com TURN — injetados no welcome
const ICE_SERVERS = [
  {
    urls: [
      'turn:gamesmultiplayer-coturn.gtalg3.easypanel.host:3478',
      'turn:gamesmultiplayer-coturn.gtalg3.easypanel.host:3478?transport=udp',
      'turn:gamesmultiplayer-coturn.gtalg3.easypanel.host:3478?transport=tcp'
    ],
    username:   'soccer',
    credential: 'soccer123'
  },
  { urls: 'stun:stun.l.google.com:19302'  },
  { urls: 'stun:stun1.l.google.com:19302' }
];

// ── Estrutura de dados ─────────────────────────────────────────────────────────
// games[gameName][instanceName][roomName] = { peers: Map<id,ws>, maxClients, locked }
const games = new Map();
let _nextId = 1;

function generateId() {
  return 'c' + (_nextId++).toString(36) + Math.random().toString(36).substr(2, 4);
}

function getRoom(game, instance, room) {
  return games.get(game)?.get(instance)?.get(room) ?? null;
}

function getOrCreateRoom(game, instance, room, maxClients) {
  if (!games.has(game))            games.set(game, new Map());
  if (!games.get(game).has(instance)) games.get(game).set(instance, new Map());
  const rooms = games.get(game).get(instance);
  if (!rooms.has(room)) rooms.set(room, { peers: new Map(), maxClients: maxClients || 2, locked: false });
  return rooms.get(room);
}

function cleanEmptyRooms(game, instance, room) {
  const gi = games.get(game);    if (!gi) return;
  const ri = gi.get(instance);   if (!ri) return;
  const r  = ri.get(room);       if (!r)  return;
  if (r.peers.size === 0) {
    ri.delete(room);
    if (ri.size === 0) gi.delete(instance);
    if (gi.size === 0) games.delete(game);
  }
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); }
    catch (e) { console.error('[Signal] send error:', e.message); }
  }
}

function findPeerById(id) {
  for (const c of wss.clients) if (c.clientId === id) return c;
  return null;
}

// ── Sair da sala ───────────────────────────────────────────────────────────────
function leaveRoom(ws, reason) {
  if (!ws.room) return;

  const room = getRoom(ws.game, ws.instance, ws.room);
  if (!room) { ws.game = ws.instance = ws.room = null; return; }

  // Verifica se este peer é o host (primeiro da lista)
  const peers      = [...room.peers.values()];
  const wasHost    = peers.length > 0 && peers[0].clientId === ws.clientId;

  room.peers.delete(ws.clientId);

  // Notifica os peers restantes
  for (const [, peer] of room.peers) {
    if (wasHost) {
      // Host saiu → kicka os clientes (igual ao Scirra)
      send(peer, { message: 'kicked', reason: 'host-left' });
    } else {
      // Peer comum saiu → notifica o host
      send(peer, { message: 'peer-quit', id: ws.clientId, reason: reason || 'disconnect' });
    }
  }

  console.log(`[Signal] LEAVE: ${ws.alias || ws.clientId} | room:"${ws.room}" | wasHost:${wasHost} | reason:${reason || 'disconnect'}`);

  cleanEmptyRooms(ws.game, ws.instance, ws.room);
  ws.game = ws.instance = ws.room = null;
}

// ── HTTP + WebSocket ───────────────────────────────────────────────────────────
process.on('uncaughtException',  err    => console.error('[Signal] UncaughtException:', err));
process.on('unhandledRejection', reason => console.error('[Signal] UnhandledRejection:', reason));

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Soccer Cup 2026 — Signal Server OK');
});

const wss = new WebSocket.Server({
  server,
  // Negociação de protocolo idêntica ao Scirra
  handleProtocols: (protocols) => {
    const list = Array.isArray(protocols) ? protocols : Array.from(protocols || []);
    if (list.includes('c2multiplayer')) return 'c2multiplayer';
    if (list.includes('c3multiplayer')) return 'c3multiplayer';
    return false; // Rejeita conexão sem protocolo válido (Scirra faz o mesmo)
  }
});

server.listen(PORT, HOST, () => {
  console.log(`[Signal] Server v3.0 listening on ${HOST}:${PORT}`);
});

// ── Conexão ────────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?';
  ws.clientId    = generateId();
  ws.alias       = null;
  ws.game        = null;
  ws.instance    = null;
  ws.room        = null;
  ws.isLoggedIn  = false;
  ws.lastPing    = Date.now();

  console.log(`[Signal] CONNECT: ${ws.clientId} from ${ip}`);

  // ── WELCOME ── (enviado imediatamente, igual ao Scirra)
  send(ws, {
    message:     'welcome',
    clientid:    ws.clientId,   // C3 lê "clientid"
    protocolrev: PROTOCOL_REV,
    version:     1,
    name:        'Soccer Cup 2026 Signal Server',
    operator:    'InfinitGames',
    motd:        'Bem-vindo ao Soccer Cup 2026!',
    ice_servers: ICE_SERVERS
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); }
    catch { console.warn(`[Signal] JSON inválido de ${ws.clientId}`); return; }

    const type = msg.message;
    console.log(`[Signal] MSG [${ws.alias || ws.clientId}] -> ${type}`);

    switch (type) {

      // ── PING ──────────────────────────────────────────────────────────────
      case 'ping': {
        ws.lastPing = Date.now();
        send(ws, { message: 'pong', 'server-time': Date.now() });
        break;
      }

      // ── LOGIN ─────────────────────────────────────────────────────────────
      // C3 envia: { message:"login", alias:"...", protocolrev:1, datachannelrev:2 }
      // C3 espera: { message:"login-ok", alias:"..." }
      case 'login': {
        ws.alias      = msg.alias || ('Player' + Math.floor(Math.random() * 9999));
        ws.isLoggedIn = true;
        send(ws, { message: 'login-ok', alias: ws.alias });
        console.log(`[Signal] LOGIN: ${ws.clientId} as "${ws.alias}"`);
        break;
      }

      // ── JOIN ROOM ─────────────────────────────────────────────────────────
      // C3 envia: { message:"join", game, instance, room, max_clients }
      // C3 espera: { message:"join-ok", host:bool, hostid, hostalias, game, instance, room }
      case 'join':
      case 'join-room': {
        if (!ws.isLoggedIn) { send(ws, { message: 'error', details: 'not-logged-in' }); break; }
        const { game, instance, room, max_clients } = msg;
        if (!game || !instance || !room) { send(ws, { message: 'error', details: 'missing-params' }); break; }

        const roomObj = getOrCreateRoom(game, instance, room, max_clients || 2);

        if (roomObj.locked || roomObj.peers.size >= roomObj.maxClients) {
          send(ws, { message: 'error', details: 'room-full' });
          break;
        }

        const isHost   = roomObj.peers.size === 0;
        const hostPeer = isHost ? ws : [...roomObj.peers.values()][0];

        // Notifica peers existentes que novo peer entrou
        if (!isHost) {
          for (const [, peer] of roomObj.peers) {
            send(peer, { message: 'peer-joined', peerid: ws.clientId, peeralias: ws.alias });
          }
        }

        roomObj.peers.set(ws.clientId, ws);
        ws.game = game; ws.instance = instance; ws.room = room;

        send(ws, {
          message:   'join-ok',
          host:      isHost,
          hostid:    hostPeer.clientId,
          hostalias: hostPeer.alias,
          game, instance, room
        });
        console.log(`[Signal] JOIN: "${ws.alias}" -> "${room}" (host:${isHost})`);
        break;
      }

      // ── AUTO-JOIN ─────────────────────────────────────────────────────────
      // C3 envia: { message:"auto-join", game, instance, room, max_clients, lock_when_full }
      // C3 espera: { message:"join-ok", ... } (mesmo do join)
      case 'auto-join':
      case 'auto-join-room': {
        if (!ws.isLoggedIn) { send(ws, { message: 'error', details: 'not-logged-in' }); break; }
        const { game, instance, room: preferredRoom, max_clients, lock_when_full } = msg;
        if (!game || !instance) { send(ws, { message: 'error', details: 'missing-params' }); break; }

        // Procura sala disponível
        let targetRoom     = null;
        let targetRoomName = null;
        const gi = games.get(game);
        if (gi?.has(instance)) {
          for (const [rName, rObj] of gi.get(instance)) {
            if (!rObj.locked && rObj.peers.size < rObj.maxClients) {
              targetRoom     = rObj;
              targetRoomName = rName;
              break;
            }
          }
        }

        // Cria sala nova se não encontrou nenhuma disponível
        if (!targetRoom) {
          targetRoomName = preferredRoom || ('room_' + Date.now());
          targetRoom     = getOrCreateRoom(game, instance, targetRoomName, max_clients || 2);
        }

        const isHost   = targetRoom.peers.size === 0;
        const hostPeer = isHost ? ws : [...targetRoom.peers.values()][0];

        // Notifica peers existentes
        if (!isHost) {
          for (const [, peer] of targetRoom.peers) {
            send(peer, { message: 'peer-joined', peerid: ws.clientId, peeralias: ws.alias });
          }
        }

        targetRoom.peers.set(ws.clientId, ws);
        ws.game = game; ws.instance = instance; ws.room = targetRoomName;

        // Trava a sala se estiver cheia e lock_when_full estiver ativo
        if (lock_when_full && targetRoom.peers.size >= targetRoom.maxClients) {
          targetRoom.locked = true;
          console.log(`[Signal] ROOM LOCKED: "${targetRoomName}"`);
        }

        send(ws, {
          message:   'join-ok',
          host:      isHost,
          hostid:    hostPeer.clientId,
          hostalias: hostPeer.alias,
          game, instance,
          room: targetRoomName
        });
        console.log(`[Signal] AUTO-JOIN: "${ws.alias}" -> "${targetRoomName}" (host:${isHost})`);
        break;
      }

      // ── LEAVE ─────────────────────────────────────────────────────────────
      // ⚠️  C3 envia {"message":"leave"} — NÃO "leave-room"!
      // C3 espera: { message:"leave-ok" }
      case 'leave':
      case 'leave-room': {
        leaveRoom(ws, 'leave');
        send(ws, { message: 'leave-ok' });
        break;
      }

      // ── CONFIRM PEER ──────────────────────────────────────────────────────
      // ⚠️  CORREÇÃO CRÍTICA: Scirra NÃO envia "peer-confirmed" de volta.
      //    A versão anterior enviava peer-confirmed, que o C3 não reconhece
      //    e dispara _OnSignallingError → DisconnectRoom → crash.
      //    Aqui apenas logamos e ignoramos.
      case 'confirm-peer': {
        console.log(`[Signal] CONFIRM-PEER: ${ws.alias} confirmou peer ${msg.id} (sem resposta — protocolo Scirra)`);
        // Intencional: nenhuma resposta enviada.
        break;
      }

      // ── RELAY: ICE CANDIDATE ─────────────────────────────────────────────
      // C3 envia: { message:"icecandidate", toclientid, icecandidate }
      // C3 espera receber: { message:"icecandidate", from, icecandidate }
      case 'icecandidate': {
        const target = findPeerById(msg.toclientid || msg.to);
        if (target) send(target, { message: 'icecandidate', from: ws.clientId, icecandidate: msg.icecandidate });
        else console.warn(`[Signal] ICE target not found: ${msg.toclientid || msg.to}`);
        break;
      }

      // ── RELAY: OFFER ──────────────────────────────────────────────────────
      // C3 envia: { message:"offer", toclientid, offer }
      // C3 espera receber: { message:"offer", from, offer }
      case 'offer': {
        const target = findPeerById(msg.toclientid || msg.to);
        if (target) send(target, { message: 'offer', from: ws.clientId, offer: msg.offer });
        else console.warn(`[Signal] OFFER target not found: ${msg.toclientid || msg.to}`);
        break;
      }

      // ── RELAY: ANSWER ─────────────────────────────────────────────────────
      // C3 envia: { message:"answer", toclientid, answer }
      // C3 espera receber: { message:"answer", from, answer }
      case 'answer': {
        const target = findPeerById(msg.toclientid || msg.to);
        if (target) send(target, { message: 'answer', from: ws.clientId, answer: msg.answer });
        else console.warn(`[Signal] ANSWER target not found: ${msg.toclientid || msg.to}`);
        break;
      }

      // ── LIST INSTANCES ────────────────────────────────────────────────────
      case 'list-instances':
      case 'list-game-instances': {
        const list = [];
        const gi   = games.get(msg.game);
        if (gi) {
          for (const [instName, rooms] of gi) {
            let peercount = 0;
            for (const r of rooms.values()) peercount += r.peers.size;
            list.push({ name: instName, peercount });
          }
        }
        send(ws, { message: 'instance-list', list });
        break;
      }

      // ── LIST ROOMS ────────────────────────────────────────────────────────
      case 'list-rooms': {
        const list = [];
        const gi   = games.get(msg.game);
        if (gi?.has(msg.instance)) {
          for (const [rName, rObj] of gi.get(msg.instance)) {
            list.push({
              name:         rName,
              peercount:    rObj.peers.size,
              maxpeercount: rObj.maxClients,
              state:        (rObj.locked || rObj.peers.size >= rObj.maxClients) ? 'locked' : 'available'
            });
          }
        }
        send(ws, { message: 'room-list', list });
        break;
      }

      default:
        console.warn(`[Signal] Mensagem desconhecida: "${type}" de ${ws.alias || ws.clientId}`);
    }
  });

  ws.on('close', () => {
    console.log(`[Signal] DISCONNECT: ${ws.alias || ws.clientId}`);
    leaveRoom(ws, 'disconnect');
  });

  ws.on('error', (err) => {
    console.error(`[Signal] Erro em ${ws.clientId}:`, err.message);
    try { ws.terminate(); } catch (_) {}
  });
});

// ── Keepalive ─────────────────────────────────────────────────────────────────
// Detecta conexões zumbis e as remove após 60s sem ping
setInterval(() => {
  const now = Date.now();
  for (const ws of wss.clients) {
    if (now - ws.lastPing > 60000) {
      console.warn(`[Signal] ZOMBIE: ${ws.alias || ws.clientId} — terminando.`);
      leaveRoom(ws, 'timeout');
      ws.terminate();
    }
  }
}, 30000);
