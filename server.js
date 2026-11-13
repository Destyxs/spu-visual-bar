const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'users.json');
const MUSIC_DIR = path.join(__dirname, 'music');

if (!fs.existsSync(MUSIC_DIR)) {
  fs.mkdirSync(MUSIC_DIR, { recursive: true });
}

app.use(express.json());
app.use('/music', express.static(MUSIC_DIR));

// ============================================================================
// 1. DATABASE, MUSIC FOLDER & YOUTUBE HELPERS
// ============================================================================
function readUsersDB() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify([], null, 2), 'utf8');
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  } catch {
    return [];
  }
}

function writeUsersDB(users) {
  fs.writeFileSync(DB_FILE, JSON.stringify(users, null, 2), 'utf8');
}

function extractYoutubeId(input) {
  if (!input) return null;
  const str = String(input).trim();
  const match = str.match(
    /(?:youtube\.com\/(?:watch\?(?:.*&)?v=|embed\/|shorts\/|v\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/
  );
  if (match && match[1]) return match[1];
  if (/^[a-zA-Z0-9_-]{11}$/.test(str)) return str;
  return null;
}

function fetchYoutubeTitle(videoId) {
  return new Promise((resolve) => {
    const oembedUrl = `https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v=${videoId}&format=json`;
    const req = https.get(oembedUrl, { timeout: 2500 }, (res) => {
      let raw = '';
      res.on('data', (chunk) => {
        raw += chunk;
      });
      res.on('end', () => {
        try {
          const parsed = JSON.parse(raw);
          resolve(parsed.title || `YouTube (${videoId})`);
        } catch {
          resolve(`YouTube (${videoId})`);
        }
      });
    });
    req.on('error', () => resolve(`YouTube (${videoId})`));
    req.on('timeout', () => {
      req.destroy();
      resolve(`YouTube (${videoId})`);
    });
  });
}

function getRequiredSkipVotes(memberCount) {
  return Math.floor(Math.max(memberCount, 1) / 2) + 1;
}

app.get('/api/music', (req, res) => {
  try {
    if (!fs.existsSync(MUSIC_DIR)) {
      return res.json({ tracks: [] });
    }
    const allowedExts = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac'];
    const files = fs.readdirSync(MUSIC_DIR);
    const tracks = files
      .filter((file) => allowedExts.includes(path.extname(file).toLowerCase()))
      .map((file) => ({
        title: file.replace(/\.[^/.]+$/, ''),
        src: '/music/' + encodeURIComponent(file),
      }));

    res.json({ tracks });
  } catch {
    res.json({ tracks: [] });
  }
});

// ============================================================================
// 2. REST API: สมัครสมาชิก / เข้าสู่ระบบ / อัปเดตโปรไฟล์และอารมณ์
// ============================================================================
app.post('/api/register', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: 'กรุณากรอกชื่อเล่นและรหัสผ่านให้ครบ' });
  }

  const users = readUsersDB();
  const exists = users.some(
    (u) => u.username.toLowerCase() === username.trim().toLowerCase()
  );

  if (exists) {
    return res.status(409).json({ error: 'ชื่อเล่นนี้มีคนใช้แล้ว กรุณาเลือกชื่ออื่น' });
  }

  const newUser = {
    id: `user_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
    username: username.trim(),
    password,
    moodId: 'sad',
    hatId: 'beret',
    createdAt: new Date().toISOString(),
  };

  users.push(newUser);
  writeUsersDB(users);

  const { password: _, ...safeUser } = newUser;
  res.json({ user: safeUser });
});

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  const users = readUsersDB();
  const found = users.find(
    (u) =>
      u.username.toLowerCase() === (username || '').trim().toLowerCase() &&
      u.password === password
  );

  if (!found) {
    return res.status(401).json({ error: 'ชื่อเล่นหรือรหัสผ่านไม่ถูกต้อง' });
  }

  const { password: _, ...safeUser } = found;
  res.json({ user: safeUser });
});

app.post('/api/user/update', (req, res) => {
  const { id, username, moodId, hatId } = req.body;
  const users = readUsersDB();
  const idx = users.findIndex((u) => u.id === id);

  if (idx === -1) {
    return res.status(404).json({ error: 'ไม่พบข้อมูลผู้ใช้' });
  }

  if (username) users[idx].username = username.trim();
  if (moodId) users[idx].moodId = moodId;
  if (hatId) users[idx].hatId = hatId;
  writeUsersDB(users);

  const { password: _, ...safeUser } = users[idx];
  res.json({ user: safeUser });
});

// ============================================================================
// 3. REAL-TIME MATCHMAKING & ROOMS (Socket.io)
// ============================================================================
const activeRooms = new Map();
const onlineSockets = new Map();

function broadcastOnlineStats() {
  const musicRoom = activeRooms.get('music-lounge');
  io.emit('stats:update', {
    onlineCount: Math.max(onlineSockets.size, 1),
    musicRoomCount: musicRoom ? musicRoom.members.length : 0,
  });
}

function isAgeCompatible(roomAge, userAge) {
  if (roomAge === 'วัยไหนก็ได้' || userAge === 'วัยไหนก็ได้') return true;
  return roomAge === userAge;
}

function leaveCurrentRoom(socket) {
  const roomCode = socket.data.roomCode;
  if (!roomCode) return;

  const room = activeRooms.get(roomCode);
  socket.leave(roomCode);
  socket.data.roomCode = null;

  if (!room) return;

  const leavingMember = room.members.find((m) => m.socketId === socket.id);
  room.members = room.members.filter((m) => m.socketId !== socket.id);

  if (room.skipVotes) {
    room.skipVotes = room.skipVotes.filter((id) => id !== socket.id);
  }

  if (room.members.length === 0) {
    activeRooms.delete(roomCode);
  } else {
    if (leavingMember) {
      room.messages.push({
        id: Date.now(),
        type: 'system',
        text: `${leavingMember.name} ออกจากโต๊ะแล้ว 👋`,
      });
    }
    if (room.mode === 'venting' && room.members.length < 2) {
      room.isWaitingListener = true;
    }

    if (room.mode === 'music' && room.youtubeQueue && room.youtubeQueue.length > 0) {
      const required = getRequiredSkipVotes(room.members.length);
      if (room.skipVotes && room.skipVotes.length >= required) {
        const skipped = room.youtubeQueue.shift();
        room.skipVotes = [];
        if (room.youtubeQueue.length > 0) {
          room.youtubeQueue[0].startedAt = Date.now();
        }
        room.messages.push({
          id: Date.now(),
          type: 'system',
          text: `⏭️ ผลโหวตครบ (${required}/${required} คน) ข้ามเพลง "${skipped.title}" แล้ว!`,
        });
      }
    }

    io.to(roomCode).emit('table:sync', room);
  }

  broadcastOnlineStats();
}

io.on('connection', (socket) => {
  onlineSockets.set(socket.id, null);
  broadcastOnlineStats();

  socket.on('user:identify', (userId) => {
    onlineSockets.set(socket.id, userId);
    broadcastOnlineStats();
  });

  socket.on('match:find', ({ user, mood, mode, ageGroup, isBartender, customCode, excludeCode }) => {
    leaveCurrentRoom(socket);

    let targetRoom = null;

    if (customCode) {
      targetRoom = activeRooms.get(customCode.trim());
      if (!targetRoom) {
        socket.emit('match:error', `ไม่พบโต๊ะรหัส "${customCode}" หรือโต๊ะถูกปิดไปแล้ว`);
        return;
      }
      if (targetRoom.members.length >= targetRoom.maxSeats) {
        socket.emit('match:error', 'โต๊ะนี้มีผู้เล่นเต็มแล้ว');
        return;
      }
    } else if (mode === 'music') {
      targetRoom = activeRooms.get('music-lounge');
      if (!targetRoom) {
        targetRoom = {
          code: 'music-lounge',
          title: 'ห้องสิงฟังเพลง YouTube 🎧',
          mode: 'music',
          maxSeats: 5,
          ageGroup: 'ทุกช่วงวัย',
          isPublic: true,
          isWaitingListener: false,
          members: [],
          playlist: [],
          youtubeQueue: [],
          skipVotes: [],
          messages: [
            {
              id: 'sys-music',
              type: 'system',
              text: 'ยินดีต้อนรับสู่ห้องสิงฟังเพลง! วางลิงก์ YouTube เพื่อเพิ่มเข้าคิว (เพลงจบแล้วจะหายไปจากคิวอัตโนมัติ)',
            },
          ],
        };
        activeRooms.set('music-lounge', targetRoom);
      }
    } else if (mode === 'venting') {
      for (const [code, room] of activeRooms.entries()) {
        if (code === excludeCode) continue;
        if (room.mode === 'venting' && room.members.length < 2) {
          const hasBartenderInRoom = room.members.some((m) => m.isBartender);
          if ((isBartender && !hasBartenderInRoom) || (!isBartender && hasBartenderInRoom)) {
            targetRoom = room;
            break;
          }
        }
      }

      if (!targetRoom) {
        const newCode = Math.random().toString(36).substring(2, 8);
        targetRoom = {
          code: newCode,
          title: 'มุมรับฟังและระบายความในใจ',
          mode: 'venting',
          maxSeats: 2,
          ageGroup: 'ระบาย',
          isPublic: false,
          isWaitingListener: true,
          members: [],
          playlist: ['Warm Piano & Rain - Comfort Mix'],
          youtubeQueue: [],
          skipVotes: [],
          messages: [
            {
              id: 'sys-vent',
              type: 'system',
              text: 'พื้นที่ปลอดภัยสำหรับรับฟังและระบาย งดเปิดเผยข้อมูลส่วนตัวนะครับ',
            },
          ],
        };
        activeRooms.set(newCode, targetRoom);
      }
    } else {
      const maxSeats = mode === 'group' ? 5 : 2;
      for (const [code, room] of activeRooms.entries()) {
        if (code === excludeCode) continue;
        if (
          room.mode === mode &&
          room.isPublic &&
          room.members.length < room.maxSeats &&
          isAgeCompatible(room.ageGroup, ageGroup)
        ) {
          targetRoom = room;
          break;
        }
      }

      if (!targetRoom) {
        const newCode = Math.random().toString(36).substring(2, 8);
        targetRoom = {
          code: newCode,
          title: 'ใส่ชื่อให้โต๊ะของคุณ',
          mode,
          maxSeats,
          ageGroup: ageGroup || 'วัยไหนก็ได้',
          isPublic: true,
          isWaitingListener: false,
          members: [],
          playlist: ['Lo-Fi Rainy Night Beats - Bar Radio'],
          youtubeQueue: [],
          skipVotes: [],
          messages: [
            {
              id: 'sys-warn',
              type: 'system',
              text: 'เพื่อความปลอดภัยในการใช้งาน เราขอแนะนำให้งดส่งข้อมูลส่วนตัวให้กับคนแปลกหน้า',
            },
          ],
        };
        activeRooms.set(newCode, targetRoom);
      }
    }

    const memberObj = {
      socketId: socket.id,
      id: user.id,
      name: user.username,
      hatId: user.hatId || 'beret',
      mood,
      isBartender: Boolean(isBartender),
      latestBubble: '',
    };

    targetRoom.members.push(memberObj);
    if (targetRoom.mode === 'venting' && targetRoom.members.length >= 2) {
      targetRoom.isWaitingListener = false;
    }

    const roleLabel = isBartender ? ' (บาร์เทนเดอร์ / ผู้รับฟัง)' : '';
    targetRoom.messages.push({
      id: Date.now(),
      type: 'system',
      text: `${user.username}${roleLabel} เข้าร่วมโต๊ะ 👋`,
    });

    socket.join(targetRoom.code);
    socket.data.roomCode = targetRoom.code;

    io.to(targetRoom.code).emit('table:sync', targetRoom);
    broadcastOnlineStats();
  });

  socket.on('table:message', ({ text }) => {
    const roomCode = socket.data.roomCode;
    const room = activeRooms.get(roomCode);
    if (!room || !text) return;

    const sender = room.members.find((m) => m.socketId === socket.id);
    if (!sender) return;

    sender.latestBubble = text;
    room.messages.push({
      id: Date.now(),
      type: 'user',
      senderName: sender.name,
      text,
    });

    io.to(roomCode).emit('table:sync', room);
  });

  socket.on('table:updateTitle', ({ title }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room) return;
    room.title = title || 'ใส่ชื่อให้โต๊ะของคุณ';
    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:togglePublic', ({ isPublic }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room) return;
    room.isPublic = Boolean(isPublic);
    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:addSong', ({ song }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room || !song) return;
    room.playlist.push(song);
    room.messages.push({
      id: Date.now(),
      type: 'system',
      text: `🎵 เพิ่มเพลง "${song}" เข้าตู้เพลงแล้ว`,
    });
    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:addYoutube', async ({ url }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room) return;
    const sender = room.members.find((m) => m.socketId === socket.id);
    if (!sender) return;

    const videoId = extractYoutubeId(url);
    if (!videoId) {
      socket.emit('match:error', 'ลิงก์ YouTube ไม่ถูกต้อง กรุณาวางลิงก์จาก YouTube (เช่น https://www.youtube.com/watch?v=... หรือ https://youtu.be/...)');
      return;
    }

    const title = await fetchYoutubeTitle(videoId);
    if (!room.youtubeQueue) room.youtubeQueue = [];
    if (!room.skipVotes) room.skipVotes = [];

    const isFirstInQueue = room.youtubeQueue.length === 0;
    room.youtubeQueue.push({
      id: `yt_${Date.now()}_${Math.floor(Math.random() * 10000)}`,
      videoId,
      title,
      addedBy: sender.name,
      startedAt: isFirstInQueue ? Date.now() : null,
    });

    room.messages.push({
      id: Date.now(),
      type: 'system',
      text: `🎬 ${sender.name} เพิ่มเพลง "${title}" เข้าคิวแล้ว`,
    });

    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:youtubeEnded', ({ trackId }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room || room.mode !== 'music' || !room.youtubeQueue || room.youtubeQueue.length === 0) return;

    const currentTrack = room.youtubeQueue[0];
    if (currentTrack.id !== trackId) return;

    const finished = room.youtubeQueue.shift();
    room.skipVotes = [];

    if (room.youtubeQueue.length > 0) {
      room.youtubeQueue[0].startedAt = Date.now();
    }

    room.messages.push({
      id: Date.now(),
      type: 'system',
      text: `✅ เพลง "${finished.title}" จบแล้วและถูกนำออกจากคิว`,
    });

    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:voteSkip', () => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room || room.mode !== 'music' || !room.youtubeQueue || room.youtubeQueue.length === 0) return;

    const voter = room.members.find((m) => m.socketId === socket.id);
    if (!voter) return;

    if (!room.skipVotes) room.skipVotes = [];
    const existingIdx = room.skipVotes.indexOf(socket.id);

    if (existingIdx !== -1) {
      room.skipVotes.splice(existingIdx, 1);
      io.to(room.code).emit('table:sync', room);
      return;
    }

    room.skipVotes.push(socket.id);
    const required = getRequiredSkipVotes(room.members.length);
    const currentTrack = room.youtubeQueue[0];

    if (room.skipVotes.length >= required) {
      const skipped = room.youtubeQueue.shift();
      room.skipVotes = [];
      if (room.youtubeQueue.length > 0) {
        room.youtubeQueue[0].startedAt = Date.now();
      }
      room.messages.push({
        id: Date.now(),
        type: 'system',
        text: `⏭️ ผลโหวตครบ (${required}/${required} คน) ข้ามเพลง "${skipped.title}" แล้ว!`,
      });
    } else {
      room.messages.push({
        id: Date.now(),
        type: 'system',
        text: `🗳️ ${voter.name} ข้ามเพลง "${currentTrack.title}" (${room.skipVotes.length}/${required} คน)`,
      });
    }

    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:icebreaker', ({ question }) => {
    const room = activeRooms.get(socket.data.roomCode);
    if (!room) return;
    room.messages.push({
      id: Date.now(),
      type: 'system',
      text: `🃏 การ์ดคำถาม: "${question}"`,
    });
    io.to(room.code).emit('table:sync', room);
  });

  socket.on('table:leave', () => {
    leaveCurrentRoom(socket);
  });

  socket.on('disconnect', () => {
    leaveCurrentRoom(socket);
    onlineSockets.delete(socket.id);
    broadcastOnlineStats();
  });
});

// ============================================================================
// 4. SINGLE-FILE FRONTEND (HTML + COZY LO-FI CSS + CLIENT JS)
// ============================================================================
const HTML_PAGE = `<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover" />
  <title>SPU VISUAL BAR - บาร์ทิพย์พื้นที่พักใจ</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Mali:wght@400;500;600;700&family=Prompt:wght@300;400;500;600&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg-paper: #faf6ee;
      --bg-dot: #e6decb;
      --surface-card: #fffdf9;
      --surface-soft: #f4efe6;
      --surface-tint: #f3ecff;
      --ink-primary: #3e322c;
      --ink-secondary: #75655c;
      --border-ink: #4a3f35;
      --accent-lavender: #9b82d0;
      --accent-lavender-light: #ebe2fc;
      --accent-amber: #f5b963;
      --accent-rose: #f29ca3;
      --accent-mint: #88c498;
      --font-display: 'Mali', cursive, sans-serif;
      --font-body: 'Prompt', sans-serif;
      --radius-sm: 10px;
      --radius-md: 16px;
      --radius-lg: 24px;
      --radius-pill: 999px;
      --shadow-hand: 0 3px 0 var(--border-ink);
      --shadow-card: 0 6px 0 rgba(74, 63, 53, 0.12);
    }

    [hidden] { display: none !important; }
    *, *::before, *::after { box-sizing: border-box; }

    html, body {
      max-width: 100%;
      overflow-x: hidden;
      padding-left: env(safe-area-inset-left);
      padding-right: env(safe-area-inset-right);
    }

    body {
      margin: 0;
      padding: 0;
      color: var(--ink-primary);
      font-family: var(--font-body);
      font-size: 15px;
      line-height: 1.6;
      background-color: var(--bg-paper);
      background-image: radial-gradient(var(--bg-dot) 1.3px, transparent 1.3px);
      background-size: 22px 22px;
      min-height: 100vh;
    }

    h1, h2, h3, h4, .brand {
      font-family: var(--font-display);
      color: var(--ink-primary);
      margin-top: 0;
    }

    a {
      color: #6f52ad;
      font-weight: 500;
      text-decoration: underline;
      text-underline-offset: 3px;
    }

    hr {
      border: none;
      border-top: 2px dashed #dcd2c0;
      margin: 18px 0;
    }

    ::-webkit-scrollbar { width: 7px; height: 7px; }
    ::-webkit-scrollbar-track { background: var(--surface-soft); border-radius: 10px; }
    ::-webkit-scrollbar-thumb { background: #cbbba8; border-radius: 10px; }

    /* Navbar */
    .navbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 14px 28px;
      background: rgba(255, 253, 249, 0.92);
      backdrop-filter: blur(6px);
      border-bottom: 2px solid var(--border-ink);
      position: sticky;
      top: 0;
      z-index: 20;
    }

    .brand {
      font-size: 1.25rem;
      font-weight: 700;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 4px 16px;
      background: var(--accent-amber);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-pill);
      box-shadow: 0 2px 0 var(--border-ink);
    }

    .brand::before { content: '🍸'; }
    .nav-left, .nav-actions { display: flex; align-items: center; gap: 8px; }

    /* Buttons & Inputs */
    button {
      font-family: var(--font-display);
      font-weight: 600;
      font-size: 0.95rem;
      color: var(--ink-primary);
      background: var(--surface-card);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-md);
      padding: 8px 16px;
      cursor: pointer;
      box-shadow: var(--shadow-hand);
      transition: transform 0.12s ease, box-shadow 0.12s ease, background-color 0.2s ease;
    }

    button:hover { background: var(--surface-soft); transform: translateY(-1px); }
    button:active { transform: translateY(2px); box-shadow: 0 1px 0 var(--border-ink); }
    button:disabled { opacity: 0.55; cursor: not-allowed; transform: none; }

    .btn-lg {
      font-size: 1.1rem;
      padding: 12px 32px;
      background: var(--accent-lavender);
      color: #fff;
      border-radius: var(--radius-pill);
    }
    .btn-lg:hover { background: #876cc2; }

    .btn-block {
      display: block;
      width: 100%;
      padding: 14px 18px;
      margin: 10px 0;
      text-align: center;
      background: var(--surface-card);
      border-radius: var(--radius-md);
    }
    .btn-block:hover { background: var(--accent-lavender-light); }

    input[type='text'], input[type='password'] {
      font-family: var(--font-body);
      font-size: 0.95rem;
      color: var(--ink-primary);
      background: #fff;
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-sm);
      padding: 9px 14px;
      outline: none;
      min-width: 0;
    }

    input[type='text']:focus, input[type='password']:focus {
      border-color: var(--accent-lavender);
      box-shadow: 0 0 0 3px rgba(155, 130, 208, 0.25);
    }

    input[type='checkbox'] {
      accent-color: var(--accent-lavender);
      width: 16px;
      height: 16px;
      cursor: pointer;
    }

    /* Layout Helpers */
    .container { max-width: 1140px; margin: 28px auto; padding: 0 20px; }
    .text-center { text-align: center; }
    .text-right { text-align: right; }

    .grid-two-col {
      display: grid;
      grid-template-columns: 1.45fr 1fr;
      gap: 28px;
      align-items: start;
    }

    .grid-two-col-sm {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
      margin: 14px 0;
    }

    .flex-between { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
    .flex-gap { display: flex; gap: 8px; flex-wrap: wrap; }

    .card {
      background: var(--surface-card);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-lg);
      padding: 20px;
      margin-bottom: 18px;
      box-shadow: var(--shadow-card);
    }

    /* Sticky Bottom Bar (ซ่อนบนคอมพิวเตอร์ แสดงอัตโนมัติบนมือถือและ iPad) */
    .mobile-bottom-bar {
      display: none;
    }

    /* Signature Mascot SVG Artworks */
    #view-landing .hero-section .placeholder-box {
      font-size: 0;
      color: transparent;
      border: none;
      width: 100%;
      max-width: 440px;
      height: 230px;
      margin: 12px auto 18px;
      background-repeat: no-repeat;
      background-position: center;
      background-size: contain;
      animation: cozyFloat 3.6s ease-in-out infinite;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 420 220'%3E%3Crect x='40' y='16' width='340' height='165' rx='24' fill='%23F3ECFF' stroke='%234A3F35' stroke-width='2.5' stroke-dasharray='6 6'/%3E%3Cpath d='M45 32 Q130 58 210 34 Q290 58 375 32' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='85' cy='43' r='5' fill='%23F5B963' stroke='%234A3F35' stroke-width='1.5'/%3E%3Ccircle cx='145' cy='46' r='5' fill='%23F29CA3' stroke='%234A3F35' stroke-width='1.5'/%3E%3Ccircle cx='275' cy='46' r='5' fill='%2388C498' stroke='%234A3F35' stroke-width='1.5'/%3E%3Ccircle cx='335' cy='43' r='5' fill='%23F5B963' stroke='%234A3F35' stroke-width='1.5'/%3E%3Cline x1='62' y1='96' x2='142' y2='96' stroke='%234A3F35' stroke-width='3' stroke-linecap='round'/%3E%3Crect x='74' y='64' width='16' height='32' rx='4' fill='%239B82D0' stroke='%234A3F35' stroke-width='2'/%3E%3Crect x='98' y='70' width='14' height='26' rx='3' fill='%23F5B963' stroke='%234A3F35' stroke-width='2'/%3E%3Cpath d='M120 96 L120 82 L132 82 L132 96' fill='%2388C498' stroke='%234A3F35' stroke-width='2'/%3E%3Cline x1='278' y1='96' x2='358' y2='96' stroke='%234A3F35' stroke-width='3' stroke-linecap='round'/%3E%3Crect x='290' y='66' width='16' height='30' rx='5' fill='%23F29CA3' stroke='%234A3F35' stroke-width='2'/%3E%3Crect x='314' y='60' width='15' height='36' rx='4' fill='%2388C498' stroke='%234A3F35' stroke-width='2'/%3E%3Crect x='164' y='44' width='92' height='34' rx='10' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.2'/%3E%3Ctext x='210' y='65' text-anchor='middle' font-family='sans-serif' font-weight='bold' font-size='13' fill='%234A3F35'%3EWELCOME ~%3C/text%3E%3Ccircle cx='185' cy='112' r='11' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='235' cy='112' r='11' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Cpath d='M174 180 C174 128 246 128 246 180 Z' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='3'/%3E%3Ccircle cx='197' cy='140' r='3.2' fill='%234A3F35'/%3E%3Ccircle cx='223' cy='140' r='3.2' fill='%234A3F35'/%3E%3Cellipse cx='190' cy='147' rx='5' ry='3' fill='%23F29CA3' opacity='0.8'/%3E%3Cellipse cx='230' cy='147' rx='5' ry='3' fill='%23F29CA3' opacity='0.8'/%3E%3Cpath d='M205 145 Q210 151 215 145' fill='none' stroke='%234A3F35' stroke-width='2.3' stroke-linecap='round'/%3E%3Cpolygon points='210,160 200,154 200,166' fill='%239B82D0' stroke='%234A3F35' stroke-width='2'/%3E%3Cpolygon points='210,160 220,154 220,166' fill='%239B82D0' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='210' cy='160' r='3' fill='%23F5B963' stroke='%234A3F35' stroke-width='1.5'/%3E%3Crect x='150' y='132' width='18' height='32' rx='5' transform='rotate(-15 150 132)' fill='%23D8E2DC' stroke='%234A3F35' stroke-width='2.5'/%3E%3Cpath d='M244 156 Q264 146 266 132' fill='none' stroke='%234A3F35' stroke-width='3' stroke-linecap='round'/%3E%3Crect x='30' y='178' width='360' height='14' rx='7' fill='%23E3C598' stroke='%234A3F35' stroke-width='3'/%3E%3C/svg%3E");
    }

    #view-landing .moments-section .placeholder-box {
      font-size: 0;
      color: transparent;
      border: none;
      width: 100%;
      max-width: 480px;
      height: 110px;
      margin: 20px auto 0;
      background-repeat: no-repeat;
      background-position: center;
      background-size: contain;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 460 100'%3E%3Cline x1='20' y1='84' x2='440' y2='84' stroke='%234A3F35' stroke-width='3.5' stroke-linecap='round'/%3E%3Ccircle cx='65' cy='58' r='18' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='60' cy='55' r='2.3' fill='%234A3F35'/%3E%3Ccircle cx='71' cy='55' r='2.3' fill='%234A3F35'/%3E%3Cpath d='M62 61 Q66 65 70 61' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Cpolygon points='84,48 98,48 91,58' fill='%23F29CA3' stroke='%234A3F35' stroke-width='2'/%3E%3Cline x1='91' y1='58' x2='91' y2='70' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='145' cy='58' r='18' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='140' cy='55' r='2.3' fill='%234A3F35'/%3E%3Ccircle cx='151' cy='55' r='2.3' fill='%234A3F35'/%3E%3Cpath d='M142 61 Q146 65 150 61' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Crect x='166' y='46' width='12' height='18' rx='3' fill='%23F5B963' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='225' cy='56' r='19' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Cpath d='M225 37 Q232 26 238 32 Q231 38 225 37' fill='%2388C498' stroke='%234A3F35' stroke-width='1.8'/%3E%3Ccircle cx='220' cy='54' r='2.3' fill='%234A3F35'/%3E%3Ccircle cx='231' cy='54' r='2.3' fill='%234A3F35'/%3E%3Cpath d='M222 60 Q226 64 230 60' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Cpolygon points='246,45 260,45 253,56' fill='%2388C498' stroke='%234A3F35' stroke-width='2'/%3E%3Cline x1='253' y1='56' x2='253' y2='69' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='305' cy='58' r='18' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='300' cy='55' r='2.3' fill='%234A3F35'/%3E%3Ccircle cx='311' cy='55' r='2.3' fill='%234A3F35'/%3E%3Cpath d='M302 61 Q306 65 310 61' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='332' cy='55' r='9' fill='%239B82D0' stroke='%234A3F35' stroke-width='2'/%3E%3Ccircle cx='385' cy='58' r='18' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='380' cy='55' r='2.3' fill='%234A3F35'/%3E%3Ccircle cx='391' cy='55' r='2.3' fill='%234A3F35'/%3E%3Cpath d='M382 61 Q386 65 390 61' fill='none' stroke='%234A3F35' stroke-width='2'/%3E%3Cpolygon points='405,46 419,46 412,57' fill='%23F5B963' stroke='%234A3F35' stroke-width='2'/%3E%3Cline x1='412' y1='57' x2='412' y2='70' stroke='%234A3F35' stroke-width='2'/%3E%3C/svg%3E");
    }

    #lobby-find-card .placeholder-box {
      font-size: 0;
      color: transparent;
      border: none;
      height: 115px;
      margin: 0 auto 10px;
      background-color: var(--surface-tint);
      border-radius: var(--radius-md);
      background-repeat: no-repeat;
      background-position: center bottom;
      background-size: 220px 105px;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 220 105'%3E%3Ccircle cx='88' cy='34' r='9' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.5'/%3E%3Ccircle cx='132' cy='34' r='9' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.5'/%3E%3Cpath d='M78 95 C78 48 142 48 142 95 Z' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='98' cy='58' r='2.8' fill='%234A3F35'/%3E%3Ccircle cx='122' cy='58' r='2.8' fill='%234A3F35'/%3E%3Cellipse cx='92' cy='64' rx='4.5' ry='2.5' fill='%23F29CA3'/%3E%3Cellipse cx='128' cy='64' rx='4.5' ry='2.5' fill='%23F29CA3'/%3E%3Cpath d='M106 62 Q110 67 114 62' fill='none' stroke='%234A3F35' stroke-width='2.2' stroke-linecap='round'/%3E%3Cpolygon points='110,75 101,70 101,80' fill='%239B82D0' stroke='%234A3F35' stroke-width='1.8'/%3E%3Cpolygon points='110,75 119,70 119,80' fill='%239B82D0' stroke='%234A3F35' stroke-width='1.8'/%3E%3Crect x='56' y='52' width='15' height='26' rx='4' transform='rotate(-18 56 52)' fill='%23D8E2DC' stroke='%234A3F35' stroke-width='2.3'/%3E%3Cline x1='20' y1='95' x2='200' y2='95' stroke='%234A3F35' stroke-width='3' stroke-linecap='round'/%3E%3C/svg%3E");
    }

    #modal-venting-intro .placeholder-box {
      font-size: 0;
      color: transparent;
      border: 2px dashed #d3c6b4;
      border-radius: var(--radius-md);
      background-color: #fff;
      height: 150px;
      margin: 14px auto;
      background-repeat: no-repeat;
      background-position: center;
      background-size: 280px 135px;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 280 135'%3E%3Cpolygon points='95,22 83,46 107,46' fill='%239B82D0' stroke='%234A3F35' stroke-width='2.3'/%3E%3Cpath d='M68 118 C68 62 122 62 122 118 Z' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Ccircle cx='86' cy='76' r='2.6' fill='%234A3F35'/%3E%3Ccircle cx='104' cy='76' r='2.6' fill='%234A3F35'/%3E%3Cpath d='M91 82 Q95 87 99 82' fill='none' stroke='%234A3F35' stroke-width='2.2'/%3E%3Crect x='124' y='74' width='14' height='24' rx='4' fill='%23D8E2DC' stroke='%234A3F35' stroke-width='2.2'/%3E%3Cellipse cx='195' cy='28' rx='38' ry='16' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2'/%3E%3Ctext x='195' y='32' text-anchor='middle' font-family='sans-serif' font-size='10' fill='%2375655C'%3Eมีเรื่องกังวล...%3C/text%3E%3Cpath d='M162 118 C162 64 216 64 216 118 Z' fill='%23FFFDF9' stroke='%234A3F35' stroke-width='2.8'/%3E%3Cpath d='M177 76 Q181 72 185 76' fill='none' stroke='%234A3F35' stroke-width='2.2'/%3E%3Cpath d='M195 76 Q199 72 203 76' fill='none' stroke='%234A3F35' stroke-width='2.2'/%3E%3Cpath d='M179 80 Q176 92 179 98' fill='none' stroke='%237CC6FE' stroke-width='3' stroke-linecap='round'/%3E%3Cpath d='M201 80 Q204 92 201 98' fill='none' stroke='%237CC6FE' stroke-width='3' stroke-linecap='round'/%3E%3Crect x='220' y='82' width='15' height='18' rx='4' fill='%23F5B963' stroke='%234A3F35' stroke-width='2.2'/%3E%3Cline x1='35' y1='118' x2='245' y2='118' stroke='%234A3F35' stroke-width='3' stroke-linecap='round'/%3E%3C/svg%3E");
    }

    .mochi-character {
      width: 112px;
      height: 100px;
      margin: 0 auto;
      animation: cozyBob 2.8s ease-in-out infinite;
    }

    .empty-barstool {
      width: 80px;
      height: 76px;
      margin: 0 auto 24px;
      opacity: 0.7;
      background-repeat: no-repeat;
      background-position: center bottom;
      background-size: contain;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 80 75'%3E%3Crect x='18' y='34' width='44' height='10' rx='5' fill='%23E3C598' stroke='%234A3F35' stroke-width='2.3'/%3E%3Cline x1='26' y1='44' x2='20' y2='72' stroke='%234A3F35' stroke-width='2.5' stroke-linecap='round'/%3E%3Cline x1='54' y1='44' x2='60' y2='72' stroke='%234A3F35' stroke-width='2.5' stroke-linecap='round'/%3E%3Cline x1='23' y1='58' x2='57' y2='58' stroke='%234A3F35' stroke-width='2'/%3E%3C/svg%3E");
    }

    /* Landing Hero & Testimonials */
    .hero-section {
      max-width: 720px;
      margin: 20px auto 48px;
      padding: 32px 24px;
      background: var(--surface-card);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-card);
    }

    .hero-section h1 { font-size: 2rem; margin-bottom: 8px; }
    .hero-section > p:first-of-type { color: var(--ink-secondary); font-size: 1.05rem; margin-top: 0; }

    .online-counter {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 16px;
      background: #eef8f0;
      border: 1.5px solid #6ba378;
      border-radius: var(--radius-pill);
      color: #2f693d;
      font-family: var(--font-display);
      font-weight: 600;
      margin-bottom: 16px;
    }

    .online-counter::before {
      content: '';
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: #48b863;
      animation: pulseDot 1.8s infinite;
    }

    .hero-links { margin: 20px 0; color: var(--ink-secondary); }
    .hero-section h2 {
      font-size: 1.25rem;
      margin: 22px 0 0;
      padding-top: 16px;
      border-top: 2px dashed #e2d8c5;
      color: var(--ink-secondary);
    }

    .moments-section { max-width: 680px; margin: 0 auto 36px; }
    .testimonial-card { position: relative; padding: 28px 32px; background: #fff; }
    #testimonial-text { font-family: var(--font-display); font-size: 1.08rem; line-height: 1.75; margin-bottom: 14px; }
    #testimonial-author {
      display: inline-block;
      padding: 3px 14px;
      background: var(--accent-lavender-light);
      border-radius: var(--radius-pill);
      font-family: var(--font-display);
    }

    .dots-group { display: flex; justify-content: center; gap: 6px; }
    .dots-group button { padding: 4px 10px; border-radius: var(--radius-pill); font-size: 0.75rem; box-shadow: none; }

    /* Cozy Night Window & Bar Counter Stage */
    .stage-box {
      position: relative;
      min-height: 270px;
      padding: 18px;
      margin: 16px 0;
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-lg);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      background:
        linear-gradient(to top, #e3c598 0px, #e3c598 16px, #4a3f35 16px, #4a3f35 19px, transparent 19px),
        radial-gradient(circle at 84% 22%, #fff3b0 0px, #fff3b0 18px, rgba(255, 243, 176, 0.25) 34px, transparent 55px),
        radial-gradient(circle at 22% 24%, #ffffff 1.5px, transparent 2px),
        radial-gradient(circle at 38% 16%, #ffffff 1.2px, transparent 2px),
        radial-gradient(circle at 62% 28%, #ffffff 1.5px, transparent 2px),
        linear-gradient(180deg, #2d3142 0%, #4f5d75 58%, #f4e8d8 58%, #faf6ee 100%);
    }

    .stage-box::before {
      content: '✦  •  ✦  •  ✦  •  ✦  •  ✦';
      position: absolute;
      top: 6px;
      left: 0;
      right: 0;
      text-align: center;
      color: #ffe082;
      font-size: 0.85rem;
      letter-spacing: 10px;
      opacity: 0.85;
      pointer-events: none;
    }

    .stage-actions { position: relative; z-index: 2; display: flex; justify-content: flex-end; gap: 8px; }
    .avatar-preview { position: relative; z-index: 2; text-align: center; margin-top: auto; }

    .bubble {
      display: inline-block;
      max-width: 230px;
      padding: 7px 14px;
      margin-bottom: 8px;
      background: #fffdf9;
      border: 2px solid var(--border-ink);
      border-radius: 18px;
      font-family: var(--font-display);
      font-size: 0.88rem;
      line-height: 1.4;
      box-shadow: 0 3px 0 rgba(74, 63, 53, 0.15);
      position: relative;
      word-break: break-word;
    }

    .bubble::after {
      content: '';
      position: absolute;
      bottom: -8px;
      left: 50%;
      transform: translateX(-50%);
      border-width: 8px 7px 0;
      border-style: solid;
      border-color: var(--border-ink) transparent transparent;
    }

    /* Lobby Controls */
    .mood-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 12px 0; }
    .mood-btn { padding: 12px 6px; display: flex; flex-direction: column; align-items: center; gap: 4px; border-radius: var(--radius-md); }
    .mood-btn div:first-child { font-size: 1.5rem; }

    #selected-mood-caption {
      margin: 8px 0 0;
      padding: 6px 12px;
      background: var(--surface-soft);
      border-radius: var(--radius-sm);
      font-size: 0.9rem;
      text-align: center;
    }

    .is-active {
      background: var(--accent-lavender-light) !important;
      border-color: var(--border-ink) !important;
      color: var(--ink-primary) !important;
      box-shadow: 0 3px 0 var(--border-ink) !important;
    }

    .tabs-nav { display: flex; gap: 6px; margin-bottom: -2px; position: relative; z-index: 1; }
    .tab-btn { border-bottom-left-radius: 0; border-bottom-right-radius: 0; background: var(--surface-soft); box-shadow: none; padding: 8px 18px; }
    .tab-btn.is-active { background: var(--accent-lavender-light) !important; border-bottom-color: var(--accent-lavender-light) !important; box-shadow: none !important; }
    #join-code-form input { flex: 1; }

    /* Table Room */
    .badges-row { display: flex; gap: 8px; flex-wrap: wrap; margin: 10px 0 14px; }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 4px 12px;
      background: var(--surface-card);
      border: 1.5px solid var(--border-ink);
      border-radius: var(--radius-pill);
      font-family: var(--font-display);
      font-size: 0.85rem;
      box-shadow: none;
    }

    .seats-row { position: relative; z-index: 2; display: flex; justify-content: space-around; align-items: flex-end; min-height: 215px; padding-top: 24px; }

    /* ปรับขนาดตัวละครให้เรียงครบ 5 ที่นั่งได้พอดีเมื่อย้ายมาอยู่ใต้ช่องแชทในห้องฟังเพลง */
    #room-right-stage-slot .stage-box {
      min-height: 205px;
      padding: 12px 8px;
      margin: 4px 0 12px;
    }
    #room-right-stage-slot .seats-row {
      min-height: 165px;
      padding-top: 18px;
      gap: 2px;
    }
    #room-right-stage-slot .seat-slot { min-width: 0; }
    #room-right-stage-slot .mochi-character,
    #room-right-stage-slot .mochi-character svg {
      width: 68px;
      height: 62px;
    }
    #room-right-stage-slot .empty-barstool {
      width: 52px;
      height: 50px;
      margin-bottom: 18px;
    }
    #room-right-stage-slot .bubble {
      max-width: 82px;
      padding: 4px 7px;
      font-size: 0.72rem;
      margin-bottom: 4px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    #room-right-stage-slot .seat-slot strong {
      font-size: 0.76rem;
      padding: 1px 6px;
      max-width: 78px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    #room-right-stage-slot .seat-slot small { font-size: 0.68rem; }

    .seat-slot { text-align: center; flex: 1; }
    .seat-slot strong {
      display: inline-block;
      margin-top: 4px;
      padding: 1px 10px;
      background: rgba(255, 253, 249, 0.92);
      border-radius: var(--radius-pill);
      font-family: var(--font-display);
      font-size: 0.9rem;
    }
    .seat-slot small { display: block; color: var(--ink-secondary); font-size: 0.78rem; }

    .banner-notice {
      background: var(--accent-lavender-light);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-md);
      padding: 12px 14px;
      margin-bottom: 12px;
      font-size: 0.9rem;
    }

    .chat-feed {
      height: 290px;
      overflow-y: auto;
      padding: 10px;
      margin-bottom: 12px;
      background: var(--bg-paper);
      border: 1.5px solid #dcd2c0;
      border-radius: var(--radius-md);
    }

    .msg-system {
      background: #ece6da;
      color: var(--ink-secondary);
      padding: 6px 12px;
      margin: 8px auto;
      border-radius: var(--radius-pill);
      font-size: 0.82rem;
      text-align: center;
      max-width: 90%;
    }

    .msg-user {
      background: #fff;
      border: 1.5px solid #dfd5c5;
      border-radius: 14px;
      padding: 8px 12px;
      margin: 8px 0;
      font-size: 0.92rem;
    }

    .msg-user strong { color: #6f52ad; font-family: var(--font-display); }
    #chat-form input, #jukebox-form input, #youtube-room-form input { flex: 1; }

    #jukebox-playlist { list-style: none; padding: 0; margin: 12px 0; }
    #jukebox-playlist li { padding: 8px 12px; margin-bottom: 6px; background: var(--surface-soft); border-radius: var(--radius-sm); border: 1px solid #dcd2c0; }

    /* YouTube Shared Music Room Stage */
    .yt-player-card {
      background: var(--surface-card);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-lg);
      padding: 16px;
      margin-bottom: 16px;
      box-shadow: var(--shadow-card);
    }
    .yt-video-wrapper {
      position: relative;
      width: 100%;
      padding-bottom: 50%;
      background: #232028;
      border-radius: var(--radius-md);
      overflow: hidden;
      border: 2px solid var(--border-ink);
      margin-bottom: 10px;
      user-select: none;
    }
    .yt-video-wrapper iframe,
    .yt-video-wrapper #yt-player-mount {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
      border: 0;
      pointer-events: none;
    }
    .yt-shield-overlay {
      position: absolute;
      inset: 0;
      z-index: 5;
      background: transparent;
      cursor: default;
    }
    .yt-empty-placeholder {
      position: absolute;
      inset: 0;
      z-index: 6;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      color: #f4efe6;
      text-align: center;
      padding: 16px;
      font-family: var(--font-display);
    }
    .yt-local-vol-bar {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 6px 12px;
      margin-bottom: 10px;
      background: var(--surface-soft);
      border: 1.5px solid #dcd2c0;
      border-radius: var(--radius-sm);
      font-size: 0.85rem;
    }
    .yt-local-vol-bar input[type='range'] {
      flex: 1;
      accent-color: var(--accent-lavender);
      cursor: pointer;
    }
    .yt-queue-list {
      max-height: 150px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-top: 12px;
    }
    .yt-queue-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      background: var(--surface-soft);
      border: 1.5px solid var(--border-ink);
      border-radius: var(--radius-sm);
      font-size: 0.9rem;
    }
    .yt-queue-item.is-active {
      background: var(--accent-lavender-light);
      font-weight: 600;
    }

    /* Modals */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(45, 39, 35, 0.55);
      backdrop-filter: blur(3px);
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 16px;
      z-index: 100;
    }

    #modal-popup { z-index: 200; }

    .modal-dialog {
      position: relative;
      background: var(--bg-paper);
      border: 2.5px solid var(--border-ink);
      border-radius: var(--radius-lg);
      padding: 28px;
      width: 100%;
      max-width: 520px;
      max-height: 92vh;
      overflow-y: auto;
      box-shadow: 0 8px 0 var(--border-ink);
      animation: modalPopIn 0.2s ease-out;
    }

    .modal-sm { max-width: 400px; }
    .modal-lg { max-width: 660px; }
    .modal-close { position: absolute; top: 14px; right: 14px; width: 34px; height: 34px; padding: 0; border-radius: 50%; display: grid; place-items: center; }
    .modal-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 18px; }

    .select-card {
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-md);
      background: var(--surface-card);
      padding: 16px;
      text-align: center;
      cursor: pointer;
      margin-bottom: 10px;
      transition: transform 0.15s ease, background-color 0.2s ease;
    }
    .select-card:hover { transform: translateY(-2px); background: var(--surface-soft); }

    .form-group { margin-bottom: 12px; }
    .form-group input { width: 100%; margin-top: 6px; }

    .modal-scroll-body {
      max-height: 50vh;
      overflow-y: auto;
      background: var(--surface-card);
      border: 2px solid var(--border-ink);
      border-radius: var(--radius-md);
      padding: 18px;
      margin: 12px 0;
      text-align: left;
    }

    .info-block, .step-card, .faq-item { border-bottom: 1.5px dashed #dfd5c5; padding-bottom: 14px; margin-bottom: 14px; }
    .info-block:last-child, .step-card:last-child, .faq-item:last-child { border-bottom: none; margin-bottom: 0; padding-bottom: 0; }
    .faq-item summary { cursor: pointer; font-family: var(--font-display); padding: 4px 0; }

    .hat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin: 14px 0; }

    .creator-footer {
      margin-top: 32px;
      padding: 14px 20px;
      border-top: 2px dashed #dcd2c0;
      color: var(--ink-secondary);
      font-family: var(--font-display);
      font-size: 0.92rem;
    }
    .creator-footer span {
      display: inline-block;
      padding: 4px 16px;
      background: var(--surface-card);
      border: 1.5px solid var(--border-ink);
      border-radius: var(--radius-pill);
      color: var(--ink-primary);
    }

    /* Sound & Music Settings */
    .volume-control-box { background: var(--surface-tint); margin-top: 12px; }
    .volume-slider {
      width: 100%;
      margin-top: 10px;
      accent-color: var(--accent-lavender);
      cursor: pointer;
      height: 8px;
    }
    .track-list {
      max-height: 220px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .track-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 14px;
      background: var(--surface-card);
      border: 1.5px solid var(--border-ink);
      border-radius: var(--radius-sm);
      cursor: pointer;
    }
    .track-item.is-active {
      background: var(--accent-lavender-light);
      font-weight: 600;
    }

    @keyframes cozyFloat {
      0%, 100% { transform: translateY(0px); }
      50% { transform: translateY(-6px); }
    }
    @keyframes cozyBob {
      0%, 100% { transform: translateY(0px) rotate(0deg); }
      50% { transform: translateY(-4px) rotate(1.5deg); }
    }
    @keyframes pulseDot {
      0%, 100% { transform: scale(1); opacity: 1; }
      50% { transform: scale(1.35); opacity: 0.6; }
    }
    @keyframes modalPopIn {
      from { opacity: 0; transform: scale(0.95) translateY(8px); }
      to { opacity: 1; transform: scale(1) translateY(0); }
    }

    /* =====================================================================
       TABLET / iPad (หน้าจอ 768px - 1100px ทั้งแนวตั้งและแนวนอน)
    ====================================================================== */
    @media (max-width: 1100px) and (min-width: 861px) {
      .container { padding: 0 16px; }
      .grid-two-col { grid-template-columns: 1.35fr 1fr; gap: 18px; }
      #room-right-stage-slot .mochi-character,
      #room-right-stage-slot .mochi-character svg {
        width: 58px;
        height: 52px;
      }
      #room-right-stage-slot .empty-barstool {
        width: 44px;
        height: 42px;
      }
      #room-right-stage-slot .bubble,
      #room-right-stage-slot .seat-slot strong {
        max-width: 64px;
        font-size: 0.7rem;
      }
    }

    @media (max-width: 860px) {
      .grid-two-col { grid-template-columns: 1fr; gap: 18px; }
      .navbar { padding: 10px 14px; }
      .brand { font-size: 1.05rem; padding: 4px 12px; }
      .mood-grid { grid-template-columns: repeat(2, 1fr); }

      /* แถบปุ่มลัดลอยติดขอบล่างจอสำหรับ iPhone และ iPad แนวตั้ง */
      #view-lobby .container {
        padding-bottom: 88px;
      }

      .mobile-bottom-bar {
        display: flex;
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        z-index: 40;
        gap: 8px;
        padding: 10px 12px calc(10px + env(safe-area-inset-bottom));
        background: rgba(255, 253, 249, 0.96);
        backdrop-filter: blur(8px);
        border-top: 2.5px solid var(--border-ink);
        box-shadow: 0 -4px 16px rgba(74, 63, 53, 0.12);
      }

      .mobile-bottom-bar .mbar-btn {
        flex: 1;
        padding: 10px 6px;
        font-size: 0.9rem;
        border-radius: 14px;
        white-space: nowrap;
        text-align: center;
      }

      .mobile-bottom-bar .mbar-primary {
        flex: 1.25;
        background: var(--accent-lavender);
        color: #fff;
      }

      .mobile-bottom-bar .mbar-primary:hover {
        background: #876cc2;
      }
    }

    @media (max-width: 860px) and (min-width: 601px) {
      #room-right-stage-slot .stage-box { min-height: 245px; padding: 16px 12px; }
      #room-right-stage-slot .seats-row { min-height: 195px; }
      #room-right-stage-slot .mochi-character,
      #room-right-stage-slot .mochi-character svg {
        width: 96px;
        height: 86px;
      }
      #room-right-stage-slot .empty-barstool { width: 70px; height: 66px; }
      #room-right-stage-slot .bubble { max-width: 130px; font-size: 0.82rem; padding: 5px 10px; }
      #room-right-stage-slot .seat-slot strong { max-width: 120px; font-size: 0.85rem; }
    }

    /* =====================================================================
       MOBILE / iPhone  600px  iPhone SE - Pro Max)
    ====================================================================== */
    @media (max-width: 600px) {
      .container { margin: 14px auto; padding: 0 12px; }
      .card { padding: 14px; border-radius: 18px; }

      .navbar { padding: 8px 10px; gap: 4px; }
      .nav-left, .nav-actions { gap: 4px; }
      .navbar button { padding: 6px 8px; font-size: 0.78rem; border-radius: 12px; }
      .brand { font-size: 0.84rem; padding: 4px 10px; gap: 4px; white-space: nowrap; }

      input[type='text'], input[type='password'] {
        font-size: 16px;
        padding: 8px 10px;
      }

      #view-table .stage-box,
      #room-right-stage-slot .stage-box {
        min-height: 195px;
        padding: 12px 6px;
      }
      .seats-row,
      #room-right-stage-slot .seats-row {
        min-height: 155px;
        padding-top: 16px;
        gap: 2px;
      }
      .seat-slot { min-width: 0; }
      .seats-row .mochi-character,
      .seats-row .mochi-character svg,
      #room-right-stage-slot .mochi-character,
      #room-right-stage-slot .mochi-character svg {
        width: 54px;
        height: 48px;
      }
      .empty-barstool,
      #room-right-stage-slot .empty-barstool {
        width: 42px;
        height: 40px;
        margin-bottom: 14px;
      }
      .seats-row .bubble,
      #room-right-stage-slot .bubble {
        max-width: 62px;
        padding: 3px 6px;
        font-size: 0.68rem;
        margin-bottom: 4px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .seat-slot strong,
      #room-right-stage-slot .seat-slot strong {
        max-width: 62px;
        font-size: 0.7rem;
        padding: 1px 5px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .seat-slot small,
      #room-right-stage-slot .seat-slot small {
        font-size: 0.64rem;
      }

      /* ปรับแชท ตู้เพลง YouTube และหน้าต่าง Pop-up ให้พอดีนิ้วโป้ง */
      .chat-feed { height: 230px; }
      .yt-player-card { padding: 12px; }
      .yt-local-vol-bar { flex-wrap: wrap; gap: 6px; font-size: 0.8rem; }
      #youtube-room-form button,
      #chat-form button,
      #join-code-form button {
        padding: 8px 12px;
        font-size: 0.85rem;
        white-space: nowrap;
      }

      .hero-section { padding: 20px 14px; margin: 12px auto 28px; }
      .hero-section h1 { font-size: 1.45rem; }
      .btn-lg { font-size: 1rem; padding: 10px 22px; }

      .modal-dialog { padding: 20px 14px; border-radius: 18px; }
      .hat-grid { grid-template-columns: repeat(2, 1fr); }
      .grid-two-col-sm { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>

  <!-- =====================================================================
       VIEW 1: LANDING PAGE (ก่อนสมัครสมาชิก / เข้าสู่ระบบ)
  ====================================================================== -->
  <div id="view-landing" class="view">
    <header class="navbar">
      <button type="button" data-action="go-landing">🏠 หน้าหลัก</button>
      <div class="nav-actions">
        <button type="button" title="ตั้งค่าเสียงและเพลง" data-action="open-modal" data-modal="modal-settings">⚙️</button>
        <button type="button" data-action="open-modal" data-modal="modal-auth">
          Log in / Register
        </button>
      </div>
    </header>

    <main class="container text-center">
      <section class="hero-section">
        <h1>SPU VISUAL BAR</h1>
        <h2>คุยกับเพื่อนใหม่ในบรรยากาศบาร์ทิพย์</h2>
        <p>มาแชร์เรื่องราวสุข ทุกข์ เรื่อยเปื่อยกับคนไม่รู้จักกัน</p>

        <div class="placeholder-box">
          [ พื้นที่รูปบาร์เทนเดอร์: WELCOME ~ ]
        </div>

        <p class="online-counter">1 คนกำลังออนไลน์อยู่ในบาร์ตอนนี้</p>

        <button type="button" class="btn-lg" data-action="open-modal" data-modal="modal-auth">
          Log in / สมัครสมาชิก
        </button>

        <div class="hero-links">
          <small>
            By continuing, you agree to our
            <a href="#terms" data-action="open-modal" data-modal="modal-terms">Terms &amp; Policy</a>
          </small><br />
          <small>
            ยังไม่แน่ใจว่าจะเริ่มยังไง? ดู
            <a href="#guide" data-action="open-modal" data-modal="modal-guide">วิธีใช้งานใน 4 ขั้นตอน</a>
            หรือ
            <a href="#faq" data-action="open-modal" data-modal="modal-faq">คำถามที่พบบ่อย</a>
          </small>
        </div>

      </section>

      <section class="moments-section">
        <h2>#VirtualBarMoment</h2>
        <article class="card testimonial-card">
          <p id="testimonial-text"></p>
          <strong id="testimonial-author"></strong>
        </article>
        <div id="testimonial-dots" class="dots-group"></div>

        <div class="placeholder-box">
          [ พื้นที่รูปตัวละครถือแก้วเครื่องดื่ม 5 ตัว ]
        </div>
      </section>

      <footer class="creator-footer">
        <span>🍸 Created with 💜 by <strong>Hyeonmin</strong></span>
      </footer>
    </main>
  </div>

  <!-- =====================================================================
       VIEW 2: SPU VISUAL BAR (หน้าแรกอัตโนมัติหลังสมัครสมาชิก / เข้าสู่ระบบ)
  ====================================================================== -->
  <div id="view-lobby" class="view" hidden>
    <header class="navbar">
      <button type="button" data-action="go-lobby">🏠</button>
      <strong class="brand">SPU VISUAL BAR</strong>
      <div class="nav-actions">
        <button type="button" title="การแจ้งเตือน" data-action="alert-feature" data-title="การแจ้งเตือน 🔔" data-msg="ไม่มีการแจ้งเตือนใหม่ในขณะนี้">🔔</button>
        <button type="button" title="ตั้งค่าเสียงและเพลง" data-action="open-modal" data-modal="modal-settings">⚙️</button>
        <button type="button" data-action="logout">👤 ออกจากระบบ</button>
      </div>
    </header>

    <main class="container grid-two-col">
      <!-- Left Column -->
      <section class="lobby-left">
        <div id="username-display-box">
          <h2>
            สวัสดี... <span id="display-username">hyeonmin</span>
            <button type="button" data-action="edit-username">✏️</button>
          </h2>
        </div>
        <form id="username-edit-form" hidden>
          <input type="text" id="input-username" required />
          <button type="submit">บันทึก</button>
          <button type="button" data-action="cancel-edit-username">ยกเลิก</button>
        </form>

        <!-- Mood Selector -->
        <div class="card">
          <p>เลือกความรู้สึกของคุณตอนนี้หน่อย</p>
          <div id="mood-options-container" class="mood-grid"></div>
          <p id="selected-mood-caption"></p>
        </div>

        <!-- Character Stage -->
        <div class="stage-box">
          <div class="stage-actions">
            <button type="button" data-action="open-modal" data-modal="modal-customize">
              👕 แต่งตัว
            </button>
            <button type="button" data-action="alert-feature" data-title="ร้านค้าในบาร์ 🛍️" data-msg="เครื่องดื่มและพร็อพทั้งหมดเปิดให้ใช้ฟรีในบาร์ทิพย์ของเรา!">
              🛍️ ช้อปปิ้ง
            </button>
          </div>

          <div class="avatar-preview">
            <div class="bubble">พิมพ์เรื่องในใจที่อยากเล่า...</div>
            <div id="lobby-avatar-sprite" class="mochi-character"></div>
          </div>
        </div>

        <!-- Volunteer Bartender Toggle -->
        <div class="card flex-between">
          <div>
            <strong>🍸 อาสาเป็นบาร์เทนเดอร์</strong>
            <p><small>คอยชวนคุย รับฟัง ดูแลสารทุกข์สุกดิบคนในบาร์</small></p>
          </div>
          <label>
            <input type="checkbox" id="toggle-bartender" />
            <span id="bartender-status-text">ปิด</span>
          </label>
        </div>
      </section>

      <!-- Right Column -->
      <aside class="lobby-right">
        <p class="text-right"><small>1 Users are Online</small></p>

        <div id="lobby-find-card" class="card">
          <h3 class="text-center">หาเพื่อนใหม่ในบาร์</h3>
          <div class="placeholder-box">[ รูปบาร์เทนเดอร์เชคเครื่องดื่ม ]</div>

          <button type="button" class="btn-block" data-action="open-modal" data-modal="modal-matchmaking">
            <strong>คุยเล่น หาเพื่อนแก้เหงา</strong>
          </button>

          <button type="button" class="btn-block" data-action="open-modal" data-modal="modal-venting-intro">
            <strong>รับฟัง ระบาย</strong><br />
            <small id="venting-btn-subtext">เข้ามาแชร์เรื่องไม่สบายใจได้เลย</small>
          </button>

          <p class="text-center"><small>หรือ</small></p>

          <form id="join-code-form" class="flex-gap">
            <input type="text" id="input-room-code" placeholder="ใส่รหัสโต๊ะ..." required />
            <button type="submit">จอย</button>
          </form>
        </div>
      </aside>
    </main>

    <!-- แถบปุ่มลัดลอยติดขอบล่างจอสำหรับ iPhone และ iPad แนวตั้ง -->
    <nav id="mobile-lobby-bar" class="mobile-bottom-bar" aria-label="เมนูลัดหาโต๊ะ">
      <button type="button" class="mbar-btn mbar-primary" data-action="open-modal" data-modal="modal-matchmaking">
        🥂 คุยเล่น
      </button>
      <button type="button" class="mbar-btn" data-action="open-modal" data-modal="modal-venting-intro">
        🍸 ระบาย
      </button>
      <button type="button" class="mbar-btn" data-action="focus-room-code">
        🔑 ใส่รหัส
      </button>
    </nav>
  </div>

  <!-- =====================================================================
       VIEW 3: TABLE ROOM (โต๊ะพูดคุย Real-time)
  ====================================================================== -->
  <div id="view-table" class="view" hidden>
    <header class="navbar">
      <div class="nav-left">
        <button type="button" data-action="leave-table">🏠</button>
        <button type="button" data-action="leave-table">ออกจากโต๊ะ</button>
      </div>
      <strong class="brand">SPU VISUAL BAR</strong>
      <div class="nav-actions">
        <button type="button" data-action="alert-feature" data-title="การแจ้งเตือน 🔔" data-msg="ไม่มีการแจ้งเตือนใหม่">🔔</button>
        <button type="button" title="ตั้งค่าเสียงและเพลง" data-action="open-modal" data-modal="modal-settings">⚙️</button>
        <button type="button" data-action="leave-table" title="กลับหน้าหลัก">👤</button>
      </div>
    </header>

    <main class="container grid-two-col">
      <section class="room-left">
        <div id="table-title-display">
          <h2>
            โต๊ะ: <span id="display-table-title">ใส่ชื่อให้โต๊ะของคุณ</span>
            <button type="button" data-action="edit-table-title">✏️</button>
          </h2>
        </div>
        <form id="table-title-form" hidden>
          <input type="text" id="input-table-title" required />
          <button type="submit">บันทึก</button>
        </form>

        <div id="table-badges-container" class="badges-row"></div>

        <!-- กล่องเล่นเพลง YouTube ร่วมกัน (เฉพาะห้องสิงฟังเพลง: ล็อกเวลา + เล่นจบหายไป + ข้ามเพลง) -->
        <div id="music-room-youtube-box" class="yt-player-card" hidden>
          <div class="flex-between" style="margin-bottom: 10px;">
            <strong>🎬 ตู้เพลง YouTube</strong>
            <button type="button" id="btn-vote-skip-yt" data-action="vote-skip-youtube">
              🗳️ ข้ามเพลง (0/1)
            </button>
          </div>

          <div id="yt-embed-container" class="yt-video-wrapper">
            <div id="yt-player-mount"></div>
            <div class="yt-shield-overlay" title="ล็อกแถบเวลาเพื่อให้ทุกคนฟังพร้อมกัน"></div>
            <div id="yt-empty-placeholder" class="yt-empty-placeholder">
              <div style="font-size:2rem; margin-bottom:6px;">🎧</div>
              <div>ยังไม่มีเพลงในคิว วางลิงก์ YouTube ด้านล่างเพื่อเปิดเพลงแรกเลย!</div>
            </div>
          </div>

          <div class="yt-local-vol-bar">
            <span>🔊 เสียง YouTube ของคุณ: <strong id="yt-local-vol-label">80%</strong></span>
            <input type="range" id="yt-local-vol-slider" min="0" max="100" value="80" />
          </div>

          <form id="youtube-room-form" class="flex-gap">
            <input type="text" id="input-youtube-url" placeholder="วางลิงก์เพลง YouTube ที่นี่ (youtube.com / youtu.be)..." required />
            <button type="submit">➕ เพิ่มเข้าคิว</button>
          </form>

          <div id="yt-queue-list" class="yt-queue-list"></div>
        </div>

        <div id="room-left-stage-slot">
          <div id="table-stage-box" class="stage-box">
            <div id="table-seats-container" class="seats-row"></div>
          </div>
        </div>

        <div class="text-center">
          <small>
            👋 หากไม่มีการส่งข้อความเกิน 5 นาทีจะกลายเป็นผี หากเกิน 10 นาทีผู้เล่นอื่นสามารถเตะออกจากห้องได้
          </small>
          <br /><br />
          <button type="button" data-action="switch-table">เปลี่ยนโต๊ะ 🔄</button>
        </div>
      </section>

      <aside class="room-right">
        <div class="flex-between">
          <div class="tabs-nav">
            <button type="button" class="tab-btn is-active" data-action="switch-room-tab" data-tab="chat">
              แชท
            </button>
            <button type="button" class="tab-btn" data-action="switch-room-tab" data-tab="jukebox">
              ตู้เพลง 🔊
            </button>
          </div>
          <div>
            <button type="button" title="ถ่ายรูปความทรงจำ" data-action="alert-feature" data-title="ถ่ายรูปความทรงจำ 📸" data-msg="บันทึกภาพความทรงจำของโต๊ะนี้เรียบร้อยแล้ว!">
              📷
            </button>
            <button type="button" title="สุ่มการ์ดคำถาม" data-action="draw-icebreaker">
              🃏
            </button>
          </div>
        </div>

        <div id="waiting-listener-banner" class="banner-notice" hidden></div>

        <div id="room-tab-chat" class="card">
          <div id="chat-messages-list" class="chat-feed"></div>

          <form id="chat-form" class="flex-gap">
            <input type="text" id="input-chat-message" placeholder="พิมพ์ข้อความ..." required />
            <button type="button" data-action="append-emoji">😊</button>
            <button type="submit">ส่ง</button>
          </form>
        </div>

        <div id="room-tab-jukebox" class="card" hidden>
          <h4 id="jukebox-tab-heading">รายการเพลงในโต๊ะ</h4>
          <ul id="jukebox-playlist"></ul>
          <form id="jukebox-form" class="flex-gap">
            <input type="text" id="input-song-name" placeholder="พิมพ์ชื่อเพลงหรือวางลิงก์..." required />
            <button type="submit">เพิ่มเพลง</button>
          </form>
        </div>

        <!-- ช่องแสดงตัวละครในห้องเมื่อเข้าห้องสิงฟังเพลง (อยู่ใต้ช่องแชท) -->
        <div id="room-right-stage-slot"></div>

        <small>*มือถืออาจมีข้อจำกัดทาง Browser แนะนำให้เล่นในคอม</small>
      </aside>
    </main>
  </div>

  <!-- =====================================================================
       POP-UP MODAL (หน้าต่างแจ้งเตือนแบบกดปิดได้)
  ====================================================================== -->
  <div id="modal-popup" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog modal-sm text-center">
      <button type="button" class="modal-close" data-action="close-popup">✖</button>
      <h3 id="popup-title">แจ้งเตือน</h3>
      <p id="popup-message" style="margin: 16px 0;"></p>
      <button type="button" class="btn-lg" data-action="close-popup">ปิด</button>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 1: AUTH (Login / Register)
  ====================================================================== -->
  <div id="modal-auth" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2 id="auth-modal-title">เข้าสู่ระบบ (Log in)</h2>

      <form id="auth-form">
        <div class="form-group">
          <label for="auth-username">ชื่อเล่นในบาร์:</label>
          <input type="text" id="auth-username" placeholder="เช่น hyeonmin" required />
        </div>
        <div class="form-group">
          <label for="auth-password">รหัสผ่าน:</label>
          <input type="password" id="auth-password" placeholder="••••••••" required />
        </div>
        <button type="submit" id="auth-submit-btn" class="btn-block">เข้าสู่ระบบ</button>
      </form>

      <p class="text-center">
        <button type="button" data-action="toggle-auth-mode" id="auth-toggle-btn">
          ยังไม่มีบัญชี? สมัครสมาชิก
        </button>
      </p>
      <p class="text-center" style="margin-bottom: 0; color: var(--ink-secondary);">
        <small>Created by <strong>Hyeonmin</strong></small>
      </p>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 2: MATCHMAKING (เลือกโต๊ะหาเพื่อน)
  ====================================================================== -->
  <div id="modal-matchmaking" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>อยากนั่งโต๊ะแบบไหนดี</h2>

      <div class="grid-two-col-sm">
        <div class="select-card is-active" data-action="select-table-mode" data-mode="one-on-one">
          <div>🥂</div>
          <strong>One on One</strong><br />
          <small>(โต๊ะเดี่ยว 2 คน)</small>
        </div>

        <div class="select-card" data-action="select-table-mode" data-mode="group">
          <div>🍻</div>
          <strong>Up to 5 people</strong><br />
          <small>(โต๊ะกลุ่มสูงสุด 5 คน)</small>
        </div>
      </div>

      <div class="select-card" data-action="select-table-mode" data-mode="music">
        <strong>🎧 ห้องสิงฟังเพลง</strong>
        <p><small>มีคนอยู่ในห้องสิงฟังเพลง 0 คน</small></p>
      </div>

      <hr />

      <h3>อยากคุยแบบไหน</h3>
      <p><small>ข้ามส่วนนี้ไป ถ้าอยากคุยแบบไหนก็ได้</small></p>
      <p>อยากเจอคนวัยใกล้ ๆ กัน ? บอกช่วงวัยของคุณมาได้เลย...</p>

      <div id="age-options-container" class="flex-gap"></div>

      <div class="modal-actions">
        <button type="button" data-action="confirm-matchmaking">เปิดโต๊ะใหม่ 🔥</button>
        <button type="button" data-action="confirm-matchmaking">จอยโต๊ะ</button>
      </div>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 3: VENTING INTRO (คำอธิบายก่อนเข้าห้องระบาย)
  ====================================================================== -->
  <div id="modal-venting-intro" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog text-center">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>โปรดอ่านก่อนเข้า</h2>
      <p>โซนนี้ไว้สำหรับคนที่มีเรื่องอยากระบาย และบาร์เทนเดอร์ที่อาสามารับฟังโดยเฉพาะ</p>

      <div class="placeholder-box">
        [ รูปบาร์เทนเดอร์ถือเชคเกอร์รับฟังคนร้องไห้ที่มีเรื่องกังวล ]
      </div>

      <p>อาจจะได้เจอบาร์เทนเดอร์มารับฟัง หรืออาจจะเจอคนอยากระบายด้วยกัน ก็แบ่งกันเล่านะ</p>
      <p><strong>*ขอให้มีเวลาอย่างน้อย 15 นาทีในการเข้ามาพูดคุย ไม่เข้ามาส่องและออกไปนะ</strong></p>

      <button type="button" class="btn-lg" data-action="confirm-venting">
        โอเค ไปที่โต๊ะกัน
      </button>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 4: CUSTOMIZE CHARACTER (แต่งตัวน้องโมจิ)
  ====================================================================== -->
  <div id="modal-customize" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog text-center">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>👕 แต่งตัวให้น้องโมจิของคุณ</h2>
      <p><small>เลือกเครื่องประดับบนหัวที่จะแสดงให้เพื่อนในโต๊ะเห็น</small></p>
      <div id="hat-options-container" class="hat-grid"></div>
      <div class="modal-actions">
        <button type="button" class="btn-lg" data-action="close-modal">เสร็จเรียบร้อย</button>
      </div>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 5: SOUND & MUSIC SETTINGS (ปรับเสียงและเปิดเพลงจากโฟลเดอร์ music)
  ====================================================================== -->
  <div id="modal-settings" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog modal-lg">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>⚙️ ตั้งค่าเสียงและเพลงในบาร์</h2>
      <p><small>ปรับระดับเสียงและเลือกเปิดเพลงจากโฟลเดอร์ music</small></p>

      <div class="card volume-control-box">
        <div class="flex-between">
          <div>
            <strong>🎵 กำลังเลือก: <span id="now-playing-title">-</span></strong>
            <div id="audio-play-status"><small>สถานะ: หยุดเล่นอยู่</small></div>
          </div>
          <button type="button" id="btn-toggle-bgm" class="btn-lg" data-action="toggle-bgm">
            ▶️ เล่นเพลง
          </button>
        </div>

        <hr />

        <div class="flex-between">
          <label for="bgm-volume-slider"><strong>🔊 ระดับเสียงเพลง: <span id="bgm-volume-label">20%</span></strong></label>
          <button type="button" id="btn-mute-bgm" data-action="toggle-mute-bgm">🔇 ปิดเสียง</button>
        </div>
        <input type="range" id="bgm-volume-slider" min="0" max="100" value="20" class="volume-slider" />
      </div>

      <h4>📻 รายการเพลงในเว็บ (คลิกเพื่อเลือกเปิด)</h4>
      <div id="web-music-list" class="track-list"></div>

      <audio id="web-audio-player" loop></audio>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 6: TERMS & POLICY
  ====================================================================== -->
  <div id="modal-terms" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog modal-lg">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>Terms of Service &amp; Privacy Policy</h2>
      <p><small>โปรดอ่านข้อตกลงและแนวทางปฏิบัติเพื่อความปลอดภัยร่วมกันในบาร์ทิพย์</small></p>

      <div class="tabs-nav">
        <button type="button" class="tab-btn is-active" data-action="switch-terms-tab" data-tab="tos">
          1. ข้อกำหนดการใช้งาน (Terms)
        </button>
        <button type="button" class="tab-btn" data-action="switch-terms-tab" data-tab="privacy">
          2. นโยบายความเป็นส่วนตัว (Privacy)
        </button>
      </div>

      <div id="terms-tab-tos" class="modal-scroll-body">
        <section class="info-block">
          <h4>1. วัตถุประสงค์ของพื้นที่</h4>
          <p>
            เว็บไซต์นี้จัดทำขึ้นเพื่อเป็นพื้นที่ปลอดภัย (Safe Space) สำหรับการพูดคุย แลกเปลี่ยนเรื่องราว
            และรับฟังซึ่งกันและกันโดยไม่เปิดเผยตัวตน ผู้ใช้งานต้องมีอายุ 15 ปีขึ้นไป
          </p>
        </section>
        <section class="info-block">
          <h4>2. กฎเหล็กด้านความปลอดภัย (งดเปิดเผยข้อมูลส่วนตัว)</h4>
          <ul>
            <li>ห้ามส่งข้อมูลติดต่อส่วนตัว เช่น เบอร์โทรศัพท์, LINE ID, Instagram, ที่อยู่ หรือรูปภาพใบหน้าจริงให้กับคนแปลกหน้า</li>
            <li>ทางระบบขอสงวนสิทธิ์ไม่รับผิดชอบต่อความเสียหายใดๆ ที่เกิดจากการเปิดเผยข้อมูลส่วนตัวของผู้ใช้งานเอง</li>
          </ul>
        </section>
        <section class="info-block">
          <h4>3. พฤติกรรมต้องห้าม (Zero Tolerance)</h4>
          <ul>
            <li>ห้ามใช้คำพูดคุกคามทางเพศ (Sexual Harassment), การกลั่นแกล้ง (Bullying) หรือดูถูกเหยียดหยามผู้อื่น</li>
            <li>ห้ามโฆษณา สแปม ชักชวนลงทุน หรือส่งลิงก์ที่เป็นอันตราย</li>
          </ul>
        </section>
        <section class="info-block">
          <h4>4. ขอบเขตของโหมด "รับฟัง ระบาย" และอาสาบาร์เทนเดอร์</h4>
          <p>
            บาร์เทนเดอร์ในระบบเป็นอาสาสมัครที่พร้อมรับฟังด้วยความเข้าใจ <strong>ไม่ใช่จิตแพทย์หรือนักจิตวิทยาคลินิก</strong>
            หากกำลังเผชิญภาวะวิกฤตทางอารมณ์ โปรดติดต่อ <strong>สายด่วนสุขภาพจิต 1323</strong> (ฟรี ตลอด 24 ชั่วโมง)
          </p>
        </section>
      </div>

      <div id="terms-tab-privacy" class="modal-scroll-body" hidden>
        <section class="info-block">
          <h4>1. ข้อมูลที่เราจัดเก็บ</h4>
          <ul>
            <li><strong>ข้อมูลบัญชีพื้นฐาน:</strong> ชื่อเล่น (Alias) รหัสผ่าน และสถานะอารมณ์/การแต่งตัวที่เลือกไว้</li>
          </ul>
        </section>
        <section class="info-block">
          <h4>2. การเก็บรักษาข้อความสนทนา</h4>
          <p>
            ข้อความแชทภายในโต๊ะทำงานแบบเรียลไทม์ชั่วคราว เมื่อทุกคนออกจากโต๊ะประวัติการสนทนาจะถูกลบออกจากหน่วยความจำทันที
          </p>
        </section>
      </div>

      <div class="modal-actions">
        <button type="button" class="btn-lg" data-action="close-modal">รับทราบและปิดหน้าต่าง</button>
      </div>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 7: HOW TO USE IN 4 STEPS
  ====================================================================== -->
  <div id="modal-guide" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog modal-lg">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>วิธีใช้งานบาร์ทิพย์ใน 4 ขั้นตอน 🍸</h2>
      <div class="modal-scroll-body">
        <article class="step-card">
          <h4>ขั้นตอนที่ 1: สมัครสมาชิกและตั้งชื่อเล่น</h4>
          <p>กดปุ่ม <strong>Log in / สมัครสมาชิก</strong> ตั้งชื่อเล่นที่สบายใจ ระบบจะจำบัญชีและพาคุณเข้าหน้า SPU VISUAL BAR ทันที</p>
        </article>
        <article class="step-card">
          <h4>ขั้นตอนที่ 2: เลือกความรู้สึกของคุณตอนนี้</h4>
          <p>เลือกความรู้สึกจาก 4 อารมณ์ (<strong>เศร้า, แฮปปี้, เบื่อเหงา, เครียด</strong>) น้องโมจิจะเปลี่ยนสีหน้าและถือแก้วเครื่องดื่มประจำอารมณ์นั้น</p>
        </article>
        <article class="step-card">
          <h4>ขั้นตอนที่ 3: จับคู่โต๊ะพูดคุยหรือรับฟังระบาย</h4>
          <p>เลือก <strong>คุยเล่น หาเพื่อนแก้เหงา</strong> (One on One, โต๊ะกลุ่ม 5 คน, ห้องสิงฟังเพลง YouTube) หรือ <strong>รับฟัง ระบาย</strong> เพื่อจับคู่กับผู้เล่นออนไลน์จริง</p>
        </article>
        <article class="step-card">
          <h4>ขั้นตอนที่ 4: พูดคุย ฟังเพลง และสุ่มการ์ดคำถาม</h4>
          <p>พิมพ์ข้อความในแชทให้เด้งขึ้นบนหัวตัวละคร กดปุ่มตั้งค่า <strong>⚙️</strong> เพื่อปรับระดับเสียง และกด <strong>🃏</strong> เพื่อสุ่มคำถามชวนคุย</p>
        </article>
      </div>
      <div class="modal-actions">
        <button type="button" data-action="close-modal">ปิด</button>
        <button type="button" class="btn-lg" data-action="open-modal" data-modal="modal-auth">เริ่มใช้งานเลย!</button>
      </div>
    </div>
  </div>

  <!-- =====================================================================
       MODAL 8: FAQ
  ====================================================================== -->
  <div id="modal-faq" class="modal-backdrop" role="dialog" aria-modal="true" hidden>
    <div class="modal-dialog modal-lg">
      <button type="button" class="modal-close" data-action="close-modal">✖</button>
      <h2>คำถามที่พบบ่อย (FAQ) ❓</h2>
      <div class="modal-scroll-body">
        <details class="faq-item" open>
          <summary><strong>1. เว็บไซต์นี้เล่นฟรีไหม?</strong></summary>
          <p>ใช้งานได้ฟรี 100% ทุกฟีเจอร์หลักครับ</p>
        </details>
        <details class="faq-item">
          <summary><strong>2. คนอื่นในโต๊ะจะรู้ตัวตนจริงของเราไหม?</strong></summary>
          <p>ไม่รู้ครับ แสดงเฉพาะชื่อเล่นและตัวละครน้องโมจิของคุณเท่านั้น</p>
        </details>
        <details class="faq-item">
          <summary><strong>3. โหมด "อาสาเป็นบาร์เทนเดอร์" ทำงานอย่างไร?</strong></summary>
          <p>เมื่อเปิดสวิตช์อาสาเป็นบาร์เทนเดอร์ แล้วกดเข้าโซน "รับฟัง ระบาย" ระบบจะจับคู่คุณเข้าโต๊ะกับผู้ใช้งานที่กำลังหาคนรับฟังโดยอัตโนมัติครับ</p>
        </details>
        <details class="faq-item">
          <summary><strong>4. ห้องสิงฟังเพลงทำงานอย่างไร?</strong></summary>
          <p>วางลิงก์ YouTube เพื่อเพิ่มเข้าคิว เมื่อเพลงจบจะถูกลบออกจากคิวอัตโนมัติ และหากต้องการข้ามเพลงต้องโหวตให้ได้คะแนนมากกว่าครึ่งของคนในห้องครับ</p>
        </details>
      </div>
      <div class="modal-actions">
        <button type="button" class="btn-lg" data-action="close-modal">เข้าใจแล้ว</button>
      </div>
    </div>
  </div>

  <script src="/socket.io/socket.io.js"></script>
  <script src="https://www.youtube.com/iframe_api"></script>
  <script>
    'use strict';

    var STORAGE_KEY = 'vb_logged_in_user';

    var MOODS = [
      { id: 'sad', label: 'เศร้า', drink: 'Blue Tear', drinkColor: '#7CC6FE', emoji: '😢' },
      { id: 'happy', label: 'แฮปปี้', drink: 'Sunny Fizz', drinkColor: '#F5B963', emoji: '😄' },
      { id: 'lonely', label: 'เบื่อเหงา', drink: 'Midnight Soda', drinkColor: '#9B82D0', emoji: '😶‍🌫️' },
      { id: 'stressed', label: 'เครียด', drink: 'Worry Wonka', drinkColor: '#F29CA3', emoji: '😫' }
    ];

    var HATS = [
      { id: 'beret', label: 'หมวกเบเร่ต์ฟ้า', icon: '🧢' },
      { id: 'cat', label: 'หูแมวเหมียว', icon: '🐱' },
      { id: 'sprout', label: 'ต้นอ่อนรักษ์โลก', icon: '🌱' },
      { id: 'headphones', label: 'หูฟัง Lo-Fi', icon: '🎧' },
      { id: 'crown', label: 'มงกุฎจิ๋ว', icon: '👑' },
      { id: 'none', label: 'ไม่ใส่หมวก', icon: '⚪' }
    ];

    var AGE_GROUPS = ['วัยไหนก็ได้', 'มัธยมปลาย', 'มหาลัย', 'วัยทำงาน'];

    var TESTIMONIALS = [
      {
        text: 'กำลังใจที่ได้จากคนแปลกหน้าในบาร์ ทำให้รู้ว่าเรายังมีคุณค่าและไปต่อได้ ขอบคุณที่สร้างพื้นที่ดีๆ แบบนี้ขึ้นมา',
        author: 'Sick'
      },
      {
        text: 'วันที่เหนื่อยๆ แค่ได้เข้ามานั่งฟังเพลงและเล่าเรื่องในใจโดยไม่ต้องเปิดเผยตัวตน ก็รู้สึกเบาใจขึ้นเยอะเลย',
        author: 'Jayden'
      },
      {
        text: 'เจอเพื่อนคุยถูกคอจากโต๊ะกลุ่ม ตอนนี้กลายเป็นเพื่อนสนิทกันไปแล้ว!',
        author: 'Celine'
      }
    ];

    var ICEBREAKER_QUESTIONS = [
      'วันนี้มีเรื่องอะไรที่ทำให้ยิ้มได้บ้างไหม?',
      'ถ้าตอนนี้วาร์ปไปที่ไหนก็ได้ 1 ที่ อยากไปที่ไหน?',
      'เพลงที่ฟังบ่อยที่สุดในช่วงนี้คือเพลงอะไร?',
      'มีเรื่องอะไรที่อยากระบายแต่ไม่กล้าบอกคนรอบตัวไหม?'
    ];

    var socket = typeof io !== 'undefined' ? io() : null;

    var state = {
      currentView: 'landing',
      user: null,
      selectedMood: MOODS[0],
      isBartender: false,
      isRegisterMode: true,
      onlineCount: 1,
      musicRoomCount: 0,
      testimonialIndex: 0,
      matchmaking: {
        mode: 'one-on-one',
        ageGroup: AGE_GROUPS[0]
      },
      table: null,
      ytApiReady: Boolean(window.YT && window.YT.Player),
      ytPlayerInstance: null,
      currentYtTrackId: null,
      ytLocalVolume: 80
    };

    window.onYouTubeIframeAPIReady = function () {
      state.ytApiReady = true;
      if (state.table && state.table.mode === 'music') {
        renderTableRoom();
      }
    };

    var audioState = {
      volume: 20,
      isMuted: false,
      isPlaying: false,
      pausedByMusicRoom: false,
      currentIndex: 0,
      tracks: []
    };

    var dom = {
      views: {
        landing: document.getElementById('view-landing'),
        lobby: document.getElementById('view-lobby'),
        table: document.getElementById('view-table')
      },
      modals: {
        auth: document.getElementById('modal-auth'),
        matchmaking: document.getElementById('modal-matchmaking'),
        venting: document.getElementById('modal-venting-intro'),
        customize: document.getElementById('modal-customize'),
        settings: document.getElementById('modal-settings'),
        terms: document.getElementById('modal-terms'),
        guide: document.getElementById('modal-guide'),
        faq: document.getElementById('modal-faq')
      }
    };

    function showPopup(message, title) {
      document.getElementById('popup-title').textContent = title || 'แจ้งเตือน 💬';
      document.getElementById('popup-message').textContent = message;
      document.getElementById('modal-popup').hidden = false;
    }

    function closePopup() {
      document.getElementById('modal-popup').hidden = true;
    }

    function destroyYoutubePlayer() {
      if (state.ytPlayerInstance) {
        try {
          state.ytPlayerInstance.destroy();
        } catch (e) {}
        state.ytPlayerInstance = null;
      }
      state.currentYtTrackId = null;
      var embedBox = document.getElementById('yt-embed-container');
      if (embedBox && !document.getElementById('yt-player-mount')) {
        var mount = document.createElement('div');
        mount.id = 'yt-player-mount';
        embedBox.insertBefore(mount, embedBox.firstChild);
      }
    }

    function buildMochiSVG(moodObj, hatId, isBartenderMode) {
      var m = moodObj || MOODS[0];
      var hatSvg = '';
      if (hatId === 'beret') {
        hatSvg = '<ellipse cx="56" cy="26" rx="24" ry="10" transform="rotate(-10 56 26)" fill="#7B8CDE" stroke="#4A3F35" stroke-width="2.5"/><path d="M56 16 L58 11" stroke="#4A3F35" stroke-width="2.5" stroke-linecap="round"/>';
      } else if (hatId === 'cat') {
        hatSvg = '<polygon points="34,36 42,14 52,32" fill="#FFFDF9" stroke="#4A3F35" stroke-width="2.5"/><polygon points="62,32 72,14 80,36" fill="#FFFDF9" stroke="#4A3F35" stroke-width="2.5"/>';
      } else if (hatId === 'sprout') {
        hatSvg = '<path d="M57 32 L57 18" stroke="#4A3F35" stroke-width="2.5"/><path d="M57 20 Q68 10 74 18 Q65 24 57 20" fill="#88C498" stroke="#4A3F35" stroke-width="2"/><path d="M57 22 Q46 12 40 20 Q49 26 57 22" fill="#88C498" stroke="#4A3F35" stroke-width="2"/>';
      } else if (hatId === 'headphones') {
        hatSvg = '<path d="M28 52 C28 18 86 18 86 52" fill="none" stroke="#4A3F35" stroke-width="3.5"/><rect x="20" y="44" width="11" height="20" rx="5" fill="#9B82D0" stroke="#4A3F35" stroke-width="2.2"/><rect x="83" y="44" width="11" height="20" rx="5" fill="#9B82D0" stroke="#4A3F35" stroke-width="2.2"/>';
      } else if (hatId === 'crown') {
        hatSvg = '<polygon points="40,30 44,16 52,24 57,14 62,24 70,16 74,30" fill="#F5B963" stroke="#4A3F35" stroke-width="2.2"/>';
      }

      var faceSvg = '';
      if (m.id === 'sad') {
        faceSvg = '<path d="M42 54 Q46 50 50 54" fill="none" stroke="#4A3F35" stroke-width="2.3"/><path d="M64 54 Q68 50 72 54" fill="none" stroke="#4A3F35" stroke-width="2.3"/><path d="M44 58 L44 68" stroke="#7CC6FE" stroke-width="2.8" stroke-linecap="round"/><path d="M53 63 Q57 59 61 63" fill="none" stroke="#4A3F35" stroke-width="2.2"/>';
      } else if (m.id === 'lonely') {
        faceSvg = '<line x1="42" y1="54" x2="50" y2="54" stroke="#4A3F35" stroke-width="2.5"/><line x1="64" y1="54" x2="72" y2="54" stroke="#4A3F35" stroke-width="2.5"/><circle cx="57" cy="62" r="2.5" fill="#4A3F35"/>';
      } else if (m.id === 'stressed') {
        faceSvg = '<path d="M42 50 L50 55 L42 58" fill="none" stroke="#4A3F35" stroke-width="2.2"/><path d="M72 50 L64 55 L72 58" fill="none" stroke="#4A3F35" stroke-width="2.2"/><path d="M51 63 Q54 60 57 63 Q60 60 63 63" fill="none" stroke="#4A3F35" stroke-width="2.2"/>';
      } else {
        faceSvg = '<circle cx="46" cy="54" r="3" fill="#4A3F35"/><circle cx="68" cy="54" r="3" fill="#4A3F35"/><path d="M52 59 Q57 65 62 59" fill="none" stroke="#4A3F35" stroke-width="2.3" stroke-linecap="round"/>';
      }

      var bowtieSvg = isBartenderMode
        ? '<polygon points="57,76 48,71 48,81" fill="#9B82D0" stroke="#4A3F35" stroke-width="1.8"/><polygon points="57,76 66,71 66,81" fill="#9B82D0" stroke="#4A3F35" stroke-width="1.8"/>'
        : '';

      return '<svg viewBox="0 0 120 105" width="112" height="100">' +
        hatSvg +
        '<path d="M26 98 C26 36 86 36 86 98 Z" fill="#FFFDF9" stroke="#4A3F35" stroke-width="3"/>' +
        '<ellipse cx="40" cy="61" rx="5" ry="2.8" fill="#F29CA3" opacity="0.85"/>' +
        '<ellipse cx="74" cy="61" rx="5" ry="2.8" fill="#F29CA3" opacity="0.85"/>' +
        faceSvg +
        bowtieSvg +
        '<rect x="90" y="64" width="18" height="24" rx="5" fill="' + (m.drinkColor || '#F5B963') + '" stroke="#4A3F35" stroke-width="2.4"/>' +
        '<line x1="99" y1="64" x2="105" y2="52" stroke="#4A3F35" stroke-width="2.2" stroke-linecap="round"/>' +
        '</svg>';
    }

    function init() {
      restoreSavedUserSession();
      bindSocketEvents();
      bindGlobalClickDelegation();
      bindKeyboardShortcuts();
      bindFormSubmissions();
      bindAudioControls();
      loadMusicFromFolder();
      renderAll();
    }

    function restoreSavedUserSession() {
      try {
        var raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return;
        var savedUser = JSON.parse(raw);
        if (savedUser && savedUser.id && savedUser.username) {
          state.user = savedUser;
          state.selectedMood = MOODS.find(function (m) { return m.id === savedUser.moodId; }) || MOODS[0];
          state.currentView = 'lobby';
          if (socket) socket.emit('user:identify', savedUser.id);
        }
      } catch (e) {
        localStorage.removeItem(STORAGE_KEY);
      }
    }

    function saveUserSession(userObj) {
      state.user = userObj;
      localStorage.setItem(STORAGE_KEY, JSON.stringify(userObj));
      if (socket) socket.emit('user:identify', userObj.id);
    }

    function bindSocketEvents() {
      if (!socket) return;

      socket.on('stats:update', function (stats) {
        state.onlineCount = stats.onlineCount;
        state.musicRoomCount = stats.musicRoomCount;
        updateOnlineCounters();
      });

      socket.on('table:sync', function (roomData) {
        var isEnteringMusicRoom = roomData && roomData.mode === 'music';
        if (isEnteringMusicRoom && audioState.isPlaying) {
          audioState.pausedByMusicRoom = true;
          stopAllMusic();
          renderSettingsModal();
        }

        state.table = roomData;
        closeAllModals();
        if (state.currentView !== 'table') {
          state.currentView = 'table';
          renderAll();
        } else {
          renderTableRoom();
        }
      });

      socket.on('match:error', function (errorMessage) {
        showPopup(errorMessage, 'แจ้งเตือน');
      });
    }

    function updateOnlineCounters() {
      var heroCounter = document.querySelector('.online-counter');
      if (heroCounter) {
        heroCounter.textContent = state.onlineCount + ' คนกำลังออนไลน์อยู่ในบาร์ตอนนี้';
      }
      var lobbyCounter = document.querySelector('#view-lobby .text-right small');
      if (lobbyCounter) {
        lobbyCounter.textContent = state.onlineCount + ' Users are Online';
      }
      var musicCountText = document.querySelector('[data-mode="music"] small');
      if (musicCountText) {
        musicCountText.textContent = 'มีคนอยู่ในห้องสิงฟังเพลง ' + state.musicRoomCount + ' คน';
      }
    }

    function bindGlobalClickDelegation() {
      document.addEventListener('click', function (event) {
        if (event.target.id === 'modal-popup') {
          closePopup();
          return;
        }
        if (event.target.classList.contains('modal-backdrop')) {
          closeAllModals();
          return;
        }

        var trigger = event.target.closest('[data-action]');
        if (!trigger) return;

        if (trigger.tagName === 'A') {
          event.preventDefault();
        }

        var action = trigger.dataset.action;
        var modal = trigger.dataset.modal;
        var tab = trigger.dataset.tab;
        var mode = trigger.dataset.mode;
        var age = trigger.dataset.age;
        var moodId = trigger.dataset.moodId;
        var hatId = trigger.dataset.hatId;
        var trackIdx = trigger.dataset.trackIdx;
        var index = trigger.dataset.index;
        var msg = trigger.dataset.msg;
        var title = trigger.dataset.title;

        switch (action) {
          case 'close-popup':
            closePopup();
            break;
          case 'go-landing':
            navigateTo(state.user ? 'lobby' : 'landing');
            break;
          case 'go-lobby':
            navigateTo('lobby');
            break;
          case 'logout':
            if (socket) socket.emit('table:leave');
            destroyYoutubePlayer();
            localStorage.removeItem(STORAGE_KEY);
            state.user = null;
            state.table = null;
            navigateTo('landing');
            break;
          case 'open-modal':
            openModal(modal);
            break;
          case 'close-modal':
            closeAllModals();
            break;
          case 'focus-room-code':
            var codeForm = document.getElementById('join-code-form');
            var codeInput = document.getElementById('input-room-code');
            if (codeForm) {
              codeForm.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }
            if (codeInput) {
              setTimeout(function () { codeInput.focus(); }, 250);
            }
            break;
          case 'switch-terms-tab':
            switchTab('terms', tab);
            break;
          case 'toggle-auth-mode':
            state.isRegisterMode = !state.isRegisterMode;
            renderAuthModal();
            break;
          case 'select-testimonial':
            state.testimonialIndex = Number(index);
            renderLanding();
            break;
          case 'select-mood':
            state.selectedMood = MOODS.find(function (m) { return m.id === moodId; }) || MOODS[0];
            if (state.user) {
              state.user.moodId = state.selectedMood.id;
              saveUserSession(state.user);
              fetch('/api/user/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: state.user.id, moodId: state.selectedMood.id })
              }).catch(function () {});
            }
            renderLobby();
            break;
          case 'select-hat':
            if (state.user) {
              state.user.hatId = hatId;
              saveUserSession(state.user);
              fetch('/api/user/update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ id: state.user.id, hatId: hatId })
              }).catch(function () {});
            }
            renderCustomizeModal();
            renderLobby();
            break;
          case 'toggle-bgm':
            if (audioState.isPlaying) {
              audioState.pausedByMusicRoom = false;
              stopAllMusic();
              renderSettingsModal();
            } else {
              playSelectedTrack(false);
            }
            break;
          case 'toggle-mute-bgm':
            audioState.isMuted = !audioState.isMuted;
            applyVolumeToPlayers();
            renderSettingsModal();
            break;
          case 'select-web-track':
            audioState.currentIndex = Number(trackIdx);
            playSelectedTrack(false);
            break;
          case 'vote-skip-youtube':
            if (socket) socket.emit('table:voteSkip');
            break;
          case 'edit-username':
            document.getElementById('username-display-box').hidden = true;
            document.getElementById('username-edit-form').hidden = false;
            document.getElementById('input-username').value = state.user.username;
            break;
          case 'cancel-edit-username':
            document.getElementById('username-display-box').hidden = false;
            document.getElementById('username-edit-form').hidden = true;
            break;
          case 'select-table-mode':
            state.matchmaking.mode = mode;
            renderMatchmakingModal();
            break;
          case 'select-age-group':
            state.matchmaking.ageGroup = age;
            renderMatchmakingModal();
            break;
          case 'confirm-matchmaking':
            requestMatchmaking({
              mode: state.matchmaking.mode,
              ageGroup: state.matchmaking.ageGroup
            });
            break;
          case 'confirm-venting':
            requestMatchmaking({
              mode: 'venting',
              ageGroup: 'ระบาย'
            });
            break;
          case 'leave-table':
            var wasMusicRoom = state.table && state.table.mode === 'music';
            if (socket) socket.emit('table:leave');
            destroyYoutubePlayer();
            state.table = null;
            navigateTo('lobby');
            if (wasMusicRoom && audioState.pausedByMusicRoom) {
              audioState.pausedByMusicRoom = false;
              playSelectedTrack(true);
            }
            break;
          case 'switch-table':
            requestMatchmaking({
              mode: state.table.mode,
              ageGroup: state.table.ageGroup,
              excludeCode: state.table.code
            });
            break;
          case 'edit-table-title':
            document.getElementById('table-title-display').hidden = true;
            document.getElementById('table-title-form').hidden = false;
            document.getElementById('input-table-title').value = state.table.title;
            break;
          case 'copy-room-code':
            if (navigator.clipboard) navigator.clipboard.writeText(state.table.code);
            showPopup('คัดลอกรหัสโต๊ะ "' + state.table.code + '" แล้ว ส่งให้เพื่อนกรอกเข้าโต๊ะได้เลย!', 'คัดลอกรหัสโต๊ะ 📋');
            break;
          case 'switch-room-tab':
            switchTab('room', tab);
            break;
          case 'draw-icebreaker':
            var question = ICEBREAKER_QUESTIONS[Math.floor(Math.random() * ICEBREAKER_QUESTIONS.length)];
            if (socket) socket.emit('table:icebreaker', { question: question });
            break;
          case 'append-emoji':
            var chatInput = document.getElementById('input-chat-message');
            chatInput.value += ' 😊';
            chatInput.focus();
            break;
          case 'alert-feature':
            showPopup(msg, title || 'แจ้งเตือน');
            break;
        }
      });

      document.addEventListener('change', function (event) {
        if (event.target.id === 'toggle-bartender') {
          state.isBartender = event.target.checked;
          renderLobby();
        }
        if (event.target.id === 'toggle-table-public' && state.table) {
          if (socket) {
            socket.emit('table:togglePublic', { isPublic: event.target.checked });
          }
        }
      });
    }

    function bindKeyboardShortcuts() {
      document.addEventListener('keydown', function (event) {
        if (event.key === 'Escape') {
          if (!document.getElementById('modal-popup').hidden) {
            closePopup();
          } else {
            closeAllModals();
          }
        }
      });
    }

    function bindFormSubmissions() {
      document.getElementById('auth-form').addEventListener('submit', async function (e) {
        e.preventDefault();
        var username = document.getElementById('auth-username').value.trim();
        var password = document.getElementById('auth-password').value;
        var endpoint = state.isRegisterMode ? '/api/register' : '/api/login';

        try {
          var response = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username, password: password })
          });
          var data = await response.json();
          if (!response.ok) {
            showPopup(data.error || 'เกิดข้อผิดพลาดในการเข้าสู่ระบบ', 'เข้าสู่ระบบไม่สำเร็จ');
            return;
          }

          saveUserSession(data.user);
          state.selectedMood = MOODS.find(function (m) { return m.id === data.user.moodId; }) || MOODS[0];
          e.target.reset();
          closeAllModals();
          navigateTo('lobby');
        } catch (err) {
          showPopup('ไม่สามารถเชื่อมต่อเซิร์ฟเวอร์ได้', 'เกิดข้อผิดพลาด');
        }
      });

      document.getElementById('username-edit-form').addEventListener('submit', async function (e) {
        e.preventDefault();
        var newName = document.getElementById('input-username').value.trim();
        if (!newName) return;

        try {
          var res = await fetch('/api/user/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: state.user.id, username: newName })
          });
          var data = await res.json();
          if (res.ok && data.user) saveUserSession(data.user);
        } catch (err) {
          state.user.username = newName;
          saveUserSession(state.user);
        }

        document.getElementById('username-display-box').hidden = false;
        document.getElementById('username-edit-form').hidden = true;
        renderLobby();
      });

      document.getElementById('join-code-form').addEventListener('submit', function (e) {
        e.preventDefault();
        var code = document.getElementById('input-room-code').value.trim();
        if (!code) return;
        requestMatchmaking({ customCode: code });
        e.target.reset();
      });

      document.getElementById('table-title-form').addEventListener('submit', function (e) {
        e.preventDefault();
        var newTitle = document.getElementById('input-table-title').value.trim();
        if (socket) socket.emit('table:updateTitle', { title: newTitle });
        document.getElementById('table-title-display').hidden = false;
        document.getElementById('table-title-form').hidden = true;
      });

      document.getElementById('chat-form').addEventListener('submit', function (e) {
        e.preventDefault();
        var input = document.getElementById('input-chat-message');
        var text = input.value.trim();
        if (!text || !state.table) return;
        if (socket) socket.emit('table:message', { text: text });
        input.value = '';
      });

      document.getElementById('jukebox-form').addEventListener('submit', function (e) {
        e.preventDefault();
        var input = document.getElementById('input-song-name');
        var val = input.value.trim();
        if (!val) return;
        if (state.table && state.table.mode === 'music') {
          if (socket) socket.emit('table:addYoutube', { url: val });
        } else {
          if (socket) socket.emit('table:addSong', { song: val });
        }
        input.value = '';
      });

      document.getElementById('youtube-room-form').addEventListener('submit', function (e) {
        e.preventDefault();
        var input = document.getElementById('input-youtube-url');
        var url = input.value.trim();
        if (!url) return;
        if (socket) socket.emit('table:addYoutube', { url: url });
        input.value = '';
      });
    }

    // =========================================================================
    // MUSIC FOLDER PLAYER & YOUTUBE LOCAL VOLUME ENGINE
    // =========================================================================
    var hasAttemptedInitialAutoPlay = false;

    function unlockAutoPlayOnFirstInteract() {
      function onFirstInteract() {
        document.removeEventListener('pointerdown', onFirstInteract);
        document.removeEventListener('keydown', onFirstInteract);
        if (state.table && state.table.mode === 'music') return;
        if (!audioState.isPlaying && audioState.tracks.length > 0) {
          playSelectedTrack(true);
        }
      }
      document.addEventListener('pointerdown', onFirstInteract);
      document.addEventListener('keydown', onFirstInteract);
    }

    function loadMusicFromFolder() {
      return fetch('/api/music')
        .then(function (res) { return res.json(); })
        .then(function (data) {
          audioState.tracks = data.tracks || [];
          if (audioState.currentIndex >= audioState.tracks.length) {
            audioState.currentIndex = 0;
          }
          renderSettingsModal();

          if (!hasAttemptedInitialAutoPlay && audioState.tracks.length > 0) {
            hasAttemptedInitialAutoPlay = true;
            playSelectedTrack(true);
          }
        })
        .catch(function () {});
    }

    function bindAudioControls() {
      var slider = document.getElementById('bgm-volume-slider');
      if (slider) {
        slider.addEventListener('input', function (e) {
          audioState.volume = Number(e.target.value);
          if (audioState.volume > 0) audioState.isMuted = false;
          applyVolumeToPlayers();
          document.getElementById('bgm-volume-label').textContent = audioState.volume + '%';
          document.getElementById('btn-mute-bgm').textContent = '🔇 ปิดเสียง';
        });
      }

      var ytVolSlider = document.getElementById('yt-local-vol-slider');
      if (ytVolSlider) {
        ytVolSlider.addEventListener('input', function (e) {
          state.ytLocalVolume = Number(e.target.value);
          document.getElementById('yt-local-vol-label').textContent = state.ytLocalVolume + '%';
          if (state.ytPlayerInstance && typeof state.ytPlayerInstance.setVolume === 'function') {
            state.ytPlayerInstance.setVolume(state.ytLocalVolume);
          }
        });
      }
    }

    function getEffectiveVolume() {
      return audioState.isMuted ? 0 : audioState.volume / 100;
    }

    function applyVolumeToPlayers() {
      var vol = getEffectiveVolume();
      var htmlAudio = document.getElementById('web-audio-player');
      if (htmlAudio) htmlAudio.volume = vol;
    }

    function stopAllMusic() {
      var htmlAudio = document.getElementById('web-audio-player');
      if (htmlAudio) htmlAudio.pause();
      audioState.isPlaying = false;
    }

    function playSelectedTrack(isAutoPlay) {
      if (audioState.tracks.length === 0) {
        if (!isAutoPlay) showPopup('ยังไม่มีไฟล์เพลงในโฟลเดอร์ music ครับ', 'ไม่พบไฟล์เพลง');
        return;
      }
      stopAllMusic();
      var track = audioState.tracks[audioState.currentIndex];
      if (!track) return;

      var htmlAudio = document.getElementById('web-audio-player');
      htmlAudio.src = track.src;
      applyVolumeToPlayers();
      audioState.isPlaying = true;
      htmlAudio.play().catch(function () {
        stopAllMusic();
        renderSettingsModal();
        if (isAutoPlay) {
          unlockAutoPlayOnFirstInteract();
        } else {
          showPopup('ไม่สามารถเล่นไฟล์เพลงนี้ได้', 'เกิดข้อผิดพลาด');
        }
      });
      renderSettingsModal();
    }

    function renderSettingsModal() {
      var currentTrack = audioState.tracks[audioState.currentIndex];
      document.getElementById('now-playing-title').textContent = currentTrack ? currentTrack.title : 'ยังไม่มีเพลงในโฟลเดอร์ music';
      document.getElementById('audio-play-status').innerHTML = audioState.isPlaying
        ? '<small style="color:#2f693d;">สถานะ: กำลังเล่นเพลงอยู่ 🎶</small>'
        : '<small>สถานะ: หยุดเล่นอยู่</small>';

      document.getElementById('btn-toggle-bgm').textContent = audioState.isPlaying ? '⏸️ หยุดเพลง' : '▶️ เล่นเพลง';
      document.getElementById('bgm-volume-label').textContent = audioState.isMuted ? 'ปิดเสียง (0%)' : audioState.volume + '%';
      document.getElementById('bgm-volume-slider').value = audioState.volume;
      document.getElementById('btn-mute-bgm').textContent = audioState.isMuted ? '🔊 เปิดเสียง' : '🔇 ปิดเสียง';

      var listEl = document.getElementById('web-music-list');
      if (audioState.tracks.length === 0) {
        listEl.innerHTML = '<div class="msg-system">ยังไม่มีไฟล์เพลงในโฟลเดอร์ music (นำไฟล์ .mp3 มาวางในโฟลเดอร์ music แล้วเปิดหน้านี้ใหม่ได้เลย)</div>';
        return;
      }

      var listHtml = '';
      for (var i = 0; i < audioState.tracks.length; i++) {
        var t = audioState.tracks[i];
        var activeCls = i === audioState.currentIndex ? 'is-active' : '';
        listHtml += '<div class="track-item ' + activeCls + '" data-action="select-web-track" data-track-idx="' + i + '">' +
          '<span>' + (i === audioState.currentIndex && audioState.isPlaying ? '🎶 ' : '🎵 ') + t.title + '</span>' +
          '<small>music</small>' +
          '</div>';
      }
      listEl.innerHTML = listHtml;
    }

    // =========================================================================
    // NAVIGATION & RENDER FUNCTIONS
    // =========================================================================
    function requestMatchmaking(params) {
      if (!socket || !state.user) return;
      socket.emit('match:find', {
        user: state.user,
        mood: state.selectedMood,
        mode: params.mode,
        ageGroup: params.ageGroup,
        isBartender: state.isBartender,
        customCode: params.customCode || null,
        excludeCode: params.excludeCode || null
      });
    }

    function navigateTo(viewName) {
      state.currentView = viewName;
      renderAll();
    }

    function openModal(modalId) {
      closeAllModals();
      var target = document.getElementById(modalId);
      if (target) target.hidden = false;
      if (modalId === 'modal-auth') renderAuthModal();
      if (modalId === 'modal-matchmaking') renderMatchmakingModal();
      if (modalId === 'modal-customize') renderCustomizeModal();
      if (modalId === 'modal-settings') {
        loadMusicFromFolder();
      }
    }

    function closeAllModals() {
      Object.keys(dom.modals).forEach(function (key) {
        if (dom.modals[key]) dom.modals[key].hidden = true;
      });
    }

    function switchTab(section, activeTab) {
      if (section === 'room') {
        document.getElementById('room-tab-chat').hidden = activeTab !== 'chat';
        document.getElementById('room-tab-jukebox').hidden = activeTab !== 'jukebox';
        document.querySelectorAll('[data-action="switch-room-tab"]').forEach(function (btn) {
          btn.classList.toggle('is-active', btn.dataset.tab === activeTab);
        });
      } else if (section === 'terms') {
        document.getElementById('terms-tab-tos').hidden = activeTab !== 'tos';
        document.getElementById('terms-tab-privacy').hidden = activeTab !== 'privacy';
        document.querySelectorAll('[data-action="switch-terms-tab"]').forEach(function (btn) {
          btn.classList.toggle('is-active', btn.dataset.tab === activeTab);
        });
      }
    }

    function renderAll() {
      Object.keys(dom.views).forEach(function (key) {
        dom.views[key].hidden = key !== state.currentView;
      });
      updateOnlineCounters();
      if (state.currentView === 'landing') renderLanding();
      if (state.currentView === 'lobby' && state.user) renderLobby();
      if (state.currentView === 'table' && state.table) renderTableRoom();
    }

    function renderLanding() {
      var current = TESTIMONIALS[state.testimonialIndex];
      document.getElementById('testimonial-text').textContent = '"' + current.text + '"';
      document.getElementById('testimonial-author').textContent = '- ' + current.author + ' -';

      var dotsHtml = '';
      for (var i = 0; i < TESTIMONIALS.length; i++) {
        var cls = i === state.testimonialIndex ? 'is-active' : '';
        dotsHtml += '<button type="button" data-action="select-testimonial" data-index="' + i + '" class="' + cls + '">●</button>';
      }
      document.getElementById('testimonial-dots').innerHTML = dotsHtml;
    }

    function renderAuthModal() {
      document.getElementById('auth-modal-title').textContent = state.isRegisterMode
        ? 'สมัครสมาชิก (Register)'
        : 'เข้าสู่ระบบ (Log in)';
      document.getElementById('auth-submit-btn').textContent = state.isRegisterMode
        ? 'สมัครสมาชิกและเข้าสู่ SPU VISUAL BAR'
        : 'เข้าสู่ระบบ';
      document.getElementById('auth-toggle-btn').textContent = state.isRegisterMode
        ? 'มีบัญชีอยู่แล้ว? เข้าสู่ระบบ'
        : 'ยังไม่มีบัญชี? สมัครสมาชิก';
    }

    function renderCustomizeModal() {
      var currentHat = (state.user && state.user.hatId) || 'beret';
      var html = '';
      for (var i = 0; i < HATS.length; i++) {
        var h = HATS[i];
        var activeCls = h.id === currentHat ? 'is-active' : '';
        html += '<button type="button" data-action="select-hat" data-hat-id="' + h.id + '" class="' + activeCls + '">' +
          '<div style="font-size:1.5rem;">' + h.icon + '</div><div>' + h.label + '</div></button>';
      }
      document.getElementById('hat-options-container').innerHTML = html;
    }

    function renderLobby() {
      document.getElementById('display-username').textContent = state.user.username;

      var moodHtml = '';
      for (var i = 0; i < MOODS.length; i++) {
        var mood = MOODS[i];
        var activeCls = state.selectedMood.id === mood.id ? 'is-active' : '';
        moodHtml += '<button type="button" data-action="select-mood" data-mood-id="' + mood.id + '" class="mood-btn ' + activeCls + '">' +
          '<div>' + mood.emoji + '</div><div>รู้สึก' + mood.label + '</div></button>';
      }
      document.getElementById('mood-options-container').innerHTML = moodHtml;

      document.getElementById('selected-mood-caption').innerHTML =
        'ตอนนี้คุณกำลังรู้สึก: <strong>' + state.selectedMood.label + '</strong> (แก้ว: ' + state.selectedMood.drink + ')';

      document.getElementById('lobby-avatar-sprite').innerHTML =
        buildMochiSVG(state.selectedMood, state.user.hatId || 'beret', state.isBartender);

      document.getElementById('toggle-bartender').checked = state.isBartender;
      document.getElementById('bartender-status-text').textContent = state.isBartender ? 'เปิดรับฟังอยู่' : 'ปิด';
      document.getElementById('venting-btn-subtext').textContent = state.isBartender
        ? 'คุณกำลังเปิดโหมดบาร์เทนเดอร์พร้อมรับฟัง'
        : 'เข้ามาแชร์เรื่องไม่สบายใจได้เลย';
    }

    function renderMatchmakingModal() {
      document.querySelectorAll('[data-action="select-table-mode"]').forEach(function (card) {
        card.classList.toggle('is-active', card.dataset.mode === state.matchmaking.mode);
      });

      var ageHtml = '';
      for (var i = 0; i < AGE_GROUPS.length; i++) {
        var age = AGE_GROUPS[i];
        var activeCls = state.matchmaking.ageGroup === age ? 'is-active' : '';
        ageHtml += '<button type="button" data-action="select-age-group" data-age="' + age + '" class="' + activeCls + '">' + age + '</button>';
      }
      document.getElementById('age-options-container').innerHTML = ageHtml;
    }

    function renderMusicRoomPlayer(table) {
      var ytBox = document.getElementById('music-room-youtube-box');
      ytBox.hidden = false;

      var ytQueue = table.youtubeQueue || [];
      var currentYt = ytQueue[0] || null;
      var skipVotes = table.skipVotes || [];
      var requiredVotes = Math.floor(Math.max(table.members.length, 1) / 2) + 1;
      var hasVoted = socket && skipVotes.indexOf(socket.id) !== -1;

      var voteBtn = document.getElementById('btn-vote-skip-yt');
      if (!currentYt) {
        voteBtn.disabled = true;
        voteBtn.classList.remove('is-active');
        voteBtn.textContent = '🗳️ ข้ามเพลง (0/' + requiredVotes + ')';
      } else {
        voteBtn.disabled = false;
        voteBtn.classList.toggle('is-active', hasVoted);
        voteBtn.textContent = (hasVoted ? '✅ โหวตข้ามแล้ว (' : '🗳️ ข้ามเพลง (') +
          skipVotes.length + '/' + requiredVotes + ' คน)';
      }

      var emptyPlaceholder = document.getElementById('yt-empty-placeholder');
      if (!currentYt) {
        destroyYoutubePlayer();
        emptyPlaceholder.hidden = false;
      } else {
        emptyPlaceholder.hidden = true;
        if (state.currentYtTrackId !== currentYt.id) {
          state.currentYtTrackId = currentYt.id;
          var startSec = currentYt.startedAt
            ? Math.max(0, Math.floor((Date.now() - currentYt.startedAt) / 1000))
            : 0;

          if (state.ytPlayerInstance && typeof state.ytPlayerInstance.loadVideoById === 'function') {
            state.ytPlayerInstance.loadVideoById({
              videoId: currentYt.videoId,
              startSeconds: startSec
            });
            state.ytPlayerInstance.setVolume(state.ytLocalVolume);
          } else if (state.ytApiReady && window.YT && window.YT.Player) {
            state.ytPlayerInstance = new YT.Player('yt-player-mount', {
              videoId: currentYt.videoId,
              playerVars: {
                autoplay: 1,
                controls: 0,
                disablekb: 1,
                fs: 0,
                modestbranding: 1,
                rel: 0,
                playsinline: 1,
                start: startSec
              },
              events: {
                onReady: function (e) {
                  e.target.setVolume(state.ytLocalVolume);
                  e.target.playVideo();
                },
                onStateChange: function (e) {
                  if (e.data === 0 && socket && state.currentYtTrackId) {
                    socket.emit('table:youtubeEnded', { trackId: state.currentYtTrackId });
                  }
                }
              }
            });
          }
        }
      }

      var ytQueueHtml = '';
      for (var q = 0; q < ytQueue.length; q++) {
        var item = ytQueue[q];
        var isCurr = q === 0;
        var prefix = isCurr ? '🎶 [กำลังเล่น] ' : '⏳ [คิวที่ ' + q + '] ';
        ytQueueHtml +=
          '<div class="yt-queue-item ' + (isCurr ? 'is-active' : '') + '">' +
          '<span>' + prefix + item.title + '</span>' +
          '<small>โดย ' + item.addedBy + '</small>' +
          '</div>';
      }
      document.getElementById('yt-queue-list').innerHTML = ytQueueHtml;
    }

    function renderTableRoom() {
      var table = state.table;
      document.getElementById('display-table-title').textContent = table.title;

      var badges = [
        '<button type="button" class="badge" data-action="copy-room-code">รหัส: ' + table.code + ' 📋</button>'
      ];
      if (table.mode === 'venting') badges.push('<span class="badge">ระบาย</span>');
      badges.push('<span class="badge">' + table.members.length + '/' + table.maxSeats + ' คน</span>');
      if (table.mode !== 'venting') badges.push('<span class="badge">' + table.ageGroup + '</span>');
      if (table.mode === 'group') {
        var checkedAttr = table.isPublic ? 'checked' : '';
        var pubLabel = table.isPublic ? 'โต๊ะเปิด' : 'โต๊ะปิด';
        badges.push('<label class="badge"><input type="checkbox" id="toggle-table-public" ' + checkedAttr + ' /> ' + pubLabel + '</label>');
      }
      document.getElementById('table-badges-container').innerHTML = badges.join('');

      var stageBox = document.getElementById('table-stage-box');
      if (table.mode === 'music') {
        renderMusicRoomPlayer(table);
        document.getElementById('room-right-stage-slot').appendChild(stageBox);
      } else {
        document.getElementById('music-room-youtube-box').hidden = true;
        destroyYoutubePlayer();
        document.getElementById('room-left-stage-slot').appendChild(stageBox);
      }

      var seatsHtml = '';
      for (var i = 0; i < table.maxSeats; i++) {
        var member = table.members[i];
        if (!member) {
          seatsHtml += '<div class="seat-slot"><div class="empty-barstool"></div></div>';
        } else {
          var roleBadge = member.isBartender ? ' 🍸' : '';
          var statusLine = member.isBartender
            ? '<small style="color:#6f52ad; font-weight:600;">(บาร์เทนเดอร์ / ผู้รับฟัง)</small>'
            : '<small>(' + member.mood.label + ')</small>';
          var bubbleText = member.latestBubble || 'กำลังนั่งฟัง...';
          seatsHtml += '<div class="seat-slot">' +
            '<div class="bubble">' + bubbleText + '</div>' +
            '<div class="mochi-character">' + buildMochiSVG(member.mood, member.hatId || 'beret', member.isBartender) + '</div>' +
            '<div><strong>' + member.name + roleBadge + '</strong></div>' +
            statusLine +
            '</div>';
        }
      }
      document.getElementById('table-seats-container').innerHTML = seatsHtml;

      var banner = document.getElementById('waiting-listener-banner');
      if (table.isWaitingListener) {
        banner.hidden = false;
        banner.innerHTML = '<p>🕒 กำลังจับคู่กับอาสาบาร์เทนเดอร์ที่ออนไลน์อยู่... เมื่อมีคนเข้าโต๊ะจะแจ้งเตือนทันที</p>';
      } else if (table.mode === 'one-on-one' && table.members.length < 2) {
        banner.hidden = false;
        banner.innerHTML = '<p>🔍 กำลังรอเพื่อนร่วมโต๊ะ (One on One) เข้ามาจับคู่... หรือส่งรหัส <strong>' + table.code + '</strong> ให้เพื่อนจอยได้เลย</p>';
      } else {
        banner.hidden = true;
      }

      var chatList = document.getElementById('chat-messages-list');
      var chatHtml = '';
      for (var j = 0; j < table.messages.length; j++) {
        var m = table.messages[j];
        if (m.type === 'system') {
          chatHtml += '<div class="msg-system">' + m.text + '</div>';
        } else {
          chatHtml += '<div class="msg-user"><strong>' + m.senderName + ':</strong> ' + m.text + '</div>';
        }
      }
      chatList.innerHTML = chatHtml;
      chatList.scrollTop = chatList.scrollHeight;

      var playlistEl = document.getElementById('jukebox-playlist');
      var jukeboxHeading = document.getElementById('jukebox-tab-heading');
      var jukeboxInput = document.getElementById('input-song-name');
      var songHtml = '';

      if (table.mode === 'music') {
        jukeboxHeading.textContent = 'คิวเพลง YouTube';
        jukeboxInput.placeholder = 'วางลิงก์ YouTube เพื่อเพิ่มเข้าคิว...';
        var ytList = table.youtubeQueue || [];
        for (var y = 0; y < ytList.length; y++) {
          var tag = y === 0 ? '🎶 [กำลังเล่น] ' : '⏳ ';
          songHtml += '<li>' + tag + ytList[y].title + ' <small>(โดย ' + ytList[y].addedBy + ')</small></li>';
        }
      } else {
        jukeboxHeading.textContent = 'รายการเพลงในโต๊ะ';
        jukeboxInput.placeholder = 'พิมพ์ชื่อเพลงหรือวางลิงก์...';
        var songs = table.playlist || [];
        for (var k = 0; k < songs.length; k++) {
          songHtml += '<li>🎵 ' + songs[k] + '</li>';
        }
      }
      playlistEl.innerHTML = songHtml;
    }

    document.addEventListener('DOMContentLoaded', init);
  </script>
</body>
</html>`;

app.get('/', (req, res) => {
  res.send(HTML_PAGE);
});

server.listen(PORT, () => {
  console.log(`🍸 SPU VISUAL BAR is running at http://localhost:${PORT}`);
});