# 🔴 Documentation Shared Redis - Module Centralisé

## 📋 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Initialisation](#initialisation)
- [RedisFactory](#redisfactory)
- [RedisManager](#redismanager)
- [CacheService](#cacheservice)
- [OnlineUserManager](#onlineusemanager)
- [RoomManager](#roommanager)
- [UnreadMessageManager](#unreadmessagemanager)
- [Configuration](#configuration)
- [Patterns d'utilisation](#patterns-dutilisation)
- [Monitoring & Métriques](#monitoring--métriques)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Vue d'ensemble

Le module `shared/redis` centralise **TOUS** les accès Redis du projet:

```
┌─────────────────────────────────────────┐
│   chat-file-service                     │
│   auth-service                          │
│   gateway                               │
└────────────────────┬────────────────────┘
                     │ (import depuis shared)
                     ↓
        ┌────────────────────────┐
        │   shared/redis         │
        │  (Module Centralisé)   │
        └─────────┬──────────────┘
                  │
        ┌─────────┴────────────────────────┐
        │                                  │
    ┌───▼──────┐               ┌───────────▼──┐
    │ RedisFactory │           │ RedisManager │
    │ (Connexions) │           │ (Singleton)  │
    └──────────┘               └──────────────┘
                               │
        ┌──────────────────────┼──────────────────┬──────────────┐
        │                      │                  │              │
    ┌───▼────────┐    ┌────────▼──────┐   ┌──────▼─────┐   ┌───▼──────────┐
    │CacheService│    │OnlineUser     │   │RoomManager │   │UnreadMessage │
    │            │    │Manager        │   │            │   │Manager       │
    └────────────┘    └───────────────┘   └────────────┘   └──────────────┘
```

### Localisation

```
shared/
  redis/
    index.js                   # Export centralisé
    redisConfig.js            # Legacy wrapper
    RedisFactory.js           # ✅ SEUL avec require("redis")
    RedisManager.js           # Singleton principal
    managers/
      CacheService.js         # Cache Redis
      OnlineUserManager.js    # Utilisateurs online
      RoomManager.js          # Rooms/salles
      UnreadMessageManager.js # Messages non lus
    workers/                  # Workers de résilience
```

### Principe clé

✅ **Un seul endroit avec require("redis")**

- RedisFactory.js = SEUL fichier avec `require("redis")`
- Tous les autres fichiers utilisent RedisManager/RedisFactory
- Injection de dépendances centralisée

---

## 🏗️ Architecture

### Pattern: Singleton + Factory + Managers

```javascript
// 1. RedisFactory crée les clients Redis
const factory = new RedisFactory("service-name");
const client = await factory.getClient("main");

// 2. RedisManager est un Singleton global
const manager = new RedisManager();
await manager.connect();

// 3. Les Managers utilisent RedisManager
const cache = new CacheService();
await cache.initialize(RedisManager);
await cache.set("key", "value");
```

### Clients Redis

| Type       | Usage                         | Nbr instances |
| ---------- | ----------------------------- | ------------- |
| **main**   | Opérations CRUD, GET/SET      | 1             |
| **pub**    | Publisher Pub/Sub             | 1             |
| **sub**    | Subscriber Pub/Sub            | 1             |
| **stream** | Stream commands (XREAD, XADD) | 1             |
| **cache**  | Cache hit/miss optimized      | 1             |

### Intégration avec résilience

```
RedisManager
    ├─ StreamManager
    │   └─ Write-Ahead Log (WAL)
    │   └─ Fallback storage
    └─ CircuitBreaker
        └─ Fail-safe pattern
```

---

## 🚀 Initialisation

### Méthode recommandée (via RedisFactory)

```javascript
const { RedisFactory, RedisService } = require("shared/redis");

// 1. Créer une instance de service
const redisService = RedisFactory.createService("chat-service");

// 2. Connecter tous les clients
await redisService.connect();

// 3. Accéder aux clients
const mainClient = redisService.getMainClient();
const pubClient = redisService.getPubClient();
const subClient = redisService.getSubClient();
```

### Méthode legacy (via redisConfig)

```javascript
const redisConfig = require("shared/redis").redisConfig;

await redisConfig.connect();
const client = redisConfig.getClient();
```

### Initialiser avec Managers

```javascript
const {
  RedisManager,
  CacheService,
  OnlineUserManager,
} = require("shared/redis");

// 1. Connecter RedisManager (Singleton)
await RedisManager.connect();

// 2. Initialiser les managers
const cache = new CacheService();
await cache.initialize(RedisManager);

const onlineUsers = new OnlineUserManager();
await onlineUsers.initialize(RedisManager);

// Maintenant ready pour utilisation
await cache.set("key", "value");
```

---

## 🏭 RedisFactory

**Rôle**: Créer et gérer les clients Redis

### Fichier

`shared/redis/RedisFactory.js`

### Classe: RedisService

```javascript
class RedisService {
  constructor(serviceName, options = {}) {
    // Configuration par service
    this.serviceName = serviceName;    // "chat-service", "auth-service"
    this.config = { ...DEFAULT_CONFIG, ...options };
    this.clients = new Map();          // Stocke tous les clients
    this.isConnected = false;
    this.metrics = { ... };            // Tracking
  }
}
```

### Méthodes clés

#### `async getClient(type)`

Obtenir ou créer un client par type.

```javascript
const service = RedisFactory.createService("chat");
const mainClient = await service.getClient("main"); // Crée + connecte
const pubClient = await service.getClient("pub"); // 2e client
const subClient = await service.getClient("sub"); // 3e client
```

#### `getMainClient()`

Accès direct au client principal.

```javascript
const client = service.getMainClient(); // Synchrone, pas d'await
if (client) {
  const value = await client.get("key");
}
```

#### `getPubClient() / getSubClient()`

Accès pub/sub.

```javascript
const pub = service.getPubClient();
const sub = service.getSubClient();

await pub.publish("channel", "message");
await sub.subscribe("channel", (message) => {
  console.log("Reçu:", message);
});
```

#### `async connect()`

Connecter tous les clients.

```javascript
const service = new RedisService("my-service");
await service.connect(); // Crée et connecte main, pub, sub, stream, cache
```

#### `async disconnect()`

Fermer tous les clients.

```javascript
await service.disconnect();
```

#### `async getHealthStatus()`

Vérifier la santé de la connexion.

```javascript
const status = await service.getHealthStatus();
// "OK" ou "Disconnected" ou message d'erreur
```

### Configuration

```javascript
const DEFAULT_CONFIG = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
  db: parseInt(process.env.REDIS_DB) || 0,
  family: parseInt(process.env.REDIS_FAMILY) || 4,
  connectTimeout: parseInt(process.env.REDIS_CONNECTION_TIMEOUT) || 5000,
  maxRetriesPerRequest: 3,
  retryDelayOnFailover: 100,
};
```

### Métriques

```javascript
service.metrics = {
  clientsCreated: 0, // Nbr clients créés
  reconnections: 0, // Nbr reconnexions
  errors: 0, // Nbr erreurs
  lastConnectedAt: Date, // Dernière connexion réussie
  lastErrorAt: Date, // Dernière erreur
};
```

---

## 👑 RedisManager (Singleton)

**Rôle**: Gestionnaire centralisé principal du projet

### Fichier

`shared/redis/RedisManager.js`

### Instance Singleton

```javascript
// RedisManager est un Singleton (une seule instance dans le projet)
const RedisManager = require("shared/redis").RedisManager;

// À chaque appel, même instance
const mgr1 = new RedisManager();
const mgr2 = new RedisManager();
console.log(mgr1 === mgr2); // true ✅
```

### Clients gérés

```javascript
RedisManager.clients = {
  main: RedisClient, // Opérations CRUD
  pub: RedisClient, // Publisher
  sub: RedisClient, // Subscriber
  stream: RedisClient, // Streams
  cache: RedisClient, // Cache optimized
};
```

### Intégration résilience

```javascript
RedisManager.streamManager = StreamManager; // WAL + Fallback
RedisManager.circuitBreaker = CircuitBreaker; // Fail-safe
```

### Méthodes clés

#### `async connect()`

Connecter tous les clients et components de résilience.

```javascript
const manager = new RedisManager();
await manager.connect();

// Après: tous les clients connectés
// Streams prêts
// CircuitBreaker activé
```

#### `getMainClient()`

Accès au client principal.

```javascript
const client = manager.getMainClient();
const value = await client.get("key");
```

#### `getPubClient() / getSubClient()`

Accès Pub/Sub.

```javascript
const pub = manager.getPubClient();
const sub = manager.getSubClient();
```

#### `getStreamClient()`

Accès Streams.

```javascript
const stream = manager.getStreamClient();
await stream.xAdd("mystream", "*", "field", "value");
```

#### `getCacheClient()`

Accès cache optimisé.

```javascript
const cache = manager.getCacheClient();
await cache.get("key");
```

#### `async disconnect()`

Fermer tous les clients.

```javascript
await manager.disconnect();
```

#### `async getHealthStatus()`

Vérifier santé globale.

```javascript
const status = await manager.getHealthStatus();
console.log(status); // "OK", "DEGRADED", ou "Disconnected"
```

### Métriques

```javascript
manager.metrics = {
  connectionsCreated: 0,
  reconnections: 0,
  errors: 0,
  lastConnectedAt: Date,
  lastErrorAt: Date,
};
```

---

## 💾 CacheService

**Rôle**: Cache général Redis avec TTL et stratégies

### Fichier

`shared/redis/managers/CacheService.js`

### Initialisation

```javascript
const { CacheService, RedisManager } = require("shared/redis");

// Option 1: Via RedisManager
const cache = new CacheService({
  defaultTTL: 3600, // 1 heure
  keyPrefix: "chat", // Préfixe clés
  maxScanCount: 100,
});
await cache.initialize(RedisManager);

// Option 2: Avec client direct (compatibilité)
const cache = new CacheService();
const client = await redisService.getClient("cache");
cache.initializeWithClient(client);
```

### Opérations de base

#### `async set(key, value, ttl)`

Ajouter une valeur.

```javascript
// Basique
await cache.set("user:123", { name: "Alice", dept: "IT" });

// Avec TTL personnalisé
await cache.set("session:abc", tokenData, 1800); // 30 min

// JSON automatique
await cache.set("config", { debug: true, workers: 5 });
```

#### `async get(key)`

Récupérer une valeur.

```javascript
const user = await cache.get("user:123");
// Retourne l'objet désérialisé

const missing = await cache.get("nonexistent");
// Retourne null
```

#### `async del(key)`

Supprimer une clé.

```javascript
await cache.del("user:123");
```

#### `async exists(key)`

Vérifier l'existence.

```javascript
const found = await cache.exists("user:123");
// true ou false
```

#### `async renewTTL(key, ttl)`

Renouveler la durée de vie.

```javascript
// L'utilisateur a utilisé le cache récemment
// Garder les données un peu plus longtemps
await cache.renewTTL("user:123", 3600);
```

#### `async setMultiple(entries, ttl)`

Ajouter plusieurs clés.

```javascript
await cache.setMultiple(
  [
    { key: "user:1", value: userData1 },
    { key: "user:2", value: userData2 },
    { key: "user:3", value: userData3 },
  ],
  3600
);
```

#### `async getMultiple(keys)`

Récupérer plusieurs clés.

```javascript
const results = await cache.getMultiple(["user:1", "user:2", "user:3"]);
// [{key, value}, {key, value}, ...]
```

#### `async deletePattern(pattern)`

Supprimer par pattern.

```javascript
await cache.deletePattern("user:*"); // Toutes les clés user
await cache.deletePattern("session:*"); // Tous les sessions
```

#### `async flush()`

Vider tout le cache.

```javascript
await cache.flush();
```

#### `async keys(pattern)`

Lister les clés.

```javascript
const keys = await cache.keys("user:*");
console.log(keys); // ["user:1", "user:2", "user:3"]
```

### Exemples réels

**Cachage utilisateur**

```javascript
// Récupérer ou créer
let user = await cache.get("user:123");
if (!user) {
  user = await UserCacheService.getUserProfile(123);
  await cache.set("user:123", user, 86400); // 24h
}
```

**Cachage conversation**

```javascript
const convId = "507f1f77bcf86cd799439011";
let conv = await cache.get(`conv:${convId}`);
if (!conv) {
  conv = await ConversationRepository.findById(convId);
  await cache.set(`conv:${convId}`, conv, 3600); // 1h
}
```

---

## 👥 OnlineUserManager

**Rôle**: Tracker des utilisateurs en ligne en temps réel

### Fichier

`shared/redis/managers/OnlineUserManager.js`

### Initialisation

```javascript
const { OnlineUserManager, RedisManager } = require("shared/redis");

const onlineUsers = new OnlineUserManager(io, {
  presencePrefix: "presence",
  userDataPrefix: "user_data",
  userSocketPrefix: "user_sockets",
  defaultTTL: 300, // 5 min
  idleTTL: 3600, // 1 heure
});

await onlineUsers.initialize(RedisManager);
```

### Opérations clés

#### `async setUserOnline(userId, userData)`

Marquer utilisateur online.

```javascript
await onlineUsers.setUserOnline("507f1f77bcf86cd799439011", {
  socketId: "socket-123",
  matricule: "USER001",
  connectedAt: new Date(),
  lastActivity: new Date(),
});
```

#### `async setUserOffline(userId)`

Marquer utilisateur offline.

```javascript
await onlineUsers.setUserOffline("507f1f77bcf86cd799439011");
```

#### `async getOnlineUsers()`

Lister tous les users online.

```javascript
const users = await onlineUsers.getOnlineUsers();
// [{userId, socketId, matricule, status, connectedAt}]
```

#### `async isUserOnline(userId)`

Vérifier si online.

```javascript
const isOnline = await onlineUsers.isUserOnline("507f1f77bcf86cd799439011");
// true ou false
```

#### `async updateLastActivity(userId)`

Renouveler TTL (marquer utilisé).

```javascript
// Chaque action (message, typing, etc.)
await onlineUsers.updateLastActivity("507f1f77bcf86cd799439011");
```

#### `async getOnlineCount()`

Nombre total d'users online.

```javascript
const count = await onlineUsers.getOnlineCount();
console.log(`${count} utilisateurs en ligne`);
```

#### `async getPresenceStats()`

Statistiques complètes de présence.

```javascript
const stats = await onlineUsers.getPresenceStats();
// {
//   totalOnlineUsers: 150,
//   newConnectionsLastHour: 30,
//   averageSessionDuration: 1800,
//   peakOnlineUsers: 200,
//   statusDistribution: {online, away, idle}
// }
```

### Durée de vie

```
User online → 5 min TTL
  ↓ (user actif)
Renew TTL → 5 min additionnelles
  ↓ (inactif > 5 min)
Expire automatiquement → Offline
  ↓ (ou après 1 heure idle)
Archive → Historique présence
```

---

## 🎪 RoomManager

**Rôle**: Gérer les rooms/salles de conversation avec présence

### Fichier

`shared/redis/managers/RoomManager.js`

### Initialisation

```javascript
const {
  RoomManager,
  OnlineUserManager,
  RedisManager,
} = require("shared/redis");

const rooms = new RoomManager(io, onlineUserManager, {
  roomPrefix: "rooms",
  roomUsersPrefix: "room_users",
  userRoomsPrefix: "user_rooms",
  defaultRoomTTL: 3600,
  idleRoomTTL: 7200,
  archivedRoomTTL: 86400,
});

await rooms.initialize(RedisManager);
```

### Opérations clés

#### `async createRoom(roomId, data)`

Créer une room.

```javascript
await rooms.createRoom("conv_507f...", {
  name: "Dev Team",
  type: "GROUP",
  createdAt: new Date(),
  metadata: { topic: "Développement" },
});
```

#### `async getRoomInfo(roomId)`

Récupérer infos de la room.

```javascript
const roomData = await rooms.getRoomInfo("conv_507f...");
// {id, name, type, createdAt, userCount, metadata}
```

#### `async joinRoom(roomId, userId)`

Ajouter utilisateur à la room.

```javascript
await rooms.joinRoom("conv_507f...", "user123");

// Tracking automatique
// ├─ room_users:conv_507f... = [user123, user456]
// └─ user_rooms:user123 = [conv_507f..., conv_abc...]
```

#### `async leaveRoom(roomId, userId)`

Retirer utilisateur de la room.

```javascript
await rooms.leaveRoom("conv_507f...", "user123");
```

#### `async getRoomUsers(roomId)`

Lister les users dans une room.

```javascript
const users = await rooms.getRoomUsers("conv_507f...");
// [userId1, userId2, userId3, ...]
```

#### `async getRoomOnlineUsers(roomId)`

Lister les users online dans une room.

```javascript
const onlineUsers = await rooms.getRoomOnlineUsers("conv_507f...");
// [userId1, userId2]

const stats = await rooms.getRoomOnlineUsersCount("conv_507f...");
// {onlineCount: 2, totalCount: 5}
```

#### `async getUserRoleInRoom(roomId, userId)`

Récupérer le rôle d'un user.

```javascript
const role = await rooms.getUserRoleInRoom("conv_507f...", "user123");
// "admin", "moderator", ou "member"
```

#### `async setUserRoleInRoom(roomId, userId, role)`

Définir le rôle d'un user.

```javascript
await rooms.setUserRoleInRoom("conv_507f...", "user123", "moderator");
```

#### `async getUserRooms(userId)`

Lister les rooms d'un user.

```javascript
const myRooms = await rooms.getUserRooms("user123");
// ["conv_507f...", "conv_abc...", "conv_def..."]
```

#### `async getRoomPeakMetrics(roomId)`

Métriques de pic pour une room.

```javascript
const peak = await rooms.getRoomPeakMetrics("conv_507f...");
// {
//   peakUsersCount: 5,
//   peakTime: Date,
//   averageActiveUsers: 3
// }
```

#### `async getRoomPresenceStats(roomId)`

Stats de présence.

```javascript
const stats = await rooms.getRoomPresenceStats("conv_507f...");
// {
//   roomId, onlineUsers, totalUsers,
//   users: [{userId, status, lastActivity}],
//   averageSessionDuration
// }
```

### Structures Redis

```redis
rooms:conv_507f...
  ├─ id: "conv_507f..."
  ├─ name: "Dev Team"
  ├─ type: "GROUP"
  └─ userCount: 5

room_users:conv_507f...
  └─ [user1, user2, user3, user4, user5]

user_rooms:user1
  └─ [conv_507f..., conv_abc...]

room_roles:conv_507f...
  ├─ user1: "admin"
  ├─ user2: "moderator"
  └─ user3: "member"
```

---

## 📬 UnreadMessageManager

**Rôle**: Gérer les compteurs de messages non lus

### Fichier

`shared/redis/managers/UnreadMessageManager.js`

### Initialisation

```javascript
const { UnreadMessageManager, RedisManager } = require("shared/redis");

const unread = new UnreadMessageManager({
  keyPrefix: "unread",
  userUnreadPrefix: "user_unread",
  conversationUnreadPrefix: "conversation_unread",
  defaultTTL: 3 * 24 * 3600, // 3 jours
});

await unread.initialize(RedisManager);

// Injecter les callbacks de recalcul
unread.setRecalculateFunction(async (convId, userId) => {
  return await MessageRepository.countUnread(convId, userId);
});

unread.setRecalculateTotalFunction(async (userId) => {
  return await MessageRepository.countUserTotalUnread(userId);
});
```

### Opérations clés

#### `async incrementUnread(conversationId, userId, count)`

Incrémenter compteur non lu.

```javascript
// Nouveau message arrives dans une conversation
await unread.incrementUnread("conv_507f...", "user123", 1);
```

#### `async decrementUnread(conversationId, userId, count)`

Décrémenter compteur.

```javascript
// User lit les messages
await unread.decrementUnread("conv_507f...", "user123", 3);
```

#### `async getConversationUnreadCount(conversationId, userId)`

Récupérer count pour une conversation.

```javascript
const count = await unread.getConversationUnreadCount(
  "conv_507f...",
  "user123"
);
// 5 (messages non lus)
```

#### `async getUserTotalUnread(userId)`

Total de tous les non lus d'un user.

```javascript
const total = await unread.getUserTotalUnread("user123");
// 15 (across all conversations)
```

#### `async markConversationRead(conversationId, userId)`

Marquer conversation comme lue.

```javascript
await unread.markConversationRead("conv_507f...", "user123");
// Remet le compteur à 0
```

#### `async recalculateUnread(conversationId, userId)`

Recalculer depuis la BD.

```javascript
// Si cache et BD sont désynchronisés
const actualCount = await unread.recalculateUnread("conv_507f...", "user123");
```

#### `async recalculateTotalUnread(userId)`

Recalculer total depuis la BD.

```javascript
const actualTotal = await unread.recalculateTotalUnread("user123");
```

### Pattern d'utilisation

**Réception message**

```javascript
const message = await sendMessage(...);

// Incrémenter pour tous les participants sauf sender
for (const recipientId of conversation.participants) {
  if (recipientId !== message.senderId) {
    await unread.incrementUnread(conversationId, recipientId, 1);
  }
}
```

**Lecture messages**

```javascript
// Marquer tous comme lus
await unread.markConversationRead(conversationId, userId);
```

---

## ⚙️ Configuration

### Variables d'environnement

```bash
# Connexion Redis
REDIS_HOST=localhost              # Défaut: localhost
REDIS_PORT=6379                   # Défaut: 6379
REDIS_PASSWORD=mypassword         # Défaut: undefined
REDIS_DB=0                         # Défaut: 0
REDIS_FAMILY=4                     # IPv4 ou 6

# Timeouts
REDIS_CONNECTION_TIMEOUT=5000      # 5 secondes
REDIS_MAX_RETRY_ATTEMPTS=3         # Nbr tentatives

# Modes
REDIS_KEEP_ALIVE=true              # Keep-alive socket
```

### Configuration par service

```javascript
const { RedisFactory } = require("shared/redis");

// Service 1: Chat avec cache agressif
const chatService = RedisFactory.createService("chat", {
  host: "redis-cache.internal",
  port: 6380,
  password: process.env.CACHE_PASSWORD,
  db: 1,
});

// Service 2: Auth avec TTL court
const authService = RedisFactory.createService("auth", {
  host: "redis-auth.internal",
  db: 0,
});
```

---

## 📚 Patterns d'utilisation

### Pattern 1 : Startup complet

```javascript
const express = require("express");
const { Server } = require("socket.io");
const {
  RedisManager,
  CacheService,
  OnlineUserManager,
  RoomManager,
  UnreadMessageManager,
} = require("shared/redis");

const app = express();
const io = new Server(app);

// Initialiser Redis centralement
async function setupRedis() {
  // 1. Connecter le manager
  await RedisManager.connect();
  console.log("✅ Redis connecté");

  // 2. Initialiser CacheService
  const cache = new CacheService();
  await cache.initialize(RedisManager);
  console.log("✅ Cache prêt");

  // 3. Initialiser OnlineUserManager
  const onlineUsers = new OnlineUserManager(io);
  await onlineUsers.initialize(RedisManager);
  console.log("✅ Online tracking prêt");

  // 4. Initialiser RoomManager
  const rooms = new RoomManager(io, onlineUsers);
  await rooms.initialize(RedisManager);
  console.log("✅ Rooms prêtes");

  // 5. Initialiser UnreadMessageManager
  const unread = new UnreadMessageManager();
  await unread.initialize(RedisManager);
  unread.setRecalculateFunction(MessageRepository.countUnread);
  console.log("✅ Unread tracking prêt");

  return { cache, onlineUsers, rooms, unread };
}

// Utiliser
const managers = await setupRedis();
```

### Pattern 2 : Injection dans ChatHandler

```javascript
class ChatHandler {
  constructor(
    io,
    // ... use cases ...
    cache,
    onlineUsers,
    rooms,
    unread
  ) {
    this.io = io;
    this.cache = cache;
    this.onlineUsers = onlineUsers;
    this.rooms = rooms;
    this.unread = unread;
  }

  async handleSendMessage(socket, data) {
    // Créer le message
    const message = await this.sendMessageUseCase.execute(data);

    // Mettre à jour unread
    await this.unread.incrementUnread(data.conversationId, data.receiverId, 1);

    // Émettre aux users online
    const onlineInRoom = await this.rooms.getRoomOnlineUsers(
      `conversation_${data.conversationId}`
    );

    for (const userId of onlineInRoom) {
      this.io.to(`user_${userId}`).emit("newMessage", message);
    }
  }
}
```

### Pattern 3 : Cache avec fallback

```javascript
async function getUserProfile(userId) {
  // 1. Essayer cache
  let user = await cache.get(`user:${userId}`);
  if (user) {
    console.log("✅ Cache hit");
    return user;
  }

  // 2. Fallback MongoDB
  console.log("📌 Cache miss, fetching from DB");
  user = await UserRepository.findById(userId);

  if (!user) {
    throw new Error("User not found");
  }

  // 3. Cacher pour prochainement
  await cache.set(`user:${userId}`, user, 86400);

  return user;
}
```

### Pattern 4 : Synchronisation présence

```javascript
// Dans ChatHandler authenticate
async handleAuthentication(socket, data) {
  const userId = data.userId;

  // Marquer online
  await this.onlineUsers.setUserOnline(userId, {
    socketId: socket.id,
    matricule: data.matricule,
    connectedAt: new Date(),
    lastActivity: new Date()
  });

  // Notifier les autres
  socket.broadcast.emit("user_online", { userId });
}

// Dans ChatHandler disconnect
async handleDisconnection(socket, reason) {
  const userId = socket.userId;

  // Marquer offline
  await this.onlineUsers.setUserOffline(userId);

  // Notifier les autres
  socket.broadcast.emit("user_offline", { userId });
}
```

---

## 📊 Monitoring & Métriques

### Vérifier la connexion

```javascript
const status = await RedisManager.getHealthStatus();
console.log(status);
// "OK" | "DEGRADED" | "Disconnected"
```

### Accéder aux métriques

```javascript
const metrics = RedisManager.metrics;
console.log({
  connectionsCreated: metrics.connectionsCreated,
  reconnections: metrics.reconnections,
  errors: metrics.errors,
  lastConnected: metrics.lastConnectedAt,
  lastError: metrics.lastErrorAt,
});
```

### Monitoring par client

```javascript
const client = RedisManager.getMainClient();

client.on("ready", () => console.log("Ready"));
client.on("error", (err) => console.error("Error:", err));
client.on("reconnecting", () => console.log("Reconnecting..."));
client.on("end", () => console.log("Disconnected"));
```

### Stats en temps réel

```javascript
// Users online
const onlineCount = await onlineUsers.getOnlineCount();
console.log(`${onlineCount} utilisateurs en ligne`);

// Rooms actives
const rooms = await roomManager.getAllRooms();
console.log(`${rooms.length} rooms actives`);

// Cache stats
const keys = await cache.keys("*");
console.log(`${keys.length} clés en cache`);
```

---

## 🚨 Troubleshooting

### Problème: Connexion Redis impossible

**Symptômes**

```
❌ Erreur Redis: connect ECONNREFUSED
```

**Solutions**

```bash
# 1. Vérifier Redis est lancé
redis-cli ping
# PONG

# 2. Vérifier les variables d'environnement
echo $REDIS_HOST
echo $REDIS_PORT

# 3. Vérifier la connectivité
telnet localhost 6379

# 4. Vérifier les logs Redis
tail -f /var/log/redis/redis-server.log
```

### Problème: Circuit Breaker ouvert

**Symptômes**

```
❌ Code: CIRCUIT_OPEN
```

**Solutions**

```javascript
// Vérifier l'état
console.log(RedisManager.circuitBreaker.state);
// "CLOSED" | "OPEN" | "HALF_OPEN"

// Attendre la récupération automatique
// ou forcer reset
RedisManager.circuitBreaker.reset();
```

### Problème: Clés en cache non mises à jour

**Symptômes**

```
Données anciennes renvoyées
```

**Solutions**

```javascript
// Option 1: Supprimer la clé
await cache.del("key");

// Option 2: Renouveler TTL
await cache.renewTTL("key", 3600);

// Option 3: Forcer recalcul
await unread.recalculateUnread(convId, userId);
```

### Problème: Mémoire Redis croissante

**Symptômes**

```
MEMORY USAGE croît continuellement
```

**Solutions**

```javascript
// 1. Vérifier les clés sans TTL
const keys = await RedisManager.getMainClient().keys("*");
// Ajouter TTL aux clés longues

// 2. Nettoyer les patterns obsolètes
await cache.deletePattern("old_prefix:*");

// 3. Configurer l'éviction
# Dans redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
```

---

## 📖 Ressources

- [Redis Documentation](https://redis.io/documentation)
- [node-redis Guide](https://github.com/redis/node-redis)
- [Redis Streams](https://redis.io/topics/streams)
- [Write-Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging)

---

**Dernière mise à jour** : 8 janvier 2026
**Version** : 1.0.0
**Auteur** : Équipe ChatApp NGOMNA
