# 📚 Documentation Technique — ChatApp nGomna

**Version:** 1.0.0 | **Dernière mise à jour:** 17 Juin 2026 | **Statut:** Production

---

## 🚀 Introduction & Objectif

### Vue d'ensemble

**ChatApp nGomna** est une **plateforme de messagerie en temps réel** basée sur une **architecture microservices**, conçue pour supporter :

- **Conversations privées**, **groupes** et **canaux de diffusion**
- **Transfert de fichiers** optimisé avec **chunks** et **MinIO** (S3-compatible)
- **Crypto de bout en bout (E2EE)** pour les conversations sensibles
- **Présence en temps réel** et **indicateurs de frappe**
- **Résilience distribuée** via **Redis Streams** et **Circuit Breakers**
- **Intégration multiplateforme** (Flask, Flutter, Web)

### Problème résolu

Le projet répond aux défis suivants :

- **Scalabilité horizontale** : Architecture microservices découplée avec Redis Streams au lieu de broadcasts directs
- **Fiabilité de livraison** : Garantie de livraison des messages via système de statuts (`DELIVERED`, `READ`, `EDITED`, `DELETED`)
- **Performance** : Cache distribué (Redis), compression, prewarmers intelligents, pagination
- **Sécurité** : Authentication JWT, rate limiting, E2EE, gestion des clés
- **Accessibilité** : Support multi-device, archivage conversationnel, recherche fulltext

---

## 🏗️ Architecture & Flux de données

### Modèle microservices

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                            │
│  (Flutter App, Web App, API External Clients)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
         HTTP + WebSocket over Socket.IO
                         │
         ┌───────────────▼──────────────────┐
         │      GATEWAY (Port 8000)          │
         │  • Route Proxy                    │
         │  • Rate Limiting Global           │
         │  • CORS & Security Headers        │
         │  • Load Balancing                 │
         └──┬─────────────┬─────────────┬────┘
            │             │             │
    ┌───────▼─┐    ┌──────▼──────┐   ┌─▼──────────────┐
    │ Auth    │    │Chat-File    │   │ Visibility     │
    │Service  │    │Service      │   │ Service        │
    │(8001)   │    │(8003)       │   │(Future)        │
    └────┬────┘    └──┬───┬──────┘   └────────────────┘
         │            │   │
         │            │   │
    ┌────▼────────────▼───▼──────────┐
    │    INFRASTRUCTURE LAYER        │
    │                                │
    │  ┌──────────────────────────┐  │
    │  │ MongoDB                  │  │
    │  │ • Messages               │  │
    │  │ • Conversations          │  │
    │  │ • Files                  │  │
    │  │ • Users (cache)          │  │
    │  └──────────────────────────┘  │
    │                                │
    │  ┌──────────────────────────┐  │
    │  │ Redis (distributed)      │  │
    │  │ • Streams (Events)       │  │
    │  │ • Caches (Hot Data)      │  │
    │  │ • Pub/Sub (Online Users) │  │
    │  └──────────────────────────┘  │
    │                                │
    │  ┌──────────────────────────┐  │
    │  │ MinIO (S3-compatible)    │  │
    │  │ • File Storage           │  │
    │  │ • Chunks                 │  │
    │  └──────────────────────────┘  │
    │                                │
    │  ┌──────────────────────────┐  │
    │  │ External Services        │  │
    │  │ • FFmpeg (Thumbnails)    │  │
    │  │ • ExifTool (Metadata)    │  │
    │  │ • E2EE (Crypto)          │  │
    └──┴──────────────────────────┘
```

### Services microservices

| Service                | Port  | Responsabilité                                          | Tech Stack                                 |
| ---------------------- | ----- | ------------------------------------------------------- | ------------------------------------------ |
| **Gateway**            | 8000  | Point d'entrée, proxy, authentification, rate limiting  | Express.js, http-proxy, helmet             |
| **Auth-User-Service**  | 8001  | Authentification JWT, gestion des utilisateurs, séeding | Node.js, Express, JWT, Redis               |
| **Chat-File-Service**  | 8003  | Cœur du chat, messages, fichiers, groupes, présence     | Node.js, Socket.IO, MongoDB, Redis Streams |
| **Visibility-Service** | (TBD) | Statut utilisateur, présence avancée                    | (Futur)                                    |

### Couches architecturales du Chat-File-Service

Le **Chat-File-Service** suit une **Clean Architecture** organisée par couches de responsabilité :

```
src/
├── interfaces/              # 🔌 Couche d'interface (I/O)
│   └── http/
│       ├── routes/          # Définitions des routes Express
│       │   ├── messageRoutes.js
│       │   ├── fileRoutes.js
│       │   ├── conversationRoutes.js
│       │   ├── groupRoutes.js
│       │   └── broadcastRoutes.js
│       ├── middleware/      # Middleware (auth, validation, cache)
│       │   ├── authMiddleware.js
│       │   ├── validationMiddleware.js
│       │   └── rateLimitMiddleware.js
│       └── (WebSocket: chatHandler.js)
│
├── application/             # 📦 Couche application (Use Cases & Controllers)
│   ├── controllers/         # HTTP Controllers
│   │   ├── MessageController.js
│   │   ├── FileController.js
│   │   ├── ConversationController.js
│   │   └── GroupController.js
│   ├── use-cases/          # Business Logic (Clean Arch)
│   │   ├── SendMessage.js
│   │   ├── GetMessages.js
│   │   ├── CreateGroup.js
│   │   ├── MarkMessageRead.js
│   │   ├── DeleteMessage.js
│   │   ├── ForwardMessage.js
│   │   ├── UploadFile.js
│   │   └── ... (voir USE_CASES_REFERENCE.md pour la liste exhaustive)
│   └── websocket/
│       └── chatHandler.js   # WebSocket Event Handler
│
├── domain/                  # 🎯 Couche domaine (Entities & Rules)
│   ├── entities/
│   │   ├── Message.js
│   │   ├── Conversation.js
│   │   ├── User.js
│   │   └── File.js
│   ├── repositories/        # Interfaces (contracts)
│   │   ├── IMessageRepository.js
│   │   └── IConversationRepository.js
│   └── rules/               # Logique métier
│
├── infrastructure/          # 🛠️ Couche infrastructure (External services)
│   ├── mongodb/
│   │   ├── connection.js
│   │   ├── models/
│   │   │   ├── Message.js
│   │   │   ├── Conversation.js
│   │   │   └── File.js
│   │   └── indexes.js
│   ├── redis/
│   │   ├── redisConfig.js
│   │   └── RedisManager.js (shared)
│   ├── repositories/        # Implémentations concrètes
│   │   ├── MongoMessageRepository.js
│   │   ├── CachedMessageRepository.js
│   │   ├── MongoConversationRepository.js
│   │   └── CachedConversationRepository.js
│   └── services/            # Services techniques
│       ├── FileStorageService.js (MinIO)
│       ├── ThumbnailService.js (FFmpeg)
│       ├── ChunkedUploadService.js
│       ├── EncryptionService.js (E2EE)
│       ├── KeyManagementService.js
│       ├── ResilientMessageService.js (Streams)
│       ├── MessageDeliveryService.js (Consumer Redis)
│       ├── TypingIndicatorService.js
│       ├── UserCacheService.js
│       ├── SmartCachePrewarmer.js
│       └── AutoGroupSyncService.js
│
└── config/
    └── envValidator.js      # Validation des variables d'env
```

### Flux de communication — Cycle de vie d'un message

```
ÉMETTEUR (Client A)
        │
        ├─► WebSocket: "sendMessage" event
        │
CHAT-FILE-SERVICE
        │
        ├─► chatHandler.handleSendMessage()
        │
        ├─► SendMessage Use Case execute()
        │   ├─► Validation (contenu, conversationId)
        │   ├─► Save MongoDB (Message document)
        │   ├─► WAL (Write-Ahead Log) Redis
        │   ├─► Publish Redis Stream "chat:stream:messages:*"
        │   └─► ACK: "message_sent" → Client A
        │
        ├─► ResilientMessageService (senderSocketId = socket.id)
        │   └─► Stream publie: { messageId, content, senderSocketId, ... }
        │
REDIS INFRASTRUCTURE
        │
        ├─► Redis Stream "chat:stream:messages:private" (ou group/channel)
        │
        ├─► MessageDeliveryService (Consumer)
        │   ├─► Lit stream avec consumer group
        │   ├─► Récupère senderSocketId
        │   ├─► EXCLUT socket.id exact (pas tous les sockets de l'userId)
        │   └─► Émet à TOUS les autres participants
        │
DESTINATAIRES (Sockets excepté A)
        │
        └─► "newMessage" event (privé)
            ou "message:group" / "message:channel" (groupes)
```

### Redis Streams — Architecture distribuée

```
Redis Streams (Résilience & Scalabilité)
│
├─ chat:stream:messages:private
│  ├─ Consumer: MessageDeliveryService
│  └─ Format: { messageId, conversationId, content, type, senderSocketId, ... }
│
├─ chat:stream:messages:group
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:messages:channel
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:events:typing
│  └─ Consumer: TypingIndicatorService (debounce 1s, timeout 10s)
│
├─ chat:stream:statusDelivered
│  ├─ Consumer: MessageDeliveryService
│  └─ ACK: Expéditeur reçoit "message:status" DELIVERED
│
├─ chat:stream:statusRead
│  ├─ Consumer: MessageDeliveryService
│  ├─ Format: { messageId, conversationId, userId, status: "READ" }
│  └─ Bulk support: { isBulk: true, messageIds: [...], ... }
│
├─ chat:stream:statusEdited
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:statusDeleted
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:reactions
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:replies
│  └─ Consumer: MessageDeliveryService
│
├─ chat:stream:conversationCreated
│  └─ Consumer: MessageDeliveryService (MDS)
│
├─ chat:stream:participantAdded
│  └─ Consumer: MessageDeliveryService
│
└─ chat:stream:calls
   └─ Consumer: MessageDeliveryService
```

### Redis Keys (Caching & State)

```
Cache Hot Data
├─ chat:message:{messageId}              # Message object (TTL: 1h)
├─ chat:conversation:{conversationId}    # Conversation object (TTL: 30m)
├─ chat:user:{userId}                    # User metadata (TTL: 1h)
├─ chat:messages:conversation:{convId}   # Messages pagination cache
├─ chat:unread:{userId}                  # Unread count (TTL: 5m)
└─ chat:typing:{conversationId}          # Typing indicators (ephemeral)

Online Presence
├─ chat:online:users                     # Set of online user IDs
├─ chat:online:user:{userId}             # Socket IDs of user
├─ chat:presence:{conversationId}        # Users in conversation
└─ chat:room:{conversationId}            # Socket.IO room members
```

---

## 📦 Prérequis & Installation

### Prérequis système

```bash
# Version minimales requises
Node.js       >= 18.x    (testé avec 18.19.0 LTS)
npm           >= 8.x
Docker        >= 20.x    (pour les services d'infrastructure)
Git           >= 2.x
```

### Dépendances services

| Service | Technologie           | Port        | Containerisé     |
| ------- | --------------------- | ----------- | ---------------- |
| MongoDB | NoSQL DB              | 27017       | ✅ Docker        |
| Redis   | Cache & Streams       | 6379        | ✅ Docker        |
| MinIO   | S3-compatible Storage | 9000/9001   | ✅ Docker        |
| FFmpeg  | Media Processing      | N/A (local) | ❌ Local install |
| ExifTool| Image Metadata (EXIF) | N/A (local) | ❌ Local install |

### Installation locale

#### 1. Cloner le dépôt

```bash
git clone https://github.com/PapsonBoyRaphael/chatapp-ngomna.git
cd chatapp-ngomna
```

#### 2. Installer les dépendances

```bash
# Auth-User-Service
cd auth-user-service && npm install && cd ..

# Chat-File-Service
cd chat-file-service && npm install && cd ..

# Gateway
cd gateway && npm install && cd ..

# Shared modules
cd shared && npm install && cd ..
```

#### 3. Configurer les variables d'environnement

**`.env` principal** (racine du projet) :

```bash
NODE_ENV=development

# Gateway
GATEWAY_PORT=8000
GATEWAY_LOG_LEVEL=debug

# Auth-User-Service
AUTH_USER_SERVICE_PORT=8001
AUTH_USER_SERVICE_URL=http://localhost:8001

# Chat-File-Service
CHAT_FILE_SERVICE_PORT=8003
CHAT_FILE_SERVICE_URL=http://localhost:8003

# MongoDB
MONGODB_URI=mongodb://localhost:27017/chatdb
MONGODB_REPLICA_SET=                    # Optionnel pour production

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=                         # Optionnel
REDIS_DB=0

# MinIO (S3)
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=chat-files
S3_REGION=us-east-1

# JWT
JWT_SECRET=your-super-secret-jwt-key-change-in-prod
JWT_EXPIRY=15m
JWT_REFRESH_EXPIRY=7d

# E2EE (Encryption)
E2EE_ENABLED=false                      # Activer pour E2EE
E2EE_KEY_ALGORITHM=RSA
E2EE_KEY_SIZE=2048

# Rate Limiting
RATE_LIMIT_WINDOW_MS=900000             # 15 min
RATE_LIMIT_MAX_REQUESTS=1000            # 1000 req/15min
RATE_LIMIT_AUTH_MAX=10                  # 10 login attempts

# Upload
FILE_MAX_SIZE=104857600                 # 100 MB
CHUNK_SIZE=5242880                      # 5 MB
TEMP_DIR=/tmp/chat-uploads

# FFmpeg (Thumbnails)
FFMPEG_PATH=/usr/bin/ffmpeg             # ou detecté automatiquement
THUMBNAIL_MAX_SIZE=200x200
```

**`auth-user-service/.env`** :

```bash
PORT=8001
NODE_ENV=development
MONGODB_URI=mongodb://localhost:27017/authdb
REDIS_HOST=localhost
REDIS_PORT=6379
JWT_SECRET=your-super-secret-jwt-key-change-in-prod
JWT_EXPIRY=15m
```

**`chat-file-service/.env`** :

```bash
PORT=8003
NODE_ENV=development
MONGODB_URI=mongodb://localhost:27017/chatdb
REDIS_HOST=localhost
REDIS_PORT=6379
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=chat-files
JWT_SECRET=your-super-secret-jwt-key-change-in-prod
```

**`gateway/.env`** :

```bash
PORT=8000
NODE_ENV=development
AUTH_SERVICE_URL=http://localhost:8001
CHAT_FILE_SERVICE_URL=http://localhost:8003
```

#### 4. Démarrer l'infrastructure (Docker)

```bash
# Via docker-compose existant
docker-compose up -d mongodb redis minio

# Vérifier les services
docker-compose ps

# Logs
docker-compose logs -f mongodb
docker-compose logs -f redis
```

#### 5. Initialiser les données (optionnel)

```bash
# Seed utilisateurs test
cd auth-user-service
npm run seed-users

# Vérifier la connexion
cd ../chat-file-service
npm run health    # Doit retourner 200 OK
```

#### 6. Démarrer les services

**Terminal 1 — Gateway** :

```bash
cd gateway
npm run dev      # nodemon auto-reload
# Écoute sur http://localhost:8000
```

**Terminal 2 — Auth-User-Service** :

```bash
cd auth-user-service
npm run dev
# Écoute sur http://localhost:8001
```

**Terminal 3 — Chat-File-Service** :

```bash
cd chat-file-service
npm run dev      # Socket.IO sur localhost:8003
# Écoute sur http://localhost:8003
```

#### 7. Vérifier le démarrage

```bash
# Health checks
curl -s http://localhost:8000/health | jq .
curl -s http://localhost:8001/health | jq .
curl -s http://localhost:8003/health | jq .

# Redis connectivity
redis-cli ping  # Doit répondre PONG

# MongoDB connectivity
mongosh "mongodb://localhost:27017/chatdb"
```

### Troubleshooting

| Problème                                      | Solution                                                             |
| --------------------------------------------- | -------------------------------------------------------------------- |
| `ECONNREFUSED 127.0.0.1:6379`                 | Redis n'est pas lancé : `docker-compose up -d redis`                 |
| `MongooseError: Cannot connect`               | MongoDB non disponible : `docker-compose up -d mongodb`              |
| `CORS policy errors`                          | Vérifier `gateway/app.js` : CORS est activé pour `*` en dev          |
| `undefined is not a function`                 | Vérifier que tous les services sont lancés (dépendances circulaires) |
| `Cannot find module '@chatapp-ngomna/shared'` | Exécuter `cd shared && npm install` dans chaque service              |

---

## ⚡ Utilisation & Exemples

### A. Authentication & Session

#### 1. Login (Auth-User-Service)

**Endpoint :** `POST /api/auth/login`

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "matricule": "EMP001",
    "password": "password123"
  }' | jq .
```

**Réponse (succès)** :

```json
{
  "success": true,
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "id": "507f1f77bcf86cd799439011",
    "matricule": "EMP001",
    "nom": "Raphaël",
    "prenom": "Boymond",
    "email": "raphael@example.com"
  }
}
```

#### 2. Valider Token

**Endpoint :** `POST /api/auth/validate`

```bash
curl -X POST http://localhost:8000/api/auth/validate \
  -H "Content-Type: application/json" \
  -d '{
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  }' | jq .
```

#### 3. Rafraîchir Access Token

**Endpoint :** `POST /api/auth/refresh`

```bash
curl -X POST http://localhost:8000/api/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{
    "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  }' | jq .
```

---

### B. Conversations & Messages

#### 1. Obtenir toutes les conversations

**Endpoint :** `GET /api/conversations`

```bash
curl -X GET "http://localhost:8000/api/conversations?page=1&limit=20" \
  -H "Authorization: Bearer {accessToken}" | jq .
```

**Réponse** :

```json
{
  "success": true,
  "conversations": [
    {
      "id": "507f1f77bcf86cd799439001",
      "name": "Conversation avec Alice",
      "type": "PRIVATE",
      "participants": [
        { "id": "507f1f77bcf86cd799439010", "nom": "Raphaël" },
        { "id": "507f1f77bcf86cd799439011", "nom": "Alice" }
      ],
      "lastMessage": {
        "id": "507f1f77bcf86cd799439100",
        "content": "À bientôt!",
        "senderId": "507f1f77bcf86cd799439011",
        "timestamp": "2026-06-17T14:30:00.000Z"
      },
      "unreadCount": 2,
      "createdAt": "2026-01-15T10:00:00.000Z",
      "updatedAt": "2026-06-17T14:30:00.000Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 5,
    "hasMore": false
  }
}
```

#### 2. Récupérer les messages d'une conversation

**Endpoint :** `GET /api/messages?conversationId={id}&page=1&limit=50`

```bash
curl -X GET "http://localhost:8000/api/messages?conversationId=507f1f77bcf86cd799439001&page=1&limit=50" \
  -H "Authorization: Bearer {accessToken}" | jq .
```

#### 3. Créer un groupe

**Endpoint :** `POST /api/groups`

```bash
curl -X POST http://localhost:8000/api/groups \
  -H "Authorization: Bearer {accessToken}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Équipe Backend",
    "type": "GROUP",
    "members": [
      "507f1f77bcf86cd799439010",
      "507f1f77bcf86cd799439011",
      "507f1f77bcf86cd799439012"
    ]
  }' | jq .
```

**Réponse** :

```json
{
  "success": true,
  "group": {
    "id": "507f1f77bcf86cd799439050",
    "name": "Équipe Backend",
    "type": "GROUP",
    "participants": [ { "id": "...", "nom": "..." }, ... ],
    "participantCount": 3,
    "createdBy": "507f1f77bcf86cd799439010",
    "createdAt": "2026-06-17T14:35:00.000Z"
  }
}
```

---

### C. Événements WebSocket (Socket.IO)

| Événement | Direction | Description | Format des données |
|---|---|---|---|
| `connect` | Client → Serveur | Connexion initiale au serveur WebSocket | `N/A` |
| `authenticate` | Client → Serveur | Authentification du socket avec JWT | `{ token, userId }` |
| `authenticated` | Serveur → Client | Confirmation de l'authentification réussie | `{ success, userId, matricule, nom, prenom, ... }` |
| `auth_error` | Serveur → Client | Erreur lors de l'authentification | `{ error, message }` |
| `sendMessage` | Client → Serveur | Envoi d'un nouveau message | `{ content, conversationId, type }` |
| `newMessage` | Serveur → Client | Réception d'un nouveau message | `{ messageId, conversationId, content, senderId, timestamp, ... }` |
| `markMessageRead` | Client → Serveur | Marquer un ou plusieurs messages comme lus | `{ messageId }` ou `{ conversationId, messageIds: [...] }` |
| `message:status` | Serveur → Client | Mise à jour du statut d'un message (`READ`, `EDITED`, `DELETED`) | `{ messageId, userId, status, ... }` |
| `typing` | Client → Serveur | Indique que l'utilisateur commence à taper | `{ conversationId }` |
| `stopTyping` | Client → Serveur | Indique que l'utilisateur a arrêté de taper | `{ conversationId }` |
| `typing:indicator` | Serveur → Client | Indique le statut de frappe d'un utilisateur | `{ conversationId, userId, status: "start"\|"refresh"\|"stop" }` |
| `editMessage` | Client → Serveur | Modification d'un message existant | `{ messageId, newContent }` |
| `deleteMessage` | Client → Serveur | Suppression d'un message existant | `{ messageId, deleteType: "EVERYONE"\|"SELF" }` |
| `addReaction` | Client → Serveur | Ajout d'une réaction (emoji) à un message | `{ messageId, emoji, conversationId }` |
| `message:reaction` | Serveur → Client | Réception d'une nouvelle réaction | `{ messageId, userId, reaction, action: "add"\|"remove" }` |
| `replyToMessage` | Client → Serveur | Réponse spécifique à un message parent | `{ messageId, content, conversationId }` |
| `forwardMessage` | Client → Serveur | Transfert d'un message vers d'autres conversations | `{ messageId, targetConversationIds: [...] }` |

*(Pour plus de détails sur le format complet des payloads, veuillez consulter la documentation [SOCKET_EVENTS_REFERENCE.md](SOCKET_EVENTS_REFERENCE.md))*

---

### D. Upload de fichiers

#### 1. Upload monolithique (< 100 MB)

**Endpoint :** `POST /api/files/upload`

```bash
# Avec curl
curl -X POST http://localhost:8000/api/files/upload \
  -H "Authorization: Bearer {accessToken}" \
  -F "file=@path/to/file.pdf" \
  -F "conversationId=507f1f77bcf86cd799439001" | jq .

# Réponse
{
  "success": true,
  "file": {
    "id": "507f1f77bcf86cd799439200",
    "name": "document.pdf",
    "size": 2048576,
    "mimeType": "application/pdf",
    "url": "http://localhost:9000/chat-files/507f1f77bcf86cd799439200/document.pdf",
    "uploadedBy": "507f1f77bcf86cd799439010",
    "uploadedAt": "2026-06-17T14:40:00.000Z"
  }
}
```

#### 2. Upload en chunks (> 100 MB)

**Étape 1 : Initier l'upload**

```bash
curl -X POST http://localhost:8000/api/files/chunk/init \
  -H "Authorization: Bearer {accessToken}" \
  -H "Content-Type: application/json" \
  -d '{
    "fileName": "large-video.mp4",
    "fileSize": 500000000,
    "mimeType": "video/mp4",
    "conversationId": "507f1f77bcf86cd799439001"
  }' | jq .

# Réponse
{
  "uploadId": "chunk_507f1f77bcf86cd799439200",
  "chunkSize": 5242880,                   # 5 MB
  "totalChunks": 96
}
```

**Étape 2 : Envoyer les chunks**

```bash
# Chunk 1
curl -X POST http://localhost:8000/api/files/chunk/upload \
  -H "Authorization: Bearer {accessToken}" \
  -F "file=@chunk_1.bin" \
  -F "uploadId=chunk_507f1f77bcf86cd799439200" \
  -F "chunkIndex=0" \
  -F "totalChunks=96" | jq .

# Réponse
{
  "success": true,
  "chunkIndex": 0,
  "receivedChunks": 1,
  "totalChunks": 96,
  "complete": false
}

# Chunk 96 (final)
curl -X POST http://localhost:8000/api/files/chunk/upload \
  -H "Authorization: Bearer {accessToken}" \
  -F "file=@chunk_96.bin" \
  -F "uploadId=chunk_507f1f77bcf86cd799439200" \
  -F "chunkIndex=95" \
  -F "totalChunks=96" | jq .

# Réponse (final)
{
  "success": true,
  "complete": true,
  "file": {
    "id": "507f1f77bcf86cd799439200",
    "name": "large-video.mp4",
    "size": 500000000,
    "url": "http://localhost:9000/chat-files/507f1f77bcf86cd799439200/large-video.mp4",
    "thumbnail": "http://localhost:9000/chat-files/507f1f77bcf86cd799439200/thumbnail.jpg"
  }
}
```

#### 3. Télécharger un fichier

**Endpoint :** `GET /api/files/{fileId}/download`

```bash
curl -X GET http://localhost:8000/api/files/507f1f77bcf86cd799439200/download \
  -H "Authorization: Bearer {accessToken}" \
  -o downloaded-file.pdf
```

#### 4. Supprimer un fichier

**Endpoint :** `DELETE /api/files/{fileId}`

```bash
curl -X DELETE http://localhost:8000/api/files/507f1f77bcf86cd799439200 \
  -H "Authorization: Bearer {accessToken}" | jq .
```

---

### E. Gestion des participants & groupes

#### 1. Ajouter des participants

**Endpoint :** `POST /api/conversations/{conversationId}/participants`

```bash
curl -X POST http://localhost:8000/api/conversations/507f1f77bcf86cd799439001/participants \
  -H "Authorization: Bearer {accessToken}" \
  -H "Content-Type: application/json" \
  -d '{
    "participantId": ["507f1f77bcf86cd799439013", "507f1f77bcf86cd799439014"]
  }' | jq .

# WebSocket ACK
socket.on('participant:added', (ack) => {
  console.log('✅ Participants ajoutés:', ack.participantIds);
});
```

#### 2. Retirer un participant

**Endpoint :** `DELETE /api/conversations/{conversationId}/participants/{participantId}`

```bash
curl -X DELETE http://localhost:8000/api/conversations/507f1f77bcf86cd799439001/participants/507f1f77bcf86cd799439013 \
  -H "Authorization: Bearer {accessToken}" | jq .
```

#### 3. Quitter une conversation

**Endpoint :** `POST /api/conversations/{conversationId}/leave`

```bash
curl -X POST http://localhost:8000/api/conversations/507f1f77bcf86cd799439001/leave \
  -H "Authorization: Bearer {accessToken}" | jq .
```

---

### F. Recherche & Archivage

#### 1. Rechercher des messages

**Endpoint :** `GET /api/messages/search?q=mot-clé&conversationId={id}`

```bash
curl -X GET "http://localhost:8000/api/messages/search?q=meeting&conversationId=507f1f77bcf86cd799439001" \
  -H "Authorization: Bearer {accessToken}" | jq .
```

#### 2. Archiver une conversation

**Endpoint :** `POST /api/conversations/{conversationId}/archive`

```bash
curl -X POST http://localhost:8000/api/conversations/507f1f77bcf86cd799439001/archive \
  -H "Authorization: Bearer {accessToken}" | jq .
```

#### 3. Récupérer les conversations archivées

**Endpoint :** `GET /api/conversations/archived?page=1&limit=20`

```bash
curl -X GET "http://localhost:8000/api/conversations/archived?page=1&limit=20" \
  -H "Authorization: Bearer {accessToken}" | jq .
```

---

## 🛠️ Normes de code & Contribution

### 1. Structure de nommage

#### Variables & Fonctions

```javascript
// ✅ CORRECT
const getUserById = async (userId) => {};
const messageCache = new Map();
const MAX_RETRY_ATTEMPTS = 3;
const isConversationActive = true;

// ❌ INCORRECT
const get_user_by_id = async (userId) => {}; // snake_case en JS
const MC = new Map(); // Acronyme court
const maxRetry = 3; // Pas constant
const active = true; // Trop vague
```

#### Fichiers & Modules

```
✅ CORRECT
- src/domain/entities/Message.js
- src/infrastructure/repositories/MongoMessageRepository.js
- src/application/use-cases/SendMessage.js
- src/interfaces/http/middleware/authMiddleware.js

❌ INCORRECT
- src/domain/message-entity.js              # Tiret au lieu de PascalCase
- src/repositories/mongo_message_repo.js    # snake_case
- src/SendMessage.js                        # Pas d'arborescence
```

### 2. Conventions de code

#### Classes & Services

```javascript
// ✅ CORRECT
class MessageRepository {
  async findById(id) {
    // Logique
  }

  async save(message) {
    // Logique
  }
}

module.exports = MessageRepository;

// ❌ INCORRECT
const MessageRepository = function () {}; // Pas de classe
class messageRepository {} // Pas de PascalCase
class MessageRepositoryService {} // Redondant (Service = pattern)
```

#### Use Cases (Clean Arch)

```javascript
// ✅ CORRECT
class SendMessage {
  constructor(messageRepository, conversationRepository, resilientService) {
    this.messageRepository = messageRepository;
    this.conversationRepository = conversationRepository;
    this.resilientService = resilientService;
  }

  async execute(data) {
    // 1. Validation
    if (!data.content || !data.conversationId) {
      throw new Error("Données invalides");
    }

    // 2. Logique métier
    const conversation = await this.conversationRepository.findById(
      data.conversationId,
    );
    if (!conversation) {
      throw new Error("Conversation introuvable");
    }

    // 3. Persistance
    const message = await this.messageRepository.save({
      content: data.content,
      conversationId: data.conversationId,
      senderId: data.senderId,
      createdAt: new Date(),
    });

    // 4. Effets secondaires (streams, cache, etc.)
    await this.resilientService.publishMessage(message);

    // 5. Retour
    return message;
  }
}

module.exports = SendMessage;
```

#### Middleware

```javascript
// ✅ CORRECT
const authMiddleware = (req, res, next) => {
  const token = req.headers.authorization?.split(" ")[1];

  if (!token) {
    return res.status(401).json({ error: "Token manquant" });
  }

  try {
    const decoded = jwtService.verifyToken(token);
    req.userId = decoded.userId;
    req.user = decoded;
    next();
  } catch (err) {
    res.status(401).json({ error: "Token invalide" });
  }
};

module.exports = authMiddleware;
```

### 3. Conventions de logging

```javascript
// ✅ CORRECT
console.log('✅ Service initialisé');        // Succès
console.error('❌ Erreur connexion DB');     // Erreur critique
console.warn('⚠️  Redis non disponible');    // Avertissement
console.log('🔌 WebSocket connecté');        // Info importante

// ❌ INCORRECT
console.log('ok')                             # Pas d'émoji, flou
console.log('[ERROR]')                       # Format inconsistant
console.log('Something went wrong')          # Pas de contexte
```

### 4. Gestion des erreurs

```javascript
// ✅ CORRECT
try {
  const message = await sendMessageUseCase.execute(data);
  res.status(200).json({ success: true, message });
} catch (error) {
  console.error('❌ Erreur SendMessage:', error.message);

  if (error.message.includes('Validation')) {
    return res.status(400).json({
      success: false,
      message: 'Données invalides',
      details: error.message
    });
  }

  if (error.message.includes('Introuvable')) {
    return res.status(404).json({
      success: false,
      message: 'Ressource introuvable'
    });
  }

  res.status(500).json({
    success: false,
    message: 'Erreur serveur interne',
    error: process.env.NODE_ENV === 'development' ? error.message : undefined
  });
}

// ❌ INCORRECT
res.status(500).json(error);                 # Exposer l'erreur complète
catch (e) { console.log('Error'); }          # Pas de détails
throw new Error('Something happened');       # Message vague
```

### 5. Principes de contribution

#### Workflow Git

```bash
# 1. Créer une branche depuis `develop`
git checkout develop
git pull origin develop
git checkout -b feature/add-emoji-reactions   # Nommer clairement

# 2. Committer régulièrement avec des messages clairs
git add .
git commit -m "feat: add emoji reactions to messages

- Implement addReaction use case
- Add reaction validation middleware
- Stream reactions via Redis Streams
- Tests included"

# 3. Push et ouvrir une PR
git push origin feature/add-emoji-reactions

# 4. Squash & merge après review
```

#### Commits

```
✅ CORRECT
feat: add file encryption support
fix: prevent duplicate message delivery
refactor: extract socket event handlers
docs: add WebSocket event reference
test: add message validation tests
perf: optimize conversation query with indexes

❌ INCORRECT
updated files
fix stuff
WIP: trying something
omg this finally works
```

### 6. Documentation du code

```javascript
/**
 * SendMessage Use Case
 *
 * Responsabilité: Valider, sauvegarder et distribuer un message
 *
 * @class
 * @example
 * const sendMessage = new SendMessage(repo, resilientService);
 * const result = await sendMessage.execute({
 *   content: "Hello",
 *   conversationId: "123"
 * });
 */
class SendMessage {
  /**
   * Crée une nouvelle instance
   * @param {MessageRepository} messageRepository - Repo persistence
   * @param {ResilientService} resilientService - Service distribution
   */
  constructor(messageRepository, resilientService) {}

  /**
   * Exécute le use case
   * @param {Object} data - Données du message
   * @param {string} data.content - Contenu du message (max 5000 chars)
   * @param {string} data.conversationId - ID conversation (ObjectId)
   * @param {string} data.senderId - ID de l'expéditeur
   * @returns {Promise<Message>} Message créé
   * @throws {ValidationError} Si données invalides
   * @throws {NotFoundError} Si conversation n'existe pas
   */
  async execute(data) {}
}
```

### 7. Tests

```javascript
// ✅ Structures de tests attendues
const SendMessage = require("../SendMessage");
const MockMessageRepository = require("./mocks/MockMessageRepository");

describe("SendMessage Use Case", () => {
  let sendMessage;
  let messageRepository;
  let resilientService;

  beforeEach(() => {
    messageRepository = new MockMessageRepository();
    resilientService = { publishMessage: jest.fn() };
    sendMessage = new SendMessage(messageRepository, resilientService);
  });

  describe("execute", () => {
    it("should create and distribute a message", async () => {
      const data = {
        content: "Hello",
        conversationId: "507f1f77bcf86cd799439001",
        senderId: "507f1f77bcf86cd799439010",
      };

      const result = await sendMessage.execute(data);

      expect(result).toHaveProperty("id");
      expect(result.content).toBe("Hello");
      expect(resilientService.publishMessage).toHaveBeenCalledWith(result);
    });

    it("should throw ValidationError for empty content", async () => {
      const data = {
        content: "",
        conversationId: "507f1f77bcf86cd799439001",
      };

      await expect(sendMessage.execute(data)).rejects.toThrow(ValidationError);
    });
  });
});
```

### 8. Checklist avant PR

- [ ] Code passe `npm run lint`
- [ ] Tests passent : `npm test`
- [ ] Pas de `console.log()` de debug
- [ ] Messages de commit clairs et en anglais
- [ ] Documentation mise à jour
- [ ] Variables d'env gérées dans `.env.example`
- [ ] Backward compatible (ou migration documentée)
- [ ] Pas de secrets/tokens hardcodés
- [ ] Performance: O(n) queries optimisées

---

## 📋 Guides spécialisés

### A. Architecture Multi-Device (senderSocketId)

**Problème :** Comment éviter que l'émetteur reçoive son propre message?

**Solution :** Utiliser `senderSocketId` dans Redis Streams

```javascript
// chatHandler.js
socket.on('sendMessage', (data) => {
  this.sendMessageUseCase.execute({
    ...data,
    senderSocketId: socket.id  // ⬅️ CLÉ: Passer le socket ID exact
  });
});

// ResilientMessageService
publishMessage(message, senderSocketId) {
  redis.xAdd('chat:stream:messages:group', '*', {
    messageId: message.id,
    content: message.content,
    senderSocketId: senderSocketId  // ⬅️ Stocker pour le MDS
    // ...
  });
}

// MessageDeliveryService (Consumer)
async deliverMessage(entry) {
  const { senderSocketId } = entry.data;

  // EXCLURE CE SOCKET EXACT (pas tous les sockets de l'userId)
  io.to(conversationRoom)
    .except(senderSocketId)  // ⬅️ Socket.IO natif: .except()
    .emit('message:group', message);
}
```

### B. Resilience Pattern — Circuit Breaker

```javascript
// CircuitBreaker usage
const circuitBreaker = new CircuitBreaker({
  failureThreshold: 5, // 5 erreurs
  resetTimeout: 60000, // 60s avant retry
  timeout: 5000, // 5s timeout max
});

try {
  await circuitBreaker.execute(async () => {
    return await mongoRepository.save(message);
  });
} catch (err) {
  if (err.message.includes("CIRCUIT_OPEN")) {
    console.error("⚠️  Circuit ouvert: fallback à cache");
    // Fallback: écrire en cache Redis temporaire
    await cacheService.set(`pending:${message.id}`, message);
  }
}
```

### C. E2EE — Encryption de bout en bout

```javascript
// Envoyer un message chiffré
const encryptedContent = await encryptionService.encrypt(
  content,
  conversationId,
  keyManagementService,
);

socket.emit("sendMessage", {
  content: encryptedContent,
  isEncrypted: true,
  conversationId,
});

// Recevoir et déchiffrer
socket.on("newMessage", async (message) => {
  if (message.isEncrypted) {
    const decrypted = await encryptionService.decrypt(
      message.content,
      message.conversationId,
      keyManagementService,
    );
    console.log("Contenu déchiffré:", decrypted);
  }
});
```

### D. Cache Prewarming — SmartCachePrewarmer

```javascript
// Préchauffer le cache au login
const prewarmer = new SmartCachePrewarmer(cacheService, repositories);

socket.on("authenticated", async (data) => {
  // 1. Charger les conversations actives
  // 2. Pré-charger les derniers messages
  // 3. Charger les métadonnées utilisateur
  await prewarmer.prewarmUserCache(userId, {
    conversationLimit: 20,
    messagesPerConversation: 50,
  });
});
```

### E. Typing Indicators — Debounce & Timeout

```javascript
// Architecture TypingIndicatorService
// Consumer dédié qui:
// 1. Lit de "chat:stream:events:typing"
// 2. Debounce 1s min entre chaque refresh du même user
// 3. Timeout 10s auto = "typing:stop"

socket.on("typing", (data) => {
  redis.xAdd("chat:stream:events:typing", "*", {
    userId: socket.userId,
    conversationId: data.conversationId,
    event: "typing:start",
  });
  // TypingIndicatorService broadcast "typing:indicator"
});
```

---

## 🐛 Troubleshooting avancé

### Redis Streams — Debug & Monitoring

```bash
# Vérifier les streams existants
redis-cli
> XINFO STREAM chat:stream:messages:group

# Voir les consumer groups
> XINFO GROUPS chat:stream:messages:group

# Lire les entries du stream
> XRANGE chat:stream:messages:group - +
> XLEN chat:stream:messages:group

# Nettoyer un stream (⚠️ ATTENTION: données perdues)
> DEL chat:stream:messages:group

# Voir les pending entries (messages non ACK)
> XPENDING chat:stream:messages:group delivery-service
```

### MongoDB — Indexes & Perf

```bash
# Lister les indexes
db.messages.getIndexes()

# Créer un index sur conversationId + timestamp
db.messages.createIndex({ conversationId: 1, createdAt: -1 })

# Analyser une query
db.messages.find({ conversationId: ObjectId("...") }).explain("executionStats")

# Nettoyer les collections obsolètes
db.messages.deleteMany({ createdAt: { $lt: ISODate("2024-01-01") } })
```

### Socket.IO — Debugging

```bash
# Activer debug mode
export DEBUG=socket.io:*
npm run dev

# Voir tous les événements Socket.IO émis/reçus
socket.onAny((eventName, ...args) => {
  console.log(`🔍 Event: ${eventName}`, args);
});

# Vérifier les rooms actuelles
io.of('/').adapter.rooms  // Server-side
```

---

## 📚 Ressources additionnelles

| Ressource             | Lien                                                                       | Description                                |
| --------------------- | -------------------------------------------------------------------------- | ------------------------------------------ |
| Socket.IO Reference   | [SOCKET_EVENTS_REFERENCE.md](SOCKET_EVENTS_REFERENCE.md)                   | Catalogue complet des événements WebSocket |
| Use-Cases Reference   | [USE_CASES_REFERENCE.md](USE_CASES_REFERENCE.md)                           | Liste exhaustive de tous les cas d'utilisation métier |
| Redis Keys Convention | [REDIS_KEYS_CONVENTION.md](REDIS_KEYS_CONVENTION.md)                       | Convention de nommage des clés Redis       |
| Redis Streams Guide   | [shared/REDIS_DOCUMENTATION.md](shared/REDIS_DOCUMENTATION.md)             | Architecture distribuée et patterns        |
| Deployment Guide      | [DEPLOYMENT.md](DEPLOYMENT.md)                                             | Instructions production & scaling          |
| API Routes            | `GET http://localhost:8003/`                                               | Health check & routes actuelles            |
| Architecture          | [SMART_CACHE_PREWARMER_INTEGRATION.md](SMARTCACHEPREWARMER_INTEGRATION.md) | Cache optimization strategy                |

---

## 📞 Support & Contribuer

Pour toute question ou bug report :

1. Consulter la [FAQ](#) ou les issues existantes
2. Créer une nouvelle issue avec contexte
3. Faire une PR selon la checklist de [Contribution](#normes-de-code--contribution)

**Équipe de maintenance :** DevOps, Backend, Architecture

---

**Version :** 1.0.0 | **Dernière mise à jour :** 17 Juin 2026 | **License :** ISC
