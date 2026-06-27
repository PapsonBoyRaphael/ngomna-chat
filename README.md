# 📚 Chat-File-Service — Documentation Technique

**Version :** 1.0.0 | **Dernière mise à jour :** 20 Juin 2026 | **Port :** 8003

---

## 🚀 Vue d'ensemble

Le **Chat-File-Service** est le microservice central de la messagerie de **ChatApp nGomna**. Il orchestre la totalité de la logique de messagerie en temps réel, la gestion complète des fichiers, et la distribution d'événements à travers une architecture distribuée résiliente.

Ce service implémente une **Clean Architecture** stricte, découplant la logique métier des détails d'infrastructure, et s'appuie sur des **patterns de résilience** avancés (Circuit Breaker, WAL, DLQ, Retry exponentiel) pour garantir la livraison des messages même en cas de défaillance partielle de l'infrastructure.

### Responsabilités principales

- **Messagerie temps réel** : envoi, réception, édition, suppression, réactions, réponses, transfert
- **Gestion des fichiers** : upload monolithique et chunké (> 100 MB), download, miniatures, traitement multimédia
- **Distribution d'événements** : publication et consommation de 20+ streams Redis (messages, statuts, typing, notifications, conversations, appels)
- **Chiffrement E2EE** : chiffrement de bout en bout optionnel (AES-256-GCM + RSA-OAEP 4096)
- **Cache distribué** : cache multi-niveaux avec invalidation intelligente et pré-chauffage
- **Présence temps réel** : tracking des utilisateurs en ligne, indicateurs de frappe avec debounce

---

## 🛠️ Technologies & Dépendances

### Stack technique principale

| Catégorie | Technologie | Version | Rôle |
|---|---|---|---|
| **Runtime** | Node.js | ≥ 18.x | Environnement d'exécution serveur |
| **Framework HTTP** | Express.js | 4.21+ | Serveur REST, middleware, routing |
| **WebSocket** | Socket.IO | 4.7+ | Communication bidirectionnelle temps réel |
| **Base de données** | MongoDB / Mongoose | 8.13+ | Persistance des messages, conversations, fichiers |
| **Cache & Streams** | Redis (node-redis) | 4.6+ | Cache distribué, Pub/Sub, Streams pour l'event-driven |
| **Stockage objet** | MinIO (S3-compatible) | 8.0+ | Stockage des fichiers uploadés (via `minio` SDK) |
| **Architecture** | Clean Architecture + DDD | — | Séparation des responsabilités en couches |

### Dépendances NPM détaillées

#### Communication & Transport

| Package | Rôle dans le service |
|---|---|
| `socket.io` | Serveur WebSocket pour la messagerie temps réel |
| `@socket.io/redis-adapter` | Adapter Redis pour la scalabilité multi-instances Socket.IO |
| `axios` | Client HTTP pour la communication inter-services (auth-service, sync, prewarmer) |
| `cors` | Middleware CORS pour les requêtes cross-origin |
| `cookie-parser` | Parsing des cookies pour l'authentification |

#### Traitement multimédia

| Package | Rôle dans le service |
|---|---|
| `sharp` | Traitement d'images : redimensionnement, conversion WebP, miniatures |
| `fluent-ffmpeg` + `ffmpeg-static` | Manipulation vidéo/audio : extraction de miniatures, compression, métadonnées |
| `music-metadata` | Extraction des métadonnées audio (durée, artiste, album, bitrate) |
| `pdf-parse` | Extraction du texte et des métadonnées des documents PDF |
| `mime-types` | Détection et validation des types MIME des fichiers |
| `multer` | Middleware de gestion d'upload de fichiers multipart/form-data |

#### Stockage & Système de fichiers

| Package | Rôle dans le service |
|---|---|
| `minio` | Client SDK MinIO/S3 pour le stockage objet (upload, download, URL signées) |
| `ssh2-sftp-client` | Client SFTP pour le stockage distant en production (backend alternatif) |
| `fs-extra` | Opérations filesystem étendues (copie récursive, suppression, ensureDir) |
| `archiver` | Création d'archives ZIP pour le téléchargement groupé de fichiers |
| `uuid` | Génération d'identifiants uniques pour les chunks d'upload et les miniatures |

#### Sécurité & Authentification

| Package | Rôle dans le service |
|---|---|
| `jsonwebtoken` | Vérification et décodage des JWT pour l'authentification |
| `sanitize-html` | Nettoyage du contenu HTML pour prévenir les attaques XSS |
| `validator` | Validation des données d'entrée (email, URL, longueur de chaînes) |
| `express-rate-limit` | Rate limiting par IP/route pour la protection contre les abus |
| `crypto` (natif) | Chiffrement AES-256-GCM, RSA-OAEP 4096, hashing SHA-256 |

#### Infrastructure & Résilience

| Package | Rôle dans le service |
|---|---|
| `redis` | Client Redis v4 pour cache, Pub/Sub et Streams |
| `mongoose` | ODM MongoDB avec schémas, indexes et middleware |
| `@chatapp-ngomna/shared` | Module partagé : CircuitBreaker, StreamManager, WorkerManager, RedisManager, UserCache |
| `dotenv` | Chargement des variables d'environnement depuis `.env` |
| `chalk` | Coloration des logs en console pour le debugging |

#### Outils système (non-NPM)

| Outil | Rôle | Installation |
|---|---|---|
| **FFmpeg** | Binaire requis par `fluent-ffmpeg` pour le traitement vidéo/audio | `apt install ffmpeg` ou fourni par `ffmpeg-static` |
| **ExifTool** | Extraction des métadonnées complexes (EXIF, GPS) des images | `apt install libimage-exiftool-perl` ou `apk add exiftool` |

---

## 🏗️ Architecture générale

### Principes architecturaux appliqués

Le Chat-File-Service applique les principes suivants :

1. **Clean Architecture** (Robert C. Martin) — Le code est organisé en couches concentriques où les dépendances pointent toujours vers l'intérieur. Le domaine ne connaît ni Express, ni MongoDB, ni Redis.

2. **Domain-Driven Design (DDD)** — Les entités métier (`Message`, `Conversation`, `File`, `Event`) encapsulent leurs règles de validation et leurs invariants.

3. **Repository Pattern** — L'accès aux données est abstrait derrière des interfaces. Les implémentations concrètes (MongoDB) sont décorées par des couches de cache (Redis).

4. **Dependency Injection** — Toutes les dépendances sont injectées au démarrage via le fichier `index.js`, qui agit comme **Composition Root**. Aucun service ne crée ses propres dépendances.

5. **Event-Driven Architecture** — La communication entre les composants internes utilise Redis Streams comme bus d'événements, avec des consumer groups pour la distribution fiable.

6. **Resilience Patterns** — Circuit Breaker, Write-Ahead Log (WAL), Dead Letter Queue (DLQ), Retry avec backoff exponentiel, et Fallback Redis.

### Diagramme des couches

```
┌─────────────────────────────────────────────────────────────────────┐
│                      INTERFACES (I/O)                               │
│  Routes HTTP (Express)  │  WebSocket (Socket.IO)  │  Middleware     │
│  messageRoutes.js       │  chatHandler.js          │  authMiddleware │
│  fileRoutes.js          │                          │  validation     │
│  conversationRoutes.js  │                          │  rateLimit      │
│  groupRoutes.js         │                          │                 │
│  broadcastRoutes.js     │                          │                 │
│  healthRoutes.js        │                          │                 │
├─────────────────────────┴──────────────────────────┴─────────────────┤
│                      APPLICATION (Use Cases)                         │
│  Controllers                    │  Use Cases (Business Logic)        │
│  MessageController.js           │  SendMessage.js                    │
│  FileController.js              │  GetMessages.js                    │
│  ConversationController.js      │  UploadFile.js                     │
│  GroupController.js             │  CreateGroup.js                    │
│  HealthController.js            │  MarkMessageRead.js                │
│                                 │  DeleteMessage.js                  │
│                                 │  ForwardMessage.js                 │
│                                 │  AddReaction.js                    │
│                                 │  ... (30 use cases)                │
├─────────────────────────────────┴────────────────────────────────────┤
│                        DOMAIN (Entities)                             │
│  Message.js    │  Conversation.js    │  File.js    │  Event.js       │
│  • validate()  │  • addParticipant() │  • validate │  • types        │
│  • markAsRead  │  • archive()        │  • metadata │  • payloads     │
│  • reactions   │  • settings         │  • checksum │                 │
│  • edit()      │  • unreadCounts     │             │                 │
├────────────────┴─────────────────────┴─────────────┴─────────────────┤
│                    INFRASTRUCTURE (External)                         │
│  MongoDB         │  Redis            │  Services techniques          │
│  ├─ connection   │  ├─ redisConfig   │  ├─ ResilientMessageService   │
│  ├─ models/      │  ├─ CacheService  │  ├─ MessageDeliveryService    │
│  │  ├─ Message   │  ├─ OnlineUser    │  ├─ TypingIndicatorService    │
│  │  ├─ Convers.  │  ├─ RoomManager   │  ├─ FileStorageService       │
│  │  ├─ File      │  └─ UnreadMsg     │  ├─ MediaProcessingService   │
│  │  └─ UserKey   │                   │  ├─ ThumbnailService          │
│  │               │  Repositories     │  ├─ ChunkedUploadService      │
│  │               │  ├─ CachedMsg     │  ├─ EncryptionService         │
│  │               │  ├─ CachedConv    │  ├─ KeyManagementService      │
│  │               │  ├─ CachedFile    │  ├─ UserCacheService          │
│  │               │  ├─ MongoMsg      │  ├─ SmartCachePrewarmer       │
│  │               │  ├─ MongoConv     │  └─ AutoGroupSyncService      │
│  │               │  └─ MongoFile     │                               │
├──┴───────────────┴──────────────────┴────────────────────────────────┤
│                       CONFIG                                         │
│  envValidator.js  │  errorHandler.js  │  responseFormatter.js        │
└───────────────────┴───────────────────┴──────────────────────────────┘
```

### Arborescence complète des fichiers

```
chat-file-service/
├── src/
│   ├── index.js                          # 🎯 Composition Root — Point d'entrée et DI
│   │
│   ├── interfaces/                       # 🔌 Couche INTERFACES (I/O)
│   │   └── http/
│   │       ├── routes/
│   │       │   ├── messageRoutes.js      # Routes REST messages
│   │       │   ├── fileRoutes.js         # Routes REST fichiers (upload, download, chunk)
│   │       │   ├── conversationRoutes.js # Routes REST conversations
│   │       │   ├── groupRoutes.js        # Routes REST groupes
│   │       │   ├── broadcastRoutes.js    # Routes REST canaux de diffusion
│   │       │   └── healthRoutes.js       # Routes monitoring et health check
│   │       └── middleware/
│   │           ├── authMiddleware.js      # Vérification JWT + extraction userId
│   │           ├── validationMiddleware.js# Validation des payloads d'entrée
│   │           ├── rateLimitMiddleware.js # Limitation de débit par IP/route
│   │           └── index.js              # Export centralisé des middleware
│   │
│   ├── application/                      # 📦 Couche APPLICATION
│   │   ├── controllers/
│   │   │   ├── MessageController.js      # Orchestration requêtes messages
│   │   │   ├── FileController.js         # Orchestration upload/download/chunk
│   │   │   ├── ConversationController.js # Orchestration conversations
│   │   │   ├── GroupController.js        # Orchestration groupes
│   │   │   └── HealthController.js       # Exposition métriques et santé
│   │   ├── use-cases/                    # 30 Use Cases métier
│   │   │   ├── SendMessage.js            # Envoi de message (texte, fichier, E2EE)
│   │   │   ├── GetMessages.js            # Récupération paginée (cursor + page)
│   │   │   ├── GetConversations.js       # Liste des conversations avec lastMessage
│   │   │   ├── GetConversation.js        # Détail d'une conversation
│   │   │   ├── CreateGroup.js            # Création de groupe avec membres
│   │   │   ├── CreateBroadcast.js        # Création de canal de diffusion
│   │   │   ├── UploadFile.js             # Upload fichier avec traitement média
│   │   │   ├── DownloadFile.js           # Téléchargement depuis MinIO
│   │   │   ├── DeleteFile.js             # Suppression fichier + nettoyage stockage
│   │   │   ├── MarkMessageRead.js        # Marquage lu (unitaire + bulk)
│   │   │   ├── MarkMessageDelivered.js   # Marquage livré
│   │   │   ├── DeleteMessage.js          # Suppression (EVERYONE / SELF)
│   │   │   ├── UpdateMessageContent.js   # Édition du contenu
│   │   │   ├── ForwardMessage.js         # Transfert multi-conversations
│   │   │   ├── ReplyMessage.js           # Réponse à un message (threading)
│   │   │   ├── AddReaction.js            # Ajout emoji réaction
│   │   │   ├── RemoveReaction.js         # Suppression réaction
│   │   │   ├── AddParticipant.js         # Ajout membre à un groupe
│   │   │   ├── RemoveParticipant.js      # Retrait membre d'un groupe
│   │   │   ├── LeaveConversation.js      # Quitter une conversation
│   │   │   ├── AddAdmin.js              # Promouvoir un membre admin
│   │   │   ├── ArchiveConversation.js    # Archiver une conversation
│   │   │   ├── GetArchivedConversations.js # Conversations archivées
│   │   │   ├── SearchOccurrences.js      # Recherche fulltext dans les messages
│   │   │   ├── UpdateCallStatus.js       # Mise à jour statut appel
│   │   │   ├── AutoGroupSync.js          # Synchronisation auto des groupes
│   │   │   └── ...
│   │   └── websocket/
│   │       └── chatHandler.js            # 🔌 Gestionnaire Socket.IO (135 Ko)
│   │
│   ├── domain/                           # 🎯 Couche DOMAINE
│   │   └── entities/
│   │       ├── Message.js                # Entité Message (validation, sérialisation)
│   │       ├── Conversation.js           # Entité Conversation (participants, settings)
│   │       ├── File.js                   # Entité File (métadonnées, checksum)
│   │       ├── Event.js                  # Types d'événements système
│   │       └── index.js                  # Export centralisé
│   │
│   ├── infrastructure/                   # 🛠️ Couche INFRASTRUCTURE
│   │   ├── mongodb/
│   │   │   ├── connection.js             # Connexion Mongoose singleton
│   │   │   └── models/
│   │   │       ├── MessageModel.js       # Schéma Mongoose Message
│   │   │       ├── ConversationModel.js  # Schéma Mongoose Conversation
│   │   │       ├── FileModel.js          # Schéma Mongoose File
│   │   │       └── UserEncryptionKeyModel.js # Schéma clés publiques E2EE
│   │   ├── redis/
│   │   │   ├── redisConfig.js            # Configuration et connexion Redis
│   │   │   ├── CacheService.js           # Service de cache générique (TTL, patterns)
│   │   │   ├── OnlineUserManager.js      # Gestion des utilisateurs en ligne
│   │   │   ├── RoomManager.js            # Gestion des rooms Socket.IO
│   │   │   └── UnreadMessageManager.js   # Compteurs de messages non lus
│   │   ├── repositories/
│   │   │   ├── MongoMessageRepository.js       # Accès MongoDB messages
│   │   │   ├── MongoConversationRepository.js  # Accès MongoDB conversations
│   │   │   ├── MongoFileRepository.js          # Accès MongoDB fichiers
│   │   │   ├── CachedMessageRepository.js      # Décorateur cache Redis sur messages
│   │   │   ├── CachedConversationRepository.js # Décorateur cache Redis sur conversations
│   │   │   └── CachedFileRepository.js         # Décorateur cache Redis sur fichiers
│   │   ├── services/
│   │   │   ├── ResilientMessageService.js      # 🛡️ Résilience + publication multi-streams
│   │   │   ├── MessageDeliveryService.js       # 📬 Consumer multi-streams partitionné
│   │   │   ├── TypingIndicatorService.js       # ⌨️ Indicateurs de frappe (debounce/timeout)
│   │   │   ├── FileStorageService.js           # 📦 Stockage MinIO/SFTP + compression
│   │   │   ├── MediaProcessingService.js       # 🖼️ Traitement images/vidéos/audio/PDF
│   │   │   ├── ThumbnailService.js             # 🖼️ Génération miniatures (Sharp)
│   │   │   ├── ChunkedUploadService.js         # 📤 Upload par morceaux (> 100 MB)
│   │   │   ├── EncryptionService.js            # 🔐 Chiffrement E2EE (AES-256-GCM + RSA)
│   │   │   ├── KeyManagementService.js         # 🔑 Gestion clés publiques RSA
│   │   │   ├── UserCacheService.js             # 👤 Cache des profils utilisateurs
│   │   │   ├── SmartCachePrewarmer.js          # 🔥 Pré-chauffage intelligent du cache
│   │   │   └── AutoGroupSyncService.js         # 🔄 Synchronisation automatique des groupes
│   │   ├── kafka/                              # (Legacy, non utilisé en production)
│   │   │   ├── config/
│   │   │   ├── consumers/
│   │   │   └── producers/
│   │   └── index.js                            # Export centralisé infrastructure
│   │
│   └── config/
│       ├── envValidator.js               # Validation des variables d'environnement
│       ├── errorHandler.js               # Middleware de gestion d'erreurs global
│       └── responseFormatter.js          # Formatteur de réponses HTTP standardisé
│
├── public/                               # Interface web de test
├── uploads/                              # Stockage local temporaire
├── storage/                              # Répertoire de stockage persistant
├── logs/                                 # Logs applicatifs
├── scripts/                              # Scripts utilitaires
├── shared/                               # Lien symbolique vers @chatapp-ngomna/shared
├── Dockerfile                            # Image Docker de production
├── package.json                          # Dépendances et scripts NPM
└── .env                                  # Variables d'environnement
```

---

## 🔬 Architecture bloc par bloc

### 1. Point d'entrée — Composition Root (`index.js`)

**Concept :** Le fichier `index.js` implémente le pattern **Composition Root**. C'est le seul endroit du code qui connaît toutes les classes concrètes. Il est responsable de :

1. **Valider l'environnement** (`EnvironmentValidator`)
2. **Connecter l'infrastructure** (MongoDB, Redis)
3. **Instancier tous les services**, repositories et use cases
4. **Injecter les dépendances** via les constructeurs (pas de service locator, pas de singleton magique)
5. **Configurer Express et Socket.IO**
6. **Démarrer les workers de fond** (streams, cache prewarmer, nettoyage périodique)
7. **Gérer l'arrêt gracieux** (`SIGTERM`, `SIGINT`)

```
Séquence de démarrage :
  1. Validation env          → EnvironmentValidator
  2. Connexion MongoDB       → connectDB()
  3. Connexion Redis         → RedisManager.connect()
  4. CacheService            → Avec client Redis
  5. OnlineUserManager       → Avec Socket.IO + Redis
  6. RoomManager             → Avec OnlineUserManager
  7. UserCache               → Cache utilisateur centralisé
  8. UserStreamConsumer      → Écoute événements du auth-service
  9. MessageDeliveryService  → Consumer multi-streams
  10. FileStorageService     → Client MinIO + config SFTP
  11. MediaProcessingService → Sharp + FFmpeg + music-metadata + pdf-parse
  12. ChunkedUploadService   → Redis + FileStorage
  13. EncryptionService      → Mode none | e2ee
  14. KeyManagementService   → Redis cache + MongoDB persistance
  15. Repositories Mongo     → MongoMessage/Conversation/File
  16. Repositories Cached    → Décorateurs Redis autour des Mongo
  17. ResilientMessageService → Redis Streams + CircuitBreaker + Workers
  18. Use Cases (×30)        → Injection des repositories + services
  19. Controllers            → Injection des use cases
  20. Routes HTTP            → Binding controllers → Express routes
  21. ChatHandler            → Binding Socket.IO events → use cases
  22. Maintenance tasks      → setInterval nettoyage Redis
  23. Cache Prewarmer        → SmartCachePrewarmer.start() (non-bloquant)
```

---

### 2. Couche Interfaces (`interfaces/`)

**Concept :** Couche la plus externe de la Clean Architecture. Elle traduit les requêtes I/O (HTTP, WebSocket) en appels aux Controllers de la couche Application. Elle ne contient aucune logique métier.

#### 2.1. Routes HTTP (`interfaces/http/routes/`)

Chaque fichier de routes est une **factory function** qui reçoit un controller injecté et retourne un `Router` Express :

```javascript
// Pattern utilisé dans toutes les routes
const createMessageRoutes = (messageController) => {
  const router = express.Router();
  router.get("/", authMiddleware, (req, res) => messageController.getMessages(req, res));
  return router;
};
```

| Fichier | Préfixe | Responsabilité |
|---|---|---|
| `messageRoutes.js` | `/messages` | CRUD messages, recherche, pagination |
| `fileRoutes.js` | `/files` | Upload, download, chunk init/upload, suppression |
| `conversationRoutes.js` | `/conversations` | Liste, détail, archivage, participants |
| `groupRoutes.js` | `/groups` | Création groupe, ajout/retrait membres, admins |
| `broadcastRoutes.js` | `/broadcasts` | Création canaux de diffusion |
| `healthRoutes.js` | `/health` | Health checks simples et détaillés, métriques |

#### 2.2. Middleware (`interfaces/http/middleware/`)

| Middleware | Responsabilité |
|---|---|
| `authMiddleware.js` | Extrait le JWT du header `Authorization`, vérifie la signature, injecte `req.userId` et `req.user` |
| `validationMiddleware.js` | Valide les payloads JSON des requêtes (longueur contenu, format conversationId, types autorisés) |
| `rateLimitMiddleware.js` | Utilise `express-rate-limit` pour limiter les requêtes par IP (configurable via `.env`) |

#### 2.3. WebSocket Handler (`application/websocket/chatHandler.js`)

Le `ChatHandler` est le cœur temps réel du service. C'est un fichier de **135 Ko** qui gère :

- **Authentification socket** : vérification JWT à la connexion
- **Mapping userId → socketId(s)** : support multi-device natif
- **Dispatch des événements** : routing de 20+ événements Socket.IO vers les use cases correspondants
- **Exclusion du sender** : utilisation de `senderSocketId` pour éviter que l'émetteur reçoive son propre message

Le handler reçoit **tous ses use cases par injection** dans le constructeur (pas d'import direct) :

```javascript
const chatHandler = new ChatHandler(
  io,                          // Socket.IO Server
  sendMessageUseCase,          // Use case envoi
  getMessagesUseCase,          // Use case lecture
  // ... 25+ use cases injectés
  encryptionService,           // E2EE
  keyManagementService,        // Gestion clés
);
```

---

### 3. Couche Application (`application/`)

**Concept :** Contient les **Controllers** et les **Use Cases**. Les Controllers orchestrent le flux HTTP (validation, appel du use case, formatage de la réponse). Les Use Cases encapsulent une seule opération métier.

#### 3.1. Controllers

Les Controllers suivent un pattern strict :
1. Extraire les paramètres de la requête
2. Appeler le Use Case approprié
3. Formater et retourner la réponse
4. Gérer les erreurs avec des codes HTTP appropriés

Ils ne contiennent **aucune logique métier** — toute la logique est déléguée aux Use Cases.

| Controller | Use Cases injectés |
|---|---|
| `MessageController` | SendMessage, GetMessages, UpdateMessageStatus |
| `FileController` | UploadFile, GetFile, DownloadFile, ChunkedUploadService, EncryptionService |
| `ConversationController` | GetConversations, GetConversation, SearchOccurrences, ArchiveConversation |
| `GroupController` | CreateGroup, AddParticipant, RemoveParticipant, LeaveConversation, AddAdmin |
| `HealthController` | Accès direct Redis pour les métriques |

#### 3.2. Use Cases (30 cas d'utilisation)

Chaque Use Case est une classe avec une méthode `execute()` unique. C'est l'implémentation du **Command Pattern** adapté à la Clean Architecture.

**Principes :**
- **Single Responsibility** : un seul use case = une seule opération métier
- **Dépendances injectées** : repositories et services reçus via le constructeur
- **Indépendant du framework** : aucune dépendance sur Express, Socket.IO ou Redis

**Exemple — `SendMessage` (le plus complexe, 35 Ko) :**

```javascript
class SendMessage {
  constructor(
    messageRepository,        // CachedMessageRepository
    conversationRepository,   // CachedConversationRepository
    cacheService,             // CacheService Redis
    resilientMessageService,  // Publication sur Redis Streams
    userCacheService,         // Cache profils utilisateurs
    getFileUseCase,           // Pour résoudre les fichiers joints
    encryptionService,        // Chiffrement E2EE optionnel
    keyManagementService,     // Récupération clés publiques
  ) { ... }

  async execute(data) {
    // 1. Validation métier (contenu, conversationId, permissions)
    // 2. Chiffrement E2EE si activé (encryptionService.encryptText)
    // 3. Persistance MongoDB (via CachedMessageRepository)
    // 4. WAL pre-write (resilientMessageService.logPreWrite)
    // 5. Publication Redis Stream (resilientMessageService.publishToMessageStream)
    // 6. WAL post-write (resilientMessageService.logPostWrite)
    // 7. Mise à jour du lastMessage de la conversation
    // 8. Retour du message sauvegardé
  }
}
```

**Liste complète des Use Cases :**

| Catégorie | Use Cases |
|---|---|
| **Messages** | SendMessage, GetMessages, GetMessageById, UpdateMessageContent, DeleteMessage, ForwardMessage, ReplyMessage, UpdateMessageStatus |
| **Statuts** | MarkMessageRead, MarkMessageDelivered |
| **Réactions** | AddReaction, RemoveReaction |
| **Fichiers** | UploadFile, GetFile, DownloadFile, DeleteFile |
| **Conversations** | GetConversation, GetConversations, GetConversationIds, ArchiveConversation, GetArchivedConversations, SearchOccurrences |
| **Groupes** | CreateGroup, CreateBroadcast, AddParticipant, RemoveParticipant, LeaveConversation, AddAdmin |
| **Système** | AutoGroupSync, UpdateCallStatus |

---

### 4. Couche Domaine (`domain/`)

**Concept :** Le cœur de l'application. Contient les **entités métier** pures, sans dépendance sur aucun framework ou bibliothèque externe (sauf `crypto` natif pour les checksums). C'est la couche la plus stable du système.

#### 4.1. Entités

**`Message.js`** — Représente un message dans le système :

| Propriété | Type | Description |
|---|---|---|
| `type` | Enum | `TEXT`, `IMAGE`, `VIDEO`, `AUDIO`, `FILE`, `LOCATION`, `CONTACT` |
| `status` | Enum | `SENT`, `DELIVERED`, `READ`, `FAILED` |
| `reactions` | Array | `[{ userId, emoji, timestamp }]` |
| `replyTo` | ObjectId | Référence au message parent (threading) |
| `metadata` | Object | Métadonnées enrichies (cache, sécurité, livraison) |

Méthodes métier : `validate()`, `markAsRead()`, `markAsDelivered()`, `addReaction()`, `removeReaction()`, `edit()`, `softDelete()`

Méthodes de sérialisation : `toKafkaPayload()`, `toRedisPayload()`, `toObject()`

Stratégie de cache dynamique : le TTL varie selon le type de message (`TEXT`=1h, `IMAGE`=2h, `VIDEO`=30min).

**`Conversation.js`** — Représente une conversation :

| Propriété | Type | Description |
|---|---|---|
| `type` | Enum | `PRIVATE`, `GROUP`, `CHANNEL` |
| `participants` | Array | Liste des userIds participants |
| `unreadCounts` | Object | `{ userId: count }` — compteurs non-lus par utilisateur |
| `settings` | Object | Notifications, confidentialité, rétention, paramètres groupe |
| `archivedBy` / `mutedBy` / `pinnedBy` | Arrays | Actions utilisateur individuelles |

Factory methods : `Conversation.createPrivateConversation()`, `Conversation.createGroup()`

**`File.js`** — Représente un fichier uploadé avec ses métadonnées enrichies (dimensions, durée, nombre de pages PDF, etc.)

**`Event.js`** — Définit les types d'événements système standardisés pour les Redis Streams.

---

### 5. Couche Infrastructure (`infrastructure/`)

La couche la plus volumineuse. Contient toutes les implémentations concrètes des interfaces définies par les couches supérieures.

#### 5.1. MongoDB (`infrastructure/mongodb/`)

**`connection.js`** — Singleton de connexion Mongoose avec retry automatique.

**Modèles Mongoose** — Schémas riches avec indexes composites pour les performances :

| Modèle | Indexes principaux |
|---|---|
| `MessageModel` | `{ conversationId: 1, createdAt: -1 }`, `{ senderId: 1 }`, `{ status: 1 }` |
| `ConversationModel` | `{ "participants.userId": 1 }`, `{ type: 1 }`, `{ updatedAt: -1 }` |
| `FileModel` | `{ conversationId: 1 }`, `{ uploadedBy: 1 }`, `{ mimeType: 1 }` |
| `UserEncryptionKeyModel` | `{ userId: 1, isActive: 1 }`, `{ fingerprint: 1 }` |

#### 5.2. Redis (`infrastructure/redis/`)

Cinq composants Redis distincts :

| Composant | Responsabilité | Clés Redis |
|---|---|---|
| `redisConfig.js` | Configuration et health check | — |
| `CacheService.js` | Cache générique avec TTL, pattern delete, renew TTL | `chat:cache:*` |
| `OnlineUserManager.js` | Tracking utilisateurs connectés, heartbeat, cleanup | `chat:online:*` |
| `RoomManager.js` | Gestion des rooms Socket.IO dans Redis | `chat:room:*` |
| `UnreadMessageManager.js` | Compteurs non-lus atomiques (INCR/DECR) | `chat:cache:unread:*` |

#### 5.3. Repositories (`infrastructure/repositories/`)

**Pattern Decorator** : Chaque repository MongoDB est enveloppé dans un repository caché qui ajoute transparemment une couche de cache Redis.

```
Requête → CachedMessageRepository → (cache hit?) → retourne
                                    → (cache miss?) → MongoMessageRepository → MongoDB
                                                   → mise en cache Redis
                                                   → retourne
```

**`CachedMessageRepository`** implémente une stratégie de cache à 3 niveaux :

| Niveau | Clé | TTL | Quand |
|---|---|---|---|
| Quick Load | `chat:cache:msgs:quick:{convId}:{limit}` | 60s | Chargement initial rapide |
| First Page | `chat:cache:msgs:{convId}:first:{limit}` | 1h | Première page de messages |
| Pagination | `chat:cache:msgs:{convId}:p{page}:{limit}` | 5min | Pages suivantes |

**Invalidation intelligente :** À chaque nouveau message, le cache de la conversation est invalidé puis **pré-rechargé en arrière-plan** (`setImmediate`), garantissant un cache warm pour la prochaine requête.

#### 5.4. Services techniques (`infrastructure/services/`)

C'est le bloc le plus critique du service. 11 services techniques gèrent les aspects non-fonctionnels.

---

##### 5.4.1. `ResilientMessageService` — Résilience & Publication

**Concepts :** Circuit Breaker, Write-Ahead Log (WAL), Dead Letter Queue (DLQ), Retry exponentiel, Fallback Redis

C'est le **service de résilience centralisé** (2 676 lignes). Il garantit qu'aucun message n'est perdu, même en cas de défaillance de MongoDB ou Redis.

**Architecture de résilience :**

```
Message entrant
      │
      ├─→ WAL Pre-Write (Redis Stream)      ← Point de récupération
      │
      ├─→ CircuitBreaker.execute()
      │     ├─→ MongoDB Save (succès)
      │     │     ├─→ WAL Post-Write (cleanup)
      │     │     └─→ Publish Redis Stream
      │     │
      │     └─→ MongoDB Save (échec)
      │           ├─→ Circuit CLOSED → Retry Queue
      │           │     └─→ Backoff exponentiel: 100ms × 2^(attempt-1)
      │           │           └─→ Max 5 retries → DLQ
      │           │
      │           └─→ Circuit OPEN → Fallback Redis
      │                 └─→ Hash Redis (TTL 24h) + Fallback Stream
      │                       └─→ Replay worker → MongoDB (quand disponible)
```

**Composants shared injectés :**
- `CircuitBreaker` : seuil de 5 échecs, reset après 30s
- `StreamManager` : normalisation des opérations Redis Streams (addToStream, readFromStream, trimming)
- `WorkerManager` : orchestration de 3 workers de fond (retry, fallback, WAL recovery)

**Streams Redis gérés (20+) :**

| Catégorie | Streams |
|---|---|
| Techniques | `chat:stream:wal`, `chat:stream:retry`, `chat:stream:dlq`, `chat:stream:fallback`, `chat:stream:metrics` |
| Messages | `chat:stream:messages:private`, `chat:stream:messages:group`, `chat:stream:messages:channel` |
| Statuts | `chat:stream:status:delivered`, `chat:stream:status:read`, `chat:stream:status:edited`, `chat:stream:status:deleted` |
| Événements | `chat:stream:events:typing`, `chat:stream:events:reactions`, `chat:stream:events:replies` |
| Système | `chat:stream:events:conversations`, `chat:stream:events:files`, `chat:stream:events:notifications`, `chat:stream:events:analytics` |

---

##### 5.4.2. `MessageDeliveryService` — Consumer Multi-Streams

**Concepts :** Consumer Groups, Worker Partitioning, Lazy Subscription, Priorité, Déduplication

C'est le **consommateur distribué** (3 425 lignes) qui lit les Redis Streams et distribue les messages aux destinataires via Socket.IO.

**Architecture de partitionnement des workers :**

```
┌──────────────────────────────────────────────────────────────┐
│              MessageDeliveryService                          │
│                                                              │
│  HIGH_PRIORITY_WORKER (3 workers)                            │
│  ├─ private (100ms)        ← Messages privés                 │
│  ├─ statusRead (50ms)      ← Accusés de lecture              │
│  ├─ statusDelivered (50ms) ← Accusés de livraison            │
│  ├─ conversationCreated    ← Nouvelles conversations         │
│  └─ call (50ms)            ← Événements d'appels             │
│                                                              │
│  GROUP_WORKER (2 workers)                                    │
│  ├─ group (200ms)          ← Messages de groupe              │
│  ├─ channel (200ms)        ← Messages canal                  │
│  ├─ reactions (2000ms)     ← Réactions emoji                 │
│  └─ replies (2000ms)       ← Réponses threadées              │
│                                                              │
│  SYSTEM_WORKER (1 worker)                                    │
│  ├─ notifications (500ms)  ← Notifications push              │
│  ├─ conversations (500ms)  ← Événements conversation         │
│  ├─ files (500ms)          ← Événements fichiers             │
│  ├─ statusEdited (1500ms)  ← Édition de messages             │
│  ├─ statusDeleted (1500ms) ← Suppression de messages         │
│  └─ analytics (3000ms)     ← Métriques d'usage               │
└──────────────────────────────────────────────────────────────┘
```

**Lazy Subscription (abonnement progressif) :**
Les streams ne sont pas tous consommés simultanément. Le service démarre par phases pour éviter la surcharge au boot :

| Phase | Délai | Streams |
|---|---|---|
| Phase 1 | Immédiat | `private`, `statusRead`, `statusDelivered`, `conversationCreated`, `call` |
| Phase 2 | +100ms | `group`, `channel` |
| Phase 3 | +300ms | `notifications`, `conversations`, participant events |
| Phase 4 | +800ms | `files`, `reactions`, `replies` |
| Phase 5 | Background | `analytics` |

**Mécanismes de fiabilité :**
- **Déduplication** : cache en mémoire (`Map`) avec TTL 30s pour éviter la double livraison (direct + stream consumer)
- **Queue sérialisée** : livraison ordonnée des statuts avec 20ms d'intervalle entre chaque `emit` pour éviter la saturation Socket.IO
- **Pending Queue** : si le destinataire est déconnecté, le message est stocké dans `chat:stream:pending:messages:{userId}` pour livraison au reconnect
- **Multi-device** : le message est livré à **tous les sockets** du destinataire, mais exclu le `senderSocketId` exact (pas tous les sockets du sender)

---

##### 5.4.3. `TypingIndicatorService` — Indicateurs de frappe

**Concepts :** Consumer Group dédié, Debounce serveur, Timeout automatique

Consumer Redis Streams dédié au stream `chat:stream:events:typing`. Séparé du `MessageDeliveryService` pour isoler le trafic haute fréquence (50ms d'intervalle de polling).

| Paramètre | Valeur | Rôle |
|---|---|---|
| `TYPING_TIMEOUT` | 10s | Si aucun refresh reçu → envoi automatique de `typing:stop` |
| `DEBOUNCE_INTERVAL` | 1s | Minimum entre deux broadcasts du même utilisateur |

**Flux :**
```
Client → emit("typing") → chatHandler → Redis Stream → TypingIndicatorService
  → broadcastTypingStatus() → Socket.IO emit("typing:indicator") à tous les participants (sauf le typeur)
```

---

##### 5.4.4. `FileStorageService` — Stockage objet

**Concept :** Strategy Pattern — Le service supporte plusieurs backends de stockage.

| Mode | Backend | Usage |
|---|---|---|
| `development` | MinIO (S3-compatible) | Développement local avec Docker |
| `production` | MinIO ou SFTP (`ssh2-sftp-client`) | Production avec stockage distant |

Fonctionnalités :
- Upload monolithique et par buffer avec retry automatique (max 3 tentatives)
- Download avec stream (pas de chargement en mémoire)
- Compression optionnelle : images → WebP via `sharp`, vidéos → MP4 via `ffmpeg`
- Chiffrement serveur optionnel (AES-256-GCM comme fallback)
- URL signées (`presignedGetObject`) pour le téléchargement sécurisé temporaire
- Métriques intégrées (uploads, downloads, deletes, errors)

---

##### 5.4.5. `MediaProcessingService` — Traitement multimédia

Service de traitement et d'extraction de métadonnées pour tous les types de fichiers supportés :

| Type | Bibliothèque | Données extraites |
|---|---|---|
| **Images** | `sharp` | Dimensions, format, taille, miniature WebP |
| **Vidéos** | `fluent-ffmpeg` | Durée, dimensions, codec, framerate, miniature |
| **Audio** | `music-metadata` | Durée, artiste, album, genre, bitrate, sample rate |
| **PDF** | `pdf-parse` | Nombre de pages, texte extrait, métadonnées auteur |
| **Tous** | `mime-types` | Type MIME détecté, extension originale |

---

##### 5.4.6. `ThumbnailService` — Miniatures

Génère des miniatures pour les images et vidéos uploadés :
- Images : redimensionnement via `sharp` en WebP (qualité 80)
- Stockage des miniatures dans MinIO sous le préfixe `thumbnails/`
- Nommage unique via `uuid`

---

##### 5.4.7. `ChunkedUploadService` — Upload par morceaux

Gère l'upload de fichiers volumineux (> 100 MB) par morceaux :

```
1. Client → POST /files/chunk/init  → ChunkedUploadService.initUpload()
   → Retourne uploadId, chunkSize (5 MB), totalChunks

2. Client → POST /files/chunk/upload (×N) → ChunkedUploadService.uploadChunk()
   → Stockage temporaire dans le filesystem (fs-extra)
   → Tracking Redis du nombre de chunks reçus

3. Dernier chunk → ChunkedUploadService.finalizeUpload()
   → Assemblage des chunks → Buffer complet
   → Upload vers MinIO via FileStorageService
   → Nettoyage des chunks temporaires

Maintenance :
   → setInterval (30 min) → cleanupExpired() → Supprime les uploads abandonnés (TTL > 2h)
```

---

##### 5.4.8. `EncryptionService` — Chiffrement E2EE

**Concepts :** Chiffrement hybride, AES-256-GCM, RSA-OAEP 4096, Switch de mode à chaud

Le service supporte deux modes, commutables **à chaud** sans redémarrage :

| Mode | Comportement |
|---|---|
| `none` | Pas de chiffrement applicatif (défaut). Le contenu transite en clair (TLS réseau uniquement). |
| `e2ee` | Chiffrement de bout en bout. Le serveur ne peut **jamais** lire le contenu. |

**Algorithmes en mode E2EE :**

```
Chiffrement d'un message :
  1. Génération d'une clé symétrique AES-256 aléatoire (32 bytes)
  2. Génération d'un IV aléatoire (16 bytes)
  3. Chiffrement du contenu avec AES-256-GCM → ciphertext + auth tag (16 bytes)
  4. Chiffrement de la clé symétrique avec la clé publique RSA-4096 du destinataire (OAEP + SHA-256)
  5. Retour : { encryptedContent, encryptionIV, encryptionTag, encryptedKey }
```

Le service chiffre aussi les fichiers (buffers binaires) avec le même schéma.

**Sécurité :** Le serveur ne stocke **jamais** les clés privées. Elles sont générées et conservées exclusivement côté client (Keychain iOS / SecureStorage Android).

---

##### 5.4.9. `KeyManagementService` — Gestion des clés publiques

**Concepts :** Rotation de clés, TOFU (Trust On First Use), Cache Redis + persistance MongoDB

| Opération | Description |
|---|---|
| `registerPublicKey()` | Enregistre ou rotate la clé publique d'un utilisateur (versionnée) |
| `getPublicKey()` | Cache Redis → Fallback MongoDB → retourne le PEM actif |
| `revokeKey()` | Désactive la clé active (cas de compromission) |
| `verifyFingerprint()` | Vérifie un fingerprint SHA-256 contre la clé active (TOFU) |
| `getKeyHistory()` | Historique des versions de clé (pour déchiffrer d'anciens messages) |

**Architecture de stockage :**
```
getPublicKey(userId)
  → Redis cache (chat:encryption:pubkey:{userId}, TTL 1h)
    → HIT → retourne PEM
    → MISS → MongoDB (UserEncryptionKeyModel, { userId, isActive: true })
      → FOUND → cache Redis + retourne PEM
      → NOT FOUND → throw Error
```

---

##### 5.4.10. `UserCacheService` — Cache profils utilisateurs

Client HTTP (`axios`) qui interroge le `auth-user-service` pour résoudre les noms/avatars des utilisateurs. Utilisé lors de l'ajout de participants pour enrichir les notifications système.

---

##### 5.4.11. `SmartCachePrewarmer` — Pré-chauffage du cache

**Concept :** Cache Warming — Pré-charger les données fréquemment accédées au démarrage pour maximiser le cache hit rate.

**Stratégie en 3 étapes :**
1. Lecture du stream `user-service:stream:events:users` via `UserStreamConsumer`
2. Fallback : requête HTTP `GET /all` vers le auth-user-service
3. Traitement par batches de 500 utilisateurs avec 1.5s de délai inter-batch

Résultat : cache hit rate de 80-95% dès le premier accès.

---

##### 5.4.12. `AutoGroupSyncService` — Synchronisation des groupes

Synchronise automatiquement les groupes organisationnels (par ministère, département) en interrogeant les données utilisateur et en créant/mettant à jour les groupes correspondants via les use cases `CreateGroup` et `AddParticipant`.

---

### 6. Module partagé (`@chatapp-ngomna/shared`)

Module NPM local (`file:../shared`) qui fournit les composants transversaux réutilisés par tous les microservices :

| Composant | Rôle |
|---|---|
| `RedisManager` | Gestionnaire de connexions Redis (main, pub, sub, stream) |
| `CacheService` | Cache générique avec TTL, pattern delete, renewal |
| `OnlineUserManager` | Tracking des utilisateurs connectés |
| `RoomManager` | Gestion des rooms Socket.IO dans Redis |
| `UnreadMessageManager` | Compteurs atomiques non-lus |
| `CircuitBreaker` | Pattern Circuit Breaker (closed → open → half-open) |
| `StreamManager` | Abstraction des opérations Redis Streams |
| `WorkerManager` | Orchestration de workers de fond |
| `UserCache` | Cache centralisé des profils utilisateurs |
| `UserStreamConsumer` | Consumer du stream événements utilisateurs |

---

## 📐 Patterns & Concepts architecturaux — Résumé

| Pattern | Où il est appliqué | Pourquoi |
|---|---|---|
| **Clean Architecture** | Structure complète du projet (`interfaces/` → `application/` → `domain/` → `infrastructure/`) | Séparation stricte des responsabilités, testabilité, maintenabilité |
| **Dependency Injection** | `index.js` (Composition Root) → tous les constructeurs | Découplage, testabilité (mock facile), configuration centralisée |
| **Repository Pattern** | `MongoMessageRepository`, `CachedMessageRepository`, etc. | Abstraction de la couche de persistance, interchangeabilité |
| **Decorator Pattern** | `CachedMessageRepository` enveloppe `MongoMessageRepository` | Ajout transparent du cache sans modifier le repository original |
| **Command Pattern** | 30 Use Cases avec `execute()` | Une classe = une opération = une responsabilité |
| **Circuit Breaker** | `ResilientMessageService` via `@chatapp-ngomna/shared` | Protection contre les défaillances en cascade de MongoDB |
| **Write-Ahead Log (WAL)** | `ResilientMessageService.logPreWrite()` / `logPostWrite()` | Récupération des messages en cas de crash entre l'écriture et la publication |
| **Dead Letter Queue (DLQ)** | `ResilientMessageService.addToDLQ()` | Isolation des messages "poison" qui échouent après tous les retries |
| **Retry avec Backoff** | `ResilientMessageService.addRetry()` — 100ms × 2^(attempt-1) | Récupération automatique des erreurs transitoires |
| **Fallback** | `ResilientMessageService.redisFallback()` | Dégradation gracieuse : Redis comme buffer temporaire si MongoDB down |
| **Event-Driven Architecture** | Redis Streams (20+ streams) + Consumer Groups | Découplage production/consommation, scalabilité horizontale |
| **Consumer Partitioning** | `MessageDeliveryService.WORKER_PARTITIONS` | Isolation des charges par priorité, pas de starvation |
| **Lazy Subscription** | `MessageDeliveryService.SUBSCRIPTION_PHASES` | Démarrage progressif pour éviter la surcharge au boot |
| **Strategy Pattern** | `FileStorageService` (MinIO vs SFTP) | Backend de stockage interchangeable par configuration |
| **Cache-Aside** | `CachedMessageRepository.findByConversation()` | Cache → Miss → DB → Cache → Réponse |
| **Cache Warming** | `SmartCachePrewarmer.start()` | Pré-chargement des données chaudes au démarrage |
| **Graceful Shutdown** | `index.js` — `SIGTERM`/`SIGINT` handlers | Arrêt propre : stop consumers → close Redis → close MongoDB |
| **Composition Root** | `index.js` — unique point d'assemblage | Tous les `new` au même endroit, pas de couplage implicite |

---

## 📞 Liens utiles

| Ressource | Lien |
|---|---|
| Documentation globale | [README.md](../README.md) |
| Événements Socket.IO | [SOCKET_EVENTS_REFERENCE.md](../SOCKET_EVENTS_REFERENCE.md) |
| Use Cases complets | [USE_CASES_REFERENCE.md](../USE_CASES_REFERENCE.md) |
| Conventions Redis | [REDIS_KEYS_CONVENTION.md](../REDIS_KEYS_CONVENTION.md) |
| Documentation Redis Streams | [shared/REDIS_DOCUMENTATION.md](../shared/REDIS_DOCUMENTATION.md) |
| Interface de test | `http://localhost:8003/` |
| Health check | `http://localhost:8003/health` |

---

**Version :** 1.0.0 | **Dernière mise à jour :** 20 Juin 2026 | **License :** ISC
