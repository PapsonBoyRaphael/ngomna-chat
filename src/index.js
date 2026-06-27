const express = require("express");
require("dotenv").config();
const { createServer } = require("http");
const cors = require("cors");
const path = require("path");
const cookieParser = require("cookie-parser");
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");
const jwt = require("jsonwebtoken");

// ✅ VALIDATION ENVIRONNEMENT
const EnvironmentValidator = require("./config/envValidator");
const envValidator = new EnvironmentValidator();

if (!envValidator.validate()) {
  console.error("❌ Configuration environnement invalide. Arrêt du service.");
  process.exit(1);
}

// Infrastructure
const connectDB = require("./infrastructure/mongodb/connection");

// ✅ SHARED MODULE - Composants partagés
const {
  CacheService,
  OnlineUserManager,
  RoomManager,
  UnreadMessageManager,
  CircuitBreaker,
  StreamManager,
  WorkerManager,
  RedisManager,
  UserCache,
  UserStreamConsumer,
} = require("../shared");

// Services
const AutoGroupSyncService = require("./infrastructure/services/AutoGroupSyncService");
const ThumbnailService = require("./infrastructure/services/ThumbnailService");
const FileStorageService = require("./infrastructure/services/FileStorageService");
const MediaProcessingService = require("./infrastructure/services/MediaProcessingService");
const ResilientMessageService = require("./infrastructure/services/ResilientMessageService");
const UserCacheService = require("./infrastructure/services/UserCacheService");
// const SmartCachePrewarmer = require("./infrastructure/services/SmartCachePrewarmer");
const ChunkedUploadService = require("./infrastructure/services/ChunkedUploadService");
const EncryptionService = require("./infrastructure/services/EncryptionService");
const KeyManagementService = require("./infrastructure/services/KeyManagementService");

// Repositories - Cached
const CachedMessageRepository = require("./infrastructure/repositories/CachedMessageRepository");
const CachedConversationRepository = require("./infrastructure/repositories/CachedConversationRepository");
const CachedFileRepository = require("./infrastructure/repositories/CachedFileRepository");

// Redis Services (locaux uniquement)
const MessageDeliveryService = require("./infrastructure/services/MessageDeliveryService");
const TypingIndicatorService = require("./infrastructure/services/TypingIndicatorService");

// Use Cases
const SendMessage = require("./application/use-cases/SendMessage");
const GetMessages = require("./application/use-cases/GetMessages");
const GetConversation = require("./application/use-cases/GetConversation");
const GetConversations = require("./application/use-cases/GetConversations");
const GetFile = require("./application/use-cases/GetFile");
const UpdateMessageStatus = require("./application/use-cases/UpdateMessageStatus");
const UploadFile = require("./application/use-cases/UploadFile");
const GetConversationIds = require("./application/use-cases/GetConversationIds");
const GetMessageById = require("./application/use-cases/GetMessageById");
const UpdateMessageContent = require("./application/use-cases/UpdateMessageContent");
const DownloadFile = require("./application/use-cases/DownloadFile");
const CreateGroup = require("./application/use-cases/CreateGroup");
const AddAdmin = require("./application/use-cases/AddAdmin");
const CreateBroadcast = require("./application/use-cases/CreateBroadcast");
const MarkMessageDelivered = require("./application/use-cases/MarkMessageDelivered");
const MarkMessageRead = require("./application/use-cases/MarkMessageRead");
const AddParticipant = require("./application/use-cases/AddParticipant");
const RemoveParticipant = require("./application/use-cases/RemoveParticipant");
const LeaveConversation = require("./application/use-cases/LeaveConversation");
const DeleteMessage = require("./application/use-cases/DeleteMessage");
const DeleteFile = require("./application/use-cases/DeleteFile");
const ForwardMessage = require("./application/use-cases/ForwardMessage");
const ReplyMessage = require("./application/use-cases/ReplyMessage");
const SearchOccurrences = require("./application/use-cases/SearchOccurrences");
const AddReaction = require("./application/use-cases/AddReaction");
const RemoveReaction = require("./application/use-cases/RemoveReaction");
const ArchiveConversation = require("./application/use-cases/ArchiveConversation");
const GetArchivedConversations = require("./application/use-cases/GetArchivedConversations");

// Controllers
const FileController = require("./application/controllers/FileController");
const MessageController = require("./application/controllers/MessageController");
const ConversationController = require("./application/controllers/ConversationController");
const HealthController = require("./application/controllers/HealthController");
const GroupController = require("./application/controllers/GroupController");

// Repositories - Mongo
const MongoMessageRepository = require("./infrastructure/repositories/MongoMessageRepository");
const MongoConversationRepository = require("./infrastructure/repositories/MongoConversationRepository");
const MongoFileRepository = require("./infrastructure/repositories/MongoFileRepository");

// Routes
const createConversationRoutes = require("./interfaces/http/routes/conversationRoutes");
const createMessageRoutes = require("./interfaces/http/routes/messageRoutes");
const createFileRoutes = require("./interfaces/http/routes/fileRoutes");
const createHealthRoutes = require("./interfaces/http/routes/healthRoutes");
const createGroupRoutes = require("./interfaces/http/routes/groupRoutes");
const createBroadcastRoutes = require("./interfaces/http/routes/broadcastRoutes");

// WebSocket Handler
const ChatHandler = require("./application/websocket/chatHandler");

// Middleware
const rateLimitMiddleware = require("./interfaces/http/middleware/rateLimitMiddleware");

// auth system
const { createAuthSystem } = require("../shared/auth/index");

// ═══════════════════════════════════════════════════════════════════════════
// INITIALISATION AUTH + CONSUMER
// ═══════════════════════════════════════════════════════════════════════════
async function initializeAuth() {
  // Créer le système d'auth avec consumer RBAC
  const auth = createAuthSystem({
    jwtSecret: process.env.JWT_SECRET,
    redisClient: null,
    serviceName: "chat-service",

    fallbackEnabled: true,
    // Fallback HTTP si user pas dans store (optionnel)
    fallbackFetcher: async (matricule) => {
      // Appel au service Identity si nécessaire
      // À n'utiliser qu'au démarrage ou en cas de désync
      console.log("................ Dans le Fallback ...........");
      try {
        const response = await fetch(
          `${process.env.IDENTITY_SERVICE_URL}/internal/permissions-detailed/${matricule}`,
          {
            headers: { "X-Internal-Service": "true" },
            timeout: 3000,
          },
        );
        if (response.ok) {
          const data = await response.json();
          // console.log(data);
          console.log("=========");
          return {
            permissions: data.permissions,
            roles: data.roles,
            // permissions: data.permissions?.map(p => p.name)  || [],
            // roles: data.roles?.map(r => r.name) || []
          };
        }
      } catch (error) {
        console.error(`Fallback failed for ${matricule}:`, error.message);
      }
      return null;
    },
  });

  //Initialiser et demarrer le consumer de rbac
  if (auth.permissionConsumer) {
    await auth.permissionConsumer.initialize();

    // Demarrer en background (ne pas faire await)
    auth.permissionConsumer.start().catch((error) => {
      console.error(`❌ Erreur demarrage consumer:`, error);
    });
  }

  return auth;
}
// ===============================
// DÉMARRAGE SERVEUR
// ===============================
const startServer = async () => {
  try {
    console.log("🚀 Démarrage du Chat-File Service...");

    // ===============================
    // 1. CRÉATION EXPRESS APP ET SERVEUR
    // ===============================
    const app = express();
    const server = createServer(app);

    // ===============================
    // 2. CONNEXIONS INFRASTRUCTURE
    // ===============================

    // MongoDB
    await connectDB();
    console.log("✅ MongoDB connecté");

    // Redis
    let redisClient = null;
    let onlineUserManager = null;
    let roomManager = null;
    let cacheServiceInstance = null;

    try {
      // Passe les vrais params de ton redisConfig
      await RedisManager.connect({
        host: process.env.REDIS_HOST, // ou redisConfig.host
        port: process.env.REDIS_PORT,
        password: process.env.REDIS_PASSWORD,
        db: process.env.REDIS_DB || 0,
      });

      redisClient = RedisManager.getMainClient();

      // Puis initialise tes services partagés avec ce client
      cacheServiceInstance = new CacheService({
        defaultTTL: 3600,
        keyPrefix: "chat",
        maxScanCount: 1000,
      });
      await cacheServiceInstance.initializeWithClient(redisClient);

      console.log("✅ Services Redis initialisés:");
      console.log("   ✅ CacheService (shared)");
    } catch (err) {
      console.error(
        "❌ Redis distant non disponible, fallback ou mode dégradé",
      );

      // Fallback ou mode dégradé ici si nécessaire
    }

    // ===============================
    // 3. CONFIGURATION EXPRESS
    // ===============================
    app.use(
      cors({
        origin: ["*"],
        credentials: true,
        methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization", "Accept", "user-id"],
      }),
    );

    app.use(cookieParser());
    app.use(express.json({ limit: "10mb" }));
    app.use(express.urlencoded({ extended: true, limit: "10mb" }));

    app.locals.redisClient = redisClient;

    // Initialiser auth
    const auth = await initializeAuth();

    // Exposer pour utilisation dans routes
    app.locals.auth = auth;

    if (rateLimitMiddleware && rateLimitMiddleware.apiLimit) {
      app.use(rateLimitMiddleware.apiLimit);
    }

    app.use(express.static(path.join(__dirname, "../public")));

    // ===============================
    // 4. CONFIGURATION SOCKET.IO
    // ===============================
    const io = new Server(server, {
      cors: {
        origin: ["*"],
        methods: ["GET", "POST"],
        credentials: true,
      },
      transports: ["websocket", "polling"],
    });

    if (redisClient) {
      try {
        // ✅ UTILISER RedisManager.getPubClient() ET RedisManager.getSubClient()
        // POUR OBTENIR LES CLIENTS PUB/SUB CORRECTEMENT INITIALISÉS
        const pubClient = RedisManager.getPubClient();
        const subClient = RedisManager.getSubClient();

        if (pubClient && subClient) {
          io.adapter(createAdapter(pubClient, subClient));
          console.log("✅ Redis adapter Socket.IO configuré");
        } else {
          console.warn(
            "⚠️ Clients Pub/Sub Redis non disponibles, adapter Socket.IO ignoré",
          );
        }
      } catch (error) {
        console.warn("⚠️ Erreur config Redis adapter:", error.message);
      }
    }

    // ✅ INITIALISER OnlineUserManager depuis shared
    if (redisClient) {
      onlineUserManager = new OnlineUserManager(io, {
        keyPrefix: "chat:online",
        userTTL: 3600,
        heartbeatInterval: 30000,
        maxScanCount: 1000,
      });
      await onlineUserManager.initializeWithClient(redisClient);
      console.log("   ✅ OnlineUserManager (shared)");

      // ✅ INITIALISER RoomManager depuis shared
      roomManager = new RoomManager(io, onlineUserManager, {
        defaultRoomTTL: 86400,
      });
      await roomManager.initializeWithClient(redisClient);
      app.locals.roomManager = roomManager;
      console.log("   ✅ RoomManager (shared)");

      // ✅ INITIALISER UserCache depuis shared
      UserCache.prefix = "chat:cache:users:";
      await UserCache.initialize();
      console.log("   ✅ UserCache (shared) - Cache utilisateur centralisé");

      // ✅ INITIALISER ET DÉMARRER UserStreamConsumer
      const userStreamConsumer = new UserStreamConsumer({
        streamName: "chat:stream:events:users",
        consumerGroup: "chat-file-service-group",
        consumerName: `chat-consumer-${process.pid}`,
        cachePrefix: "chat:cache:users:",
      });
      await userStreamConsumer.initialize();
      await userStreamConsumer.start();
      app.locals.userStreamConsumer = userStreamConsumer;
      console.log("   ✅ UserStreamConsumer - Écoute événements utilisateurs");
    }

    // ✅ INITIALISER MessageDeliveryService MAINTENANT QUE IO EST CRÉÉ
    let messageDeliveryService = null;
    let typingIndicatorService = null;
    if (redisClient) {
      try {
        console.log("🚀 Initialisation MessageDeliveryService...");
        // ✅ UTILISER LE CLIENT STREAM POUR LES OPÉRATIONS DE STREAMING
        const streamClient = RedisManager.getStreamClient();
        messageDeliveryService = new MessageDeliveryService(streamClient, io);
        console.log("⏳ Attente de l'initialisation du consumer...");
        await messageDeliveryService.initialize();
        app.locals.messageDeliveryService = messageDeliveryService;
        console.log("   ✅ MessageDeliveryService initialisé");
        // ⏳ TypingIndicatorService sera initialisé après conversationRepository
      } catch (error) {
        console.error(
          "❌ Erreur initialisation MessageDeliveryService/TypingIndicatorService:",
          error.message,
        );
      }
    } else {
      console.log(
        "⚠️ Redis non disponible, MessageDeliveryService et TypingIndicatorService non créés",
      );
    }

    // ===============================
    // 5. SERVICES FICHIERS
    // ===============================
    const fileStorageService = new FileStorageService({
      env: process.env.NODE_ENV || "development",
      s3Endpoint: process.env.S3_ENDPOINT || "http://localhost:9000",
      s3AccessKeyId: process.env.S3_ACCESS_KEY || "minioadmin",
      s3SecretAccessKey: process.env.S3_SECRET_KEY || "minioadmin",
      s3Bucket: process.env.S3_BUCKET || "chat-files",
      sftpConfig: {
        host: process.env.SFTP_HOST,
        port: process.env.SFTP_PORT || 22,
        username: process.env.SFTP_USER,
        password: process.env.SFTP_PASS,
        remotePath: process.env.SFTP_REMOTE_PATH || "/uploads",
      },
    });

    // ✅ CRÉER MediaProcessingService
    const mediaProcessingService = new MediaProcessingService();
    console.log("✅ MediaProcessingService initialisé");

    // Initialiser le service de traitement multimédia
    const thumbnailService = new ThumbnailService(fileStorageService);

    // ✅ INITIALISER ChunkedUploadService (upload par morceaux > 100 MB)
    const chunkedUploadService = new ChunkedUploadService(
      redisClient,
      fileStorageService,
    );
    console.log("✅ ChunkedUploadService initialisé");

     // ✅ NETTOYAGE AUTOMATIQUE DES CHUNKS EXPIRÉS (toutes les 30 minutes)
    // Supprime les dossiers temporaires d'uploads abandonnés ou crashés (TTL > 2h)
    const CHUNK_CLEANUP_INTERVAL_MS = 30 * 60 * 1000; // 30 min
    setInterval(async () => {
      try {
        await chunkedUploadService.cleanupExpired();
      } catch (err) {
        console.warn("⚠️ Erreur nettoyage périodique chunks:", err.message);
      }
    }, CHUNK_CLEANUP_INTERVAL_MS);
    console.log(
      "✅ Nettoyage automatique des chunks planifié (toutes les 30 min)",
    );

    console.log("✅ Services de fichiers initialisés");

    // ===============================
    // 5b. SERVICES CHIFFREMENT E2EE
    // ===============================
    const encryptionService = new EncryptionService({
      mode: process.env.ENCRYPTION_MODE || "none",
    });
    const keyManagementService = new KeyManagementService(redisClient);
    app.locals.encryptionService = encryptionService;
    app.locals.keyManagementService = keyManagementService;
    console.log(
      `✅ EncryptionService initialisé (mode: ${encryptionService.getMode()})`,
    );
    console.log("✅ KeyManagementService initialisé");

    // ===============================
    // 6. INITIALISATION REPOSITORIES
    // ===============================
    // Créer d'abord les repos Mongo
    const mongoMessageRepository = new MongoMessageRepository();
    const mongoConversationRepository = new MongoConversationRepository();

    const mongoFileRepository = new MongoFileRepository(
      redisClient,
      null,
      thumbnailService,
      cacheServiceInstance,
    );

    // ✅ SUPPRIMER UnreadManager séparé si present
    // const unreadManager = new UnreadManager(cacheServiceInstance);

    // ✅ CRÉER CachedMessageRepository SANS UnreadManager
    const messageRepository = new CachedMessageRepository(
      mongoMessageRepository,
      cacheServiceInstance,
      // ← unreadManager SUPPRIMÉ - intégré dans CachedMessageRepository
    );

    const conversationRepository = new CachedConversationRepository(
      mongoConversationRepository,
      cacheServiceInstance,
    );

    // ✅ CONFIGURER LE CALLBACK DE DÉCONNEXION POUR METTRE À JOUR lastSeen
    if (onlineUserManager && conversationRepository) {
      onlineUserManager.setOnUserDisconnectCallback(
        async (userId, timestamp) => {
          try {
            await conversationRepository.updateLastSeenForUser(userId);
            console.log(`📝 lastSeen mis à jour pour ${userId} dans MongoDB`);
          } catch (err) {
            console.warn(
              `⚠️ Erreur mise à jour lastSeen MongoDB:`,
              err.message,
            );
          }
        },
      );

      // ✅ CONFIGURER LE FALLBACK MongoDB POUR lastSeen
      onlineUserManager.setConversationRepository(conversationRepository);
    }

    // ✅ INITIALISER TypingIndicatorService (après conversationRepository)
    if (redisClient && conversationRepository) {
      try {
        console.log("🚀 Initialisation TypingIndicatorService...");
        typingIndicatorService = new TypingIndicatorService(
          redisClient,
          io,
          conversationRepository,
        );
        await typingIndicatorService.startConsumer();
        app.locals.typingIndicatorService = typingIndicatorService;
        console.log("   ✅ TypingIndicatorService initialisé");
      } catch (error) {
        console.error(
          "❌ Erreur initialisation TypingIndicatorService:",
          error.message,
        );
      }
    }

    const fileRepository = new CachedFileRepository(
      mongoFileRepository,
      cacheServiceInstance,
    );

    // ===============================
    // CRÉER LE SERVICE RÉSILIENT
    // ===============================
    let resilientMessageService = null;
    if (redisClient && messageRepository) {
      resilientMessageService = new ResilientMessageService(
        redisClient,
        messageRepository,
        mongoMessageRepository,
        mongoConversationRepository,
        io, // ✅ PASSER Socket.io DIRECTEMENT
      );

      // ✅ DÉMARRER LES WORKERS INTERNES (PAS BESOIN D'UN WORKER SÉPARÉ)
      await resilientMessageService.startWorkers();

      // ✅ INJECTER LE SERVICE DANS LE REPOSITORY POUR LES ÉVÉNEMENTS
      mongoConversationRepository.resilientMessageService =
        resilientMessageService;

      // ✅ PASSER LA RÉFÉRENCE À messageDeliveryService POUR LA VÉRIFICATION D'ÉTAT ONLINE
      if (messageDeliveryService) {
        resilientMessageService.messageDeliveryService = messageDeliveryService;
        console.log(
          "✅ Référence messageDeliveryService injectée dans resilientMessageService",
        );
      }

      app.locals.resilientMessageService = resilientMessageService;
      console.log(
        "✅ ResilientMessageService avec workers et synchronisation démarré",
      );
    }

    // ===============================
    // 7. INITIALISATION USE CASES
    // ===============================

    // ✅ INITIALISER getFileUseCase EN PREMIER (requis par SendMessage)
    const getFileUseCase = new GetFile(
      fileRepository, // Cached
      cacheServiceInstance,
    );

    // ✅ PASSER resilientService À SendMessage
    const sendMessageUseCase = new SendMessage(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance,
      resilientMessageService, // ← NOUVEAU
      null, // userCacheService
      getFileUseCase, // ✅ AJOUT DE getFileUseCase
      encryptionService, // ✅ Chiffrement E2EE
      keyManagementService, // ✅ Gestion clés publiques
    );

    const getMessagesUseCase = new GetMessages(
      messageRepository, // Cached
    );

    const getConversationUseCase = new GetConversation(
      conversationRepository, // Cached
      messageRepository, // Cached
      cacheServiceInstance,
    );

    const getConversationsUseCase = new GetConversations(
      conversationRepository, // Cached
      messageRepository, // Cached
      cacheServiceInstance,
      onlineUserManager, // ✅ AJOUTÉ pour statuts de présence
    );

    const updateMessageStatusUseCase = new UpdateMessageStatus(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance,
    );

    const archiveConversationUseCase = new ArchiveConversation(
      conversationRepository,
    );
    const getArchivedConversationsUseCase = new GetArchivedConversations(
      conversationRepository,
      onlineUserManager,
    );

    const updateMessageContentUseCase = new UpdateMessageContent(
      messageRepository, // Cached
      conversationRepository, // ✅ AJOUTÉ pour récupérer les participants
      null, // kafkaProducer
      resilientMessageService, // ✅ AJOUTÉ pour publication events:messages
    );

    const uploadFileUseCase = new UploadFile(
      fileRepository, // Cached
      null, // kafkaProducer
      resilientMessageService, // ✅ AJOUTÉ pour publication events:files
    );

    const getConversationIdsUseCase = new GetConversationIds(
      conversationRepository, // Cached
    );

    const getMessageByIdUseCase = new GetMessageById(
      messageRepository, // Cached
    );

    const downloadFileUseCase = new DownloadFile(
      fileRepository, // Cached
      fileStorageService,
      resilientMessageService, // Pour publier les événements de téléchargement
    );

    const createGroupUseCase = new CreateGroup(
      conversationRepository, // Cached
      resilientMessageService, // Pour publier les notifications système
    );
    const createBroadcastUseCase = new CreateBroadcast(
      conversationRepository, // Cached
      resilientMessageService, // Pour publier les notifications système
    );

    const addAdminUseCase = new AddAdmin(
      conversationRepository,
      resilientMessageService,
    );

    const markMessageDeliveredUseCase = new MarkMessageDelivered(
      messageRepository, // Cached
      conversationRepository, // Cached
      null, // kafkaProducer
      resilientMessageService, // ✅ AJOUTÉ pour publication events:messages
    );

    const forwardMessageUseCase = new ForwardMessage(
      messageRepository,
      sendMessageUseCase,
    );

    const replyMessageUseCase = new ReplyMessage(
      messageRepository,
      sendMessageUseCase,
    );

    const searchOccurrencesUseCase = new SearchOccurrences({
      fileRepository,
      conversationRepository,
      messageRepository,
    });

    // ✅ NOUVEAUX USE CASES - Réactions
    const addReactionUseCase = new AddReaction(
      messageRepository,
      resilientMessageService,
    );

    const removeReactionUseCase = new RemoveReaction(
      messageRepository,
      resilientMessageService,
    );

    const markMessageReadUseCase = new MarkMessageRead(
      messageRepository, // Cached
      conversationRepository, // Cached
      null, // kafkaProducer
      resilientMessageService, // ✅ AJOUTÉ pour publication events:messages
    );

    // ✅ NOUVEAUX USE CASES - Gestion participants
    const userCacheService = new UserCacheService();

    const addParticipantUseCase = new AddParticipant(
      conversationRepository,
      resilientMessageService,
      userCacheService,
    );

    const removeParticipantUseCase = new RemoveParticipant(
      conversationRepository,
      resilientMessageService,
      userCacheService,
    );

    const leaveConversationUseCase = new LeaveConversation(
      conversationRepository,
      resilientMessageService,
      userCacheService,
    );

    // ✅ NOUVEAUX USE CASES - Suppression
    const deleteMessageUseCase = new DeleteMessage(
      messageRepository,
      conversationRepository,
      null, // kafkaProducer
      resilientMessageService,
    );

    const deleteFileUseCase = new DeleteFile(
      fileRepository,
      null, // kafkaProducer
      resilientMessageService,
    );

    // ===============================
    // INITIALISATION AutoGroupSyncService
    // ===============================
    const autoGroupSyncService = new AutoGroupSyncService({
      conversationRepository,
      createGroupUseCase,
      addParticipantUseCase,
      userCacheService,
      // visibilityServiceUrl: par défaut via .env
    });
    app.locals.autoGroupSyncService = autoGroupSyncService;

    const AutoGroupSyncUseCase = require("./application/use-cases/AutoGroupSync");
    const autoGroupSyncUseCase = new AutoGroupSyncUseCase(autoGroupSyncService);

    // Rendre disponibles globalement (injection simple pour controllers / handlers)
    app.locals.useCases = app.locals.useCases || {};
    app.locals.useCases.markMessageDelivered = markMessageDeliveredUseCase;
    app.locals.useCases.markMessageRead = markMessageReadUseCase;
    app.locals.useCases.addParticipant = addParticipantUseCase;
    app.locals.useCases.removeParticipant = removeParticipantUseCase;
    app.locals.useCases.leaveConversation = leaveConversationUseCase;
    app.locals.useCases.deleteMessage = deleteMessageUseCase;
    app.locals.useCases.deleteFile = deleteFileUseCase;
    app.locals.repositories = {
      message: messageRepository,
      conversation: conversationRepository,
      file: fileRepository,
    };

    // ===============================
    // 8. INITIALISATION CONTROLLERS
    // ===============================
    const fileController = new FileController(
      uploadFileUseCase,
      getFileUseCase,
      redisClient,
      fileStorageService,
      downloadFileUseCase,
      mediaProcessingService,
      null, // searchOccurrencesUseCase
      chunkedUploadService, // ✅ Upload chunké > 100 MB
      encryptionService, // ✅ Chiffrement E2EE
      keyManagementService, // ✅ Gestion clés publiques
    );

    const messageController = new MessageController(
      sendMessageUseCase,
      getMessagesUseCase,
      updateMessageStatusUseCase,
      redisClient,
    );

    const conversationController = new ConversationController(
      getConversationsUseCase,
      getConversationUseCase,
      redisClient,
      null, // cacheService
      searchOccurrencesUseCase,
      archiveConversationUseCase,
      getArchivedConversationsUseCase,
    );

    const groupController = new GroupController({
      createGroupUseCase,
      getConversationUseCase,
      addParticipantUseCase,
      removeParticipantUseCase,
      leaveConversationUseCase,
      addAdminUseCase,
      searchOccurrencesUseCase,
    });

    const healthController = new HealthController(redisClient);

    // ===============================
    // 9. CONFIGURATION ROUTES HTTP
    // ===============================

    // ✅ IMPORT ET CONFIGURATION DES ROUTES CONVERSATIONS
    const createConversationRoutes = require("./interfaces/http/routes/conversationRoutes");
    const createMessageRoutes = require("./interfaces/http/routes/messageRoutes");
    const createFileRoutes = require("./interfaces/http/routes/fileRoutes");
    const createHealthRoutes = require("./interfaces/http/routes/healthRoutes");
    const createGroupRoutes = require("./interfaces/http/routes/groupRoutes");
    const createBroadcastRoutes = require("./interfaces/http/routes/broadcastRoutes");

    app.use("/files", createFileRoutes(fileController, auth));
    app.use("/messages", createMessageRoutes(messageController, auth));
    // ✅ AJOUTER LA ROUTE CONVERSATIONS
    app.use(
      "/conversations",
      createConversationRoutes(conversationController, auth),
    );
    app.use("/health", createHealthRoutes(healthController));
    app.use("/groups", createGroupRoutes(createGroupUseCase, auth));
    app.use("/broadcasts", createBroadcastRoutes(createBroadcastUseCase, auth));
    app.use("/groups", createGroupRoutes(groupController, auth));

    // ===============================
    // 10. CONFIGURATION WEBSOCKET
    // ===============================
    console.log("🔌 Configuration du gestionnaire WebSocket...");

    // ✅ CRÉER LE CHATHANDLER SANS UserConsumerManager

    const UpdateCallStatus = require("./application/use-cases/UpdateCallStatus");
    const updateCallStatusUseCase = new UpdateCallStatus(
      messageRepository,
      resilientMessageService,
    );

    const chatHandler = new ChatHandler(
      io,
      sendMessageUseCase,
      getMessagesUseCase,
      updateMessageStatusUseCase,
      onlineUserManager, // ← VÉRIFIER QU'IL EST PASSÉ (5e paramètre)
      getConversationIdsUseCase,
      getConversationUseCase,
      getConversationsUseCase,
      getMessageByIdUseCase,
      updateMessageContentUseCase,
      createGroupUseCase,
      createBroadcastUseCase,
      roomManager,
      markMessageDeliveredUseCase,
      markMessageReadUseCase,
      resilientMessageService,
      messageDeliveryService,
      null, // userCacheService
      addParticipantUseCase,
      removeParticipantUseCase,
      leaveConversationUseCase,
      deleteMessageUseCase,
      deleteFileUseCase,
      updateCallStatusUseCase,
      forwardMessageUseCase,
      addReactionUseCase,
      removeReactionUseCase,
      replyMessageUseCase,
      autoGroupSyncUseCase,
      encryptionService, // ✅ E2EE
      keyManagementService, // ✅ E2EE
      archiveConversationUseCase, // ✅ Archivage
      getArchivedConversationsUseCase, // ✅ Archivage
    );

    // ✅ CONFIGURER LES GESTIONNAIRES D'ÉVÉNEMENTS SOCKET.IO
    chatHandler.setupSocketHandlers();

    console.log("✅ ChatHandler configuré avec succès");

    // ===============================
    // 10. ROUTES PERSONNALISÉES
    // ===============================

    // Route de health check détaillée
    app.get("/health", async (req, res) => {
      try {
        const redisStatus = redisClient ? "✅ Connecté" : "⚠️ Déconnecté";

        let redisHealthStatus = "Non connecté";
        let connectedUsersCount = 0;
        let onlineUsersCount = 0;
        let activeRoomsCount = 0;

        // Health check Redis sécurisé
        if (redisClient) {
          try {
            redisHealthStatus = await redisConfig.getHealthStatus();
          } catch (error) {
            console.warn("⚠️ Erreur health check Redis:", error.message);
            redisHealthStatus = `Erreur: ${error.message}`;
          }
        }

        // Stats utilisateurs sécurisées
        try {
          connectedUsersCount = chatHandler
            ? chatHandler.getConnectedUserCount()
            : 0;
        } catch (error) {
          console.warn("⚠️ Erreur count users:", error.message);
        }

        try {
          onlineUsersCount = onlineUserManager
            ? await onlineUserManager.getOnlineUsersCount()
            : 0;
        } catch (error) {
          console.warn("⚠️ Erreur online users:", error.message);
        }

        try {
          activeRoomsCount = roomManager
            ? await roomManager.getRoomsCount()
            : 0;
        } catch (error) {
          console.warn("⚠️ Erreur rooms count:", error.message);
        }

        const health = {
          service: "CENADI Chat-File-Service",
          version: "1.0.0",
          status: "running",
          timestamp: new Date().toISOString(),
          serverId: process.env.SERVER_ID || "chat-file-1",
          services: {
            mongodb: "✅ Connecté",
            redis: {
              status: redisStatus,
              details: redisHealthStatus,
            },
            websocket: "✅ Actif",
          },
          endpoints: {
            files: "/files",
            messages: "/messages",
            conversations: "/conversations",
            health: "/health",
            stats: "/stats",
            interface: "/",
          },
          features: {
            chat: !!messageController,
            fileUpload: !!fileController,
            caching: !!redisClient,
            userManagement: !!onlineUserManager,
            roomManagement: !!roomManager,
          },
          stats: {
            connectedUsers: connectedUsersCount,
            onlineUsers: onlineUsersCount,
            activeRooms: activeRoomsCount,
          },
        };
        res.json(health);
      } catch (error) {
        console.error("❌ Erreur health check:", error);
        res.status(500).json({
          service: "CENADI Chat-File-Service",
          status: "error",
          error: error.message,
          timestamp: new Date().toISOString(),
        });
      }
    });

    // Route de statistiques
    app.get("/stats", async (req, res) => {
      try {
        // ✅ PROTECTION CONTRE LES ERREURS
        let websocketStats = { connectedUsers: 0, stats: {} };
        let redisStats = {
          isConnected: false,
          onlineUsers: 0,
          activeRooms: 0,
          usersList: [],
          roomsList: [],
        };

        // Stats WebSocket sécurisées
        try {
          if (
            chatHandler &&
            typeof chatHandler.getConnectedUserCount === "function"
          ) {
            websocketStats = {
              connectedUsers: chatHandler.getConnectedUserCount(),
              stats:
                typeof chatHandler.getStats === "function"
                  ? chatHandler.getStats()
                  : {},
            };
          }
        } catch (error) {
          console.warn("⚠️ Erreur stats WebSocket:", error.message);
        }

        // Stats Redis sécurisées
        try {
          if (redisClient) {
            redisStats = {
              isConnected: true,
              onlineUsers: onlineUserManager
                ? await onlineUserManager.getOnlineUsersCount()
                : 0,
              activeRooms: roomManager ? await roomManager.getRoomsCount() : 0,
              usersList: onlineUserManager
                ? await onlineUserManager.getOnlineUsers()
                : [],
              roomsList: roomManager ? await roomManager.getRooms() : [],
            };
          }
        } catch (error) {
          console.warn("⚠️ Erreur stats Redis:", error.message);
          redisStats.error = error.message;
        }

        const stats = {
          timestamp: new Date().toISOString(),
          websocket: websocketStats,
          redis: redisStats,
        };
        res.json(stats);
      } catch (error) {
        console.error("❌ Erreur stats:", error);
        res.status(500).json({
          error: error.message,
          timestamp: new Date().toISOString(),
        });
      }
    });

    // Route principale
    app.get("/", (req, res) => {
      res.json({
        service: "CENADI Chat-File-Service",
        version: "1.0.0",
        status: "running",
        timestamp: new Date().toISOString(),
        endpoints: {
          files: "/files",
          messages: "/messages",
          conversations: "/conversations",
          health: "/health",
          stats: "/stats",
        },
        features: {
          chat: "✅ Chat en temps réel",
          files: "✅ Upload/Download fichiers",
          websocket: "✅ WebSocket activé",
          redis: redisClient ? "✅ Redis activé" : "⚠️ Mode mémoire locale",
          userManagement: onlineUserManager
            ? "✅ Gestion utilisateurs"
            : "⚠️ Non disponible",
          roomManagement: roomManager
            ? "✅ Gestion salons"
            : "⚠️ Non disponible",
        },
      });
    });

    // ===============================
    // 12. GESTION D'ERREURS
    // ===============================
    app.use((error, req, res, next) => {
      console.error("❌ Erreur serveur:", error);
      if (res.headersSent) {
        return next(error);
      }
      res.status(error.status || 500).json({
        success: false,
        message: error.message || "Erreur interne du serveur",
        error: process.env.NODE_ENV === "development" ? error.stack : undefined,
        timestamp: new Date().toISOString(),
      });
    });

    app.use((req, res) => {
      res.status(404).json({
        success: false,
        message: "Endpoint non trouvé",
        path: req.path,
        method: req.method,
        timestamp: new Date().toISOString(),
      });
    });

    // ===============================
    // 13. TÂCHES DE MAINTENANCE
    // ===============================

    // Maintenance Redis
    if (onlineUserManager && roomManager) {
      setInterval(
        async () => {
          try {
            console.log("🧹 Nettoyage périodique Redis...");
            const cleanedUsers = await onlineUserManager.cleanupInactiveUsers();
            const cleanedRooms = await roomManager.cleanupInactiveRooms();

            if (cleanedUsers > 0 || cleanedRooms > 0) {
              console.log(
                `🧹 Nettoyage terminé: ${cleanedUsers} utilisateurs, ${cleanedRooms} salons`,
              );
            }
          } catch (error) {
            console.error("❌ Erreur nettoyage Redis:", error);
          }
        },
        30 * 60 * 1000,
      ); // 30 minutes
    }

    // ===============================
    // 14. DÉMARRAGE SERVEUR
    // ===============================
    const PORT = process.env.CHAT_FILE_SERVICE_PORT || 8003;
    server.listen(PORT, async () => {
      console.log(`🚀 Chat-File Service démarré sur le port ${PORT}`);
      console.log(`🌍 Serveur ID: ${process.env.SERVER_ID || "chat-file-1"}`);

      console.log("📋 Fonctionnalités disponibles:");
      console.log("   💬 Chat en temps réel");
      console.log("   📁 Upload/Download de fichiers");
      console.log("   🖼️ Traitement d'images");
      console.log("   📱 Interface web");
      console.log("   👥 Gestion utilisateurs en ligne");
      console.log("   🏠 Gestion des salons");
      console.log("   📊 Monitoring Redis");
      console.log("   🗄️ Cache utilisateur centralisé (UserCache)");
      console.log("   🔄 Synchronisation profils utilisateurs");

      console.log("\n📊 Statut des services:");
      console.log(`   MongoDB: ✅ Connecté`);
      console.log(
        `   Redis:   ${redisClient ? "✅ Connecté" : "⚠️ Mode mémoire locale"}`,
      );
      console.log(
        `   UserMgr: ${onlineUserManager ? "✅ Actif" : "⚠️ Désactivé"}`,
      );
      console.log(`   RoomMgr: ${roomManager ? "✅ Actif" : "⚠️ Désactivé"}`);
      console.log(`   UserCache: ${UserCache ? "✅ Actif" : "⚠️ Désactivé"}`);

      console.log("\n" + "=".repeat(70));
      console.log("🎯 LIENS RAPIDES - CHAT-FILE-SERVICE");
      console.log("=".repeat(70));
      console.log(`🌐 Interface Web     : http://localhost:${PORT}/`);
      console.log(`📁 API Fichiers     : http://localhost:${PORT}/files`);
      console.log(`💬 API Messages     : http://localhost:${PORT}/messages`);
      console.log(
        `🗣️ API Conversations: http://localhost:${PORT}/conversations`,
      );
      console.log(`📊 Statistiques     : http://localhost:${PORT}/stats`);
      console.log(`🔌 WebSocket        : ws://localhost:${PORT}`);
      console.log(`❤️ Health Check     : http://localhost:${PORT}/health`);
      console.log("=".repeat(70) + "\n");
    });
  } catch (error) {
    console.error("❌ Erreur au démarrage:", error);
    process.exit(1);
  }

  // ===============================
  // GESTION FERMETURE PROPRE
  // ===============================
  const gracefulShutdown = async () => {
    console.log("🛑 Arrêt gracieux du service...");

    try {
      // ✅ ARRÊTER LE MESSAGE DELIVERY SERVICE (Redis Streams Consumer)
      if (
        typeof messageDeliveryService !== "undefined" &&
        messageDeliveryService
      ) {
        messageDeliveryService.stopConsumer();
        console.log("✅ MessageDeliveryService arrêté");
      }

      // ✅ ARRÊTER LES WORKERS INTERNES (ResilientMessageService)
      if (
        typeof resilientMessageService !== "undefined" &&
        resilientMessageService
      ) {
        if (resilientMessageService.stopWorkers) {
          resilientMessageService.stopWorkers();
        }
        if (resilientMessageService.memoryMonitorInterval) {
          clearInterval(resilientMessageService.memoryMonitorInterval);
        }
        if (resilientMessageService.trimInterval) {
          clearInterval(resilientMessageService.trimInterval);
        }
        if (resilientMessageService.metricsInterval) {
          clearInterval(resilientMessageService.metricsInterval);
        }
        console.log("✅ ResilientMessageService arrêté");
      }

      // ✅ FERMER LE CLIENT REDIS STREAMS (séparé du client principal)
      if (typeof redisStreamsClient !== "undefined" && redisStreamsClient) {
        try {
          await redisStreamsClient.quit();
          console.log("✅ Redis Streams Client déconnecté");
        } catch (err) {
          console.warn(
            "⚠️ Erreur fermeture Redis Streams Client:",
            err.message,
          );
        }
      }

      // ✅ FERMER LE CLIENT REDIS PRINCIPAL
      if (typeof redisClient !== "undefined" && redisClient) {
        try {
          await redisClient.quit();
          console.log("✅ Redis déconnecté");
        } catch (err) {
          console.warn("⚠️ Erreur fermeture Redis:", err.message);
        }
      }

      // ✅ FERMER LA CONNEXION MONGODB
      if (typeof mongoConnection !== "undefined" && mongoConnection) {
        try {
          await mongoConnection.close();
          console.log("✅ MongoDB déconnecté");
        } catch (err) {
          console.warn("⚠️ Erreur fermeture MongoDB:", err.message);
        }
      }

      console.log("✅ Arrêt gracieux complété");
      process.exit(0);
    } catch (error) {
      console.error("❌ Erreur arrêt gracieux:", error.message);
      process.exit(1);
    }
  };

  process.on("SIGTERM", gracefulShutdown);
  process.on("SIGINT", gracefulShutdown);

  process.on("uncaughtException", (error) => {
    console.error("❌ Exception non gérée:", error);
    process.exit(1);
  });

  process.on("unhandledRejection", (reason, promise) => {
    console.error("❌ Promesse rejetée non gérée:", reason);
    process.exit(1);
  });
};

if (require.main === module) {
  startServer();
} else {
  module.exports = { startServer };
}
