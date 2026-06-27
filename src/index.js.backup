const express = require("express");
const { createServer } = require("http");
const cors = require("cors");
const path = require("path");
const { Server } = require("socket.io");
const { createAdapter } = require("@socket.io/redis-adapter");

// Configuration
require("dotenv").config();

// ✅ VALIDATION ENVIRONNEMENT
const EnvironmentValidator = require("./config/envValidator");
const envValidator = new EnvironmentValidator();

if (!envValidator.validate()) {
  console.error("❌ Configuration environnement invalide. Arrêt du service.");
  process.exit(1);
}

// Infrastructure
const connectDB = require("./infrastructure/mongodb/connection");
const redisConfig = require("./infrastructure/redis/redisConfig");

// Services
const ThumbnailService = require("./infrastructure/services/ThumbnailService");
const FileStorageService = require("./infrastructure/services/FileStorageService");
const MediaProcessingService = require("./infrastructure/services/MediaProcessingService");
const ResilientMessageService = require("./infrastructure/services/ResilientMessageService");

// Repositories - Cached
const CachedMessageRepository = require("./infrastructure/repositories/CachedMessageRepository");
const CachedConversationRepository = require("./infrastructure/repositories/CachedConversationRepository");
const CachedFileRepository = require("./infrastructure/repositories/CachedFileRepository");

// Redis Services
const CacheService = require("./infrastructure/redis/CacheService");
const RoomManager = require("./infrastructure/redis/RoomManager");
const OnlineUserManager = require("./infrastructure/redis/OnlineUserManager");
const UnreadManager = require("./infrastructure/redis/UnreadMessageManager");
const MessageDeliveryService = require("./infrastructure/services/MessageDeliveryService");

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
const CreateBroadcast = require("./application/use-cases/CreateBroadcast");
const MarkMessageDelivered = require("./application/use-cases/MarkMessageDelivered");
const MarkMessageRead = require("./application/use-cases/MarkMessageRead");

// Controllers
const FileController = require("./application/controllers/FileController");
const MessageController = require("./application/controllers/MessageController");
const ConversationController = require("./application/controllers/ConversationController");
const HealthController = require("./application/controllers/HealthController");

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
const { rateLimitMiddleware } = require("./interfaces/http/middleware");

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
      const redisConnected = await redisConfig.connect();
      if (redisConnected) {
        redisClient = redisConfig.getClient();

        // Initialiser CacheService
        cacheServiceInstance = new CacheService(redisClient, {
          defaultTTL: 3600,
          keyPrefix: "chat",
          maxScanCount: 1000,
        });

        // Initialiser RoomManager
        roomManager = new RoomManager(redisClient);
        app.locals.roomManager = roomManager;

        // ✅ INITIALISER OnlineUserManager
        onlineUserManager = new OnlineUserManager(redisClient, io);

        console.log("✅ Services Redis initialisés:");
        console.log("   ✅ CacheService");
        console.log("   ✅ RoomManager");
        console.log("   ✅ OnlineUserManager");
      }
    } catch (error) {
      console.warn("⚠️ Redis non disponible:", error.message);
      onlineUserManager = null;
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
      })
    );

    app.use(express.json({ limit: "10mb" }));
    app.use(express.urlencoded({ extended: true, limit: "10mb" }));

    app.locals.redisClient = redisClient;

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
        io.adapter(
          createAdapter(
            redisConfig.createPubClient(),
            redisConfig.createSubClient()
          )
        );
        console.log("✅ Redis adapter Socket.IO configuré");
      } catch (error) {
        console.warn("⚠️ Erreur config Redis adapter:", error.message);
      }
    }

    // Initialiser OnlineUserManager
    onlineUserManager = new OnlineUserManager(redisClient, io, {
      keyPrefix: "chat:online",
      userTTL: 3600,
      heartbeatInterval: 30000,
      maxScanCount: 1000,
    });

    // ✅ INITIALISER MessageDeliveryService MAINTENANT QUE IO EST CRÉÉ
    let messageDeliveryService = null;
    if (redisClient) {
      try {
        console.log("🚀 Initialisation MessageDeliveryService...");
        messageDeliveryService = new MessageDeliveryService(redisClient, io);
        console.log("⏳ Attente de l'initialisation du consumer...");
        await messageDeliveryService.initialize();
        app.locals.messageDeliveryService = messageDeliveryService;
        console.log("   ✅ MessageDeliveryService initialisé");
      } catch (error) {
        console.error(
          "❌ Erreur initialisation MessageDeliveryService:",
          error.message
        );
      }
    } else {
      console.log("⚠️ Redis non disponible, MessageDeliveryService non créé");
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

    console.log("✅ Services de fichiers initialisés");

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
      cacheServiceInstance
    );

    // ✅ SUPPRIMER UnreadManager séparé si present
    // const unreadManager = new UnreadManager(cacheServiceInstance);

    // ✅ CRÉER CachedMessageRepository SANS UnreadManager
    const messageRepository = new CachedMessageRepository(
      mongoMessageRepository,
      cacheServiceInstance
      // ← unreadManager SUPPRIMÉ - intégré dans CachedMessageRepository
    );

    const conversationRepository = new CachedConversationRepository(
      mongoConversationRepository,
      cacheServiceInstance
    );

    const fileRepository = new CachedFileRepository(
      mongoFileRepository,
      cacheServiceInstance
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
        io // ✅ PASSER Socket.io DIRECTEMENT
      );

      // ✅ DÉMARRER LES WORKERS INTERNES (PAS BESOIN D'UN WORKER SÉPARÉ)
      await resilientMessageService.startWorkers();

      // resilientMessageService.nukeAllRedisData(); //
      // ✅ NOUVELLE : SYNCHRONISER LES MESSAGES EXISTANTS
      // console.log(
      //   "🔄 Démarrage de la synchronisation MongoDB → Redis Streams..."
      // );
      // const syncResult =
      //   await resilientMessageService.syncExistingMessagesToStream();
      // console.log(
      //   `✅ Synchronisation complétée: ${syncResult.synced} messages, ${syncResult.errors} erreur(s)`
      // );

      app.locals.resilientMessageService = resilientMessageService;
      console.log(
        "✅ ResilientMessageService avec workers et synchronisation démarré"
      );
    }

    // ===============================
    // 7. INITIALISATION USE CASES
    // ===============================

    // ✅ PASSER resilientService À SendMessage
    const sendMessageUseCase = new SendMessage(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance,
      resilientMessageService // ← NOUVEAU
    );

    const getMessagesUseCase = new GetMessages(
      messageRepository // Cached
    );

    const getConversationUseCase = new GetConversation(
      conversationRepository, // Cached
      messageRepository, // Cached
      cacheServiceInstance
    );

    const getConversationsUseCase = new GetConversations(
      conversationRepository, // Cached
      messageRepository, // Cached
      cacheServiceInstance
    );

    const updateMessageStatusUseCase = new UpdateMessageStatus(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance
    );

    const updateMessageContentUseCase = new UpdateMessageContent(
      messageRepository, // Cached
      cacheServiceInstance
    );

    const uploadFileUseCase = new UploadFile(
      fileRepository, // Cached
      null
    );

    const getFileUseCase = new GetFile(
      fileRepository, // Cached
      cacheServiceInstance
    );

    const getConversationIdsUseCase = new GetConversationIds(
      conversationRepository // Cached
    );

    const getMessageByIdUseCase = new GetMessageById(
      messageRepository // Cached
    );

    const downloadFileUseCase = new DownloadFile(
      fileRepository, // Cached
      fileStorageService,
      cacheServiceInstance
    );

    const createGroupUseCase = new CreateGroup(
      conversationRepository // Cached
    );
    const createBroadcastUseCase = new CreateBroadcast(
      conversationRepository // Cached
    );

    const markMessageDeliveredUseCase = new MarkMessageDelivered(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance
    );

    const markMessageReadUseCase = new MarkMessageRead(
      messageRepository, // Cached
      conversationRepository, // Cached
      cacheServiceInstance
    );

    // Rendre disponibles globalement (injection simple pour controllers / handlers)
    app.locals.useCases = app.locals.useCases || {};
    app.locals.useCases.markMessageDelivered = markMessageDeliveredUseCase;
    app.locals.useCases.markMessageRead = markMessageReadUseCase;
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
      mediaProcessingService
    );

    const messageController = new MessageController(
      sendMessageUseCase,
      getMessagesUseCase,
      updateMessageStatusUseCase,
      redisClient
    );

    const conversationController = new ConversationController(
      getConversationsUseCase,
      getConversationUseCase,
      redisClient
    );

    const healthController = new HealthController(redisClient);

    // ===============================
    // 9. CONFIGURATION ROUTES HTTP
    // ===============================

    // ✅ IMPORT ET CONFIGURATION DES ROUTES CONVERSATIONS
    const createConversationRoutes = require("./interfaces/http/routes/conversationRoutes");

    app.use("/files", createFileRoutes(fileController));
    app.use("/messages", createMessageRoutes(messageController));
    // ✅ AJOUTER LA ROUTE CONVERSATIONS
    app.use("/conversations", createConversationRoutes(conversationController));
    app.use("/health", createHealthRoutes(healthController));
    app.use("/groups", createGroupRoutes(createGroupUseCase));
    app.use("/broadcasts", createBroadcastRoutes(createBroadcastUseCase));

    // ===============================
    // 10. CONFIGURATION WEBSOCKET
    // ===============================
    console.log("🔌 Configuration du gestionnaire WebSocket...");

    // ✅ CRÉER LE CHATHANDLER SANS UserConsumerManager
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
      messageDeliveryService
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
      setInterval(async () => {
        try {
          console.log("🧹 Nettoyage périodique Redis...");
          const cleanedUsers = await onlineUserManager.cleanupInactiveUsers();
          const cleanedRooms = await roomManager.cleanupInactiveRooms();

          if (cleanedUsers > 0 || cleanedRooms > 0) {
            console.log(
              `🧹 Nettoyage terminé: ${cleanedUsers} utilisateurs, ${cleanedRooms} salons`
            );
          }
        } catch (error) {
          console.error("❌ Erreur nettoyage Redis:", error);
        }
      }, 30 * 60 * 1000); // 30 minutes
    }

    // ===============================
    // 14. DÉMARRAGE SERVEUR
    // ===============================
    const PORT = process.env.CHAT_FILE_SERVICE_PORT || 8003;
    server.listen(PORT, () => {
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

      console.log("\n📊 Statut des services:");
      console.log(`   MongoDB: ✅ Connecté`);
      console.log(
        `   Redis:   ${redisClient ? "✅ Connecté" : "⚠️ Mode mémoire locale"}`
      );
      console.log(
        `   UserMgr: ${onlineUserManager ? "✅ Actif" : "⚠️ Désactivé"}`
      );
      console.log(`   RoomMgr: ${roomManager ? "✅ Actif" : "⚠️ Désactivé"}`);

      console.log("\n" + "=".repeat(70));
      console.log("🎯 LIENS RAPIDES - CHAT-FILE-SERVICE");
      console.log("=".repeat(70));
      console.log(`🌐 Interface Web     : http://localhost:${PORT}/`);
      console.log(`📁 API Fichiers     : http://localhost:${PORT}/files`);
      console.log(`💬 API Messages     : http://localhost:${PORT}/messages`);
      console.log(
        `🗣️ API Conversations: http://localhost:${PORT}/conversations`
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
};

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
        console.warn("⚠️ Erreur fermeture Redis Streams Client:", err.message);
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

if (require.main === module) {
  startServer();
} else {
  module.exports = { startServer };
}
