/**
 * Gestionnaire WebSocket pour le chat en temps réel
 * ✅ RESPONSABILITÉ UNIQUE : Gérer les événements WebSocket
 * ✅ PAS DE REDIS, PAS DE KAFKA → Déléguer aux Use Cases
 */
// const AuthMiddleware = require("../../interfaces/http/middleware/authMiddleware");
const AuthMiddleware = require("../../../shared/auth/valide-token.middleware");
const UserCacheService = require("../../infrastructure/services/UserCacheService");

class ChatHandler {
  constructor(
    io,
    sendMessageUseCase,
    getMessagesUseCase,
    updateMessageStatusUseCase,
    onlineUserManager,
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
    resilientMessageService = null,
    messageDeliveryService = null,
    userCacheService = null,
    addParticipantUseCase = null,
    removeParticipantUseCase = null,
    leaveConversationUseCase = null,
    deleteMessageUseCase = null,
    deleteFileUseCase = null,
    updateCallStatusUseCase = null,
    forwardMessageUseCase = null,
    addReactionUseCase = null,
    removeReactionUseCase = null,
    replyMessageUseCase = null,
    autoGroupSyncUseCase = null,
    encryptionService = null, // ✅ E2EE
    keyManagementService = null, // ✅ E2EE
    archiveConversationUseCase = null, // ✅ Archivage
    getArchivedConversationsUseCase = null, // ✅ Archivage
  ) {
    this.io = io;
    this.sendMessageUseCase = sendMessageUseCase;
    this.resilientService = resilientMessageService;
    this.getMessagesUseCase = getMessagesUseCase;
    this.updateMessageStatusUseCase = updateMessageStatusUseCase;
    this.onlineUserManager = onlineUserManager;
    this.getConversationIdsUseCase = getConversationIdsUseCase;
    this.getConversationUseCase = getConversationUseCase;
    this.getConversationsUseCase = getConversationsUseCase;
    this.getMessageByIdUseCase = getMessageByIdUseCase;
    this.updateMessageContentUseCase = updateMessageContentUseCase;
    this.createGroupUseCase = createGroupUseCase;
    this.createBroadcastUseCase = createBroadcastUseCase;
    this.roomManager = roomManager;
    this.markMessageDeliveredUseCase = markMessageDeliveredUseCase;
    this.markMessageReadUseCase = markMessageReadUseCase;
    this.messageDeliveryService = messageDeliveryService;
    this.userCacheService = userCacheService || new UserCacheService();
    this.addParticipantUseCase = addParticipantUseCase;
    this.removeParticipantUseCase = removeParticipantUseCase;
    this.leaveConversationUseCase = leaveConversationUseCase;
    this.deleteMessageUseCase = deleteMessageUseCase;
    this.deleteFileUseCase = deleteFileUseCase;
    this.updateCallStatusUseCase = updateCallStatusUseCase;
    this.forwardMessageUseCase = forwardMessageUseCase;
    this.addReactionUseCase = addReactionUseCase;
    this.removeReactionUseCase = removeReactionUseCase;
    this.replyMessageUseCase = replyMessageUseCase;
    this.autoGroupSyncUseCase = autoGroupSyncUseCase;
    this.encryptionService = encryptionService; // ✅ E2EE
    this.keyManagementService = keyManagementService; // ✅ E2EE
    this.archiveConversationUseCase = archiveConversationUseCase; // ✅ Archivage
    this.getArchivedConversationsUseCase = getArchivedConversationsUseCase; // ✅ Archivage

    // ✅ LOG DE DEBUG
    console.log(
      "🔍 ChatHandler reçu messageDeliveryService:",
      this.messageDeliveryService ? "✅ OUI" : "❌ NON",
    );

    console.log(
      "🔍 ChatHandler reçu autoGroupSyncUseCase:",
      this.autoGroupSyncUseCase ? "✅ OUI" : "❌ NON",
    );
  }

  setupSocketHandlers() {
    try {
      console.log("🔌 Configuration des gestionnaires Socket.IO...");

      this.io.on("connection", (socket) => {
        console.log(`🔗 Nouvelle connexion WebSocket: ${socket.id}`);

        socket.on("authenticate", async (data) => {
          try {
            await this.handleAuthentication(socket, data);
          } catch (err) {
            console.error("❌ Erreur authentification:", err.message);
            socket.emit("auth_error", {
              message: "Erreur lors de l'authentification",
              code: "AUTH_ERROR",
            });
          }
        });

        // Événement on-demand pour synchroniser les groupes auto (appel explicite par le client)
        socket.on("syncAutoGroups", async (data) => {
          try {
            if (!socket.userId) {
              socket.emit("autoGroupsSyncError", {
                success: false,
                message: "Utilisateur non authentifié",
              });
              return;
            }

            if (!this.autoGroupSyncUseCase) {
              socket.emit("autoGroupsSyncError", {
                success: false,
                message: "Service de synchronisation non disponible",
              });
              return;
            }

            const result = await this.autoGroupSyncUseCase.execute(
              socket.userId,
            );

            socket.emit("autoGroupsSynced", { success: true, result });
            console.log(
              `[AutoGroupSync] Groupes synchronisés pour ${socket.userId}:`,
              result,
            );
          } catch (err) {
            console.warn(
              `[AutoGroupSync] Erreur sync groupes (socket request): ${socket.userId}:`,
              err.message,
            );
            socket.emit("autoGroupsSyncError", {
              success: false,
              message: err.message || "Erreur synchronisation",
            });
          }
        });

        socket.on("sendMessage", (data) => {
          console.log("💬 Envoi message:", data);
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleSendMessage(socket, data);
        });

        socket.on("joinConversation", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleJoinConversation(socket, data);
        });

        socket.on("leaveConversation", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleLeaveConversation(socket, data);
        });

        socket.on("typing", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleTyping(socket, data);
        });

        socket.on("stopTyping", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleStopTyping(socket, data);
        });

        socket.on("markMessageDelivered", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleMarkMessageDelivered(socket, data);
        });

        socket.on("markMessageRead", (data) => {
          if (this.onlineUserManager && socket.userId) {
            console.log(
              `📖 Marquage lu par ${socket.matricule} (${socket.userId})...`,
            );
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleMarkMessageRead(socket, data);
        });

        socket.on("getMessages", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleGetMessages(socket, data);
        });

        socket.on("getConversations", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleGetConversations(socket, data);
        });

        socket.on("getConversation", (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          this.handleGetConversation(socket, data);
        });

        socket.on("ping", () => {
          socket.emit("pong");
          // console.log("Ping reçu, Pong emit");
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
        });

        socket.on("heartbeat", () => {
          socket.emit("heartbeat_ack");
          // console.log("Heartbeat reçu, heartbeat_ack émis");
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
        });

        // ──────────────────────────────────────────────────────────────────
        // 🔐 ÉVÉNEMENTS CHIFFREMENT E2EE
        // ──────────────────────────────────────────────────────────────────

        /**
         * Enregistre ou met à jour la clé publique RSA de l'utilisateur connecté.
         * Le client doit émettre cet événement à chaque connexion si E2EE est actif.
         *
         * Émission client  : encryption:registerKey { publicKey, deviceInfo? }
         * Réponse serveur  : encryption:keyRegistered { keyVersion, fingerprint, isRotation }
         *                 ou encryption:error { message }
         */
        socket.on("encryption:registerKey", async (data) => {
          try {
            if (!this.keyManagementService) {
              return socket.emit("encryption:error", {
                event: "encryption:registerKey",
                message: "Service de gestion des clés non disponible",
              });
            }

            const userId = socket.userId;
            if (!userId) {
              return socket.emit("encryption:error", {
                event: "encryption:registerKey",
                message: "Authentification requise",
              });
            }

            const { publicKey, deviceInfo = {} } = data || {};
            if (!publicKey) {
              return socket.emit("encryption:error", {
                event: "encryption:registerKey",
                message: "publicKey manquante",
              });
            }

            const result = await this.keyManagementService.registerPublicKey(
              userId,
              publicKey,
              deviceInfo,
            );

            socket.emit("encryption:keyRegistered", {
              userId,
              keyVersion: result.keyVersion,
              fingerprint: result.fingerprint,
              isRotation: result.isRotation,
            });

            console.log(
              `🔐 Clé publique enregistrée pour userId=${userId} v${result.keyVersion}${result.isRotation ? " (rotation)" : ""}`,
            );
          } catch (err) {
            console.error(`❌ encryption:registerKey:`, err.message);
            socket.emit("encryption:error", {
              event: "encryption:registerKey",
              message: err.message,
            });
          }
        });

        /**
         * Retourne la clé publique d'un utilisateur cible.
         * Le client l'utilise pour chiffrer les messages côté client (facultatif, server-side E2EE géré par SendMessage).
         *
         * Émission client  : encryption:getPublicKey { targetUserId }
         * Réponse serveur  : encryption:publicKey { userId, publicKey, fingerprint, keyVersion }
         *                 ou encryption:error { message }
         */
        socket.on("encryption:getPublicKey", async (data) => {
          try {
            if (!this.keyManagementService) {
              return socket.emit("encryption:error", {
                event: "encryption:getPublicKey",
                message: "Service de gestion des clés non disponible",
              });
            }

            if (!socket.userId) {
              return socket.emit("encryption:error", {
                event: "encryption:getPublicKey",
                message: "Authentification requise",
              });
            }

            const { targetUserId } = data || {};
            if (!targetUserId) {
              return socket.emit("encryption:error", {
                event: "encryption:getPublicKey",
                message: "targetUserId manquant",
              });
            }

            const publicKey = await this.keyManagementService.getPublicKey(
              String(targetUserId),
            );
            const meta = await this.keyManagementService.getKeyMetadata(
              String(targetUserId),
            );

            socket.emit("encryption:publicKey", {
              userId: targetUserId,
              publicKey,
              fingerprint: meta?.fingerprint ?? null,
              keyVersion: meta?.keyVersion ?? null,
            });
          } catch (err) {
            console.error(`❌ encryption:getPublicKey:`, err.message);
            socket.emit("encryption:error", {
              event: "encryption:getPublicKey",
              message: err.message,
            });
          }
        });

        /**
         * Retourne le mode de chiffrement actif et la config du service.
         *
         * Émission client  : encryption:getConfig
         * Réponse serveur  : encryption:config { mode, algorithm, keyLength, ... }
         */
        socket.on("encryption:getConfig", () => {
          if (!this.encryptionService) {
            return socket.emit("encryption:config", { mode: "none" });
          }
          socket.emit("encryption:config", this.encryptionService.getConfig());
        });

        /**
         * Change le mode de chiffrement à chaud (admin uniquement).
         *
         * Émission client  : encryption:switchMode { mode: 'none' | 'e2ee' }
         * Broadcast serveur: encryption:modeChanged { mode, changedBy, timestamp }
         *                 ou encryption:error { message }
         */
        socket.on("encryption:switchMode", (data) => {
          try {
            if (!socket.isAdmin) {
              return socket.emit("encryption:error", {
                event: "encryption:switchMode",
                message: "Accès refusé — droits administrateur requis",
              });
            }

            if (!this.encryptionService) {
              return socket.emit("encryption:error", {
                event: "encryption:switchMode",
                message: "EncryptionService non disponible",
              });
            }

            const { mode } = data || {};
            const result = this.encryptionService.switchMode(mode);

            // Notifier tous les clients connectés du changement de mode
            this.io.emit("encryption:modeChanged", {
              mode: result.newMode,
              previous: result.previousMode,
              changedBy: socket.userId,
              timestamp: Date.now(),
            });

            console.log(
              `🔄 Mode E2EE changé: ${result.previousMode} → ${result.newMode} par ${socket.userId}`,
            );
          } catch (err) {
            console.error(`❌ encryption:switchMode:`, err.message);
            socket.emit("encryption:error", {
              event: "encryption:switchMode",
              message: err.message,
            });
          }
        });

        /**
         * Révoque la clé publique de l'utilisateur connecté (cas de compromission).
         *
         * Émission client  : encryption:revokeKey
         * Réponse serveur  : encryption:keyRevoked { userId, revokedCount }
         *                 ou encryption:error { message }
         */
        socket.on("encryption:revokeKey", async () => {
          try {
            if (!this.keyManagementService) {
              return socket.emit("encryption:error", {
                event: "encryption:revokeKey",
                message: "Service de gestion des clés non disponible",
              });
            }

            const userId = socket.userId;
            if (!userId) {
              return socket.emit("encryption:error", {
                event: "encryption:revokeKey",
                message: "Authentification requise",
              });
            }

            const result = await this.keyManagementService.revokeKey(userId);

            socket.emit("encryption:keyRevoked", {
              userId,
              revokedCount: result.revokedCount,
            });

            console.warn(`🚫 Clé révoquée pour userId=${userId}`);
          } catch (err) {
            console.error(`❌ encryption:revokeKey:`, err.message);
            socket.emit("encryption:error", {
              event: "encryption:revokeKey",
              message: err.message,
            });
          }
        });

        /**
         * Archive une conversation pour l'utilisateur connecté.
         * Emission client  : conversation:archive { conversationId }
         * Réponse serveur  : conversation:archived { conversationId, archivedAt }
         *                 ou conversation:archiveError { message }
         */
        socket.on("conversation:archive", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;
            if (!userId)
              return socket.emit("conversation:archiveError", {
                message: "Authentification requise",
              });
            if (!this.archiveConversationUseCase)
              return socket.emit("conversation:archiveError", {
                message: "Fonctionnalité non disponible",
              });

            const { conversationId } = data || {};
            if (!conversationId)
              return socket.emit("conversation:archiveError", {
                message: "conversationId manquant",
              });

            const result = await this.archiveConversationUseCase.execute(
              userId,
              conversationId,
              "archive",
            );
            socket.emit("conversation:archived", {
              conversationId,
              archivedAt: result.archivedAt,
              alreadyArchived: result.alreadyInState || false,
            });
            console.log(
              `📂 Conversation ${conversationId} archivée par ${userId}`,
            );
          } catch (err) {
            console.error("❌ conversation:archive:", err.message);
            socket.emit("conversation:archiveError", { message: err.message });
          }
        });

        /**
         * Désarchive une conversation pour l'utilisateur connecté.
         * Emission client  : conversation:unarchive { conversationId }
         * Réponse serveur  : conversation:unarchived { conversationId }
         *                 ou conversation:archiveError { message }
         */
        socket.on("conversation:unarchive", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;
            if (!userId)
              return socket.emit("conversation:archiveError", {
                message: "Authentification requise",
              });
            if (!this.archiveConversationUseCase)
              return socket.emit("conversation:archiveError", {
                message: "Fonctionnalité non disponible",
              });

            const { conversationId } = data || {};
            if (!conversationId)
              return socket.emit("conversation:archiveError", {
                message: "conversationId manquant",
              });

            const result = await this.archiveConversationUseCase.execute(
              userId,
              conversationId,
              "unarchive",
            );
            socket.emit("conversation:unarchived", {
              conversationId,
              alreadyUnarchived: result.alreadyInState || false,
            });
            console.log(
              `📂 Conversation ${conversationId} désarchivée par ${userId}`,
            );
          } catch (err) {
            console.error("❌ conversation:unarchive:", err.message);
            socket.emit("conversation:archiveError", { message: err.message });
          }
        });

        /**
         * Récupère les conversations archivées de l'utilisateur connecté.
         * Emission client  : conversation:getArchived { page?, limit? }
         * Réponse serveur  : conversation:archivedList { conversations, totalCount, pagination }
         *                 ou conversation:archiveError { message }
         */
        socket.on("conversation:getArchived", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;
            if (!userId)
              return socket.emit("conversation:archiveError", {
                message: "Authentification requise",
              });
            if (!this.getArchivedConversationsUseCase)
              return socket.emit("conversation:archiveError", {
                message: "Fonctionnalité non disponible",
              });

            const { page = 1, limit = 20 } = data || {};
            const result = await this.getArchivedConversationsUseCase.execute(
              userId,
              {
                page: Math.max(1, parseInt(page) || 1),
                limit: Math.min(50, parseInt(limit) || 20),
              },
            );

            socket.emit("conversation:archivedList", {
              conversations: result.conversations || [],
              totalCount: result.totalCount || 0,
              pagination: result.pagination || {},
            });
          } catch (err) {
            console.error("❌ conversation:getArchived:", err.message);
            socket.emit("conversation:archiveError", { message: err.message });
          }
        });

        // ──────────────────────────────────────────────────────────────────

        socket.on("disconnect", (reason) => {
          this.handleDisconnection(socket, reason);
        });

        socket.on("error", (error) => {
          console.error(`❌ Erreur Socket ${socket.id}:`, error);
        });

        // ✅ QUICK LOAD - Navigation rapide (SANS cache controller)
        socket.on("messages:quickload", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const { conversationId, limit = 20 } = data;
            const userId = socket.userId;

            if (!conversationId || !userId) {
              return socket.emit("messages:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            console.log(`⚡ QuickLoad: ${conversationId} pour ${userId}`);

            // ✅ APPEL DIRECT AU USE CASE (cache géré par le repository)
            const result = await this.getMessagesUseCase.execute(
              conversationId,
              {
                cursor: null, // Toujours à null pour quick load
                limit,
                userId,
                useCache: true, // Le repository décide du cache
              },
            );

            const quickData = {
              messages: result.messages || [],
              hasMore: (result.messages?.length || 0) === limit,
              fromCache: result.fromCache || false,
            };

            socket.emit("messages:quick", {
              conversationId,
              ...quickData,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur messages:quickload:", error);
            socket.emit("messages:error", {
              error: "Erreur chargement rapide",
              code: "QUICKLOAD_FAILED",
            });
          }
        });

        // ✅ FULL LOAD - Chargement complet (SANS cache controller)
        socket.on("messages:fullload", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const { conversationId, cursor = null, limit = 50 } = data;
            const userId = socket.userId;

            // ✅ APPEL DIRECT AU USE CASE
            const result = await this.getMessagesUseCase.execute(
              conversationId,
              {
                cursor,
                limit,
                userId,
                useCache: !cursor, // Cache seulement première page
              },
            );

            socket.emit("messages:full", {
              conversationId,
              ...result,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur messages:fullload:", error);
            socket.emit("messages:error", {
              error: "Erreur chargement complet",
              code: "FULLLOAD_FAILED",
            });
          }
        });

        // ✅ CONVERSATIONS QUICK LOAD - Navigation rapide (SANS cache controller)
        socket.on("conversations:quickload", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const { limit = 10 } = data;
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("conversations:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            console.log(`⚡ Conversations QuickLoad pour ${userId}`);

            // ✅ APPEL DIRECT AU USE CASE (cache géré par le repository)
            const result = await this.getConversationsUseCase.execute(userId, {
              page: 1,
              limit,
              useCache: true, // Le repository décide du cache
            });

            const quickData = {
              conversations: result.conversations || [],
              hasMore: (result.conversations?.length || 0) === limit,
              fromCache: result.fromCache || false,
              totalUnreadMessages: result.totalUnreadMessages || 0,
              unreadConversations: result.unreadConversations || 0,
            };

            socket.emit("conversations:quick", {
              ...quickData,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur conversations:quickload:", error);
            socket.emit("conversations:error", {
              error: "Erreur chargement rapide conversations",
              code: "QUICKLOAD_FAILED",
            });
          }
        });

        // ✅ CONVERSATIONS FULL LOAD - Chargement complet (SANS cache controller)
        socket.on("conversations:fullload", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const { page = 1, limit = 20, cursor = null } = data;
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("conversations:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            // ✅ APPEL DIRECT AU USE CASE
            const result = await this.getConversationsUseCase.execute(userId, {
              page: Math.max(1, parseInt(page)),
              limit: Math.min(parseInt(limit), 50),
              cursor,
              useCache: !cursor, // Cache seulement première page
            });

            socket.emit("conversations:full", {
              ...result,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur conversations:fullload:", error);
            socket.emit("conversations:error", {
              error: "Erreur chargement complet conversations",
              code: "FULLLOAD_FAILED",
            });
          }
        });

        // ✅ CONVERSATION DETAIL LOAD - Charger une conversation spécifique (SANS cache)
        socket.on("conversation:load", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const { conversationId } = data;
            const userId = socket.userId;

            if (!conversationId || !userId) {
              return socket.emit("conversation:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            console.log(
              `🔍 Chargement conversation ${conversationId} pour ${userId}`,
            );

            // ✅ APPEL DIRECT AU USE CASE (cache géré par le repository)
            const result = await this.getConversationUseCase.execute(
              conversationId,
              userId,
              true,
            );

            socket.emit("conversation:loaded", {
              conversation: result.conversation || result,
              fromCache: result.fromCache || false,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur conversation:load:", error);
            socket.emit("conversation:error", {
              error: "Erreur chargement conversation",
              code: "LOAD_FAILED",
            });
          }
        });

        // ✅ HANDLERS EXISTANTS MODIFIÉS (SANS CACHE)
        socket.on("getConversations", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;
            const { page = 1, limit = 20 } = data || {};

            if (!userId) {
              return socket.emit("conversations_error", {
                message: "ID utilisateur manquant",
                code: "MISSING_USER_ID",
              });
            }

            // ✅ APPEL DIRECT AU USE CASE (SANS cache controller)
            const result = await this.getConversationsUseCase.execute(userId, {
              page: Math.max(1, parseInt(page)),
              limit: Math.min(parseInt(limit), 50),
              useCache: page === 1, // Cache seulement première page
            });

            socket.emit("conversationsLoaded", {
              conversations: result.conversations || [],
              pagination: result.pagination || {},
              totalUnreadMessages: result.totalUnreadMessages || 0,
              unreadConversations: result.unreadConversations || 0,
              fromCache: result.fromCache || false,
              timestamp: Date.now(),
            });
          } catch (error) {
            console.error("❌ Erreur getConversations:", error);
            socket.emit("conversations_error", {
              message: "Erreur lors de la récupération des conversations",
              code: "GET_CONVERSATIONS_ERROR",
            });
          }
        });

        socket.on("getConversation", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;
            const { conversationId } = data || {};

            if (!conversationId || !userId) {
              return socket.emit("conversation_error", {
                message: "ID conversation ou utilisateur manquant",
                code: "MISSING_DATA",
              });
            }

            // ✅ APPEL DIRECT AU USE CASE (SANS cache controller)
            const result = await this.getConversationUseCase.execute(
              conversationId,
              userId,
              true, // Le repository décide du cache
            );

            socket.emit("conversationLoaded", {
              conversation: result.conversation || result,
              metadata: {
                fromCache: result.fromCache || false,
                timestamp: new Date().toISOString(),
              },
            });
          } catch (error) {
            console.error("❌ Erreur getConversation:", error);
            socket.emit("conversation_error", {
              message: "Erreur lors de la récupération de la conversation",
              code: "GET_CONVERSATION_ERROR",
            });
          }
        });

        // ========================================
        // ✅ NOUVEAUX ÉVÉNEMENTS GROUPES ET DIFFUSION
        // ========================================

        // ✅ CRÉER UN GROUPE
        socket.on("createGroup", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("group:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { name, type, members, groupId, admins = [] } = data;

            // ✅ VALIDATION
            if (!name || typeof name !== "string" || name.trim().length === 0) {
              return socket.emit("group:error", {
                error: "Nom du groupe requis",
                code: "MISSING_GROUP_NAME",
              });
            }

            if (!Array.isArray(members) || members.length === 0) {
              return socket.emit("group:error", {
                error: "Liste des membres requise (minimum 1 membre)",
                code: "MISSING_MEMBERS",
              });
            }

            if (members.includes(userId)) {
              return socket.emit("group:error", {
                error:
                  "Vous ne devez pas vous inclure dans la liste des membres",
                code: "ADMIN_IN_MEMBERS",
              });
            }

            console.log(
              `👥 Création groupe "${name}" par ${userId} avec ${members.length} membre(s)`,
            );

            const finalAdmins =
              Array.isArray(admins) && admins.length > 0
                ? [
                    ...new Set([
                      userId,
                      ...admins.filter((id) => id !== userId),
                    ]),
                  ]
                : [userId];

            // ✅ GÉNÉRER ID SI NON FOURNI
            const finalGroupId = groupId || this.generateObjectId();

            // ✅ APPEL USE CASE
            const group = await this.createGroupUseCase.execute({
              groupId: finalGroupId,
              name: name.trim(),
              type: type,
              adminId: userId,
              members: members.filter((id) => id !== userId), // S'assurer que admin n'est pas dans members
              finalAdmins: finalAdmins, // Passer les admins pour les groupes de diffusion
              senderSocketId: socket.id,
            });

            // ✅ RÉPONSE SUCCÈS À L'ADMIN
            socket.emit("group:created", {
              success: true,
              group: {
                id: group._id,
                name: group.name,
                type: group.type,
                participants: group.participants,
                createdBy: group.createdBy,
                createdAt: group.createdAt,
                participantCount: group.participants.length,
              },
              timestamp: new Date().toISOString(),
            });

            // 🔄 Notification participants supprimée — distribution via MDS (stream → conversation:created)

            /// ✅ JOINDRE AUTOMATIQUEMENT LA ROOM DU GROUPE
            const groupRoom = `conversation_${group._id}`;
            socket.join(groupRoom);

            console.log(`✅ Groupe "${name}" créé avec succès: ${group._id}`);
          } catch (error) {
            console.error("❌ Erreur createGroup:", error);
            socket.emit("group:error", {
              error: "Erreur lors de la création du groupe",
              code: "CREATE_GROUP_FAILED",
              details:
                process.env.NODE_ENV === "development"
                  ? error.message
                  : undefined,
            });
          }
        });

        // ✅ CRÉER UNE LISTE DE DIFFUSION
        socket.on("createBroadcast", async (data) => {
          if (this.onlineUserManager && socket.userId) {
            this.onlineUserManager.updateLastActivity(socket.userId, socket);
          }
          try {
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("broadcast:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { name, recipients, broadcastId, admins = [] } = data;

            // ✅ VALIDATION
            if (!name || typeof name !== "string" || name.trim().length === 0) {
              return socket.emit("broadcast:error", {
                error: "Nom de la diffusion requis",
                code: "MISSING_BROADCAST_NAME",
              });
            }

            if (!Array.isArray(recipients) || recipients.length === 0) {
              return socket.emit("broadcast:error", {
                error:
                  "Liste des destinataires requise (minimum 1 destinataire)",
                code: "MISSING_RECIPIENTS",
              });
            }

            if (recipients.includes(userId)) {
              return socket.emit("broadcast:error", {
                error:
                  "Vous ne devez pas vous inclure dans la liste des destinataires",
                code: "ADMIN_IN_RECIPIENTS",
              });
            }

            console.log(
              `📢 Création diffusion "${name}" par ${userId} avec ${recipients.length} destinataire(s)`,
            );

            // ✅ GÉNÉRER ID SI NON FOURNI
            const finalBroadcastId = broadcastId || this.generateObjectId();

            // ✅ PRÉPARER LES ADMINS
            const finalAdmins =
              Array.isArray(admins) && admins.length > 0
                ? [
                    ...new Set([
                      userId,
                      ...admins.filter((id) => id !== userId),
                    ]),
                  ]
                : [userId];

            // ✅ APPEL USE CASE
            const broadcast = await this.createBroadcastUseCase.execute({
              broadcastId: finalBroadcastId,
              name: name.trim(),
              adminIds: finalAdmins,
              recipientIds: recipients.filter(
                (id) => !finalAdmins.includes(id),
              ),
              senderSocketId: socket.id,
            });

            // ✅ RÉPONSE SUCCÈS À L'ADMIN
            socket.emit("broadcast:created", {
              success: true,
              broadcast: {
                id: broadcast._id,
                name: broadcast.name,
                type: broadcast.type,
                participants: broadcast.participants,
                createdBy: broadcast.createdBy,
                createdAt: broadcast.createdAt,
                participantCount: broadcast.participants.length,
                adminIds: finalAdmins,
                recipientIds: recipients,
              },
              timestamp: new Date().toISOString(),
            });

            // 🔄 Notification admins supprimée — distribution via MDS (stream → conversation:created)
            // 🔄 Notification destinataires supprimée — distribution via MDS (stream → conversation:created)

            // ✅ JOINDRE AUTOMATIQUEMENT LA ROOM DE LA DIFFUSION
            const broadcastRoom = `conversation_${broadcast._id}`;
            socket.join(broadcastRoom);

            console.log(
              `✅ Diffusion "${name}" créée avec succès: ${broadcast._id}`,
            );
          } catch (error) {
            console.error("❌ Erreur createBroadcast:", error);
            socket.emit("broadcast:error", {
              error: "Erreur lors de la création de la diffusion",
              code: "CREATE_BROADCAST_FAILED",
              details:
                process.env.NODE_ENV === "development"
                  ? error.message
                  : undefined,
            });
          }
        });

        // ✅ REJOINDRE UN GROUPE/DIFFUSION EXISTANT
        socket.on("joinGroup", async (data) => {
          try {
            const userId = socket.userId;
            const { conversationId, accept = true } = data;

            if (!userId || !conversationId) {
              return socket.emit("group:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            if (accept) {
              // ✅ JOINDRE LA ROOM
              const groupRoom = `conversation_${conversationId}`;
              socket.join(groupRoom);

              // ✅ NOTIFIER LES AUTRES PARTICIPANTS
              socket.to(groupRoom).emit("group:member_joined", {
                conversationId,
                user: {
                  userId: userId,
                  matricule: socket.matricule,
                },
                timestamp: new Date().toISOString(),
              });

              socket.emit("group:joined", {
                success: true,
                conversationId,
                timestamp: new Date().toISOString(),
              });

              console.log(
                `✅ ${socket.matricule} a rejoint le groupe/diffusion: ${conversationId}`,
              );
            } else {
              // ✅ REFUSER L'INVITATION
              socket.emit("group:invitation_declined", {
                conversationId,
                timestamp: new Date().toISOString(),
              });

              console.log(
                `❌ ${socket.matricule} a refusé l'invitation: ${conversationId}`,
              );
            }
          } catch (error) {
            console.error("❌ Erreur joinGroup:", error);
            socket.emit("group:error", {
              error: "Erreur lors de la jointure",
              code: "JOIN_GROUP_FAILED",
            });
          }
        });

        // ✅ QUITTER UN GROUPE/DIFFUSION
        socket.on("leaveGroup", async (data) => {
          try {
            const userId = socket.userId;
            const { conversationId } = data;

            if (!userId || !conversationId) {
              return socket.emit("group:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            // ✅ QUITTER LA ROOM
            const groupRoom = `conversation_${conversationId}`;
            socket.leave(groupRoom);

            // ✅ NOTIFIER LES AUTRES PARTICIPANTS
            socket.to(groupRoom).emit("group:member_left", {
              conversationId,
              user: {
                userId: userId,
                matricule: socket.matricule,
              },
              timestamp: new Date().toISOString(),
            });

            socket.emit("group:left", {
              success: true,
              conversationId,
              timestamp: new Date().toISOString(),
            });

            console.log(
              `👋 ${socket.matricule} a quitté le groupe/diffusion: ${conversationId}`,
            );

            // ✅ TODO: Implémenter la suppression du participant de la conversation en DB
            // if (this.leaveGroupUseCase) {
            //   await this.leaveGroupUseCase.execute({ conversationId, userId });
            // }
          } catch (error) {
            console.error("❌ Erreur leaveGroup:", error);
            socket.emit("group:error", {
              error: "Erreur lors de la sortie du groupe",
              code: "LEAVE_GROUP_FAILED",
            });
          }
        });

        // ✅ OBTENIR INFO D'UN GROUPE/DIFFUSION
        socket.on("getGroupInfo", async (data) => {
          try {
            const userId = socket.userId;
            const { conversationId } = data;

            const normalizedConversationId =
              this.normalizeMongoId(conversationId);

            if (!userId || !normalizedConversationId) {
              return socket.emit("group:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            // ✅ APPEL USE CASE POUR RÉCUPÉRER INFO
            const result = await this.getConversationUseCase.execute(
              normalizedConversationId,
              {
                userId,
                useCache: true,
              },
            );

            if (!result.conversation) {
              return socket.emit("group:error", {
                error: "Groupe/Diffusion non trouvé",
                code: "GROUP_NOT_FOUND",
              });
            }

            const conversation = result.conversation;

            // ✅ VÉRIFIER QUE L'UTILISATEUR EST PARTICIPANT
            if (!conversation.participants.includes(userId)) {
              return socket.emit("group:error", {
                error: "Vous n'êtes pas membre de ce groupe/diffusion",
                code: "NOT_MEMBER",
              });
            }

            socket.emit("group:info", {
              success: true,
              group: {
                id: conversation._id,
                name: conversation.name,
                type: conversation.type,
                participants: conversation.participants,
                participantCount: conversation.participants.length,
                createdBy: conversation.createdBy,
                createdAt: conversation.createdAt,
                lastMessage: conversation.lastMessage,
                settings: conversation.settings,
                metadata: conversation.metadata,
              },
              fromCache: result.fromCache || false,
              timestamp: new Date().toISOString(),
            });
          } catch (error) {
            console.error("❌ Erreur getGroupInfo:", error);
            socket.emit("group:error", {
              error: "Erreur lors de la récupération des informations",
              code: "GET_GROUP_INFO_FAILED",
            });
          }
        });

        // ========================================
        // ✅ ÉVÉNEMENTS APPELS (CALL / VIDEO_CALL)
        // ========================================

        // ✅ INITIER UN APPEL
        socket.on("initiateCall", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("call:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { conversationId, receiverId, callType = "AUDIO" } = data;
            if (!conversationId && !receiverId) {
              return socket.emit("call:error", {
                error: "conversationId ou receiverId requis",
                code: "MISSING_PARAMS",
              });
            }

            const callId = data.callId || this.generateObjectId();
            const messageType = callType === "VIDEO" ? "VIDEO_CALL" : "CALL";
            const receiverIds = receiverId
              ? Array.isArray(receiverId)
                ? receiverId
                : [receiverId]
              : [];

            console.log(`📞 ${socket.matricule} lance un appel ${callType}:`, {
              callId,
              conversationId,
              receiverIds,
            });

            // ✅ Créer le message d'appel via SendMessage
            const callMetadata = {
              callId,
              callType,
              status: "INITIATED",
              initiatorId: userId,
              receiverIds,
              startedAt: null,
              endedAt: null,
              duration: 0,
              endReason: null,
            };

            let result;
            try {
              result = await this.sendMessageUseCase.execute({
                content: "",
                senderId: userId,
                conversationId: conversationId
                  ? this.normalizeMongoId(conversationId)
                  : null,
                type: messageType,
                receiverId: receiverIds.length === 1 ? receiverIds[0] : null,
                callMetadata,
              });
            } catch (sendError) {
              console.error(
                "❌ Erreur création message appel:",
                sendError.message,
              );
              return socket.emit("call:error", {
                error: "Erreur lors de l'initiation de l'appel",
                code: "CALL_INIT_FAILED",
              });
            }

            const messageId = result.message.id;
            const createdConvId = result.conversation.id;

            // ✅ Rejoindre la room de la conversation (peut être nouvellement créée)
            socket.join(`conversation_${createdConvId}`);
            console.log(
              `🚪 Appelant ${socket.matricule} a rejoint conversation_${createdConvId}`,
            );

            // ✅ ACK à l'appelant
            socket.emit("call:initiated", {
              success: true,
              callId,
              messageId,
              callType,
              conversationId: result.conversation.id,
              participants: receiverIds,
              timestamp: new Date().toISOString(),
            });

            // ✅ Notifier les destinataires (sonnerie)
            for (const rid of receiverIds) {
              this.io.to(`user_${rid}`).emit("call:incoming", {
                callId,
                messageId,
                callType,
                conversationId: result.conversation.id,
                callerId: userId,
                caller: {
                  userId,
                  matricule: socket.matricule,
                  nom: socket.nom,
                  prenom: socket.prenom,
                  avatar: socket.avatar,
                },
                timestamp: new Date().toISOString(),
              });
            }

            console.log(`✅ Appel ${callType} initié: ${callId}`);
          } catch (error) {
            console.error("❌ Erreur initiateCall:", error);
            socket.emit("call:error", {
              error: error.message,
              code: "CALL_INIT_ERROR",
            });
          }
        });

        // ✅ RÉPONDRE À UN APPEL (DÉCROCHER)
        socket.on("answerCall", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("call:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { callId, messageId, conversationId } = data;
            if (!callId || !messageId) {
              return socket.emit("call:error", {
                error: "callId et messageId requis",
                code: "MISSING_PARAMS",
              });
            }

            console.log(`📞 ${socket.matricule} répond à l'appel ${callId}`);

            // ✅ Récupérer les participants pour la publication stream
            let participants = [];
            if (conversationId && this.getConversationUseCase) {
              try {
                const conv = await this.getConversationUseCase.execute(
                  conversationId,
                  userId,
                );
                participants = conv?.participants || [];
              } catch (e) {
                /* ignore */
              }
            }

            // ✅ Mettre à jour le message en base + publier via stream
            try {
              if (this.updateCallStatusUseCase) {
                await this.updateCallStatusUseCase.execute({
                  messageId,
                  updates: { status: "ANSWERED", startedAt: new Date() },
                  conversationId,
                  userId,
                  callId,
                  participants,
                  senderSocketId: socket.id,
                });
              }
            } catch (updateErr) {
              console.warn(
                "⚠️ Erreur mise à jour status appel:",
                updateErr.message,
              );
            }

            // ✅ Rejoindre la room de la conversation (au cas où pas encore dedans)
            if (conversationId) {
              socket.join(`conversation_${conversationId}`);
              console.log(
                `🚪 Répondant ${socket.matricule} a rejoint conversation_${conversationId}`,
              );
            }

            // ✅ ACK au décrocheur
            socket.emit("call:answered", {
              success: true,
              callId,
              messageId,
              conversationId,
              answeredBy: userId,
              answeredByMatricule: socket.matricule,
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → call:statusUpdated)

            console.log(`✅ Appel ${callId} décroché par ${socket.matricule}`);
          } catch (error) {
            console.error("❌ Erreur answerCall:", error);
            socket.emit("call:error", {
              error: error.message,
              code: "CALL_ANSWER_ERROR",
            });
          }
        });

        // ✅ REFUSER UN APPEL
        socket.on("declineCall", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("call:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { callId, messageId, conversationId } = data;
            if (!callId) {
              return socket.emit("call:error", {
                error: "callId requis",
                code: "MISSING_PARAMS",
              });
            }

            console.log(`📞 ${socket.matricule} refuse l'appel ${callId}`);

            // ✅ Récupérer les participants pour la publication stream
            let participants = [];
            if (conversationId && this.getConversationUseCase) {
              try {
                const conv = await this.getConversationUseCase.execute(
                  conversationId,
                  userId,
                );
                participants = conv?.participants || [];
              } catch (e) {
                /* ignore */
              }
            }

            // ✅ Mettre à jour le message en base + publier via stream
            if (messageId) {
              try {
                if (this.updateCallStatusUseCase) {
                  await this.updateCallStatusUseCase.execute({
                    messageId,
                    updates: {
                      status: "DECLINED",
                      endedAt: new Date(),
                      endReason: "user_declined",
                    },
                    conversationId,
                    userId,
                    callId,
                    participants,
                    senderSocketId: socket.id,
                  });
                }
              } catch (updateErr) {
                console.warn(
                  "⚠️ Erreur mise à jour status appel:",
                  updateErr.message,
                );
              }
            }

            // ✅ ACK
            socket.emit("call:declined", {
              success: true,
              callId,
              messageId,
              conversationId,
              declinedBy: userId,
              declinedByMatricule: socket.matricule,
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → call:statusUpdated)

            console.log(`✅ Appel ${callId} refusé par ${socket.matricule}`);
          } catch (error) {
            console.error("❌ Erreur declineCall:", error);
            socket.emit("call:error", {
              error: error.message,
              code: "CALL_DECLINE_ERROR",
            });
          }
        });

        // ✅ TERMINER UN APPEL
        socket.on("endCall", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("call:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const {
              callId,
              messageId,
              conversationId,
              reason = "user_hangup",
            } = data;
            if (!callId) {
              return socket.emit("call:error", {
                error: "callId requis",
                code: "MISSING_PARAMS",
              });
            }

            console.log(`📞 ${socket.matricule} termine l'appel ${callId}`);

            const endedAt = new Date();

            // ✅ Mettre à jour le message en base avec durée
            if (messageId) {
              try {
                // Récupérer le message pour calculer la durée
                let duration = 0;
                if (this.getMessageByIdUseCase) {
                  try {
                    const msg =
                      await this.getMessageByIdUseCase.execute(messageId);
                    const startedAt =
                      msg?.metadata?.contentMetadata?.call?.startedAt;
                    if (startedAt) {
                      duration = Math.round(
                        (endedAt - new Date(startedAt)) / 1000,
                      );
                    }
                  } catch (e) {
                    console.warn(
                      "⚠️ Impossible de calculer la durée:",
                      e.message,
                    );
                  }
                }

                // ✅ Récupérer les participants pour la publication stream
                let participants = [];
                if (conversationId && this.getConversationUseCase) {
                  try {
                    const conv = await this.getConversationUseCase.execute(
                      conversationId,
                      userId,
                    );
                    participants = conv?.participants || [];
                  } catch (e) {
                    /* ignore */
                  }
                }

                if (this.updateCallStatusUseCase) {
                  await this.updateCallStatusUseCase.execute({
                    messageId,
                    updates: {
                      status: "ENDED",
                      endedAt,
                      duration,
                      endReason: reason,
                    },
                    conversationId,
                    userId,
                    callId,
                    participants,
                    senderSocketId: socket.id,
                  });
                }

                // ✅ Mettre à jour le contenu du message avec la durée
                const durationStr =
                  duration > 0
                    ? `${Math.floor(duration / 60)}:${String(duration % 60).padStart(2, "0")}`
                    : "0:00";
                try {
                  if (this.updateMessageContentUseCase) {
                    await this.updateMessageContentUseCase.execute({
                      messageId,
                      newContent: `📞 Appel terminé (${durationStr})`,
                      userId,
                    });
                  }
                } catch (e) {
                  console.warn(
                    "⚠️ Erreur mise à jour contenu appel:",
                    e.message,
                  );
                }
              } catch (updateErr) {
                console.warn(
                  "⚠️ Erreur mise à jour status appel:",
                  updateErr.message,
                );
              }
            }

            // ✅ ACK
            socket.emit("call:ended", {
              success: true,
              callId,
              messageId,
              conversationId,
              endedBy: userId,
              endedByMatricule: socket.matricule,
              reason,
              duration: messageId
                ? typeof duration !== "undefined"
                  ? duration
                  : 0
                : 0,
              timestamp: endedAt.toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → call:statusUpdated)

            console.log(`✅ Appel ${callId} terminé par ${socket.matricule}`);
          } catch (error) {
            console.error("❌ Erreur endCall:", error);
            socket.emit("call:error", {
              error: error.message,
              code: "CALL_END_ERROR",
            });
          }
        });

        // ✅ APPEL MANQUÉ (timeout côté client ou serveur)
        socket.on("missedCall", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) return;

            const { callId, messageId, conversationId } = data;
            if (!callId) return;

            console.log(`📞 Appel manqué: ${callId}`);

            // ✅ Récupérer les participants pour la publication stream
            let participants = [];
            if (conversationId && this.getConversationUseCase) {
              try {
                const conv = await this.getConversationUseCase.execute(
                  conversationId,
                  userId,
                );
                participants = conv?.participants || [];
              } catch (e) {
                /* ignore */
              }
            }

            if (messageId) {
              try {
                if (this.updateCallStatusUseCase) {
                  await this.updateCallStatusUseCase.execute({
                    messageId,
                    updates: {
                      status: "MISSED",
                      endedAt: new Date(),
                      endReason: "no_answer",
                    },
                    conversationId,
                    userId,
                    callId,
                    participants,
                    senderSocketId: socket.id,
                  });
                }
              } catch (updateErr) {
                console.warn(
                  "⚠️ Erreur mise à jour appel manqué:",
                  updateErr.message,
                );
              }
            }

            // 🔄 Broadcast supprimé — distribution via MDS (stream → call:statusUpdated)
          } catch (error) {
            console.error("❌ Erreur missedCall:", error);
          }
        });

        // ========================================
        // ✅ NOUVEAUX HANDLERS DE PRÉSENCE
        // ========================================

        // ✅ OBTENIR LES UTILISATEURS EN LIGNE D'UNE CONVERSATION
        socket.on("getConversationOnlineUsers", async (data) => {
          try {
            const { conversationId } = data;
            const userId = socket.userId;

            if (!conversationId || !userId) {
              return socket.emit("conversation_users:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.roomManager) {
              return socket.emit("conversation_users:error", {
                error: "Service de présence non disponible",
                code: "PRESENCE_SERVICE_UNAVAILABLE",
              });
            }

            const roomName = `conv_${conversationId}`;

            // Vérifier que l'utilisateur fait partie de la conversation
            const roomUsers = await this.roomManager.getRoomUsers(roomName);
            const isMember = roomUsers.some((user) => user.userId === userId);

            if (!isMember) {
              return socket.emit("conversation_users:error", {
                error: "Vous n'êtes pas membre de cette conversation",
                code: "NOT_A_MEMBER",
              });
            }

            // Récupérer les statistiques de présence
            const presenceStats =
              await this.roomManager.getRoomPresenceStats(roomName);

            socket.emit("conversation_online_users", {
              conversationId,
              ...presenceStats,
              userRole: await this.roomManager.getUserRoleInRoom(
                roomName,
                userId,
              ),
              currentUserStatus: presenceStats.users.find(
                (u) => u.userId === userId,
              ),
            });

            console.log(
              `👥 Statistiques envoyées pour ${conversationId}: ${presenceStats.onlineUsers}/${presenceStats.totalUsers}`,
            );
          } catch (error) {
            console.error("❌ Erreur getConversationOnlineUsers:", error);
            socket.emit("conversation_users:error", {
              error: "Erreur lors de la récupération des utilisateurs",
              code: "GET_USERS_ERROR",
              details:
                process.env.NODE_ENV === "development"
                  ? error.message
                  : undefined,
            });
          }
        });

        // ✅ OBTENIR TOUTES LES CONVERSATIONS AVEC PRÉSENCE
        socket.on("getConversationsWithPresence", async () => {
          try {
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("conversations_presence:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            if (!this.roomManager) {
              return socket.emit("conversations_presence:error", {
                error: "Service de présence non disponible",
                code: "PRESENCE_SERVICE_UNAVAILABLE",
              });
            }

            const conversations =
              await this.roomManager.getConversationsWithPresence(userId);

            socket.emit("conversations_with_presence", {
              userId,
              conversations,
              count: conversations.length,
              summary: {
                totalConversations: conversations.length,
                activeConversations: conversations.filter((c) => c.isActive)
                  .length,
                totalOnlineUsers: conversations.reduce(
                  (sum, c) => sum + c.onlineUsers,
                  0,
                ),
                averageHealth:
                  conversations.length > 0
                    ? conversations.reduce((sum, c) => {
                        const healthScore =
                          c.roomHealth === "healthy"
                            ? 3
                            : c.roomHealth === "moderate"
                              ? 2
                              : c.roomHealth === "low"
                                ? 1
                                : 0;
                        return sum + healthScore;
                      }, 0) / conversations.length
                    : 0,
              },
              timestamp: new Date().toISOString(),
            });

            console.log(
              `📋 Conversations avec présence envoyées à ${socket.matricule}: ${conversations.length}`,
            );
          } catch (error) {
            console.error("❌ Erreur getConversationsWithPresence:", error);
            socket.emit("conversations_presence:error", {
              error: "Erreur lors de la récupération des conversations",
              code: "GET_CONVERSATIONS_ERROR",
              details:
                process.env.NODE_ENV === "development"
                  ? error.message
                  : undefined,
            });
          }
        });

        // ✅ SURVEILLANCE EN TEMPS RÉEL (subscribe aux updates)
        socket.on("subscribeToPresence", async (data) => {
          try {
            const { conversationId } = data;
            const userId = socket.userId;

            if (!conversationId || !userId) {
              return socket.emit("presence:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.roomManager) {
              return socket.emit("presence:error", {
                error: "Service de présence non disponible",
                code: "PRESENCE_SERVICE_UNAVAILABLE",
              });
            }

            const roomName = `conv_${conversationId}`;

            // Joindre la room de présence
            socket.join(`presence_${roomName}`);

            // Envoyer les données initiales
            const presenceStats =
              await this.roomManager.getRoomPresenceStats(roomName);

            socket.emit("presence:initial", {
              conversationId,
              ...presenceStats,
              subscribed: true,
              timestamp: new Date().toISOString(),
            });

            // Broadcast la mise à jour à tous les abonnés
            await this.roomManager.broadcastPresenceUpdate(roomName);

            console.log(
              `👁️ ${socket.matricule} surveille la présence de ${conversationId}`,
            );
          } catch (error) {
            console.error("❌ Erreur subscribeToPresence:", error);
            socket.emit("presence:error", {
              error: "Erreur lors de l'abonnement",
              code: "SUBSCRIBE_ERROR",
              details:
                process.env.NODE_ENV === "development"
                  ? error.message
                  : undefined,
            });
          }
        });

        // ✅ SE DÉSABONNER DE LA SURVEILLANCE
        socket.on("unsubscribeFromPresence", (data) => {
          try {
            const { conversationId } = data;

            if (conversationId) {
              const roomName = `conv_${conversationId}`;
              socket.leave(`presence_${roomName}`);

              socket.emit("presence:unsubscribed", {
                conversationId,
                timestamp: new Date().toISOString(),
              });

              console.log(
                `🚫 ${socket.matricule} ne surveille plus ${conversationId}`,
              );
            }
          } catch (error) {
            console.error("❌ Erreur unsubscribeFromPresence:", error);
          }
        });

        // ✅ DASHBOARD GLOBAL DE PRÉSENCE
        socket.on("getPresenceDashboard", async () => {
          try {
            const userId = socket.userId;

            if (!userId) {
              return socket.emit("presence_dashboard:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            if (!this.roomManager) {
              return socket.emit("presence_dashboard:error", {
                error: "Service de présence non disponible",
                code: "PRESENCE_SERVICE_UNAVAILABLE",
              });
            }

            const dashboard =
              await this.roomManager.getGlobalPresenceDashboard();

            socket.emit("presence_dashboard", dashboard);

            console.log(
              `📊 Dashboard de présence envoyé à ${socket.matricule}`,
            );
          } catch (error) {
            console.error("❌ Erreur getPresenceDashboard:", error);
            socket.emit("presence_dashboard:error", {
              error: "Erreur lors de la génération du dashboard",
              code: "DASHBOARD_ERROR",
            });
          }
        });

        // ✅ DÉFINIR LE RÔLE D'UN UTILISATEUR
        socket.on("setUserRole", async (data) => {
          try {
            const { conversationId, targetUserId, role } = data;
            const adminUserId = socket.userId;

            if (!conversationId || !targetUserId || !role || !adminUserId) {
              return socket.emit("role:error", {
                error: "Paramètres manquants",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.roomManager) {
              return socket.emit("role:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            const roomName = `conv_${conversationId}`;

            // Vérifier que l'admin a les droits
            const adminRole = await this.roomManager.getUserRoleInRoom(
              roomName,
              adminUserId,
            );
            if (adminRole !== "admin" && adminRole !== "moderator") {
              return socket.emit("role:error", {
                error: "Permissions insuffisantes",
                code: "INSUFFICIENT_PERMISSIONS",
              });
            }

            // Valider le rôle
            const validRoles = ["member", "moderator", "admin"];
            if (!validRoles.includes(role)) {
              return socket.emit("role:error", {
                error: "Rôle invalide",
                code: "INVALID_ROLE",
              });
            }

            // Définir le rôle
            const success = await this.roomManager.setUserRoleInRoom(
              roomName,
              targetUserId,
              role,
            );

            if (success) {
              socket.emit("role:updated", {
                conversationId,
                targetUserId,
                role,
                updatedBy: adminUserId,
                timestamp: new Date().toISOString(),
              });

              // Notifier la room
              socket.to(roomName).emit("user:role_changed", {
                conversationId,
                userId: targetUserId,
                newRole: role,
                changedBy: {
                  userId: adminUserId,
                  matricule: socket.matricule,
                },
                timestamp: new Date().toISOString(),
              });

              // Broadcast la mise à jour de présence
              await this.roomManager.broadcastPresenceUpdate(roomName);
            } else {
              socket.emit("role:error", {
                error: "Erreur lors de la mise à jour du rôle",
                code: "UPDATE_FAILED",
              });
            }
          } catch (error) {
            console.error("❌ Erreur setUserRole:", error);
            socket.emit("role:error", {
              error: "Erreur lors de la définition du rôle",
              code: "ROLE_ERROR",
            });
          }
        });

        // ========================================
        // ✅ NOUVEAUX ÉVÉNEMENTS: Gestion participants, messages, fichiers
        // ========================================

        // ✅ AJOUTER UN OU PLUSIEURS PARTICIPANTS À UN GROUPE
        socket.on("addParticipant", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("participant:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { conversationId, participantId } = data;
            if (!conversationId || !participantId) {
              return socket.emit("participant:error", {
                error: "conversationId et participantId requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.addParticipantUseCase) {
              return socket.emit("participant:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            // ✅ Supporter un ID unique ou un tableau d'IDs
            const participantIds = Array.isArray(participantId)
              ? participantId
              : [participantId];

            // Filtrer les valeurs vides
            const validIds = participantIds
              .map((id) => (typeof id === "string" ? id.trim() : String(id)))
              .filter((id) => id.length > 0);

            if (validIds.length === 0) {
              return socket.emit("participant:error", {
                error: "Aucun participantId valide fourni",
                code: "INVALID_PARAMS",
              });
            }

            const results = { added: [], failed: [] };

            for (const pid of validIds) {
              try {
                await this.addParticipantUseCase.execute({
                  conversationId,
                  participantId: pid,
                  addedBy: userId,
                  senderSocketId: socket.id,
                });
                results.added.push(pid);
              } catch (err) {
                results.failed.push({ participantId: pid, error: err.message });
                console.warn(
                  `⚠️ Échec ajout participant ${pid}: ${err.message}`,
                );
              }
            }

            socket.emit("participant:added", {
              success: results.added.length > 0,
              conversationId,
              participantIds: results.added,
              failed: results.failed,
              addedBy: userId,
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → conversation:participant:added)

            console.log(
              `✅ ${socket.matricule} a ajouté ${results.added.length}/${validIds.length} participant(s) à ${conversationId}`,
            );
          } catch (error) {
            console.error("❌ Erreur addParticipant:", error);
            socket.emit("participant:error", {
              error: error.message,
              code: "ADD_PARTICIPANT_FAILED",
            });
          }
        });

        // ✅ RETIRER UN OU PLUSIEURS PARTICIPANTS D'UN GROUPE
        socket.on("removeParticipant", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("participant:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { conversationId, participantId } = data;
            if (!conversationId || !participantId) {
              return socket.emit("participant:error", {
                error: "conversationId et participantId requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.removeParticipantUseCase) {
              return socket.emit("participant:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            // ✅ Supporter un ID unique ou un tableau d'IDs
            const participantIds = Array.isArray(participantId)
              ? participantId
              : [participantId];

            // Filtrer les valeurs vides
            const validIds = participantIds
              .map((id) => (typeof id === "string" ? id.trim() : String(id)))
              .filter((id) => id.length > 0);

            if (validIds.length === 0) {
              return socket.emit("participant:error", {
                error: "Aucun participantId valide fourni",
                code: "INVALID_PARAMS",
              });
            }

            const results = { removed: [], failed: [] };

            for (const pid of validIds) {
              try {
                await this.removeParticipantUseCase.execute({
                  conversationId,
                  participantId: pid,
                  removedBy: userId,
                  senderSocketId: socket.id,
                });
                results.removed.push(pid);
              } catch (err) {
                results.failed.push({ participantId: pid, error: err.message });
                console.warn(
                  `⚠️ Échec retrait participant ${pid}: ${err.message}`,
                );
              }
            }

            socket.emit("participant:removed", {
              success: results.removed.length > 0,
              conversationId,
              participantIds: results.removed,
              failed: results.failed,
              removedBy: userId,
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → conversation:participant:removed)

            console.log(
              `✅ ${socket.matricule} a retiré ${results.removed.length}/${validIds.length} participant(s) de ${conversationId}`,
            );
          } catch (error) {
            console.error("❌ Erreur removeParticipant:", error);
            socket.emit("participant:error", {
              error: error.message,
              code: "REMOVE_PARTICIPANT_FAILED",
            });
          }
        });

        // ✅ QUITTER UNE CONVERSATION (USE-CASE COMPLET - DB + ROOM)
        socket.on("leaveConversationPermanent", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("conversation:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { conversationId } = data;
            if (!conversationId) {
              return socket.emit("conversation:error", {
                error: "conversationId requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.leaveConversationUseCase) {
              return socket.emit("conversation:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            const result = await this.leaveConversationUseCase.execute({
              conversationId,
              userId,
            });

            // Quitter la room Socket.IO
            socket.leave(`conversation_${conversationId}`);

            // Retirer de la room Redis
            if (this.roomManager) {
              try {
                await this.roomManager.removeUserFromRoom(
                  `conv_${conversationId}`,
                  userId,
                );
              } catch (err) {
                console.warn("⚠️ Erreur retrait room Redis:", err.message);
              }
            }

            socket.emit("conversation:left_permanent", {
              success: true,
              conversationId,
              userId,
              remainingParticipants: result.remainingParticipants,
              timestamp: new Date().toISOString(),
            });

            // Notifier la room
            socket
              .to(`conversation_${conversationId}`)
              .emit("participant:left", {
                conversationId,
                userId,
                matricule: socket.matricule,
                timestamp: new Date().toISOString(),
              });

            console.log(
              `👋 ${socket.matricule} a quitté définitivement ${conversationId}`,
            );
          } catch (error) {
            console.error("❌ Erreur leaveConversationPermanent:", error);
            socket.emit("conversation:error", {
              error: error.message,
              code: "LEAVE_CONVERSATION_FAILED",
            });
          }
        });

        // ✅ MODIFIER LE CONTENU D'UN MESSAGE
        socket.on("editMessage", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("message:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, newContent } = data;
            if (!messageId || !newContent) {
              return socket.emit("message:error", {
                error: "messageId et newContent requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.updateMessageContentUseCase) {
              return socket.emit("message:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            const result = await this.updateMessageContentUseCase.execute({
              messageId,
              newContent,
              userId,
              senderSocketId: socket.id,
            });

            socket.emit("message:edited", {
              success: true,
              messageId,
              conversationId: result.conversationId
                ? String(result.conversationId)
                : undefined,
              userId: String(userId),
              status: "EDITED",
              newContent,
              editedAt: result.editedAt || new Date().toISOString(),
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → message:status EDITED)

            console.log(
              `✏️ ${socket.matricule} a modifié le message ${messageId}`,
            );
          } catch (error) {
            console.error("❌ Erreur editMessage:", error);
            socket.emit("message:error", {
              error: error.message,
              code: "EDIT_MESSAGE_FAILED",
            });
          }
        });

        // ✅ SUPPRIMER UN MESSAGE
        socket.on("deleteMessage", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("message:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, deleteType } = data;
            if (!messageId) {
              return socket.emit("message:error", {
                error: "messageId requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.deleteMessageUseCase) {
              return socket.emit("message:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            const result = await this.deleteMessageUseCase.execute({
              messageId,
              userId,
              deleteType: deleteType || "FOR_ME",
              senderSocketId: socket.id,
            });

            socket.emit("message:deleted", {
              success: true,
              messageId,
              conversationId: result.conversationId
                ? String(result.conversationId)
                : undefined,
              userId: String(userId),
              status: "DELETED",
              deleteType: result.deleteType,
              deletedAt: result.deletedAt,
              timestamp: new Date().toISOString(),
            });

            // 🔄 Broadcast supprimé — distribution via MDS (stream → message:status DELETED)

            console.log(
              `🗑️ ${socket.matricule} a supprimé le message ${messageId} (${result.deleteType})`,
            );
          } catch (error) {
            console.error("❌ Erreur deleteMessage:", error);
            socket.emit("message:error", {
              error: error.message,
              code: "DELETE_MESSAGE_FAILED",
            });
          }
        });

        // ✅ SUPPRIMER UN FICHIER
        // ✅ AJOUTER UNE RÉACTION À UN MESSAGE
        socket.on("addReaction", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("reaction:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, emoji, conversationId } = data;
            if (!messageId || !emoji) {
              return socket.emit("reaction:error", {
                error: "messageId et emoji requis",
                code: "MISSING_PARAMS",
              });
            }

            // ✅ DÉLÉGUER AU USE CASE
            const result = await this.addReactionUseCase.execute({
              messageId,
              userId: String(userId),
              emoji,
              conversationId,
              senderSocketId: socket.id,
            });

            // ✅ ACK IMMÉDIAT À L'ÉMETTEUR
            socket.emit("reaction:added", result);

            console.log(
              `😀 ${socket.matricule} a réagi ${emoji} au message ${messageId}`,
            );
          } catch (error) {
            console.error("❌ Erreur addReaction:", error);
            socket.emit("reaction:error", {
              error: error.message,
              code: "ADD_REACTION_FAILED",
            });
          }
        });

        // ✅ SUPPRIMER UNE RÉACTION D'UN MESSAGE
        socket.on("removeReaction", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("reaction:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, conversationId } = data;
            if (!messageId) {
              return socket.emit("reaction:error", {
                error: "messageId requis",
                code: "MISSING_PARAMS",
              });
            }

            // ✅ DÉLÉGUER AU USE CASE
            const result = await this.removeReactionUseCase.execute({
              messageId,
              userId: String(userId),
              conversationId,
              senderSocketId: socket.id,
            });

            // ✅ ACK IMMÉDIAT
            socket.emit("reaction:removed", result);

            console.log(
              `🚫 ${socket.matricule} a retiré sa réaction du message ${messageId}`,
            );
          } catch (error) {
            console.error("❌ Erreur removeReaction:", error);
            socket.emit("reaction:error", {
              error: error.message,
              code: "REMOVE_REACTION_FAILED",
            });
          }
        });

        // ✅ RÉPONDRE À UN MESSAGE (via ReplyMessage use case)
        socket.on("replyToMessage", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("reply:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, content, conversationId, type } = data;
            if (!messageId || !content) {
              return socket.emit("reply:error", {
                error: "messageId et content requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.replyMessageUseCase) {
              return socket.emit("reply:error", {
                error: "Service de réponse non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            // ✅ DÉLÉGUER AU USE CASE ReplyMessage
            const result = await this.replyMessageUseCase.execute({
              messageId,
              content,
              senderId: userId,
              conversationId,
              senderSocketId: socket.id,
              type: type || "TEXT",
            });

            // ✅ ACK IMMÉDIAT (le message envoyé + info reply)
            socket.emit("reply:sent", {
              success: true,
              messageId: String(messageId),
              replyId: String(result.message?.id || result.message?._id),
              conversationId: result.conversationId,
              userId: String(userId),
              senderName:
                [socket.prenom, socket.nom].filter(Boolean).join(" ") ||
                socket.matricule ||
                String(userId),
              senderMatricule: socket.matricule || null,
              content: content.substring(0, 200),
              replyTo: result.replyTo,
              timestamp: new Date().toISOString(),
            });

            // ✅ Le message de réponse est publié dans le stream par SendMessage (via replyTo)
            console.log(
              `💬 ${socket.matricule} a répondu au message ${messageId}`,
            );
          } catch (error) {
            console.error("❌ Erreur replyToMessage:", error);
            socket.emit("reply:error", {
              error: error.message,
              code: "REPLY_FAILED",
            });
          }
        });

        // ✅ TRANSFÉRER UN MESSAGE VERS UNE OU PLUSIEURS CONVERSATIONS
        socket.on("forwardMessage", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("forward:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { messageId, targetConversationIds } = data;
            if (!messageId) {
              return socket.emit("forward:error", {
                error: "messageId est requis",
                code: "MISSING_PARAMS",
              });
            }

            if (
              !targetConversationIds ||
              (Array.isArray(targetConversationIds) &&
                targetConversationIds.length === 0)
            ) {
              return socket.emit("forward:error", {
                error: "targetConversationIds est requis (string ou array)",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.forwardMessageUseCase) {
              return socket.emit("forward:error", {
                error: "Service de transfert non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            // ✅ EXÉCUTER LE USE CASE
            const result = await this.forwardMessageUseCase.execute({
              originalMessageId: messageId,
              targetConversationIds,
              senderId: userId,
              senderSocketId: socket.id,
            });

            // ✅ ACK IMMÉDIAT
            socket.emit("forward:sent", {
              success: true,
              originalMessageId: String(messageId),
              forwarded: result.forwarded,
              errors: result.errors,
              count: result.count,
              userId: String(userId),
              timestamp: new Date().toISOString(),
            });

            // ✅ Chaque message transféré est publié dans le stream par SendMessage (via ForwardMessage)
            console.log(
              `📤 ${socket.matricule} a transféré le message ${messageId} vers ${result.count} conversation(s)`,
            );
          } catch (error) {
            console.error("❌ Erreur forwardMessage:", error);
            socket.emit("forward:error", {
              error: error.message,
              code: "FORWARD_FAILED",
            });
          }
        });

        // ✅ SUPPRIMER UN FICHIER
        socket.on("deleteFile", async (data) => {
          try {
            const userId = socket.userId;
            if (!userId) {
              return socket.emit("file:error", {
                error: "Authentification requise",
                code: "AUTH_REQUIRED",
              });
            }

            const { fileId, physicalDelete } = data;
            if (!fileId) {
              return socket.emit("file:error", {
                error: "fileId requis",
                code: "MISSING_PARAMS",
              });
            }

            if (!this.deleteFileUseCase) {
              return socket.emit("file:error", {
                error: "Service non disponible",
                code: "SERVICE_UNAVAILABLE",
              });
            }

            const result = await this.deleteFileUseCase.execute({
              fileId,
              userId,
              physicalDelete: physicalDelete !== false,
            });

            socket.emit("file:deleted", {
              success: true,
              fileId: result.fileId,
              deletedAt: result.deletedAt,
              physicalDelete: result.physicalDelete,
              message: result.message,
              timestamp: new Date().toISOString(),
            });

            console.log(
              `🗑️ ${socket.matricule} a supprimé le fichier ${fileId}`,
            );
          } catch (error) {
            console.error("❌ Erreur deleteFile:", error);
            socket.emit("file:error", {
              error: error.message,
              code: "DELETE_FILE_FAILED",
            });
          }
        });

        // Override joinConversation
        socket.on("joinConversation", async (data) => {
          try {
            // Appeler le handler original
            await originalHandlers.joinConversation(socket, data);

            // Mettre à jour la présence
            if (this.roomManager && data.conversationId) {
              const roomName = `conv_${data.conversationId}`;
              await this.roomManager.updateRoomActivity(roomName);
              await this.roomManager.broadcastPresenceUpdate(roomName);
            }
          } catch (error) {
            console.error("❌ Erreur joinConversation avec présence:", error);
          }
        });

        // ... autres overrides si nécessaire ...
      });

      console.log("✅ Gestionnaires Socket.IO configurés avec présence");
    } catch (error) {
      console.error("❌ Erreur configuration Socket.IO:", error);
    }
  }

  // ✅ AUTHENTIFICATION
  async handleAuthentication(socket, data) {
    const authStartTime = Date.now();
    const authStartDate = new Date().toISOString();
    console.log(`\n🔐 [${authStartDate}] ⏱️ AUTHENTIFICATION DÉBUTÉE`);
    try {
      console.log(
        `🔐 [${new Date().toISOString()}] Authentification demande:`,
        data,
      );

      let userPayload = null;
      if (data.token || true) {
        const token = data.token;
        try {
          const fakeReq = {
            headers: { authorization: `Bearer ${token}` },
          };
          const fakeRes = {}; //
          await new Promise((resolve, reject) => {
            AuthMiddleware.valideToken(fakeReq, fakeRes, (err) => {
              if (err) reject(err);
              else resolve();
            });
          });
          if (fakeReq.user) {
            const cacheUserId =
              fakeReq.user.id || fakeReq.user.userId || fakeReq.user.matricule;

            let cachedUserInfo = null;
            if (this.userCacheService && cacheUserId) {
              try {
                cachedUserInfo =
                  await this.userCacheService.fetchUserInfo(cacheUserId);
                console.log("-------------------------------------------");
                console.log(cachedUserInfo);
                console.log("-------------------------------------------");
              } catch (cacheError) {
                console.warn(
                  `⚠️ [Auth] Erreur UserCacheService pour ${cacheUserId}:`,
                  cacheError.message,
                );
              }
            }

            userPayload = {
              ...fakeReq.user,
              nom: cachedUserInfo?.nom || fakeReq.user.nom,
              avatar: cachedUserInfo?.avatar || fakeReq.user.avatar || null,
              matricule:
                fakeReq.user.matricule ||
                cachedUserInfo?.matricule ||
                cacheUserId,
              structure:
                fakeReq.user.structure || cachedUserInfo?.structure || null,
              ministere:
                fakeReq.user.ministere || cachedUserInfo?.ministere || null,
              fcmToken: fakeReq.user.fcmToken || null,
            };
          } else {
            socket.emit("auth_error", {
              message: "Token JWT invalide ou expiré",
              code: "INVALID_TOKEN",
            });
            return;
          }
        } catch (jwtError) {
          socket.emit("auth_error", {
            message: "Token JWT invalide ou expiré",
            code: "INVALID_TOKEN",
          });
          return;
        }
      } else {
        if (!data.userId && !data.matricule) {
          socket.emit("auth_error", {
            message: "Données d'authentification manquantes",
            code: "MISSING_CREDENTIALS",
          });
          return;
        }
        userPayload = {
          id: String(data.matricule),
          userId: String(data.userId),
          matricule: String(data.matricule),
          nom: data.nom || "",
          ministere: data.ministere || "",
          structure: data.structure || "",
          fcmToken: data.fcmToken || null,
        };
      }

      const resolvedMatricule =
        userPayload.matricule || userPayload.userId || "";
      const resolvedUserId = userPayload.matricule || resolvedMatricule;
      socket.userId = resolvedUserId;
      socket.matricule = resolvedMatricule;
      socket.nom = userPayload.nom || "";
      socket.avatar = userPayload.avatar || null;
      socket.ministere = userPayload.ministere || "";
      socket.structure = userPayload.structure || "";
      socket.isAuthenticated = true;

      const userIdString = socket.matricule;
      const matriculeString = socket.matricule;

      const userData = {
        socketId: socket.id,
        matricule: matriculeString,
        connectedAt: new Date(),
        lastActivity: new Date(),
      };

      socket.join(`user_${userIdString}`);

      // ✅ SYNCHRONISATION REDIS EN ARRIÈRE-PLAN (non-bloquante)
      this.syncUserWithRedis(userIdString, userData);

      let conversationIds = [];

      // ✅ ÉTAPE 2 : RÉCUPÉRER LES IDs ET REJOINDRE LES ROOMS
      if (this.getConversationIdsUseCase) {
        const idsStartTime = Date.now();
        try {
          conversationIds =
            await this.getConversationIdsUseCase.execute(userIdString);

          const idsDuration = Date.now() - idsStartTime;
          console.log(
            `✅ [${new Date().toISOString()}] ${
              conversationIds.length
            } ID(s) de conversation récupéré(s) (⏱️ ${idsDuration}ms)`,
          );

          if (conversationIds.length > 0) {
            const joinStartTime = Date.now();
            for (const convId of conversationIds) {
              socket.join(`conversation_${convId}`);
            }
            const joinDuration = Date.now() - joinStartTime;
            console.log(
              `👥 Rooms conversations rejointes (${conversationIds.length}) en ${joinDuration}ms`,
            );

            // ✅ INITIALISER LES ROOMS DANS REDIS (présence)
            if (this.roomManager) {
              for (const convId of conversationIds) {
                const roomName = `conv_${convId}`;
                await this.roomManager.addUserToRoom(roomName, userIdString, {
                  matricule: matriculeString,
                  conversationId: convId,
                });
              }
            }
          }
        } catch (idsError) {
          console.warn(
            `⚠️ Erreur récupération IDs conversations:`,
            idsError.message,
          );
        }
      }

      // ✅ ÉTAPE 1 : RÉCUPÉRER LES CONVERSATIONS COMPLÈTES ET LES LIVRER AU CLIENT
      if (this.getConversationsUseCase) {
        const convStartTime = Date.now();
        try {
          const convResult = await this.getConversationsUseCase.execute(
            userIdString,
            {
              page: 1,
              limit: 200,

              useCache: true,
            },
          );

          const convDuration = Date.now() - convStartTime;
          console.log(
            `✅ [${new Date().toISOString()}] ${
              convResult.conversations?.length || 0
            } conversation(s) récupérée(s) pour ${userIdString} (⏱️ ${convDuration}ms)`,
          );

          // ✅ LIVRER LES CONVERSATIONS AU CLIENT IMMÉDIATEMENT
          if (convResult && convResult.conversations) {
            const convEmitStartTime = Date.now();

            try {
              socket.emit("conversationsLoaded", {
                conversations: convResult.conversations || [],
                pagination: convResult.pagination || {},
                totalUnreadMessages: convResult.totalUnreadMessages || 0,
                unreadConversations: convResult.unreadConversations || 0,
                fromCache: convResult.fromCache || false,
                timestamp: Date.now(),
              });
              const convEmitDuration = Date.now() - convEmitStartTime;
              console.log(
                `📤 [${new Date().toISOString()}] ${
                  convResult.conversations.length
                } conversation(s) envoyée(s) au client (⏱️ ${convEmitDuration}ms)`,
              );
            } catch (convEmitError) {
              console.error(
                `❌ Erreur envoi conversations: ${convEmitError.message}`,
              );
            }
          }
        } catch (convError) {
          console.warn(
            `⚠️ Erreur récupération conversations:`,
            convError.message,
          );
        }
      }

      if (
        socket.ministere &&
        typeof socket.ministere === "string" &&
        socket.ministere.trim()
      ) {
        try {
          const ministereRoom = `ministere_${socket.ministere
            .replace(/\s+/g, "_")
            .toLowerCase()}`;
          socket.join(ministereRoom);
          console.log(
            `🏛️ Utilisateur ${userIdString} rejoint room ministère: ${ministereRoom}`,
          );
        } catch (ministereError) {
          console.error(
            `❌ Erreur jointure room ministère: ${ministereError.message}`,
          );
        }
      } else {
        if (socket.ministere) {
          console.warn(
            `⚠️ socket.ministere n'est pas une chaîne valide: ${typeof socket.ministere} = ${JSON.stringify(
              socket.ministere,
            )}`,
          );
        }
      }

      // ✅ ENREGISTRER LE SOCKET DANS MessageDeliveryService AVANT l'ACK
      // Pour que le client ne reçoive pas d'événements avant que MDS soit prêt
      console.log(
        `🔍 [${new Date().toISOString()}] messageDeliveryService disponible? ${
          this.messageDeliveryService ? "✅ OUI" : "❌ NON"
        }`,
      );

      if (this.messageDeliveryService) {
        try {
          console.log(
            `📤 [${new Date().toISOString()}] Enregistrement socket pour ${userIdString}...`,
          );
          this.messageDeliveryService.registerUserSocket(
            userIdString,
            socket,
            conversationIds,
          );
          console.log(
            `✅ [${new Date().toISOString()}] Socket enregistré pour ${userIdString}`,
          );
        } catch (mdsError) {
          console.error(
            `❌ Erreur enregistrement MessageDeliveryService: ${mdsError.message}`,
          );
        }
      } else {
        console.warn(
          `⚠️ [${new Date().toISOString()}] messageDeliveryService est NULL/UNDEFINED!`,
        );
      }

      // ✅ ENVOYER L'ACK AUTHENTICATED (après enregistrement MDS)

      const emitStartTime = Date.now();
      console.log(
        `📤 [${new Date().toISOString()}] Avant socket.emit('authenticated')...`,
      );
      try {
        socket.emit("authenticated", {
          success: true,
          userId: userIdString,
          matricule: matriculeString,
          nom: socket.nom,
          ministere: socket.ministere,
          structure: socket.structure,
          autoJoinedConversations: conversationIds.length,
          timestamp: new Date().toISOString(),
        });
        const emitDuration = Date.now() - emitStartTime;
        console.log(
          `✅ [${new Date().toISOString()}] socket.emit('authenticated') succès (⏱️ ${emitDuration}ms)`,
        );
      } catch (emitErr) {
        console.error(`❌ Erreur lors du socket.emit: ${emitErr.message}`);
        throw emitErr;
      }

      console.log(
        `✅ [${new Date().toISOString()}] Utilisateur authentifié: ${matriculeString} (${userIdString})`,
      );

      // ✅ LIVRER LES MESSAGES EN ATTENTE (NON-BLOQUANT, après l'ACK)
      if (this.messageDeliveryService) {
        this.messageDeliveryService
          .deliverPendingMessagesOnConnect(userIdString, socket)
          .catch((mdsError) => {
            console.error(
              `❌ Erreur livraison messages en attente: ${mdsError.message}`,
            );
          });
      }

      const totalDuration = Date.now() - authStartTime;
      console.log(
        `\n✅ [${new Date().toISOString()}] ⏱️ AUTHENTIFICATION COMPLÈTE (⏱️ TOTAL: ${totalDuration}ms)\n`,
      );
    } catch (error) {
      console.error("❌ Erreur authentification WebSocket:", error);
      socket.emit("auth_error", {
        message: "Erreur d'authentification",
        code: "AUTH_ERROR",
        details:
          process.env.NODE_ENV === "development" ? error.message : undefined,
      });
    }
  }

  // ✅ DÉCONNEXION
  async handleDisconnection(socket, reason = "unknown") {
    const userId = socket.userId;
    const socketId = socket.id;
    const matricule = socket.matricule;

    try {
      if (userId) {
        // ✅ DÉSENREGISTRER DU MessageDeliveryService
        if (this.messageDeliveryService) {
          this.messageDeliveryService.unregisterUserSocket(userId, socketId);
        }

        // ✅ MARQUER OFFLINE DANS Redis (avec socketId pour multi-connexions)
        if (this.onlineUserManager) {
          await this.onlineUserManager.setUserOffline(userId, socketId);

          // ✅ VÉRIFIER SI L'UTILISATEUR EST RÉELLEMENT OFFLINE APRÈS LA DÉCONNEXION
          const isStillOnline =
            await this.onlineUserManager.isUserOnline(userId);

          if (!isStillOnline) {
            // L'utilisateur n'a plus de connexions actives, broadcaster la déconnexion
            socket.broadcast.emit("user_disconnected", {
              userId,
              matricule,
              timestamp: new Date().toISOString(),
              reason,
            });

            console.log(
              `👋 Utilisateur ${matricule} (${userId}) complètement déconnecté`,
            );
          } else {
            console.log(
              `📱 Socket ${socketId} déconnecté mais ${matricule} (${userId}) reste en ligne`,
            );
          }
        }
      }
    } catch (error) {
      console.error("❌ Erreur déconnexion:", error);
    }

    // ✅ BROADCASTER LES MISES À JOUR DE PRÉSENCE
    if (this.roomManager && socket.userId) {
      const userRooms = await this.roomManager.getUserRooms(socket.userId);

      for (const roomName of userRooms) {
        if (roomName.startsWith("conv_")) {
          setTimeout(() => {
            this.roomManager.broadcastPresenceUpdate(roomName);
          }, 500);
        }
      }
    }
  }

  // ✅ SYNC REDIS - Via OnlineUserManager UNIQUEMENT
  async syncUserWithRedis(userId, userData) {
    const syncStartTime = Date.now();
    console.log(
      `🔴 [${new Date().toISOString()}] Sync Redis lancé en arrière-plan pour ${userId}`,
    );
    if (this.onlineUserManager) {
      try {
        const sanitizedData = {
          socketId: userData.socketId ? String(userData.socketId) : null,
          matricule: userData.matricule
            ? String(userData.matricule)
            : "Unknown",
          connectedAt:
            userData.connectedAt instanceof Date
              ? userData.connectedAt
              : new Date(),
          lastActivity:
            userData.lastActivity instanceof Date
              ? userData.lastActivity
              : new Date(),
        };

        await this.onlineUserManager.setUserOnline(
          String(userId),
          sanitizedData,
        );
        const syncDuration = Date.now() - syncStartTime;
        console.log(
          `✅ [${new Date().toISOString()}] Utilisateur ${userId} synchronisé avec Redis (⏱️ ${syncDuration}ms)`,
        );
      } catch (error) {
        console.warn("⚠️ Erreur sync utilisateur Redis:", error.message);
      }
    }
  }

  // ========================================
  // ✅ ENVOYER UN MESSAGE - SIMPLIFIÉ
  // ========================================
  /**
   * ✅ RESPONSABILITÉ UNIQUE : Valider et déléguer au Use Case
   * SendMessage Use Case gère : MongoDB + Kafka + ResilientService
   */
  async handleSendMessage(socket, data) {
    try {
      const {
        content = "",
        conversationId = "",
        type = "TEXT",
        receiverId = null,
        conversationName = null,
        temporaryId = null,
        fileId = null,
        callMetadata = null,
      } = data;

      const userId = socket.userId;
      const matricule = socket.matricule;

      const normalizedConversationId = this.normalizeMongoId(conversationId);
      const isCallType = type === "CALL" || type === "VIDEO_CALL";

      console.log("💬 Traitement envoi message:", {
        userId,
        conversationId: normalizedConversationId,
        contentLength: content ? content.length : 0,
        type,
        isCall: isCallType,
      });

      // ✅ VALIDATION
      if (!userId) {
        socket.emit("message_error", {
          message: "Authentification requise",
          code: "AUTH_REQUIRED",
        });
        return;
      }

      if (
        !isCallType &&
        type === "TEXT" &&
        (!content || typeof content !== "string" || content.trim().length === 0)
      ) {
        socket.emit("message_error", {
          message: "Le contenu du message est requis",
          code: "MISSING_CONTENT",
        });
        return;
      }

      if (!isCallType && content.trim().length > 10000) {
        socket.emit("message_error", {
          message: "Le message ne peut pas dépasser 10000 caractères",
          code: "CONTENT_TOO_LONG",
        });
        return;
      }

      // if (!normalizedConversationId && !receiverId) {
      //   socket.emit("message_error", {
      //     message: "ID de conversation requis",
      //     code: "MISSING_CONVERSATION_ID",
      //   });
      //   return;
      // }

      if (
        normalizedConversationId &&
        !this.isValidObjectId(normalizedConversationId)
      ) {
        console.log(
          "❌ ID de conversation invalide:",
          normalizedConversationId,
        );
        socket.emit("message_error", {
          message: "ID de conversation invalide",
          code: "INVALID_CONVERSATION_ID",
        });
        return;
      }

      if (receiverId) {
        if (Array.isArray(receiverId)) {
          if (receiverId.includes(userId)) {
            socket.emit("message_error", {
              message:
                "Vous ne pouvez pas vous ajouter vous-même comme destinataire",
              code: "INVALID_RECEIVER",
            });
            return;
          }
        } else if (receiverId === userId) {
          socket.emit("message_error", {
            message: "Vous ne pouvez pas vous envoyer un message à vous-même",
            code: "INVALID_RECEIVER",
          });
          return;
        }
      }

      // ✅ ÉTAPE 1 : DÉLÉGUER AU USE CASE
      // SendMessage gère : MongoDB + Kafka + ResilientService (tout en internal)
      let result;
      try {
        if (this.resilientService) {
          result = await this.resilientService.circuitBreaker.execute(() =>
            this.sendMessageUseCase.execute({
              content: isCallType ? content || "" : content.trim(),
              senderId: userId,
              senderSocketId: socket.id,
              conversationId: normalizedConversationId,
              type,
              receiverId,
              fileId,
              conversationName,
              callMetadata: isCallType ? callMetadata : null,
              temporaryId,
            }),
          );
        } else {
          result = await this.sendMessageUseCase.execute({
            content: isCallType ? content || "" : content.trim(),
            senderId: userId,
            senderSocketId: socket.id,
            conversationId: normalizedConversationId,
            type,
            receiverId,
            fileId,
            conversationName,
            callMetadata: isCallType ? callMetadata : null,
            temporaryId,
          });
        }
      } catch (saveError) {
        console.error("❌ Erreur sendMessageUseCase:", saveError.message);
        socket.emit("message_error", {
          message: "Erreur lors de l'envoi du message",
          code: "SEND_ERROR",
        });
        return;
      }

      if (!result || !result.message) {
        socket.emit("message_error", {
          message: "Erreur lors de l'envoi du message",
          code: "SEND_ERROR",
        });
        return;
      }

      const messageId = result.message._id || result.message.id;

      // ✅ ÉTAPE 2 : RÉPONDRE À L'EXPÉDITEUR (ACK IMMÉDIAT)
      socket.emit("message_sent", {
        success: true,
        messageId,
        message: result.message,
        conversation: result.conversation,
        temporaryId: data.temporaryId,
        status: "sent",
        timestamp: new Date().toISOString(),
      });

      console.log(
        `✅ Message envoyé (Use Case gère Kafka + ResilientService): ${messageId}`,
      );
      // ✅ FIN
      // Tout le reste (Kafka, livraison, retry) est géré en interne par le Use Case
    } catch (error) {
      console.error("❌ Erreur handleSendMessage:", error);

      socket.emit("message_error", {
        message: "Erreur lors de l'envoi du message",
        code:
          this.resilientService?.circuitBreaker.state === "OPEN"
            ? "CIRCUIT_OPEN"
            : "SEND_ERROR",
        error:
          process.env.NODE_ENV === "development" ? error.message : undefined,
      });
    }
  }

  // ✅ UTILITAIRES
  isValidObjectId(id) {
    if (!id || typeof id !== "string") return false;
    return /^[0-9a-fA-F]{24}$/.test(id);
  }

  // ✅ NORMALISER LES IDs MONGODB (gère { $oid: "..." })
  normalizeMongoId(id) {
    if (!id) return id;

    // Cas { $oid: "..." }
    if (typeof id === "object" && id.$oid) {
      return id.$oid;
    }

    return id;
  }

  generateObjectId() {
    const timestamp = Math.floor(Date.now() / 1000)
      .toString(16)
      .padStart(8, "0");
    const machineId = Math.floor(Math.random() * 16777216)
      .toString(16)
      .padStart(6, "0");
    const processId = Math.floor(Math.random() * 65536)
      .toString(16)
      .padStart(4, "0");
    const counter = Math.floor(Math.random() * 16777216)
      .toString(16)
      .padStart(6, "0");

    return timestamp + machineId + processId + counter;
  }

  // ========================================
  // AUTRES GESTIONNAIRES (inchangés)
  // ========================================

  async handleJoinConversation(socket, data) {
    try {
      const { conversationId } = data;
      const userId = socket.userId;

      if (!conversationId || !userId) return;

      // ✅ ACK IMMÉDIAT - ne pas bloquer le client
      socket.emit("conversation_joined", {
        conversationId,
        timestamp: new Date().toISOString(),
      });

      console.log(
        `✅ ${socket.matricule} a rejoint conversation ${conversationId}`,
      );

      // ✅ SYNCHRONISER LA MAP userConversations DANS LE MDS
      if (this.messageDeliveryService) {
        this.messageDeliveryService.addUserConversation(userId, conversationId);
      }

      // ✅ Opérations post-ACK (non-bloquantes pour le client)
      // Marquage des messages comme lus (fire-and-forget)
      if (this.markMessageReadUseCase) {
        this.markMessageReadUseCase
          .execute({
            conversationId,
            userId,
          })
          .catch((err) => {
            console.warn("⚠️ Erreur marquage read:", err.message);
          });
      }

      if (this.onlineUserManager) {
        try {
          await this.onlineUserManager.updateLastActivity(userId);
        } catch (err) {
          console.warn("⚠️ Erreur renouvellement présence:", err.message);
        }
      }

      if (this.roomManager) {
        try {
          const roomName = `conv_${conversationId}`;
          await this.roomManager.addUserToRoom(roomName, userId, {
            matricule: socket.matricule,
            conversationId: conversationId,
          });

          // ✅ Mise à jour présence (intégrée ici au lieu d'un override séparé)
          await this.roomManager.updateRoomActivity(roomName);
          await this.roomManager.broadcastPresenceUpdate(roomName);
        } catch (err) {
          console.warn("⚠️ Erreur ajout room Redis:", err.message);
        }
      }
    } catch (error) {
      console.error("❌ Erreur handleJoinConversation:", error);
      socket.emit("conversation_error", {
        message: "Erreur lors de la connexion à la conversation",
        code: "JOIN_ERROR",
      });
    }
  }

  async handleLeaveConversation(socket, data) {
    try {
      const { conversationId } = data;
      const userId = socket.userId;

      if (!conversationId || !userId) return;

      socket.leave(`conversation_${conversationId}`);

      // ✅ SYNCHRONISER LA MAP userConversations DANS LE MDS
      if (this.messageDeliveryService) {
        this.messageDeliveryService.removeUserConversation(
          userId,
          conversationId,
        );
      }

      socket
        .to(`conversation_${conversationId}`)
        .emit("user_left_conversation", {
          userId,
          matricule: socket.matricule,
          conversationId,
          timestamp: new Date().toISOString(),
        });

      if (this.roomManager) {
        try {
          const roomName = `conv_${conversationId}`;
          await this.roomManager.removeUserFromRoom(roomName, userId);
        } catch (err) {
          console.warn("⚠️ Erreur retrait room Redis:", err.message);
        }
      }

      console.log(
        `👋 ${socket.matricule} a quitté conversation ${conversationId}`,
      );
    } catch (error) {
      console.error("❌ Erreur handleLeaveConversation:", error);
    }
  }

  handleTyping(socket, data) {
    try {
      const { conversationId, event } = data;
      const userId = socket.userId;

      if (!conversationId || !userId) return;

      // ✅ DÉTERMINER L'ÉVÉNEMENT: start, refresh ou auto-détection
      const typingEvent = event || "typing:start";

      // ✅ VALIDATION: événements autorisés
      const allowedEvents = ["typing:start", "typing:refresh"];
      const finalEvent = allowedEvents.includes(typingEvent)
        ? typingEvent
        : "typing:start";

      // ✅ PUBLIER DANS REDIS STREAM
      const resilientService = this.resilientService;

      if (resilientService && resilientService.redis) {
        resilientService.redis
          .xAdd("chat:stream:events:typing", "*", {
            conversationId: String(conversationId),
            senderId: String(userId),
            senderSocketId: String(socket.id),
            event: finalEvent,
            timestamp: String(Date.now()),
          })
          .catch((err) => {
            console.error(`❌ Erreur publication ${finalEvent}:`, err.message);
          });

        console.log(`📝 Événement ${finalEvent} publié:`, {
          conversationId,
          userId,
        });
      }

      // 🔄 Broadcast supprimé — distribution via MDS (stream → typing:event)
    } catch (error) {
      console.error("❌ Erreur handleTyping:", error);
    }
  }

  handleStopTyping(socket, data) {
    try {
      const { conversationId } = data;
      const userId = socket.userId;

      if (!conversationId || !userId) return;

      // ✅ PUBLIER DANS REDIS STREAM: "typing:stop"
      const resilientService = this.resilientService;

      if (resilientService && resilientService.redis) {
        resilientService.redis
          .xAdd("chat:stream:events:typing", "*", {
            conversationId: String(conversationId),
            senderId: String(userId),
            senderSocketId: String(socket.id),
            event: "typing:stop",
            timestamp: String(Date.now()),
          })
          .catch((err) => {
            console.error("❌ Erreur publication typing:stop:", err.message);
          });

        console.log(`🛑 Événement typing:stop publié:`, {
          conversationId,
          userId,
        });
      }

      // 🔄 Broadcast supprimé — distribution via MDS (stream → typing:event)
    } catch (error) {
      console.error("❌ Erreur handleStopTyping:", error);
    }
  }

  async handleMarkMessageDelivered(socket, data) {
    try {
      const { messageId, conversationId } = data;
      const userId = socket.userId;

      if (!messageId || !userId) return;

      if (!this.markMessageDeliveredUseCase) {
        console.warn("⚠️ MarkMessageDeliveredUseCase non disponible");
        return;
      }

      try {
        await this.markMessageDeliveredUseCase.execute({
          messageId,
          userId,
          conversationId,
        });
        // ✅ PAS D'ÉMISSION ICI
        // Le use case publie déjà le statut DELIVERED dans Redis Streams
        // → Le consumer MessageDeliveryService distribue via 'message:status'
        // → Évite la triple émission (messageStatusChanged + messageDelivered + message:status)
      } catch (err) {
        console.warn("⚠️ Erreur marquage delivered:", err.message);
      }
    } catch (error) {
      console.error("❌ Erreur handleMarkMessageDelivered:", error);
    }
  }

  async handleMarkMessageRead(socket, data) {
    try {
      const { messageId, conversationId, messageIds } = data;
      const userId = socket.userId;

      if (!userId) return;

      if (!this.markMessageReadUseCase) {
        console.warn("⚠️ MarkMessageReadUseCase non disponible");
        return;
      }

      // ✅ Validation : il faut au moins messageId OU (conversationId + messageIds)
      if (
        !messageId &&
        !(conversationId && Array.isArray(messageIds) && messageIds.length > 0)
      ) {
        console.warn("⚠️ markMessageRead: données insuffisantes", {
          messageId,
          conversationId,
          messageIdsCount: messageIds?.length,
        });
        return;
      }

      try {
        await this.markMessageReadUseCase.execute({
          messageId,
          userId,
          conversationId,
          messageIds: Array.isArray(messageIds) ? messageIds : null,
        });
        // ✅ PAS D'ÉMISSION ICI
        // Le use case publie déjà le statut READ dans Redis Streams
        // → Le consumer MessageDeliveryService distribue via 'message:status'
        // → Évite la triple émission (messageStatusChanged + messageRead + message:status)

        if (Array.isArray(messageIds) && messageIds.length > 1) {
          console.log(
            `📖 Batch markRead: ${messageIds.length} messages marqués lus par ${socket.matricule || userId}`,
          );
        }
      } catch (err) {
        console.warn("⚠️ Erreur marquage read:", err.message);
      }
    } catch (error) {
      console.error("❌ Erreur handleMarkMessageRead:", error);
    }
  }

  async handleGetMessages(socket, data) {
    try {
      const {
        conversationId,
        page = data.page || 1,
        limit = data.limit || 50,
        cursor = data.cursor,
      } = data;
      const userId = socket.userId;

      const normalizedConversationId = this.normalizeMongoId(conversationId);

      console.log("📨 Récupération messages:", {
        conversationId: normalizedConversationId,
        page,
        limit,
        userId,
      });

      if (!normalizedConversationId || !userId) {
        socket.emit("messages_error", {
          message: "ID conversation ou utilisateur manquant",
          code: "MISSING_DATA",
        });
        return;
      }

      if (!this.isValidObjectId(normalizedConversationId)) {
        console.log(
          "❌ ID de conversation invalide:",
          normalizedConversationId,
        );
        socket.emit("messages_error", {
          message: "ID de conversation invalide",
          code: "INVALID_CONVERSATION_ID",
        });
        return;
      }

      if (!this.getMessagesUseCase) {
        socket.emit("messages_error", {
          message: "Service non disponible",
          code: "SERVICE_UNAVAILABLE",
        });
        return;
      }

      const result = await this.getMessagesUseCase.execute(
        normalizedConversationId,
        {
          cursor: cursor, // Toujours à null pour quick load
          page: parseInt(page),
          limit: parseInt(limit),
          userId,
        },
      );

      socket.emit("messagesLoaded", result);
    } catch (error) {
      console.error("❌ Erreur handleGetMessages:", error);
      socket.emit("messages_error", {
        message: "Erreur lors de la récupération des messages",
        code: "GET_MESSAGES_ERROR",
      });
    }
  }

  async handleGetConversations(socket, data) {
    try {
      const userId = socket.userId;
      const { page = 1, limit = 20 } = data || {};

      if (!userId) {
        socket.emit("conversations_error", {
          message: "ID utilisateur manquant",
          code: "MISSING_USER_ID",
        });
        return;
      }

      if (!this.getConversationsUseCase) {
        socket.emit("conversations_error", {
          message: "Service non disponible",
          code: "SERVICE_UNAVAILABLE",
        });
        return;
      }

      const result = await this.getConversationsUseCase.execute(userId);

      socket.emit("conversationsLoaded", {
        conversations: result.conversations || [],
        pagination: result.pagination || {},
        totalUnreadMessages: result.totalUnreadMessages || 0,
        fromCache: result.fromCache || false,
      });
    } catch (error) {
      console.error("❌ Erreur handleGetConversations:", error);
      socket.emit("conversations_error", {
        message: "Erreur lors de la récupération des conversations",
        code: "GET_CONVERSATIONS_ERROR",
      });
    }
  }

  async handleGetConversation(socket, data) {
    try {
      const userId = socket.userId;
      const { conversationId } = data || {};

      const normalizedConversationId = this.normalizeMongoId(conversationId);

      if (!normalizedConversationId || !userId) {
        socket.emit("conversation_error", {
          message: "ID conversation ou utilisateur manquant",
          code: "MISSING_DATA",
        });
        return;
      }

      if (!this.isValidObjectId(normalizedConversationId)) {
        console.log(
          "❌ ID de conversation invalide:",
          normalizedConversationId,
        );
        socket.emit("conversation_error", {
          message: "ID de conversation invalide",
          code: "INVALID_CONVERSATION_ID",
        });
        return;
      }

      if (!this.getConversationUseCase) {
        socket.emit("conversation_error", {
          message: "Service non disponible",
          code: "SERVICE_UNAVAILABLE",
        });
        return;
      }

      const result = await this.getConversationUseCase.execute(
        normalizedConversationId,
        userId,
      );

      socket.emit("conversationLoaded", {
        conversation: result.conversation || result,
        metadata: {
          fromCache: result.fromCache || false,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      console.error("❌ Erreur handleGetConversation:", error);
      socket.emit("conversation_error", {
        message: "Erreur lors de la récupération de la conversation",
        code: "GET_CONVERSATION_ERROR",
      });
    }
  }

  // ✅ DÉCONNEXION - NETTOYER LES RESSOURCES
  async handleDisconnection(socket, reason = "unknown") {
    const userId = socket.userId;
    const socketId = socket.id;

    try {
      if (userId) {
        // ✅ DÉSENREGISTRER DU MessageDeliveryService
        if (this.messageDeliveryService) {
          this.messageDeliveryService.unregisterUserSocket(userId, socketId);
        }

        // ✅ MARQUER OFFLINE DANS Redis
        if (this.onlineUserManager) {
          await this.onlineUserManager.setUserOffline(userId, socketId);
        }

        // Notifier les autres utilisateurs
        socket.broadcast.emit("user_disconnected", {
          userId,
          matricule: socket.matricule,
          timestamp: new Date().toISOString(),
          reason,
        });

        console.log(
          `👋 Utilisateur ${socket.matricule} (${userId}) déconnecté`,
        );
      }
    } catch (error) {
      console.error("❌ Erreur handleDisconnection:", error);
    }
  }
}

module.exports = ChatHandler;
