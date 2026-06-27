/**
 * MessageDeliveryService - CONSOMMATEUR MULTI-STREAMS avec xReadGroup
 * ✅ Consomme PLUSIEURS streams par type (privé, groupe, typing, etc.)
 * ✅ Priorisation automatique (typing > privé > groupe)
 * ✅ Acknowledge après livraison
 * ✅ Messages en attente pour utilisateurs déconnectés
 * ✅ Scalable jusqu'à des millions d'utilisateurs
 */

class MessageDeliveryService {
  constructor(redis, io) {
    if (!redis || !io) {
      throw new Error(
        "Redis et Socket.io sont requis pour MessageDeliveryService"
      );
    }

    this.redis = redis;
    this.io = io;

    // ✅ CONFIGURATION DES STREAMS PAR PRIORITÉ
    this.STREAM_CONFIGS = {
      // Priorité 0 : Ultra-temps réel (typing, présence)
      typing: {
        streamKey: "stream:events:typing",
        groupId: "delivery-typing",
        priority: 0,
        interval: 50, // Consommer TRÈS souvent
      },
      // Priorité 1 : Temps réel (messages privés)
      private: {
        streamKey: "stream:messages:private",
        groupId: "delivery-private",
        priority: 1,
        interval: 100,
      },
      // Priorité 2 : Normal (messages groupe)
      group: {
        streamKey: "stream:messages:group",
        groupId: "delivery-group",
        priority: 2,
        interval: 200,
      },
      // Priorité 3 : Notifications
      notifications: {
        streamKey: "stream:messages:system",
        groupId: "delivery-notifications",
        priority: 3,
        interval: 500,
      },
      // Priorité 4 : Read receipts (faible priorité)
      readReceipts: {
        streamKey: "stream:events:read",
        groupId: "delivery-read",
        priority: 4,
        interval: 1000,
      },
    };

    this.streamConsumers = new Map(); // streamKey → { redis, config, isRunning, interval }
    this.userSockets = new Map(); // userId → [socketIds]
    this.userConversations = new Map(); // userId → [conversationIds]

    // ✅ CONFIGURATION GÉNÉRALE
    this.pendingMessagesPrefix = "pending:messages:"; // pending:messages:2
    this.blockTimeout = 1000; // 1 sec max per stream
    this.maxMessagesPerRead = 20;

    this.isRunning = false;
  }

  /**
   * ✅ INITIALISER TOUS LES CONSUMERS
   */
  async initialize() {
    try {
      console.log("🚀 Initialisation MessageDeliveryService Multi-Streams...");

      // Créer un consumer pour chaque stream
      for (const [streamType, config] of Object.entries(this.STREAM_CONFIGS)) {
        await this.createStreamConsumer(streamType, config);
      }

      // Démarrer les consumers
      this.startAllConsumers();

      console.log(
        `✅ MessageDeliveryService initialisé avec ${this.streamConsumers.size} streams`
      );
      return true;
    } catch (error) {
      console.error("❌ Erreur initialisation MessageDeliveryService:", error);
      throw error;
    }
  }

  /**
   * ✅ CRÉER UN CONSUMER POUR UN STREAM (CORRIGÉ)
   */
  async createStreamConsumer(streamType, config) {
    try {
      const redisConsumer = this.redis.duplicate();
      await redisConsumer.connect();

      // ✅ CRÉER UN SEUL CONSUMER GROUP (pas par utilisateur)
      try {
        await redisConsumer.xGroupCreate(
          config.streamKey,
          config.groupId,
          "$",
          { MKSTREAM: true }
        );
        console.log(
          `✅ Consumer group créé: ${config.groupId} pour ${streamType}`
        );
      } catch (groupErr) {
        if (!groupErr.message.includes("BUSYGROUP")) {
          throw groupErr;
        }
        console.log(`ℹ️ Consumer group existant: ${config.groupId}`);
      }

      // ✅ ENREGISTRER LE CONSUMER
      this.streamConsumers.set(config.streamKey, {
        redis: redisConsumer,
        config,
        streamType,
        isRunning: false,
        interval: null,
      });

      console.log(`🔧 Consumer configuré: ${streamType} (${config.streamKey})`);
    } catch (error) {
      console.error(
        `❌ Erreur création consumer ${streamType}:`,
        error.message
      );
    }
  }

  /**
   * ✅ DÉMARRER TOUS LES CONSUMERS AVEC PRIORITÉ
   */
  startAllConsumers() {
    this.isRunning = true;

    // Trier par priorité (0 = plus haute)
    const sorted = Array.from(this.streamConsumers.values()).sort(
      (a, b) => a.config.priority - b.config.priority
    );

    for (const consumer of sorted) {
      this.startConsumerForStream(consumer);
    }

    console.log("▶️ Tous les consumers démarrés avec priorisation");
  }

  /**
   * ✅ DÉMARRER UN CONSUMER AVEC SON INTERVALLE
   */
  startConsumerForStream(consumer) {
    if (consumer.isRunning) {
      return;
    }

    consumer.isRunning = true;

    const interval = consumer.config.interval;

    consumer.interval = setInterval(async () => {
      if (!this.isRunning) return;

      try {
        await this.consumeStream(consumer);
      } catch (error) {
        console.error(
          `❌ Erreur boucle ${consumer.streamType}:`,
          error.message
        );
      }
    }, interval);

    console.log(
      `⏱️ Consumer ${consumer.streamType} démarré (interval: ${interval}ms, priorité: ${consumer.config.priority})`
    );
  }

  /**
   * ✅ CONSOMMER UN STREAM (CORRIGÉ)
   */
  async consumeStream(consumer) {
    try {
      // ✅ UTILISER UN CONSUMER ID GÉNÉRIQUE (pas par utilisateur)
      const consumerId = `${consumer.config.groupId}:delivery-worker`;

      try {
        // ✅ LIRE TOUS LES MESSAGES DU STREAM
        const messages = await consumer.redis.xReadGroup(
          consumer.config.groupId,
          consumerId,
          { key: consumer.config.streamKey, id: ">" },
          { COUNT: this.maxMessagesPerRead, BLOCK: this.blockTimeout }
        );

        if (messages && messages.length > 0) {
          const entries = messages[0]?.messages || [];

          for (const entry of entries) {
            try {
              const message = entry.message;

              // ✅ DISTRIBUER LE MESSAGE AU BON DESTINATAIRE
              await this.distributeMessageToRecipient(
                consumer.streamType,
                message,
                entry.id
              );

              // ✅ ACK APRÈS LIVRAISON RÉUSSIE
              await consumer.redis.xAck(
                consumer.config.streamKey,
                consumer.config.groupId,
                entry.id
              );
            } catch (messageError) {
              console.warn(
                `⚠️ Erreur traitement message ${consumer.streamType}:`,
                messageError.message
              );
            }
          }
        }
      } catch (streamError) {
        if (!streamError.message.includes("timeout")) {
          console.warn(
            `⚠️ Erreur consommation stream ${consumer.streamType}:`,
            streamError.message
          );
        }
      }
    } catch (error) {
      console.error(
        `❌ Erreur consumeStream ${consumer.streamType}:`,
        error.message
      );
    }
  }

  /**
   * ✅ NOUVELLE MÉTHODE : DISTRIBUER LE MESSAGE AU BON DESTINATAIRE
   */
  async distributeMessageToRecipient(streamType, message, entryId) {
    try {
      console.log(`📬 Distribution message ${streamType}:`, {
        messageId: message.messageId,
        senderId: message.senderId,
        receiverId: message.receiverId,
        conversationId: message.conversationId,
      });

      switch (streamType) {
        // ✅ CAS 1 : MESSAGES PRIVÉS
        case "private":
          if (message.receiverId) {
            const receiverId = String(message.receiverId);

            console.log(
              `➡️ Livraison message privé: ${message.senderId} → ${receiverId}`
            );

            // ✅ VÉRIFIER QUE LE DESTINATAIRE EST CONNECTÉ
            if (this.userSockets.has(receiverId)) {
              await this.deliverPrivateMessage(message, receiverId);
            } else {
              console.log(
                `⏳ Destinataire ${receiverId} déconnecté, message en attente`
              );
              await this.addToPendingQueue(receiverId, message);
            }
          } else {
            console.warn("⚠️ Message privé sans receiverId:", message);
          }
          break;

        // ✅ CAS 2 : MESSAGES DE GROUPE
        case "group":
          if (message.conversationId) {
            console.log(
              `➡️ Livraison message groupe: ${message.conversationId}`
            );

            // ✅ LIVRER À TOUS LES PARTICIPANTS CONNECTÉS
            await this.deliverGroupMessageToAllParticipants(message);
          } else {
            console.warn("⚠️ Message groupe sans conversationId:", message);
          }
          break;

        // ✅ CAS 3 : TYPING EVENTS
        case "typing":
          if (message.conversationId) {
            await this.deliverTypingEventToConversationParticipants(message);
          }
          break;

        // ✅ CAS 4 : READ RECEIPTS
        case "readReceipts":
          if (message.messageId && message.senderId) {
            // Livrer à l'expéditeur original du message
            const originalSender = String(message.senderId);
            if (this.userSockets.has(originalSender)) {
              await this.deliverReadReceipt(message, originalSender);
            }
          }
          break;

        // ✅ CAS 5 : NOTIFICATIONS SYSTÈME
        case "notifications":
          if (message.userId) {
            const targetUser = String(message.userId);
            if (this.userSockets.has(targetUser)) {
              await this.deliverNotification(message, targetUser);
            } else {
              await this.addToPendingQueue(targetUser, message);
            }
          }
          break;

        default:
          console.warn(`⚠️ Stream type inconnu: ${streamType}`);
      }
    } catch (error) {
      console.error(`❌ Erreur distribution message ${streamType}:`, error);
      throw error;
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE GROUPE À TOUS LES PARTICIPANTS
   */
  async deliverGroupMessageToAllParticipants(message) {
    try {
      const conversationId = String(message.conversationId);
      const senderId = String(message.senderId);

      // ✅ RÉCUPÉRER TOUS LES UTILISATEURS CONNECTÉS DE LA CONVERSATION
      const connectedUsers = [];

      for (const [userId, socketIds] of this.userSockets.entries()) {
        // ✅ IGNORER L'EXPÉDITEUR
        if (userId === senderId) continue;

        // ✅ VÉRIFIER SI L'UTILISATEUR EST DANS LA CONVERSATION
        const userConversations = this.userConversations.get(userId) || [];
        if (userConversations.includes(conversationId)) {
          connectedUsers.push(userId);
        }
      }

      console.log(
        `👥 Livraison message groupe à ${connectedUsers.length} utilisateur(s) connecté(s)`
      );

      // ✅ LIVRER À CHAQUE UTILISATEUR CONNECTÉ
      for (const userId of connectedUsers) {
        await this.deliverGroupMessage(message, userId);
      }

      console.log(
        `✅ Message groupe livré: ${senderId} → conv:${conversationId} (${connectedUsers.length} destinataires)`
      );
    } catch (error) {
      console.error("❌ Erreur livraison message groupe:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT TYPING AUX PARTICIPANTS
   */
  async deliverTypingEventToConversationParticipants(message) {
    try {
      const conversationId = String(message.conversationId);
      const senderId = String(message.senderId);

      // ✅ LIVRER À TOUS LES PARTICIPANTS SAUF L'EXPÉDITEUR
      for (const [userId, socketIds] of this.userSockets.entries()) {
        if (userId === senderId) continue;

        const userConversations = this.userConversations.get(userId) || [];
        if (userConversations.includes(conversationId)) {
          await this.deliverTypingEvent(message, userId);
        }
      }

      console.log(`⌨️ Typing event livré pour conversation: ${conversationId}`);
    } catch (error) {
      console.error("❌ Erreur livraison typing event:", error);
    }
  }

  /**
   * ✅ ROUTER LES MESSAGES SELON LE TYPE DE STREAM
   */
  async routeMessageByStreamType(streamType, message, userId) {
    const userIdStr = String(userId);

    console.log(
      `➡️ Routing message ${streamType} pour utilisateur ${userIdStr}`
    );

    console.log(
      "Receiver check:",
      message.receiverId && String(message.receiverId) === userIdStr
    );

    switch (streamType) {
      // ✅ CAS 1 : MESSAGES PRIVÉS
      case "private":
        if (message.receiverId && String(message.receiverId) === userIdStr) {
          console.log("Livraison message privé à", userIdStr);
          await this.deliverPrivateMessage(message, userIdStr);
        }
        break;

      // ✅ CAS 2 : MESSAGES DE GROUPE
      case "group":
        if (
          message.conversationId &&
          (await this.isUserInConversation(userIdStr, message.conversationId))
        ) {
          await this.deliverGroupMessage(message, userIdStr);
        }
        break;

      // ✅ CAS 3 : TYPING EVENTS
      case "typing":
        if (message.receiverId && String(message.receiverId) === userIdStr) {
          await this.deliverTypingEvent(message, userIdStr);
        }
        break;

      // ✅ CAS 4 : READ RECEIPTS
      case "readReceipts":
        if (message.senderId && String(message.senderId) === userIdStr) {
          await this.deliverReadReceipt(message, userIdStr);
        }
        break;

      // ✅ CAS 5 : NOTIFICATIONS SYSTÈME
      case "notifications":
        await this.deliverNotification(message, userIdStr);
        break;

      default:
        console.warn(`⚠️ Stream type inconnu: ${streamType}`);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE PRIVÉ
   */
  async deliverPrivateMessage(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      console.log("userSockets", socketIds);

      if (!socketIds || socketIds.length === 0) {
        // Utilisateur pas connecté - ajouter en queue d'attente
        await this.addToPendingQueue(userId, message);
        return;
      }

      // Envoyer à toutes les connexions de l'utilisateur
      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("newMessage", {
            messageId: message.messageId,
            conversationId: message.conversationId,
            senderId: message.senderId,
            receiverId: message.receiverId,
            content: message.content,
            type: message.type,
            status: message.status || "SENT",
            timestamp: message.timestamp,
            metadata: message.metadata,
          });
        }
      }

      console.log(`✅ Message privé livré: ${message.senderId} → ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverPrivateMessage:", error);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE GROUPE
   */
  async deliverGroupMessage(message, userId) {
    try {
      const room = `conversation_${message.conversationId}`;
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("message:group", {
            messageId: message.messageId,
            conversationId: message.conversationId,
            senderId: message.senderId,
            content: message.content,
            type: message.type,
            status: message.status || "SENT",
            timestamp: message.timestamp,
            metadata: message.metadata,
          });
        }
      }

      console.log(
        `✅ Message groupe livré à ${userId} (${message.conversationId})`
      );
    } catch (error) {
      console.error("❌ Erreur deliverGroupMessage:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT TYPING (ULTRA-RAPIDE)
   */
  async deliverTypingEvent(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("typing:event", {
            conversationId: message.conversationId,
            userId: message.senderId,
            isTyping: message.event === "TYPING_STARTED",
            timestamp: message.timestamp,
          });
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverTypingEvent:", error);
    }
  }

  /**
   * ✅ LIVRER UN ACCUSÉ DE LECTURE
   */
  async deliverReadReceipt(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("read:receipt", {
            messageId: message.messageId,
            conversationId: message.conversationId,
            readBy: message.readBy,
            readAt: message.timestamp,
          });
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverReadReceipt:", error);
    }
  }

  /**
   * ✅ LIVRER UNE NOTIFICATION
   */
  async deliverNotification(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("notification:system", {
            notificationId: message.messageId,
            title: message.title,
            message: message.content,
            type: message.type,
            timestamp: message.timestamp,
          });
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverNotification:", error);
    }
  }

  /**
   * ✅ ENREGISTRER UN SOCKET UTILISATEUR
   */
  registerUserSocket(userId, socket, conversationIds = []) {
    try {
      const userIdStr = String(userId);

      if (!this.userSockets.has(userIdStr)) {
        this.userSockets.set(userIdStr, []);
      }

      this.userSockets.get(userIdStr).push(socket.id);
      this.userConversations.set(userIdStr, conversationIds);

      console.log(
        `✅ Socket enregistré: ${userIdStr} (${
          this.userSockets.get(userIdStr).length
        } socket(s))`
      );

      return true;
    } catch (error) {
      console.error("❌ Erreur registerUserSocket:", error);
      return false;
    }
  }

  /**
   * ✅ DÉSENREGISTRER UN SOCKET
   */
  unregisterUserSocket(userId, socketId) {
    try {
      const userIdStr = String(userId);

      if (!this.userSockets.has(userIdStr)) {
        return true;
      }

      const sockets = this.userSockets.get(userIdStr);
      const index = sockets.indexOf(socketId);

      if (index > -1) {
        sockets.splice(index, 1);
      }

      if (sockets.length === 0) {
        this.userSockets.delete(userIdStr);
        this.userConversations.delete(userIdStr);
      }

      return true;
    } catch (error) {
      console.error("❌ Erreur unregisterUserSocket:", error);
      return false;
    }
  }

  /**
   * ✅ LIVRER LES MESSAGES EN ATTENTE À LA CONNEXION
   */
  async deliverPendingMessagesOnConnect(userId, socket) {
    try {
      const userIdStr = String(userId);

      console.log(`📥 Livraison messages en attente pour ${userIdStr}...`);

      let deliveredCount = 0;

      // ✅ RÉCUPÉRER LES MESSAGES EN ATTENTE
      const pendingKey = `${this.pendingMessagesPrefix}${userIdStr}`;

      try {
        const pendingMessages = await this.redis.lRange(pendingKey, 0, -1);

        console.log(
          `📨 ${pendingMessages.length} message(s) en attente trouvé(s) pour ${userIdStr}`
        );

        for (const messageJson of pendingMessages) {
          try {
            const message = JSON.parse(messageJson);

            // ✅ DISTRIBUER LE MESSAGE
            await this.deliverPrivateMessage(message, userIdStr);

            // ✅ SUPPRIMER DE LA LISTE D'ATTENTE
            await this.redis.lRem(pendingKey, 1, messageJson);

            deliveredCount++;
            console.log(
              `✅ Message en attente livré et supprimé: ${message.messageId}`
            );
          } catch (error) {
            console.error(
              `❌ Erreur traitement message en attente:`,
              error.message
            );
          }
        }
      } catch (pendingError) {
        console.warn(
          `⚠️ Erreur récupération messages en attente:`,
          pendingError.message
        );
      }

      console.log(
        `✅ ${deliveredCount} message(s) livré(s) à ${userIdStr} à la connexion`
      );

      return deliveredCount;
    } catch (error) {
      console.error("❌ Erreur livraison messages en attente:", error);
      return 0;
    }
  }

  /**
   * ✅ VÉRIFIER SI L'UTILISATEUR EST DANS LA CONVERSATION
   */
  async isUserInConversation(userId, conversationId) {
    try {
      const conversationIds = this.userConversations.get(String(userId)) || [];
      return conversationIds.includes(String(conversationId));
    } catch (error) {
      console.warn("⚠️ Erreur isUserInConversation:", error);
      return false;
    }
  }

  /**
   * ✅ AJOUTER UN MESSAGE EN ATTENTE
   */
  async addToPendingQueue(userId, message) {
    try {
      const pendingKey = `${this.pendingMessagesPrefix}${userId}`;
      const messageJson = JSON.stringify({
        messageId: message.messageId,
        conversationId: message.conversationId,
        senderId: message.senderId,
        receiverId: message.receiverId,
        content: message.content,
        type: message.type,
        status: message.status || "SENT",
        timestamp: message.timestamp,
        metadata: message.metadata,
      });

      await this.redis.lPush(pendingKey, messageJson);
      await this.redis.expire(pendingKey, 86400); // 24h TTL

      console.log(`📝 Message ajouté en attente pour ${userId}`);
    } catch (error) {
      console.error("❌ Erreur addToPendingQueue:", error);
    }
  }

  /**
   * ✅ ARRÊTER TOUS LES CONSUMERS
   */
  async stopAllConsumers() {
    this.isRunning = false;

    for (const [streamKey, consumer] of this.streamConsumers.entries()) {
      if (consumer.interval) {
        clearInterval(consumer.interval);
      }

      if (consumer.redis) {
        try {
          await consumer.redis.quit();
        } catch (err) {
          console.warn(
            `⚠️ Erreur fermeture consumer ${streamKey}:`,
            err.message
          );
        }
      }
    }

    console.log("✅ Tous les consumers arrêtés");
  }

  /**
   * ✅ STATISTIQUES
   */
  getStats() {
    return {
      isRunning: this.isRunning,
      streams: Array.from(this.streamConsumers.keys()),
      streamConsumers: Array.from(this.streamConsumers.values()).map((c) => ({
        streamType: c.streamType,
        streamKey: c.config.streamKey,
        priority: c.config.priority,
        interval: c.config.interval,
        isRunning: c.isRunning,
      })),
      connectedUsers: this.userSockets.size,
      totalSockets: Array.from(this.userSockets.values()).reduce(
        (sum, sockets) => sum + sockets.length,
        0
      ),
      users: Array.from(this.userSockets.entries()).map(
        ([userId, sockets]) => ({
          userId,
          socketsCount: sockets.length,
          conversationsCount: (this.userConversations.get(userId) || []).length,
        })
      ),
    };
  }

  /**
   * ✅ NETTOYER ET ARRÊTER
   */
  async cleanup() {
    try {
      await this.stopAllConsumers();
      this.userSockets.clear();
      this.userConversations.clear();
      console.log("✅ MessageDeliveryService nettoyé");
    } catch (error) {
      console.error("❌ Erreur nettoyage MessageDeliveryService:", error);
    }
  }

  /**
   * ✅ DIAGNOSTIC COMPLET DE LA LIVRAISON
   */
  async diagnoseDelivery(userId) {
    const userIdStr = String(userId);

    console.log(
      `🔍 ========== DIAGNOSTIC LIVRAISON POUR ${userIdStr} ==========`
    );

    try {
      const diagnostics = {
        userId: userIdStr,
        timestamp: new Date().toISOString(),
        checks: {},
      };

      // ✅ CHECK 1 : Utilisateur enregistré dans userSockets?
      const isRegistered = this.userSockets.has(userIdStr);
      const socketIds = this.userSockets.get(userIdStr) || [];

      diagnostics.checks.userRegistration = {
        registered: isRegistered,
        socketCount: socketIds.length,
        socketIds: socketIds,
        status: isRegistered ? "✅ OK" : "❌ PAS ENREGISTRÉ",
      };

      console.log(`   ${diagnostics.checks.userRegistration.status}`);
      if (socketIds.length > 0) {
        console.log(`   Sockets: ${socketIds.join(", ")}`);
      }

      // ✅ CHECK 2 : Vérifier chaque stream Redis
      console.log("\n📊 État des streams Redis:");

      const streamChecks = {};

      for (const [streamType, config] of Object.entries(this.STREAM_CONFIGS)) {
        try {
          const streamKey = config.streamKey;
          const length = await this.redis.xLen(streamKey);

          // Récupérer les derniers messages du stream
          const recentMessages = await this.redis.xRevRange(
            streamKey,
            "+",
            "-",
            {
              COUNT: 5,
            }
          );

          const relevantMessages = recentMessages.filter((msg) => {
            const data = msg.message || msg;
            // Messages pour cet utilisateur ou dans ses conversations
            return (
              data.receiverId === userIdStr ||
              (this.userConversations.get(userIdStr) || []).includes(
                data.conversationId
              )
            );
          });

          streamChecks[streamType] = {
            streamKey,
            totalMessages: length,
            relevantMessages: relevantMessages.length,
            priority: config.priority,
            status: relevantMessages.length > 0 ? "⚠️ EN ATTENTE" : "✅ VIDE",
          };

          console.log(
            `   ${streamChecks[streamType].status} ${streamType}: ${length} total, ${relevantMessages.length} pour ${userIdStr}`
          );

          if (relevantMessages.length > 0) {
            relevantMessages.forEach((msg, i) => {
              const data = msg.message || msg;
              console.log(
                `      ${i + 1}. ID: ${msg.id} | receiver: ${
                  data.receiverId || "N/A"
                } | conv: ${data.conversationId}`
              );
            });
          }
        } catch (streamErr) {
          console.log(`   ❌ ERREUR ${streamType}: ${streamErr.message}`);
          streamChecks[streamType] = { error: streamErr.message };
        }
      }

      diagnostics.checks.streams = streamChecks;

      // ✅ CHECK 3 : Messages en attente (Redis List)
      console.log("\n📨 Messages en attente (Redis List):");

      const pendingKey = `${this.pendingMessagesPrefix}${userIdStr}`;
      try {
        const pendingMessages = await this.redis.lRange(pendingKey, 0, -1);

        diagnostics.checks.pendingQueue = {
          count: pendingMessages.length,
          status: pendingMessages.length > 0 ? "⚠️ EN ATTENTE" : "✅ VIDE",
        };

        console.log(
          `   ${diagnostics.checks.pendingQueue.status}: ${pendingMessages.length} message(s)`
        );

        if (pendingMessages.length > 0) {
          pendingMessages.slice(0, 3).forEach((msgJson, i) => {
            try {
              const msg = JSON.parse(msgJson);
              console.log(
                `      ${i + 1}. De: ${msg.senderId} | Conv: ${
                  msg.conversationId
                }`
              );
            } catch (e) {
              console.log(`      ${i + 1}. [JSON invalide]`);
            }
          });
        }
      } catch (pendingErr) {
        console.log(`   ❌ ERREUR: ${pendingErr.message}`);
        diagnostics.checks.pendingQueue = { error: pendingErr.message };
      }

      // ✅ CHECK 4 : Conversations de l'utilisateur
      console.log("\n🏢 Conversations associées:");

      const conversations = this.userConversations.get(userIdStr) || [];
      diagnostics.checks.conversations = {
        count: conversations.length,
        ids: conversations,
        status: conversations.length > 0 ? "✅ OK" : "⚠️ AUCUNE",
      };

      console.log(
        `   ${diagnostics.checks.conversations.status}: ${conversations.length} conversation(s)`
      );

      // ✅ CHECK 5 : Consumer groups
      console.log("\n👥 Consumer Groups:");

      const consumerChecks = {};

      for (const [streamType, consumer] of this.streamConsumers.entries()) {
        try {
          const consumerGroupInfo = await this.redis.xInfoConsumers(
            consumer.config.streamKey,
            consumer.config.groupId
          );

          consumerChecks[streamType] = {
            groupId: consumer.config.groupId,
            consumerCount: consumerGroupInfo.length,
            active: consumer.isRunning,
            interval: consumer.config.interval,
          };

          console.log(
            `   ${streamType}: ${consumerGroupInfo.length} consumer(s) [${
              consumer.isRunning ? "▶️ ACTIF" : "⏸️ INACTIF"
            }]`
          );
        } catch (groupErr) {
          console.log(`   ❌ ${streamType}: ${groupErr.message}`);
          consumerChecks[streamType] = { error: groupErr.message };
        }
      }

      diagnostics.checks.consumerGroups = consumerChecks;

      // ✅ RÉSUMÉ
      console.log("\n📋 RÉSUMÉ:");
      console.log(`   Utilisateur: ${userIdStr}`);
      console.log(
        `   Connecté: ${isRegistered ? "✅ OUI" : "❌ NON"} (${
          socketIds.length
        } socket(s))`
      );
      console.log(
        `   Messages en attente: ${diagnostics.checks.pendingQueue.count}`
      );
      const totalRelevant = Object.values(streamChecks).reduce(
        (sum, s) => sum + (s.relevantMessages || 0),
        0
      );
      console.log(`   Messages dans les streams: ${totalRelevant}`);
      console.log(`🔍 ========== FIN DIAGNOSTIC ==========\n`);

      return diagnostics;
    } catch (error) {
      console.error("❌ Erreur diagnostic:", error);
      return { error: error.message };
    }
  }

  /**
   * ✅ RÉSOUDRE UN PROBLÈME DE LIVRAISON
   */
  async troubleshootDelivery(userId) {
    const diagnostics = await this.diagnoseDelivery(userId);
    const userIdStr = String(userId);

    console.log("🔧 RÉSOLUTION AUTOMATIQUE:");

    // ✅ PROBLÈME 1 : Utilisateur pas connecté mais messages en attente
    if (
      !diagnostics.checks.userRegistration.registered &&
      diagnostics.checks.pendingQueue.count > 0
    ) {
      console.log("   ⚠️ Messages en attente mais utilisateur déconnecté");
      console.log("   → Les messages seront livrés à la reconnexion");
    }

    // ✅ PROBLÈME 2 : Messages dans le stream mais pas livrés
    const totalInStreams =
      Object.values(diagnostics.checks.streams || {}).reduce(
        (sum, s) => sum + (s.relevantMessages || 0),
        0
      ) || 0;

    if (totalInStreams > 0 && !diagnostics.checks.userRegistration.registered) {
      console.log(
        "   ⚠️ Messages bloqués dans le stream (utilisateur déconnecté)"
      );
      console.log(
        "   → Les consumers continuent à tourner, messages seront livrés"
      );
    }

    // ✅ PROBLÈME 3 : Aucun consumer actif
    const inactiveConsumers = Object.entries(
      diagnostics.checks.consumerGroups || {}
    ).filter((entry) => !entry[1].active);

    if (inactiveConsumers.length > 0) {
      console.log(`   ⚠️ ${inactiveConsumers.length} consumer(s) inactif(s)`);
      console.log("   → Redémarrage des consumers...");
      this.startAllConsumers();
    }
  }
}

module.exports = MessageDeliveryService;
