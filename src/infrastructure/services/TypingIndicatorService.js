/**
 * TypingIndicatorService - Gestion centralisée des indicateurs de typing
 * ✅ Consumer groupe Redis Streams pour les événements typing
 * ✅ Broadcast aux destinataires (membres de la conversation)
 * ✅ Gestion des timeouts automatiques (fallback si client crash)
 * ✅ Limitation du trafic (debounce côté serveur)
 * ✅ Tracking des états actifs (qui tape, dans quelle conversation)
 */

class TypingIndicatorService {
  constructor(redis, io, conversationRepository) {
    this.redis = redis;
    this.io = io;
    this.conversationRepository = conversationRepository;

    // ✅ MAP: conversationId → {userId → {lastTypingAt, status}}
    this.activeTypings = new Map();

    // ✅ MAP: userId → {conversationId → timeout}
    this.typingTimeouts = new Map();

    // ✅ Configuration
    this.TYPING_TIMEOUT = 10000; // 10s - Timeout côté serveur si pas de refresh
    this.DEBOUNCE_INTERVAL = 1000; // 1s - Minimum entre chaque broadcast du même user
    this.STREAM_NAME = "chat:stream:events:typing";
    this.CONSUMER_GROUP = "typing-indicators";
    this.CONSUMER_NAME = "typing-indicator-consumer";

    // ✅ Cache court de la map userId → socketIds (évite de la reconstruire
    //    pour chaque participant lors d'un broadcast typing)
    this._userSocketsCache = null;
    this._userSocketsCacheAt = 0;
    this.USER_SOCKETS_CACHE_TTL = 500; // ms

    console.log("✅ TypingIndicatorService initialisé");
  }

  /**
   * ✅ INITIALISER LE CONSUMER GROUP
   */
  async initConsumerGroup() {
    try {
      await this.redis.xGroupCreate(
        this.STREAM_NAME,
        this.CONSUMER_GROUP,
        "$",
        {
          MKSTREAM: true,
        },
      );
      console.log(`✅ Consumer group créé: ${this.CONSUMER_GROUP}`);
    } catch (err) {
      if (err.message.includes("BUSYGROUP")) {
        console.log(`ℹ️ Consumer group ${this.CONSUMER_GROUP} existe déjà`);
      } else {
        console.error(`❌ Erreur création consumer group:`, err.message);
      }
    }
  }

  /**
   * ✅ DÉMARRER LE CONSUMER POUR LES ÉVÉNEMENTS TYPING
   */
  async startConsumer() {
    console.log("🚀 Démarrage du consumer typing...");

    try {
      await this.initConsumerGroup();

      // Lancer la consommation en arrière-plan (handle stocké pour pouvoir
      // l'arrêter proprement lors d'un shutdown).
      this._consumerInterval = setInterval(async () => {
        await this.consumeTypingEvents();
      }, 50); // Consommer TRÈS souvent (50ms) pour typing temps-réel

      console.log("✅ Consumer typing démarré");
    } catch (err) {
      console.error("❌ Erreur démarrage consumer typing:", err);
    }
  }

  /**
   * ✅ ARRÊTER LE CONSUMER TYPING
   */
  async stopConsumer() {
    if (this._consumerInterval) {
      clearInterval(this._consumerInterval);
      this._consumerInterval = null;
      console.log("✅ Consumer typing arrêté");
    }
  }

  /**
   * ✅ CONSOMMER LES ÉVÉNEMENTS TYPING
   */
  async consumeTypingEvents() {
    try {
      const messages = await this.redis.xReadGroup(
        this.CONSUMER_GROUP,
        this.CONSUMER_NAME,
        [{ key: this.STREAM_NAME, id: ">" }],
        { COUNT: 10 },
      );

      if (!messages || messages.length === 0) {
        return;
      }

      const streamMessages = messages[0]?.messages || [];

      for (const msg of streamMessages) {
        try {
          await this.processTypingEvent(msg);
          // ✅ ACKNOWLEDGE APRÈS TRAITEMENT
          await this.redis.xAck(this.STREAM_NAME, this.CONSUMER_GROUP, msg.id);
        } catch (err) {
          console.error(
            `❌ Erreur traitement événement typing ${msg.id}:`,
            err.message,
          );
        }
      }
    } catch (err) {
      if (!err.message.includes("NOGROUP")) {
        console.error("❌ Erreur consommation typing:", err.message);
      }
    }
  }

  /**
   * ✅ TRAITER UN ÉVÉNEMENT TYPING
   */
  async processTypingEvent(msg) {
    const {
      conversationId,
      userId: rawUserId,
      senderId,
      event, // "typing:start", "typing:refresh", "typing:stop"
      timestamp,
    } = msg.message;

    // ✅ Accepter userId OU senderId (compatibilité chatHandler qui publie senderId)
    const userId = rawUserId || senderId;

    console.log(`📝 Événement typing reçu:`, {
      conversationId,
      userId,
      event,
    });

    // ✅ VALIDER LES CHAMPS REQUIS
    if (!conversationId || !userId || !event) {
      console.warn(`⚠️ Événement typing incomplet:`, msg.message);
      return;
    }

    // ✅ GÉRER CHAQUE TYPE D'ÉVÉNEMENT
    switch (event) {
      case "typing:start":
        await this.handleTypingStart(conversationId, userId, timestamp);
        break;
      case "typing:refresh":
        await this.handleTypingRefresh(conversationId, userId, timestamp);
        break;
      case "typing:stop":
        await this.handleTypingStop(conversationId, userId);
        break;
      default:
        console.warn(`⚠️ Événement typing inconnu: ${event}`);
    }
  }

  /**
   * ✅ GÉRER TYPING:START - Première fois que l'utilisateur tape
   */
  async handleTypingStart(conversationId, userId, timestamp) {
    try {
      // ✅ INITIALISER SI NÉCESSAIRE
      if (!this.activeTypings.has(conversationId)) {
        this.activeTypings.set(conversationId, new Map());
      }

      const convTypings = this.activeTypings.get(conversationId);

      // ✅ VÉRIFIER SI DÉJÀ EN TRAIN DE TAPER (éviter les doublons)
      if (
        convTypings.has(userId) &&
        convTypings.get(userId).status === "active"
      ) {
        console.log(`ℹ️ Utilisateur ${userId} déjà en train de taper`);
        return;
      }

      // ✅ MARQUER COMME ACTIF
      convTypings.set(userId, {
        status: "active",
        startTime: Date.now(),
        lastRefreshAt: Date.now(),
      });

      console.log(`✅ Typing START: ${userId} in ${conversationId}`);

      // ✅ BROADCASTER À TOUS LES PARTICIPANTS
      await this.broadcastTypingStatus(conversationId, userId, "start");

      // ✅ CONFIGURER LE TIMEOUT AUTOMATIQUE
      this.setTypingTimeout(conversationId, userId);
    } catch (err) {
      console.error(`❌ Erreur handleTypingStart:`, err.message);
    }
  }

  /**
   * ✅ GÉRER TYPING:REFRESH - Utilisateur continue à taper
   */
  async handleTypingRefresh(conversationId, userId, timestamp) {
    try {
      if (!this.activeTypings.has(conversationId)) {
        return;
      }

      const convTypings = this.activeTypings.get(conversationId);
      const userTyping = convTypings.get(userId);

      if (!userTyping) {
        // Si pas trouvé, traiter comme un START
        await this.handleTypingStart(conversationId, userId, timestamp);
        return;
      }

      // ✅ VÉRIFIER LE DEBOUNCE (au moins 1s entre chaque broadcast)
      const timeSinceLastRefresh = Date.now() - userTyping.lastRefreshAt;
      if (timeSinceLastRefresh < this.DEBOUNCE_INTERVAL) {
        console.log(
          `ℹ️ Debounce: refresh trop rapide (${timeSinceLastRefresh}ms)`,
        );
        return;
      }

      // ✅ METTRE À JOUR LE TIMESTAMP
      userTyping.lastRefreshAt = Date.now();

      console.log(`✅ Typing REFRESH: ${userId} in ${conversationId}`);

      // ✅ BROADCASTER (mais seulement si debounce ok)
      await this.broadcastTypingStatus(conversationId, userId, "refresh");

      // ✅ RÉINITIALISER LE TIMEOUT
      this.setTypingTimeout(conversationId, userId);
    } catch (err) {
      console.error(`❌ Erreur handleTypingRefresh:`, err.message);
    }
  }

  /**
   * ✅ GÉRER TYPING:STOP - L'utilisateur arrête de taper
   */
  async handleTypingStop(conversationId, userId) {
    try {
      if (!this.activeTypings.has(conversationId)) {
        return;
      }

      const convTypings = this.activeTypings.get(conversationId);
      if (!convTypings.has(userId)) {
        return;
      }

      // ✅ SUPPRIMER L'ÉTAT ACTIF
      convTypings.delete(userId);

      // ✅ NETTOYER LA MAP SI VIDE
      if (convTypings.size === 0) {
        this.activeTypings.delete(conversationId);
      }

      console.log(`✅ Typing STOP: ${userId} in ${conversationId}`);

      // ✅ BROADCASTER À TOUS LES PARTICIPANTS
      await this.broadcastTypingStatus(conversationId, userId, "stop");

      // ✅ ANNULER LE TIMEOUT
      this.clearTypingTimeout(conversationId, userId);
    } catch (err) {
      console.error(`❌ Erreur handleTypingStop:`, err.message);
    }
  }

  /**
   * ✅ BROADCASTER LE STATUT DE TYPING À TOUS LES PARTICIPANTS
   */
  async broadcastTypingStatus(conversationId, typingUserId, status) {
    try {
      // ✅ RÉCUPÉRER LES PARTICIPANTS DE LA CONVERSATION
      const conversation =
        await this.conversationRepository.findById(conversationId);

      if (!conversation) {
        console.warn(`⚠️ Conversation ${conversationId} non trouvée`);
        return;
      }

      // ✅ EXTRAIRE LES IDS DES PARTICIPANTS
      const participants =
        conversation.participants || conversation.participantIds || [];
      const participantIds = participants.map((p) =>
        typeof p === "string" ? p : p.userId || p._id,
      );

      console.log(`📢 Broadcasting typing ${status}:`, {
        conversationId,
        typingUserId,
        participantIds,
      });

      // ✅ ENVOYER À CHAQUE PARTICIPANT (SAUF LE TYPEUR)
      for (const participantId of participantIds) {
        if (participantId === typingUserId) continue; // Ne pas envoyer au typeur

        const userSocketMap = this.getUserSockets();
        const socketIds = userSocketMap.get(String(participantId));

        if (!socketIds || socketIds.length === 0) {
          console.log(`ℹ️ Participant ${participantId} non connecté`);
          continue;
        }

        // ✅ ENVOYER À TOUS SES SOCKETS
        for (const socketId of socketIds) {
          const socket = this.io.sockets.sockets.get(socketId);
          if (socket) {
            socket.emit("typing:indicator", {
              conversationId,
              userId: typingUserId,
              status, // "start", "refresh", "stop"
              timestamp: Date.now(),
            });

            console.log(`✅ Typing event envoyé:`, {
              socketId,
              participantId,
              status,
            });
          }
        }
      }
    } catch (err) {
      console.error(
        `❌ Erreur broadcast typing ${conversationId}:`,
        err.message,
      );
    }
  }

  /**
   * ✅ CONFIGURER LE TIMEOUT AUTOMATIQUE (Fallback si client crash)
   */
  setTypingTimeout(conversationId, userId) {
    try {
      // ✅ ANNULER LE TIMEOUT PRÉCÉDENT S'IL EXISTE
      this.clearTypingTimeout(conversationId, userId);

      // ✅ CRÉER UN NOUVEAU TIMEOUT
      const timeout = setTimeout(async () => {
        console.log(`⏱️ Typing timeout expiré: ${userId} in ${conversationId}`);
        await this.handleTypingStop(conversationId, userId);
      }, this.TYPING_TIMEOUT);

      // ✅ STOCKER LE TIMEOUT
      if (!this.typingTimeouts.has(userId)) {
        this.typingTimeouts.set(userId, new Map());
      }
      this.typingTimeouts.get(userId).set(conversationId, timeout);

      console.log(`⏱️ Timeout configuré: ${userId} in ${conversationId}`);
    } catch (err) {
      console.error(`❌ Erreur setTypingTimeout:`, err.message);
    }
  }

  /**
   * ✅ ANNULER LE TIMEOUT
   */
  clearTypingTimeout(conversationId, userId) {
    try {
      if (!this.typingTimeouts.has(userId)) return;

      const userTimeouts = this.typingTimeouts.get(userId);
      const timeout = userTimeouts.get(conversationId);

      if (timeout) {
        clearTimeout(timeout);
        userTimeouts.delete(conversationId);
        console.log(`✅ Timeout annulé: ${userId} in ${conversationId}`);
      }

      // ✅ NETTOYER SI LA MAP EST VIDE
      if (userTimeouts.size === 0) {
        this.typingTimeouts.delete(userId);
      }
    } catch (err) {
      console.error(`❌ Erreur clearTypingTimeout:`, err.message);
    }
  }

  /**
   * ✅ OBTENIR LA MAP DES SOCKETS (depuis Socket.IO)
   */
  getUserSockets() {
    // ✅ Renvoyer le cache s'il est encore valide (< 500ms)
    const now = Date.now();
    if (
      this._userSocketsCache &&
      now - this._userSocketsCacheAt < this.USER_SOCKETS_CACHE_TTL
    ) {
      return this._userSocketsCache;
    }

    // ✅ CONSTRUIRE LA MAP DEPUIS TOUS LES SOCKETS CONNECTÉS
    const userSockets = new Map();

    for (const [socketId, socket] of this.io.sockets.sockets) {
      const userId = socket.userId || socket.handshake?.auth?.userId;

      if (userId) {
        const userIdStr = String(userId);
        if (!userSockets.has(userIdStr)) {
          userSockets.set(userIdStr, []);
        }
        userSockets.get(userIdStr).push(socketId);
      }
    }

    this._userSocketsCache = userSockets;
    this._userSocketsCacheAt = now;

    return userSockets;
  }

  /**
   * ✅ OBTENIR LES UTILISATEURS ACTUELLEMENT EN TRAIN DE TAPER
   */
  getTypingUsers(conversationId) {
    if (!this.activeTypings.has(conversationId)) {
      return [];
    }

    const convTypings = this.activeTypings.get(conversationId);
    return Array.from(convTypings.entries())
      .filter(([_, data]) => data.status === "active")
      .map(([userId, data]) => ({
        userId,
        startTime: data.startTime,
        lastRefreshAt: data.lastRefreshAt,
      }));
  }

  /**
   * ✅ OBTENIR TOUTES LES TYPINGS ACTIVES
   */
  getAllActiveTypings() {
    const result = {};

    for (const [conversationId, typings] of this.activeTypings) {
      const activeUsers = Array.from(typings.entries())
        .filter(([_, data]) => data.status === "active")
        .map(([userId, data]) => ({
          userId,
          startTime: data.startTime,
        }));

      if (activeUsers.length > 0) {
        result[conversationId] = activeUsers;
      }
    }

    return result;
  }

  /**
   * ✅ NETTOYER LES RESSOURCES
   */
  async cleanup() {
    console.log("🧹 Nettoyage TypingIndicatorService...");

    // ✅ ANNULER TOUS LES TIMEOUTS
    for (const [userId, timeouts] of this.typingTimeouts) {
      for (const [_, timeout] of timeouts) {
        clearTimeout(timeout);
      }
    }

    this.typingTimeouts.clear();
    this.activeTypings.clear();

    console.log("✅ TypingIndicatorService nettoyé");
  }
}

module.exports = TypingIndicatorService;
