// const CacheService = require("../../infrastructure/redis/CacheService");

class MessageController {
  constructor(
    sendMessageUseCase,
    getMessagesUseCase,
    updateMessageStatusUseCase,
    redisClient = null,
    getMessageByIdUseCase = null,
    searchOccurrencesUseCase = null
  ) {
    this.sendMessageUseCase = sendMessageUseCase;
    this.getMessagesUseCase = getMessagesUseCase;
    this.updateMessageStatusUseCase = updateMessageStatusUseCase;
    this.getMessageByIdUseCase = getMessageByIdUseCase;
    this.searchOccurrencesUseCase = searchOccurrencesUseCase;
  }

  async sendMessage(req, res) {
    const startTime = Date.now();

    try {
      const {
        senderId,
        receiverId,
        content,
        conversationId,
        type = "TEXT",
        metadata = {},
      } = req.body;

      // Validation
      if (!senderId || !receiverId || !content?.trim()) {
        return res.status(400).json({
          success: false,
          message: "senderId, receiverId et content sont requis",
          code: "MISSING_PARAMETERS",
        });
      }

      // Validation de la longueur du contenu
      if (content.trim().length > 10000) {
        return res.status(400).json({
          success: false,
          message: "Le contenu du message est trop long (max 10000 caractères)",
          code: "CONTENT_TOO_LONG",
        });
      }

      // Enrichir les métadonnées avec les infos de la requête
      const enrichedMetadata = {
        ...metadata,
        userAgent: req.headers["user-agent"],
        ip: req.ip,
        requestId: req.headers["x-request-id"] || `req_${Date.now()}`,
        timestamp: new Date().toISOString(),
      };

      // Envoyer le message
      const message = await this.sendMessageUseCase.execute({
        senderId,
        receiverId,
        content: content.trim(),
        conversationId,
        type,
        metadata: enrichedMetadata,
      });

      const processingTime = Date.now() - startTime;

      // ❌ SUPPRIMER TOUTE INVALIDATION CACHE DU CONTROLLER
      // Le cache est maintenant géré uniquement par les repositories

      res.status(201).json({
        success: true,
        data: message,
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur envoi message:", error);

      res.status(500).json({
        success: false,
        message: "Erreur lors de l'envoi du message",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        code: "SEND_MESSAGE_FAILED",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  async getMessages(req, res) {
    const startTime = Date.now();

    try {
      const { conversationId } = req.query;
      const { cursor = null, limit = 50, direction = "older" } = req.query;
      const userId = req.user?.id || req.headers["user-id"];

      console.log(
        `🔍 getMessages: conversation=${conversationId}, cursor=${cursor}, direction=${direction}`
      );

      if (!conversationId || !userId) {
        return res.status(400).json({
          success: false,
          message: "conversationId et userId requis",
          code: "MISSING_PARAMETERS",
        });
      }

      // ✅ DIRECTEMENT APPELER LE USE CASE (il gère le cache via les repositories)
      const result = await this.getMessagesUseCase.execute(conversationId, {
        cursor,
        limit: Math.min(parseInt(limit), 100),
        direction,
        userId,
        useCache: !cursor, // Cache seulement première page
      });

      const processingTime = Date.now() - startTime;

      // ✅ HEADERS BASÉS SUR LA RÉPONSE DU REPOSITORY
      res.set({
        "X-Cache": result.fromCache ? "HIT" : "MISS",
        "Cache-Control": cursor ? "no-cache" : "public, max-age=60",
        "X-Load-Source": result.fromCache ? "cache" : "database",
        "X-Cursor": cursor || "none",
      });

      res.json({
        success: true,
        data: result,
        metadata: {
          processingTime: `${processingTime}ms`,
          fromCache: result.fromCache || false,
          cursor,
          direction,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur récupération messages:", error);

      res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération des messages",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        code: "GET_MESSAGES_FAILED",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  /**
   * ✅ VERSION INTERNE POUR WEBSOCKET (sans cache controller)
   */
  async getMessagesInternal(conversationId, userId, options = {}) {
    const { cursor = null, limit = 50, direction = "older" } = options;

    try {
      // ✅ APPEL DIRECT AU USE CASE (qui gère le cache)
      return await this.getMessagesUseCase.execute(conversationId, {
        cursor,
        limit: Math.min(parseInt(limit), 100),
        direction,
        userId,
        useCache: !cursor,
      });
    } catch (error) {
      console.error("❌ Erreur getMessagesInternal:", error);
      throw error;
    }
  }

  async updateMessageStatus(req, res) {
    const startTime = Date.now();

    try {
      const { conversationId, status, messageIds } = req.body;
      const userId = req.user?.id || req.headers["user-id"];

      if (!conversationId || !userId || !status) {
        return res.status(400).json({
          success: false,
          message: "conversationId, userId et status requis",
          code: "MISSING_PARAMETERS",
        });
      }

      const result = await this.updateMessageStatusUseCase.execute({
        conversationId,
        receiverId: userId,
        status,
        messageIds,
      });

      const processingTime = Date.now() - startTime;

      if (this.cacheService && result.modifiedCount > 0) {
        try {
          const cachePatterns = [
            `messages:${conversationId}:*`,
            `conversation:${conversationId}:*`,
            `conversations:${userId}`,
          ];
          for (const pattern of cachePatterns) {
            await this.cacheService.del(pattern);
          }
        } catch (redisError) {
          console.warn(
            "⚠️ Erreur invalidation cache statut:",
            redisError.message
          );
        }
      }

      res.json({
        success: true,
        message: `Statut mis à jour pour ${result.modifiedCount} message(s)`,
        data: result,
        metadata: {
          processingTime: `${processingTime}ms`,
          kafkaPublished: !!this.kafkaProducer,
          cacheInvalidated: !!this.cacheService && result.modifiedCount > 0,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur mise à jour statut:", error);

      res.status(500).json({
        success: false,
        message: "Erreur lors de la mise à jour du statut",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        code: "UPDATE_STATUS_FAILED",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  // ✅ GET /messages/:messageId
  async getMessage(req, res) {
    try {
      const { messageId } = req.params;
      if (!messageId) {
        return res.status(400).json({
          success: false,
          message: "messageId requis",
        });
      }

      // Utiliser le use-case GetMessageById
      const message = await this.getMessageByIdUseCase.execute(messageId);

      if (!message) {
        return res.status(404).json({
          success: false,
          message: "Message introuvable",
        });
      }

      // Interdire l'accès si le message est supprimé (status DELETED)
      if (message.status === "DELETED") {
        return res.status(403).json({
          success: false,
          message: "Ce message a été supprimé",
        });
      }

      res.json({
        success: true,
        data: message,
        message: "Message récupéré avec succès",
      });
    } catch (error) {
      console.error("❌ Erreur getMessage:", error);
      res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération du message",
        error: error.message,
      });
    }
  }

  // ✅ DELETE /messages/:messageId
  async deleteMessage(req, res) {
    try {
      const { messageId } = req.params;
      if (!messageId) {
        return res.status(400).json({
          success: false,
          message: "messageId requis",
        });
      }
      // Ici, il faut passer le status à DELETED (soft delete)
      // Utiliser le repository ou use-case approprié
      const updated = await this.updateMessageStatusUseCase.markSingleMessage({
        messageId,
        receiverId: req.user?.id || req.headers["user-id"],
        status: "DELETED",
      });

      // Invalider le cache du message et de la conversation
      if (this.cacheService) {
        try {
          await this.cacheService.del(`message:${messageId}`);
          // Invalider aussi la liste des messages de la conversation si besoin
        } catch (err) {
          console.warn(
            "⚠️ Erreur invalidation cache suppression:",
            err.message
          );
        }
      }

      res.json({
        success: true,
        message: "Message marqué comme supprimé (DELETED)",
        data: updated,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la suppression du message",
        error: error.message,
      });
    }
  }

  async addReaction(req, res) {
    try {
      const { messageId } = req.params;
      const { emoji } = req.body;

      res.json({
        success: true,
        data: {
          messageId,
          emoji,
          userId: req.user?.id,
          timestamp: new Date().toISOString(),
        },
        message: "Réaction ajoutée",
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        error: error.message,
      });
    }
  }

  async searchOccurrences(req, res) {
    const startTime = Date.now();
    try {
      const {
        query,
        page = 1,
        limit = 20,
        useLike = true,
        scope = "messages",
      } = req.query;
      const userId = req.user?.id || req.headers["user-id"];
      if (!query || query.length < 2) {
        return res.status(400).json({
          success: false,
          message:
            "Le mot-clé de recherche doit contenir au moins 2 caractères",
          code: "INVALID_QUERY",
        });
      }
      const result = await this.searchOccurrencesUseCase.execute(query, {
        userId,
        page: parseInt(page),
        limit: parseInt(limit),
        useLike,
        scope,
      });
      res.json({
        success: true,
        data: result,
        metadata: {
          processingTime: Date.now() - startTime,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la recherche globale",
        error: error.message,
      });
    }
  }
}

module.exports = MessageController;
