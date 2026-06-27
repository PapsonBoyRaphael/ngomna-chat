const mongoose = require("mongoose");

class ValidationMiddleware {
  // ✅ AJOUTER LA MÉTHODE SANITIZEINPUT QUI MANQUE
  static sanitizeInput = (req, res, next) => {
    try {
      // Nettoyer le body
      if (req.body && typeof req.body === "object") {
        Object.keys(req.body).forEach((key) => {
          if (typeof req.body[key] === "string") {
            req.body[key] = req.body[key].trim();
            // Supprimer les caractères potentiellement dangereux
            req.body[key] = req.body[key].replace(/[<>]/g, "");
          }
        });
      }

      // Nettoyer les query params
      if (req.query && typeof req.query === "object") {
        Object.keys(req.query).forEach((key) => {
          if (typeof req.query[key] === "string") {
            req.query[key] = req.query[key].trim();
            req.query[key] = req.query[key].replace(/[<>]/g, "");
          }
        });
      }

      next();
    } catch (error) {
      console.error("❌ Erreur sanitizeInput:", error);
      next(); // Continuer même en cas d'erreur
    }
  };

  // ✅ AJOUTER LA MÉTHODE VALIDATEMESSAGESEND QUI MANQUE
  static validateMessageSend = (req, res, next) => {
    const { content, conversationId, type = "TEXT" } = req.body;

    if (!content || content.trim().length === 0) {
      return res.status(400).json({
        success: false,
        message: "Contenu du message requis",
        code: "MISSING_MESSAGE_CONTENT",
      });
    }

    if (!conversationId) {
      return res.status(400).json({
        success: false,
        message: "ID de conversation requis",
        code: "MISSING_CONVERSATION_ID",
      });
    }

    if (!mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({
        success: false,
        message: "ID de conversation invalide",
        code: "INVALID_CONVERSATION_ID",
      });
    }

    if (!["TEXT", "FILE", "IMAGE", "VIDEO", "AUDIO"].includes(type)) {
      return res.status(400).json({
        success: false,
        message: "Type de message invalide",
        code: "INVALID_MESSAGE_TYPE",
      });
    }

    // Limiter la taille du contenu
    if (content.length > 10000) {
      return res.status(400).json({
        success: false,
        message: "Message trop long (max 10000 caractères)",
        code: "MESSAGE_TOO_LONG",
      });
    }

    next();
  };

  // Valider un ID MongoDB
  static validateMongoId = (paramName) => {
    return (req, res, next) => {
      const id = req.params[paramName];

      if (!id) {
        return res.status(400).json({
          success: false,
          message: `Paramètre ${paramName} requis`,
          code: "MISSING_PARAMETER",
        });
      }

      if (!mongoose.Types.ObjectId.isValid(id)) {
        return res.status(400).json({
          success: false,
          message: `ID ${paramName} invalide`,
          code: "INVALID_ID",
        });
      }

      next();
    };
  };

  // Valider les données de création de conversation
  static validateConversationCreation = (req, res, next) => {
    const { participantId, type = "PRIVATE" } = req.body;

    if (!participantId) {
      return res.status(400).json({
        success: false,
        message: "ID du participant requis",
        code: "MISSING_PARTICIPANT_ID",
      });
    }

    if (!mongoose.Types.ObjectId.isValid(participantId)) {
      return res.status(400).json({
        success: false,
        message: "ID du participant invalide",
        code: "INVALID_PARTICIPANT_ID",
      });
    }

    if (!["PRIVATE", "GROUP"].includes(type)) {
      return res.status(400).json({
        success: false,
        message: "Type de conversation invalide",
        code: "INVALID_CONVERSATION_TYPE",
      });
    }

    if (type === "GROUP" && !req.body.name) {
      return res.status(400).json({
        success: false,
        message: "Nom requis pour les conversations de groupe",
        code: "MISSING_GROUP_NAME",
      });
    }

    next();
  };

  // Valider les données d'envoi de message
  static validateMessageCreation = (req, res, next) => {
    const { content, conversationId, type = "TEXT" } = req.body;

    if (!content || content.trim().length === 0) {
      return res.status(400).json({
        success: false,
        message: "Contenu du message requis",
        code: "MISSING_MESSAGE_CONTENT",
      });
    }

    if (!conversationId) {
      return res.status(400).json({
        success: false,
        message: "ID de conversation requis",
        code: "MISSING_CONVERSATION_ID",
      });
    }

    if (!mongoose.Types.ObjectId.isValid(conversationId)) {
      return res.status(400).json({
        success: false,
        message: "ID de conversation invalide",
        code: "INVALID_CONVERSATION_ID",
      });
    }

    if (!["TEXT", "FILE", "IMAGE", "VIDEO"].includes(type)) {
      return res.status(400).json({
        success: false,
        message: "Type de message invalide",
        code: "INVALID_MESSAGE_TYPE",
      });
    }

    // Limiter la taille du contenu
    if (content.length > 10000) {
      return res.status(400).json({
        success: false,
        message: "Message trop long (max 10000 caractères)",
        code: "MESSAGE_TOO_LONG",
      });
    }

    next();
  };

  // Valider les paramètres de pagination
  static validatePagination = (req, res, next) => {
    const { page = 1, limit = 20 } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);

    if (isNaN(pageNum) || pageNum < 1) {
      return res.status(400).json({
        success: false,
        message: "Numéro de page invalide",
        code: "INVALID_PAGE",
      });
    }

    if (isNaN(limitNum) || limitNum < 1 || limitNum > 100) {
      return res.status(400).json({
        success: false,
        message: "Limite invalide (1-100)",
        code: "INVALID_LIMIT",
      });
    }

    req.query.page = pageNum;
    req.query.limit = limitNum;

    next();
  };

  // Valider la mise à jour de statut de message
  static validateMessageStatus = (req, res, next) => {
    const { status } = req.body;
    const validStatuses = ["SENT", "DELIVERED", "READ"];

    if (!status) {
      return res.status(400).json({
        success: false,
        message: "Statut requis",
        code: "MISSING_STATUS",
      });
    }

    if (!validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        message: `Statut invalide. Valeurs acceptées: ${validStatuses.join(
          ", "
        )}`,
        code: "INVALID_STATUS",
      });
    }

    next();
  };

  // Valider les données de recherche
  static validateSearch = (req, res, next) => {
    const { q } = req.query;

    if (!q || q.trim().length === 0) {
      return res.status(400).json({
        success: false,
        message: "Terme de recherche requis",
        code: "MISSING_SEARCH_TERM",
      });
    }

    if (q.length < 2) {
      return res.status(400).json({
        success: false,
        message: "Terme de recherche trop court (min 2 caractères)",
        code: "SEARCH_TERM_TOO_SHORT",
      });
    }

    if (q.length > 100) {
      return res.status(400).json({
        success: false,
        message: "Terme de recherche trop long (max 100 caractères)",
        code: "SEARCH_TERM_TOO_LONG",
      });
    }

    next();
  };
}

module.exports = ValidationMiddleware;
