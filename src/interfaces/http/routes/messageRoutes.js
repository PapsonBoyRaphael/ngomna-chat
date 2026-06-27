const express = require("express");

// ✅ CHANGEMENT: Utiliser l'import centralisé au lieu des imports individuels
// const {
//   authMiddleware,
//   rateLimitMiddleware,
//   validationMiddleware,
//   cacheMiddleware,
// } = require("../middleware");
const rateLimitMiddleware = require("../middleware/rateLimitMiddleware");
const validationMiddleware = require("../middleware/validationMiddleware");


function createMessageRoutes(messageController,auth) {
  const router = express.Router();

  // **VALIDATION CRITIQUE : S'ASSURER QUE LE CONTRÔLEUR EXISTE**
  if (!messageController) {
    console.error("❌ MessageController manquant dans createMessageRoutes");
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de messages temporairement indisponible",
        error: "MessageController non initialisé",
      });
    });
    return router;
  }

  // **VALIDATION DES MÉTHODES DU CONTRÔLEUR**
  const requiredMethods = [
    "sendMessage",
    "getMessages",
    "getMessage",
    "updateMessageStatus",
    "deleteMessage",
    "addReaction",
  ];
  const missingMethods = requiredMethods.filter(
    (method) => typeof messageController[method] !== "function"
  );

  if (missingMethods.length > 0) {
    console.error(
      `❌ Méthodes manquantes dans MessageController: ${missingMethods.join(
        ", "
      )}`
    );
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de messages incomplet",
        error: `Méthodes manquantes: ${missingMethods.join(", ")}`,
      });
    });
    return router;
  }

  // Routes des messages avec gestion d'erreurs
  try {
    /**
     * @api {post} /messages Send Message
     * @apiName SendMessage
     * @apiGroup Messages
     */
    router.post(
      "/",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.createLimit,
      validationMiddleware.sanitizeInput,
      validationMiddleware.validateMessageSend,
      async (req, res) => {
        try {
          await messageController.sendMessage(req, res);
        } catch (error) {
          console.error("❌ Erreur route POST /messages:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de l'envoi du message",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {get} /messages Get Messages
     * @apiName GetMessages
     * @apiGroup Messages
     */
    router.get(
      "/",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.apiLimit,
      async (req, res) => {
        try {
          await messageController.getMessages(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /messages:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la récupération des messages",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {get} /messages/:messageId Get Single Message
     * @apiName GetMessage
     * @apiGroup Messages
     */
    router.get(
      "/:messageId",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.apiLimit,
      validationMiddleware.validateMongoId("messageId"),
      async (req, res) => {
        try {
          await messageController.getMessage(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /messages/:messageId:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la récupération du message",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {put} /messages/:messageId/status Update Message Status
     * @apiName UpdateMessageStatus
     * @apiGroup Messages
     */
    router.put(
      "/:messageId/status",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.apiLimit,
      validationMiddleware.validateMongoId("messageId"),
      validationMiddleware.validateMessageStatus,
      async (req, res) => {
        try {
          await messageController.updateMessageStatus(req, res);
        } catch (error) {
          console.error(
            "❌ Erreur route PUT /messages/:messageId/status:",
            error
          );
          res.status(500).json({
            success: false,
            message: "Erreur lors de la mise à jour du statut",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {delete} /messages/:messageId Delete Message
     * @apiName DeleteMessage
     * @apiGroup Messages
     */
    router.delete(
      "/:messageId",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.apiLimit,
      validationMiddleware.validateMongoId("messageId"),
      async (req, res) => {
        try {
          await messageController.deleteMessage(req, res);
        } catch (error) {
          console.error("❌ Erreur route DELETE /messages/:messageId:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la suppression du message",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {post} /messages/:messageId/reactions Add Reaction
     * @apiName AddReaction
     * @apiGroup Messages
     */
    router.post(
      "/:messageId/reactions",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.reactionLimit,
      validationMiddleware.validateMongoId("messageId"),
      validationMiddleware.sanitizeInput,
      async (req, res) => {
        try {
          await messageController.addReaction(req, res);
        } catch (error) {
          console.error(
            "❌ Erreur route POST /messages/:messageId/reactions:",
            error
          );
          res.status(500).json({
            success: false,
            message: "Erreur lors de l'ajout de la réaction",
            error: error.message,
          });
        }
      }
    );

    /**
     * @api {get} /messages/search Recherche globale messages/fichiers/conversations/groups/broadcast
     * @apiName SearchOccurrences
     * @apiGroup Messages
     */
    router.get(
      "/search",
      auth.valideToken(),
      auth.requireRole('agent_public'),
      rateLimitMiddleware.apiLimit,
      validationMiddleware.sanitizeInput,
      async (req, res) => {
        try {
          await messageController.searchOccurrences(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /messages/search:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la recherche globale",
            error: error.message,
          });
        }
      }
    );

    console.log("✅ Routes de messages configurées");
  } catch (error) {
    console.error("❌ Erreur configuration routes messages:", error);

    // Route de fallback en cas d'erreur
    router.all("*", (req, res) => {
      res.status(500).json({
        success: false,
        message: "Erreur de configuration du service de messages",
        error: error.message,
      });
    });
  }

  return router;
}

module.exports = createMessageRoutes;
