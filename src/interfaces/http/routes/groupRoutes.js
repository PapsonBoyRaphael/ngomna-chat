const express = require("express");
const router = express.Router();
const { validationMiddleware } = require("../middleware");

module.exports = function createGroupRoutes(groupController, auth) {
  // ✅ Vérification du controller
  if (!groupController) {
    console.error("❌ GroupController manquant");
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de groupes non disponible",
        error: "GroupController non initialisé",
      });
    });
    return router;
  }

  /**
   * @api {post} /groups Créer un groupe
   */

  router.post(
    "/",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    async (req, res) => {
      try {
        await groupController.createGroup(req, res);
      } catch (error) {
        console.error("❌ Erreur route POST /groups:", error);
        res.status(500).json({
          success: false,
          message: "Erreur lors de la création du groupe",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  /**
   * @api {get} /groups/search Recherche globale messages/fichiers/conversations/groups/broadcast
   * @apiName SearchOccurrences
   * @apiGroup Groups
   */
  router.get(
    "/search",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    async (req, res) => {
      try {
        await groupController.searchOccurrences(req, res);
      } catch (error) {
        console.error("❌ Erreur route GET /groups/search:", error);
        res.status(500).json({
          success: false,
          message: "Erreur lors de la recherche globale",
          error: error.message,
        });
      }
    },
  );

  /**
   * @api {get} /groups/:groupId Récupérer un groupe
   */
  router.get(
    "/:groupId",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    validationMiddleware.validateMongoId("groupId"),
    async (req, res) => {
      try {
        await groupController.getGroup(req, res);
      } catch (error) {
        console.error("❌ Erreur route GET /groups/:groupId:", error);
        res.status(500).json({
          success: false,
          message: "Erreur lors de la récupération du groupe",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  /**
   * @api {post} /groups/:groupId/participants Ajouter un participant
   */
  router.post(
    "/:groupId/participants",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    validationMiddleware.sanitizeInput,
    validationMiddleware.validateMongoId("groupId"),
    async (req, res) => {
      try {
        await groupController.addParticipant(req, res);
      } catch (error) {
        console.error(
          "❌ Erreur route POST /groups/:groupId/participants:",
          error,
        );
        res.status(500).json({
          success: false,
          message: "Erreur lors de l'ajout du participant",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  /**
   * @api {delete} /groups/:groupId/participants/:participantId Retirer un participant
   */
  router.delete(
    "/:groupId/participants/:participantId",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    validationMiddleware.validateMongoId("groupId"),
    async (req, res) => {
      try {
        await groupController.removeParticipant(req, res);
      } catch (error) {
        console.error(
          "❌ Erreur route DELETE /groups/:groupId/participants/:participantId:",
          error,
        );
        res.status(500).json({
          success: false,
          message: "Erreur lors du retrait du participant",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  /**
   * @api {post} /groups/:groupId/admins Promouvoir un ou plusieurs membres comme admins
   */
  router.post(
    "/:groupId/admins",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    validationMiddleware.sanitizeInput,
    validationMiddleware.validateMongoId("groupId"),
    async (req, res) => {
      try {
        await groupController.addAdmin(req, res);
      } catch (error) {
        console.error("❌ Erreur route POST /groups/:groupId/admins:", error);
        res.status(500).json({
          success: false,
          message: "Erreur lors de l'ajout d'admin",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  /**
   * @api {post} /groups/:groupId/leave Quitter un groupe
   */
  router.post(
    "/:groupId/leave",
    auth.valideToken(),
    auth.requireRole("agent_public"),
    validationMiddleware.validateMongoId("groupId"),
    async (req, res) => {
      try {
        await groupController.leaveGroup(req, res);
      } catch (error) {
        console.error("❌ Erreur route POST /groups/:groupId/leave:", error);
        res.status(500).json({
          success: false,
          message: "Erreur lors de la sortie du groupe",
          error:
            process.env.NODE_ENV === "development"
              ? error.message
              : "Erreur interne",
        });
      }
    },
  );

  console.log("✅ Routes groupes configurées");

  return router;
};
