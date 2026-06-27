const express = require("express");
const multer = require("multer");
const path = require("path");
const { authMiddleware, validationMiddleware } = require("../middleware");

// Configuration multer pour l'upload
const storage = multer.memoryStorage(); // ✅ Utiliser la mémoire uniquement

// Supprimer toute restriction sur le type de fichier
const fileFilter = (req, file, cb) => {
  cb(null, true); // Accepte tous les fichiers
};

const upload = multer({
  storage: storage,
  fileFilter: fileFilter,
  limits: {
    fileSize: 20 * 1024 * 1024, // ✅ 20 MB max pour upload monolithique
  },
});

// ✅ Multer séparé pour les chunks (5 MB + marge)
const chunkUpload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 6 * 1024 * 1024, // 6 MB max par chunk
  },
});

function createFileRoutes(fileController, auth) {
  const router = express.Router();

  // **VALIDATION CRITIQUE : S'ASSURER QUE LE CONTRÔLEUR EXISTE**
  if (!fileController) {
    console.error("❌ FileController manquant dans createFileRoutes");
    // Retourner un router avec des routes d'erreur
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de fichiers temporairement indisponible",
        error: "FileController non initialisé",
      });
    });
    return router;
  }

  // **VALIDATION DES MÉTHODES DU CONTRÔLEUR**
  const requiredMethods = ["uploadFile", "getFile", "deleteFile", "getFiles"];
  const missingMethods = requiredMethods.filter(
    (method) => typeof fileController[method] !== "function",
  );

  if (missingMethods.length > 0) {
    console.error(
      `❌ Méthodes manquantes dans FileController: ${missingMethods.join(", ")}`,
    );
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de fichiers incomplet",
        error: `Méthodes manquantes: ${missingMethods.join(", ")}`,
      });
    });
    return router;
  }

  // Middleware d'authentification pour toutes les routes
  // router.use(authMiddleware.validateToken);

  // Routes des fichiers avec gestion d'erreurs
  try {
    // GET /files - Lister les fichiers
    router.get(
      "/",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      async (req, res) => {
        try {
          await fileController.getFiles(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la récupération des fichiers",
            error: error.message,
          });
        }
      },
    );

    // POST /files/upload - Upload d'un fichier
    router.post(
      "/upload",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      upload.single("file"),
      async (req, res) => {
        try {
          await fileController.uploadFile(req, res);
        } catch (error) {
          console.error("❌ Erreur route POST /files/upload:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de l'upload du fichier",
            error: error.message,
          });
        }
      },
    );

    // =============================================
    // ✅ ROUTES UPLOAD CHUNKÉ (fichiers > 100 MB)
    // =============================================

    // GET /files/upload/status?token=xxx - Vérifier le statut d'un upload
    router.get(
      "/upload/status",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      async (req, res) => {
        try {
          await fileController.checkUploadStatus(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files/upload/status:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la vérification du statut",
            error: error.message,
          });
        }
      },
    );

    // POST /files/upload/init - Initialiser un upload chunké
    router.post(
      "/upload/init",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      async (req, res) => {
        try {
          await fileController.initChunkedUpload(req, res);
        } catch (error) {
          console.error("❌ Erreur route POST /files/upload/init:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de l'initialisation de l'upload chunké",
            error: error.message,
          });
        }
      },
    );

    // POST /files/upload/chunk/:uploadId - Envoyer un chunk
    router.post(
      "/upload/chunk/:uploadId",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      chunkUpload.single("chunk"),
      async (req, res) => {
        try {
          await fileController.uploadChunk(req, res);
        } catch (error) {
          console.error("❌ Erreur route POST /files/upload/chunk:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de l'upload du chunk",
            error: error.message,
          });
        }
      },
    );

    // POST /files/upload/complete/:uploadId - Finaliser l'upload chunké
    router.post(
      "/upload/complete/:uploadId",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      async (req, res) => {
        try {
          await fileController.completeChunkedUpload(req, res);
        } catch (error) {
          console.error("❌ Erreur route POST /files/upload/complete:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la finalisation de l'upload chunké",
            error: error.message,
          });
        }
      },
    );

    // GET /files/:fileId - Récupérer les métadonnées d'un fichier
    router.get(
      "/:fileId",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      // ✅ PAS DE VALIDATION MONGO ID CAR FileModel._id EST UN STRING
      async (req, res) => {
        try {
          await fileController.getFile(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files/:fileId:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la récupération du fichier",
            error: error.message,
          });
        }
      },
    );

    // DELETE /files/:fileId - Supprimer un fichier
    router.delete(
      "/:fileId",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      // ✅ PAS DE VALIDATION MONGO ID CAR FileModel._id EST UN STRING
      async (req, res) => {
        try {
          await fileController.deleteFile(req, res);
        } catch (error) {
          console.error("❌ Erreur route DELETE /files/:fileId:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la suppression du fichier",
            error: error.message,
          });
        }
      },
    );

    // GET /files/conversation/:conversationId - Fichiers d'une conversation
    router.get(
      "/conversation/:conversationId",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      validationMiddleware.validateMongoId("conversationId"),
      async (req, res) => {
        try {
          await fileController.getConversationFiles(req, res);
        } catch (error) {
          console.error(
            "❌ Erreur route GET /files/conversation/:conversationId:",
            error,
          );
          res.status(500).json({
            success: false,
            message:
              "Erreur lors de la récupération des fichiers de conversation",
            error: error.message,
          });
        }
      },
    );

    // ✅ AJOUTER CETTE ROUTE POUR LES THUMBNAILS
    router.get(
      "/:fileId/thumbnail",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      // ✅ PAS DE VALIDATION MONGO ID CAR FileModel._id EST UN STRING
      async (req, res) => {
        try {
          await fileController.getThumbnail(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files/:fileId/thumbnail:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la récupération du thumbnail",
            error: error.message,
          });
        }
      },
    );

    // GET /files/:fileId/download - Télécharger un fichier
    router.get(
      "/:fileId/download",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      // ✅ PAS DE VALIDATION MONGO ID CAR FileModel._id EST UN STRING
      async (req, res) => {
        try {
          await fileController.downloadFile(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files/:fileId/download:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors du téléchargement du fichier",
            error: error.message,
          });
        }
      },
    );

    /**
     * @api {get} /files/search Recherche globale messages/fichiers/conversations/groups/broadcast
     * @apiName SearchOccurrences
     * @apiGroup Files
     */
    router.get(
      "/search",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      validationMiddleware.sanitizeInput,
      async (req, res) => {
        try {
          await fileController.searchOccurrences(req, res);
        } catch (error) {
          console.error("❌ Erreur route GET /files/search:", error);
          res.status(500).json({
            success: false,
            message: "Erreur lors de la recherche globale",
            error: error.message,
          });
        }
      },
    );

    // (Optionnel) POST /files/download-multiple - Télécharger plusieurs fichiers en ZIP
    router.post(
      "/download-multiple",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      async (req, res) => {
        try {
          await fileController.downloadMultipleFiles(req, res);
        } catch (error) {
          console.error(
            "❌ Erreur route POST /files/download-multiple:",
            error,
          );
          res.status(500).json({
            success: false,
            message: "Erreur lors du téléchargement multiple",
            error: error.message,
          });
        }
      },
    );

    console.log("✅ Routes de fichiers configurées");
  } catch (error) {
    console.error("❌ Erreur configuration routes fichiers:", error);

    // Route de fallback en cas d'erreur
    router.all(
      "*",
      auth.valideToken(),
      auth.requireRole("agent_public"),
      (req, res) => {
        res.status(500).json({
          success: false,
          message: "Erreur de configuration du service de fichiers",
          error: error.message,
        });
      },
    );
  }

  return router;
}

module.exports = createFileRoutes;
