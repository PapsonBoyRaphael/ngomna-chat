const path = require("path");
const fs = require("fs").promises;
const multer = require("multer");
const { v4: uuidv4 } = require("uuid");
const UploadFile = require("../../application/use-cases/UploadFile");
const DownloadFile = require("../use-cases/DownloadFile");
const { json } = require("stream/consumers");
const upload = multer({ dest: "uploads/" });

class FileController {
  constructor(
    uploadFileUseCase,
    getFileUseCase,
    redisClient = null,
    fileStorageService = null,
    downloadFileUseCase = null,
    mediaProcessingService = null,
    searchOccurrencesUseCase = null,
    chunkedUploadService = null,
    encryptionService = null, // ✅ E2EE
    keyManagementService = null, // ✅ E2EE
  ) {
    this.uploadFileUseCase = uploadFileUseCase;
    this.getFileUseCase = getFileUseCase;
    this.redisClient = redisClient;
    this.fileStorageService = fileStorageService;
    this.downloadFileUseCase = downloadFileUseCase;
    this.searchOccurrencesUseCase = searchOccurrencesUseCase;
    this.mediaProcessingService = mediaProcessingService;
    this.chunkedUploadService = chunkedUploadService;
    this.encryptionService = encryptionService; // ✅ E2EE
    this.keyManagementService = keyManagementService; // ✅ E2EE

    this.maxListLimit = 50; // Limit pour lists/multiple

    console.log("✅ FileController initialisé avec:", {
      uploadFileUseCase: !!this.uploadFileUseCase,
      getFileUseCase: !!this.getFileUseCase,
      redisClient: !!this.redisClient,
      kafkaProducer: !!this.kafkaProducer,
      fileStorageService: !!this.fileStorageService,
      downloadFileUseCase: !!this.downloadFileUseCase,
      searchOccurrencesUseCase: !!this.searchOccurrencesUseCase,
      mediaProcessingService: !!this.mediaProcessingService,
      chunkedUploadService: !!this.chunkedUploadService,
    });
  }

  async uploadFile(req, res) {
    const startTime = Date.now();

    console.log("🔍 Requête reçue dans le contrôleur uploadFile:", {
      headers: req.headers,
      body: req.body,
      file: req.file,
    });

    try {
      // ✅ IDEMPOTENCE : vérifier upload_token (client peut réessayer sans doublon)
      const uploadToken =
        req.body.upload_token || req.headers["x-upload-token"];
      if (uploadToken && this.chunkedUploadService) {
        const existingResult =
          await this.chunkedUploadService.checkToken(uploadToken);
        if (existingResult) {
          console.log(
            `🔄 Upload idempotent détecté (token: ${uploadToken.substring(0, 8)}...)`,
          );
          return res.status(200).json({
            success: true,
            data: existingResult,
            idempotent: true,
            metadata: {
              processingTime: `${Date.now() - startTime}ms`,
              timestamp: new Date().toISOString(),
            },
          });
        }
      }

      if (!req.file) {
        return res.status(400).json({
          success: false,
          message: "Aucun fichier fourni",
          code: "NO_FILE",
        });
      }

      // ✅ VÉRIFIER QUE LES SERVICES SONT DISPONIBLES
      if (!this.fileStorageService) {
        throw new Error("Service de stockage de fichiers non disponible");
      }

      if (!this.mediaProcessingService) {
        throw new Error("Service de traitement des médias non disponible");
      }

      const userId = req.user?.id || req.user?.userId || req.headers["user-id"];
      if (!userId) {
        return res.status(401).json({
          success: false,
          message: "Utilisateur non authentifié",
          code: "UNAUTHORIZED",
        });
      }

      // ✅ GÉNÉRER UUID POUR LE FICHIER (utilisé comme ID et fileName)
      const fileId = uuidv4().replace(/-/g, "");
      const ext = path.extname(req.file.originalname) || ".bin";
      const safeFileName = `${fileId}${ext.toLowerCase()}`;
      console.log(`🆔 ID fichier généré (UUID): ${fileId}`);
      console.log(`📝 Nom sécurisé généré: ${safeFileName}`);

      // ✅ CHIFFREMENT E2EE DU BUFFER (si activé)
      let uploadBuffer = req.file.buffer;
      let fileEncryptionMeta = {
        mode: "none",
        iv: null,
        tag: null,
        encryptedKey: null,
        keyVersion: null,
      };

      const fileOwnerId = req.body.receiverId || userId; // destinataire ou proprio
      if (
        this.encryptionService?.isE2EEEnabled() &&
        this.keyManagementService &&
        fileOwnerId
      ) {
        try {
          const recipientPublicKey =
            await this.keyManagementService.getPublicKey(String(fileOwnerId));
          const keyMeta = await this.keyManagementService.getKeyMetadata(
            String(fileOwnerId),
          );
          const encResult = await this.encryptionService.encryptFile(
            uploadBuffer,
            recipientPublicKey,
          );

          uploadBuffer = encResult.buffer;
          fileEncryptionMeta = {
            mode: "e2ee",
            iv: encResult.iv,
            tag: encResult.tag,
            encryptedKey: encResult.encryptedKey,
            keyVersion: keyMeta?.keyVersion ?? null,
          };
          console.log(
            `🔐 Fichier chiffré E2EE pour ${fileOwnerId} (keyVersion=${fileEncryptionMeta.keyVersion})`,
          );
        } catch (encErr) {
          console.warn(
            `⚠️ Chiffrement E2EE fichier ignoré pour ${fileOwnerId}: ${encErr.message}`,
          );
        }
      }

      // ✅ UPLOAD VERS LE STOCKAGE (utiliser le safeFileName)
      const remotePath = await this.fileStorageService.uploadFromBuffer(
        uploadBuffer,
        safeFileName,
        req.file.mimetype,
      );

      // ✅ EXTRACTION DES MÉTADONNÉES AVEC LE SERVICE
      let fileMetadata = {};
      try {
        fileMetadata = await this.mediaProcessingService.processFile(
          req.file.buffer,
          req.file.originalname, // Correction: originalname au lieu de originalName
          req.file.mimetype,
        );
        console.log(`✅ Métadonnées extraites pour: ${req.file.originalname}`);
      } catch (metadataError) {
        console.warn(
          `⚠️ Erreur extraction métadonnées:`,
          metadataError.message,
        );
        // Continuer avec des métadonnées basiques même en cas d'erreur
        fileMetadata = {
          technical: {
            extension: path.extname(req.file.originalname).toLowerCase(),
            fileType: this.mediaProcessingService.getFileType(
              req.file.mimetype,
              req.file.originalname,
            ),
            category: "other",
            encoding: "binary",
          },
          content: {}, // Supprimer la référence à duration qui n'est pas définie
        };
      }

      // ✅ CONSTRUCTION COMPLÈTE DU FILEDATA AVEC MÉTADONNÉES
      // ✅ Normaliser l'encodage UTF-8 du nom de fichier
      const originalName = Buffer.from(
        req.file.originalname,
        "latin1",
      ).toString("utf8");

      const fileData = {
        originalName: originalName,
        fileName: safeFileName, // ✅ Utiliser le nom sécurisé généré (UUID + extension)
        path: remotePath,
        size: req.file.size,
        mimeType: req.file.mimetype,
        uploadedBy: String(userId),
        conversationId: req.body.conversationId
          ? String(req.body.conversationId)
          : null,
        url: remotePath, // ✅ Utiliser le même chemin pour l'URL
        status: "UPLOADING",
        metadata: {
          technical: {
            ...fileMetadata.technical,
            extension: path.extname(originalName).toLowerCase(),
            fileType: fileMetadata.technical?.fileType || "AUDIO",
            category: fileMetadata.technical?.category || "media",
            encoding: "binary",
          },
          // ✅ CONSERVER TOUTES les métadonnées content (audio, vidéo, image, thumbnail, etc.)
          content: {
            ...fileMetadata.content,
          },
          // ✅ MÉTADONNÉES DE TRAITEMENT
          processing: {
            status: "pending",
            thumbnailGenerated: false,
            compressed: false,
            processed: false,
          },

          // ✅ MÉTADONNÉES REDIS
          redisMetadata: {
            cacheKey: `file:${Date.now()}`,
            ttl: 7200,
            cachedAt: new Date(),
            cacheHits: 0,
          },

          // ✅ MÉTADONNÉES DE SÉCURITÉ
          security: {
            encrypted: fileEncryptionMeta.mode === "e2ee",
            accessLevel: "private",
            scanStatus: "pending",
          },

          // ✅ MÉTADONNÉES DE STOCKAGE
          storage: {
            provider: this.fileStorageService.constructor.name.includes("S3")
              ? "s3"
              : "sftp",
            bucket: process.env.S3_BUCKET || "default",
            region: process.env.S3_REGION || "us-east-1",
            storageClass: "standard",
            backupStatus: "pending",
          },

          // ✅ STATISTIQUES D'UTILISATION
          usage: {
            downloadCount: 0,
            firstDownload: null,
            lastDownload: null,
            downloadHistory: [],
            shareCount: 0,
            viewCount: 0,
          },
        },
        downloadCount: 0,
        isPublic: false,
        tags: req.body.tags
          ? req.body.tags.split(",").map((tag) => tag.trim())
          : [],
        isClientRecorded:
          req.body.isClientRecorded === true ||
          req.body.isClientRecorded === "true",
        // ✅ MÉTADONNÉES DE CHIFFREMENT E2EE
        encryptionMetadata: fileEncryptionMeta,
      };

      let result;
      if (this.uploadFileUseCase) {
        result = await this.uploadFileUseCase.execute(fileData);
        console.log(`✅ Fichier enregistré en base: ${result}`);
      } else {
        result = {
          id: Date.now().toString(),
          ...fileData,
          uploadedAt: new Date().toISOString(),
        };
      }

      // ✅ STOCKER le mapping token → résultat pour idempotence
      if (uploadToken && this.chunkedUploadService) {
        await this.chunkedUploadService.storeTokenResult(uploadToken, result);
      }

      const processingTime = Date.now() - startTime;

      // ✅ RÉPONSE AVEC MÉTADONNÉES ENRICHIES
      res.status(201).json({
        success: true,
        data: {
          ...result,
          // ✅ INCLURE LES MÉTADONNÉES EXTRACTES DANS LA RÉPONSE
          metadata: {
            ...result.metadata,
            extracted: fileMetadata, // Métadonnées brutes extraites
          },
        },
        metadata: {
          processingTime: `${processingTime}ms`,
          fileType: fileMetadata.technical?.fileType || "UNKNOWN",
          hasMetadata: !!fileMetadata.technical,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur upload fichier:", error);

      // ✅ NETTOYAGE EN CAS D'ERREUR
      if (req.file?.path) {
        try {
          await fs.unlink(req.file.path);
        } catch (cleanupError) {
          console.warn(
            "⚠️ Erreur nettoyage après erreur:",
            cleanupError.message,
          );
        }
      }

      res.status(500).json({
        success: false,
        message: "Erreur lors de l'upload du fichier",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  // GET /files/:fileId - Métadonnées d'un fichier (AMÉLIORÉ)
  async getFile(req, res) {
    const startTime = Date.now();
    try {
      const { fileId } = req.params;
      const userId = req.user?.id || req.user?.userId;

      if (!this.getFileUseCase) {
        throw new Error("Service de récupération de fichiers non disponible");
      }

      const file = await this.getFileUseCase.execute(fileId, String(userId));
      if (!file) {
        return res.status(404).json({
          success: false,
          message: "Fichier non trouvé",
          code: "FILE_NOT_FOUND",
        });
      }

      const { displayInfo } = {
        formattedSize: this._formatFileSize(file.size),
        type: file.metadata?.technical?.fileType || "UNKNOWN",
        category: file.metadata?.technical?.category || "other",
        canDownload: file.status === "COMPLETED",
        canPreview: this._canPreviewFile(file),
        previewUrl: file.metadata?.processing?.thumbnailUrl || file.url,
      };

      // ✅ FORMATAGE DES MÉTADONNÉES POUR LA RÉPONSE
      const formattedFile = {
        ...file,
        // ✅ INFORMATIONS FORMATÉES POUR LE CLIENT
        displayInfo,
      };

      const processingTime = Date.now() - startTime;

      res.json({
        success: true,
        data: formattedFile,
        metadata: {
          processingTime: `${processingTime}ms`,
          fromCache: file.fromCache || false,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur récupération fichier:", error);

      res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération des métadonnées",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  // GET /files - Liste paginée des fichiers (métadonnées)
  async getFiles(req, res) {
    try {
      const userId = req.user?.id || req.user?.userId;
      const { page = 1, limit = 20, type, conversationId } = req.query;

      const result = await this.uploadFileUseCase.fileRepository.findByUploader(
        userId,
        {
          page: parseInt(page),
          limit: parseInt(limit),
          type,
          conversationId,
        },
      );

      // ✅ ENRICHIR LES FICHIERS AVEC DES INFORMATIONS DE FORMATAGE
      if (result.files) {
        result.files = result.files.map((file) => ({
          ...file,
          displayInfo: {
            formattedSize: this._formatFileSize(file.size),
            type: file.metadata?.technical?.fileType || "UNKNOWN",
            previewUrl: file.metadata?.processing?.thumbnailUrl || file.url,
            canDownload: file.status === "COMPLETED",
          },
        }));
      }

      res.json({
        success: true,
        data: result,
        metadata: {
          timestamp: new Date().toISOString(),
          user: userId,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération des fichiers",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // GET /files/conversation/:conversationId - Fichiers d'une conversation (métadonnées)
  async getConversationFiles(req, res) {
    try {
      const { conversationId } = req.params;
      const { page = 1, limit = 20, type } = req.query;

      const result =
        await this.uploadFileUseCase.fileRepository.findByConversation(
          conversationId,
          {
            page: parseInt(page),
            limit: parseInt(limit),
            type,
          },
        );

      // ✅ ENRICHIR LES FICHIERS
      if (result.files) {
        result.files = result.files.map((file) => ({
          ...file,
          displayInfo: {
            formattedSize: this._formatFileSize(file.size),
            type: file.metadata?.technical?.fileType || "UNKNOWN",
            previewUrl: file.metadata?.processing?.thumbnailUrl || file.url,
            uploadedBy: file.uploadedBy, // Inclure l'uploader pour le contexte
          },
        }));
      }

      res.json({
        success: true,
        data: result,
        metadata: {
          conversationId,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération des fichiers de conversation",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // DELETE /files/:fileId - Suppression logique, retourne le document supprimé
  async deleteFile(req, res) {
    try {
      const { fileId } = req.params;
      const userId = req.user?.id || req.user?.userId;

      const deletedFile =
        await this.uploadFileUseCase.fileRepository.deleteFile(fileId, true);

      res.json({
        success: true,
        data: deletedFile,
        message: "Fichier supprimé avec succès",
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Erreur lors de la suppression du fichier",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // Nouvelle méthode pour le téléchargement (stream direct)
  async downloadFile(req, res) {
    const startTime = Date.now();
    try {
      const { fileId } = req.params;
      const userId = req.user?.id || req.user?.userId;

      if (!fileId) {
        return res.status(400).json({
          success: false,
          message: "ID du fichier requis",
        });
      }

      if (!this.downloadFileUseCase) {
        throw new Error("Service de téléchargement non disponible");
      }

      const result = await this.downloadFileUseCase.executeSingle(
        fileId,
        String(userId),
      );

      if (result.downloadUrl) {
        // Mode DEV simple : rediriger vers l'URL signée
        return res.redirect(result.downloadUrl);
      } else {
        // Mode actuel : streamer le fichier
        const originalName =
          result.file.originalName || result.file.fileName || "download";
        // Fallback ASCII : retirer les caractères non-ASCII
        const safeAsciiName = originalName.replace(/[^\x20-\x7E]/g, "_");
        // Encodage UTF-8 (RFC 5987) pour les navigateurs modernes
        const encodedName = encodeURIComponent(originalName).replace(
          /'/g,
          "%27",
        );
        res.setHeader(
          "Content-Disposition",
          `attachment; filename="${safeAsciiName}"; filename*=UTF-8''${encodedName}`,
        );
        res.setHeader(
          "Content-Type",
          result.file.mimeType || "application/octet-stream",
        );
        if (result.file.size) {
          res.setHeader("Content-Length", result.file.size);
        }

        result.fileStream.pipe(res);
      }
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur téléchargement fichier:", error);

      // ✅ DÉTERMINER LE BON STATUS HTTP
      let statusCode = 500;
      if (
        error.message.includes("non trouvé") ||
        error.message.includes("not found")
      ) {
        statusCode = 404;
      } else if (
        error.message.includes("supprimé") ||
        error.message.includes("deleted") ||
        error.message.includes("Accès refusé")
      ) {
        statusCode = 400;
      }

      res.status(statusCode).json({
        success: false,
        message: error.message || "Erreur lors du téléchargement du fichier",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  // Pour le téléchargement multiple (optionnel, à appeler via une autre route)
  async downloadMultipleFiles(req, res) {
    const startTime = Date.now();
    try {
      const { fileIds } = req.body;
      const userId = req.user?.id || req.user?.userId;

      if (!Array.isArray(fileIds) || fileIds.length === 0) {
        return res.status(400).json({
          success: false,
          message: "Liste de fichiers requise",
        });
      }

      if (!this.downloadFileUseCase) {
        throw new Error("Service de téléchargement non disponible");
      }

      const { zipStream, files } =
        await this.downloadFileUseCase.executeMultiple(fileIds, String(userId));

      res.setHeader(
        "Content-Disposition",
        `attachment; filename="fichiers_${Date.now()}.zip"`,
      );
      res.setHeader("Content-Type", "application/zip");

      zipStream.pipe(res);
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur téléchargement multiple:", error);

      res.status(500).json({
        success: false,
        message: "Erreur lors du téléchargement des fichiers",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
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
        scope = "files",
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
          query,
          scope,
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

  async getThumbnail(req, res) {
    const { fileId } = req.params;
    const { size = "medium" } = req.query;
    const userId = req.user?.id;

    const file = await this.getFileUseCase.execute(fileId, userId);
    if (!file || !file.metadata.processing.thumbnailGenerated) {
      return res
        .status(404)
        .json({ success: false, message: "Thumbnail non disponible" });
    }

    const thumbnail = file.metadata.processing.thumbnails.find(
      (t) => t.size === size,
    );
    if (!thumbnail) throw new Error("Taille thumbnail invalide");

    // ✅ Utiliser le path MinIO (clé exacte) plutôt que l'URL HTTP
    const thumbnailKey = thumbnail.path || thumbnail.url;
    // Stream le thumbnail
    const thumbnailStream = await this.fileStorageService.download(
      null,
      thumbnailKey,
    );
    res.setHeader("Content-Type", "image/webp");
    thumbnailStream.pipe(res);
  }

  // ================================
  // UPLOAD CHUNKÉ (FICHIERS > 100 MB)
  // ================================

  /**
   * GET /files/upload/status?token=xxx
   * Vérifie le statut d'un upload (monolithique ou chunké)
   */
  async checkUploadStatus(req, res) {
    try {
      const { token, uploadId } = req.query;
      const identifier = token || uploadId;

      if (!identifier) {
        return res.status(400).json({
          success: false,
          message: "Paramètre 'token' ou 'uploadId' requis",
          code: "MISSING_PARAM",
        });
      }

      if (!this.chunkedUploadService) {
        return res.status(503).json({
          success: false,
          message: "Service d'upload chunké non disponible",
        });
      }

      const status =
        await this.chunkedUploadService.getUploadStatus(identifier);

      return res.status(200).json({
        success: true,
        ...status,
      });
    } catch (error) {
      console.error("❌ Erreur checkUploadStatus:", error);
      return res.status(500).json({
        success: false,
        message: "Erreur lors de la vérification du statut",
        error: error.message,
      });
    }
  }

  /**
   * POST /files/upload/init
   * Initialise un upload chunké pour fichiers > 100 MB
   * Body: { fileName, fileSize, mimeType, totalChunks, upload_token?, conversationId? }
   */
  async initChunkedUpload(req, res) {
    try {
      const userId = req.user?.id || req.user?.userId || req.headers["user-id"];
      if (!userId) {
        return res.status(401).json({
          success: false,
          message: "Utilisateur non authentifié",
          code: "UNAUTHORIZED",
        });
      }

      if (!this.chunkedUploadService) {
        return res.status(503).json({
          success: false,
          message: "Service d'upload chunké non disponible",
        });
      }

      const { fileName, fileSize, mimeType, totalChunks, conversationId } =
        req.body;
      const uploadToken =
        req.body.upload_token || req.headers["x-upload-token"];

      const result = await this.chunkedUploadService.initUpload({
        fileName,
        fileSize: parseInt(fileSize),
        mimeType,
        totalChunks: parseInt(totalChunks),
        uploadToken,
        userId: String(userId),
        conversationId: conversationId || null,
      });

      // Si déjà complété (idempotence)
      if (result.status === "already_completed") {
        return res.status(200).json({
          success: true,
          ...result,
          idempotent: true,
        });
      }

      return res.status(201).json({
        success: true,
        ...result,
      });
    } catch (error) {
      console.error("❌ Erreur initChunkedUpload:", error);
      return res.status(400).json({
        success: false,
        message: error.message,
      });
    }
  }

  /**
   * POST /files/upload/chunk/:uploadId
   * Envoie un chunk (body: file multipart 'chunk' + chunkIndex)
   */
  async uploadChunk(req, res) {
    try {
      const { uploadId } = req.params;
      const chunkIndex = req.body.chunkIndex || req.headers["x-chunk-index"];

      if (chunkIndex === undefined || chunkIndex === null) {
        return res.status(400).json({
          success: false,
          message: "chunkIndex requis (body ou header x-chunk-index)",
          code: "MISSING_CHUNK_INDEX",
        });
      }

      if (!req.file) {
        return res.status(400).json({
          success: false,
          message: "Aucun chunk fourni (multipart field 'chunk')",
          code: "NO_CHUNK",
        });
      }

      if (!this.chunkedUploadService) {
        return res.status(503).json({
          success: false,
          message: "Service d'upload chunké non disponible",
        });
      }

      const result = await this.chunkedUploadService.storeChunk(
        uploadId,
        parseInt(chunkIndex),
        req.file.buffer,
      );

      return res.status(200).json({
        success: true,
        ...result,
      });
    } catch (error) {
      console.error("❌ Erreur uploadChunk:", error);
      return res.status(400).json({
        success: false,
        message: error.message,
      });
    }
  }

  /**
   * POST /files/upload/complete/:uploadId
   * Assemble les chunks, uploade vers MinIO, crée en DB
   */
  async completeChunkedUpload(req, res) {
    const startTime = Date.now();
    try {
      const { uploadId } = req.params;
      const userId = req.user?.id || req.user?.userId || req.headers["user-id"];

      if (!userId) {
        return res.status(401).json({
          success: false,
          message: "Utilisateur non authentifié",
          code: "UNAUTHORIZED",
        });
      }

      if (!this.chunkedUploadService) {
        return res.status(503).json({
          success: false,
          message: "Service d'upload chunké non disponible",
        });
      }

      // ✅ ASSEMBLER ET UPLOADER VERS MINIO
      const { assembledFilePath, remotePath, safeFileName, session } =
        await this.chunkedUploadService.completeUpload(uploadId);

      // ✅ EXTRAIRE MÉTADONNÉES (lecture du fichier assemblé)
      let fileMetadata = {};
      try {
        const assembledBuffer = await fs.readFile(assembledFilePath);
        fileMetadata = await this.mediaProcessingService.processFile(
          assembledBuffer,
          session.fileName,
          session.mimeType,
        );
        console.log(
          `✅ Métadonnées extraites pour fichier chunké: ${session.fileName}`,
        );
      } catch (metadataError) {
        console.warn(
          `⚠️ Erreur extraction métadonnées (chunked):`,
          metadataError.message,
        );
        fileMetadata = {
          technical: {
            extension: path.extname(session.fileName).toLowerCase(),
            fileType:
              this.mediaProcessingService?.getFileType?.(
                session.mimeType,
                session.fileName,
              ) || "OTHER",
            category: "other",
            encoding: "binary",
          },
          content: {},
        };
      }

      // ✅ NORMALISER LE NOM D'ORIGINE UTF-8
      const originalName = Buffer.from(session.fileName, "latin1").toString(
        "utf8",
      );

      // ✅ CONSTRUIRE LES DONNÉES FICHIER
      const fileData = {
        originalName: originalName,
        fileName: safeFileName,
        path: remotePath,
        size: session.fileSize,
        mimeType: session.mimeType,
        uploadedBy: String(session.userId),
        conversationId: session.conversationId || null,
        url: remotePath,
        status: "UPLOADING",
        metadata: {
          technical: {
            ...fileMetadata.technical,
            extension: path.extname(session.fileName).toLowerCase(),
            fileType: fileMetadata.technical?.fileType || "OTHER",
            category: fileMetadata.technical?.category || "media",
            encoding: "binary",
          },
          content: { ...fileMetadata.content },
          processing: {
            status: "pending",
            thumbnailGenerated: false,
            compressed: false,
            processed: false,
          },
          redisMetadata: {
            cacheKey: `file:${Date.now()}`,
            ttl: 7200,
            cachedAt: new Date(),
            cacheHits: 0,
          },
          security: {
            encrypted: false,
            accessLevel: "private",
            scanStatus: "pending",
          },
          storage: {
            provider: this.fileStorageService?.constructor.name.includes("S3")
              ? "s3"
              : "sftp",
            bucket: process.env.S3_BUCKET || "default",
            region: process.env.S3_REGION || "us-east-1",
            storageClass: "standard",
            backupStatus: "pending",
          },
          usage: {
            downloadCount: 0,
            firstDownload: null,
            lastDownload: null,
            downloadHistory: [],
            shareCount: 0,
            viewCount: 0,
          },
        },
        downloadCount: 0,
        isPublic: false,
        tags: [],
        isClientRecorded:
          req.body.isClientRecorded === true ||
          req.body.isClientRecorded === "true",
      };

      // ✅ ENREGISTRER EN DB
      let result;
      if (this.uploadFileUseCase) {
        result = await this.uploadFileUseCase.execute(fileData);
      } else {
        result = {
          id: safeFileName.replace(/\.[^/.]+$/, ""),
          ...fileData,
          uploadedAt: new Date().toISOString(),
        };
      }

      // ✅ STOCKER TOKEN → RÉSULTAT POUR IDEMPOTENCE
      if (session.uploadToken && this.chunkedUploadService) {
        await this.chunkedUploadService.storeTokenResult(
          session.uploadToken,
          result,
        );
      }

      // ✅ NETTOYAGE CHUNKS TEMPORAIRES
      await this.chunkedUploadService.cleanup(uploadId);

      const processingTime = Date.now() - startTime;

      console.log(
        `✅ Upload chunké terminé: ${session.fileName} (${Math.round(session.fileSize / 1024 / 1024)} MB) en ${processingTime}ms`,
      );

      return res.status(201).json({
        success: true,
        data: {
          ...result,
          metadata: {
            ...result.metadata,
            extracted: fileMetadata,
          },
        },
        metadata: {
          processingTime: `${processingTime}ms`,
          fileType: fileMetadata.technical?.fileType || "UNKNOWN",
          hasMetadata: !!fileMetadata.technical,
          chunkedUpload: true,
          totalChunks: session.totalChunks,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error("❌ Erreur completeChunkedUpload:", error);

            // ✅ NETTOYAGE DES CHUNKS EN CAS D'ERREUR
      // Évite l'accumulation de fichiers temporaires orphelins sur le disque
      if (this.chunkedUploadService) {
        const { uploadId } = req.params;
        try {
          await this.chunkedUploadService.cleanup(uploadId);
          console.log(`🗑️ Chunks nettoyés après erreur pour: ${uploadId}`);
        } catch (cleanupError) {
          console.warn(
            `⚠️ Erreur nettoyage chunks après échec (${uploadId}):`,
            cleanupError.message,
          );
        }
      }

      return res.status(500).json({
        success: false,
        message: "Erreur lors de la finalisation de l'upload chunké",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
        metadata: {
          processingTime: `${processingTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    }
  }

  // ================================
  // MÉTHODES PRIVÉES
  // ================================

  _formatFileSize(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }

  _canPreviewFile(file) {
    const previewableTypes = ["IMAGE", "PDF", "TEXT"];
    return previewableTypes.includes(file.metadata?.technical?.fileType);
  }

  _addE2EENote(res) {
    res.setHeader("X-E2EE", "encrypted"); // Ou note in JSON si needed
  }
}

module.exports = FileController;
