const FileModel = require("../mongodb/models/FileModel");
const fs = require("fs-extra");
const path = require("path");

class MongoFileRepository {
  constructor(
    redisClient = null,
    kafkaProducer = null,
    thumbnailService = null,
  ) {
    this.redisClient = redisClient;
    this.kafkaProducer = kafkaProducer;
    this.thumbnailService = thumbnailService;
    this.maxRetries = 3;

    this.metrics = {
      kafkaEvents: 0,
      kafkaErrors: 0,
      dbQueries: 0,
      errors: 0,
    };

    console.log("✅ MongoFileRepository initialisé avec:", {
      redisClient: !!redisClient,
      kafkaProducer: !!kafkaProducer,
      kafkaProducerType: kafkaProducer?.constructor?.name,
      hasPublishMessage: typeof kafkaProducer?.publishMessage === "function",
    });
  }

  // ================================
  // MÉTHODES PRINCIPALES
  // ================================

  async save(file) {
    const startTime = Date.now();

    try {
      // ✅ VALIDER L'OBJET FILE
      if (!file || typeof file.validate !== "function") {
        throw new Error("Objet File invalide ou méthode validate manquante");
      }

      file.validate();
      this.metrics.dbQueries++;

      // Valider et nettoyer les métadonnées audio si nécessaire
      if (file.mimeType.startsWith("audio/")) {
        file.metadata.content = {
          duration: file.metadata.content?.duration || null,
          bitrate: file.metadata.content?.bitrate || null,
          sampleRate: file.metadata.content?.sampleRate || null,
          channels: file.metadata.content?.channels || null,
          codec: file.metadata.content?.codec || null,
        };
      }

      console.log("💾 Sauvegarde fichier:", file.originalName, file.mimeType);

      // ✅ Extraire les buffers de miniature vidéo AVANT la sauvegarde MongoDB
      // (les buffers ne doivent pas être envoyés à Mongoose, ils seront uploadés vers MinIO séparément)
      let videoThumbnailData = null;
      if (
        file.mimeType?.startsWith("video/") &&
        file.metadata?.content?.thumbnail?.generated &&
        file.metadata.content.thumbnail.thumbnails?.length > 0
      ) {
        videoThumbnailData = { ...file.metadata.content.thumbnail };
        // Retirer les buffers de l'objet qui sera sauvegardé en BDD
        delete file.metadata.content.thumbnail.thumbnails;
        console.log(
          `🎬 ${videoThumbnailData.thumbnails.length} buffers miniature vidéo extraits avant sauvegarde BDD`,
        );
      }

      // ✅ CRÉER LE FICHIER EN BASE AVEC CREATE AU LIEU DE FINDBRIDANDUPDATE
      const savedFile = await FileModel.create(file.toObject());

      if (!savedFile) {
        throw new Error("Échec de la sauvegarde en base de données");
      }

      // Mise à jour des métadonnées audio après sauvegarde si nécessaire
      if (savedFile.mimeType.startsWith("audio/") && file.metadata.content) {
        await FileModel.findByIdAndUpdate(
          savedFile._id,
          {
            "metadata.content": file.metadata.content,
          },
          { new: true },
        );
      }

      const processingTime = Date.now() - startTime;

      // ✅ GÉNÉRATION SYNCHRONE DE THUMBNAILS POUR LES IMAGES (< 10MB)
      // Les thumbnails sont générés AVANT de retourner le résultat pour que
      // la réponse API inclue les URLs des thumbnails dès le premier appel.
      const SYNC_THUMBNAIL_MAX_SIZE = 10 * 1024 * 1024; // 10 MB
      let finalFile = savedFile;

      if (
        this.thumbnailService &&
        this.thumbnailService.isImageFile(savedFile.mimeType)
      ) {
        if (savedFile.size <= SYNC_THUMBNAIL_MAX_SIZE) {
          // ✅ SYNC : attendre les thumbnails avant de retourner
          try {
            await this.processThumbnailsAsync(savedFile);
            // Recharger le document mis à jour avec les thumbnails
            finalFile =
              (await FileModel.findById(savedFile._id).lean()) || savedFile;
          } catch (error) {
            console.error(
              `❌ Erreur traitement thumbnails sync ${savedFile._id}:`,
              error.message,
            );
            // Continuer sans thumbnails — le fichier est déjà sauvegardé
            finalFile = savedFile;
          }
        } else {
          // ✅ ASYNC : fire-and-forget pour les très gros fichiers
          this.processThumbnailsAsync(savedFile).catch((error) => {
            console.error(
              `❌ Erreur traitement thumbnails async ${savedFile._id}:`,
              error,
            );
          });
        }

        // ✅ PUBLIER MESSAGE KAFKA SEULEMENT SI KAFKA EST DISPONIBLE
        if (
          this.kafkaProducer &&
          typeof this.kafkaProducer.publishMessage === "function"
        ) {
          try {
            await this.kafkaProducer.publishMessage({
              eventType: "GENERATE_THUMBNAILS",
              fileId: savedFile._id.toString(),
              originalPath: savedFile.path,
              mimeType: savedFile.mimeType,
              originalName: savedFile.originalName,
            });
          } catch (kafkaError) {
            console.warn(
              "⚠️ Erreur publication Kafka GENERATE_THUMBNAILS:",
              kafkaError.message,
            );
          }
        } else {
          console.log(
            "ℹ️ Kafka non disponible, génération thumbnails en mode local",
          );
        }
      }

      // ✅ SAUVEGARDER LES MINIATURES VIDÉO (buffers extraits avant la sauvegarde BDD)
      if (videoThumbnailData && videoThumbnailData.thumbnails?.length > 0) {
        // ✅ SYNC pour les vidéos aussi (les buffers sont déjà en mémoire)
        try {
          await this.processVideoThumbnailsAsync(savedFile, videoThumbnailData);
          finalFile =
            (await FileModel.findById(savedFile._id).lean()) || finalFile;
        } catch (error) {
          console.error(
            `❌ Erreur sauvegarde miniatures vidéo ${savedFile._id}:`,
            error,
          );
        }
      }

      // ✅ KAFKA AVEC VÉRIFICATION DE LA MÉTHODE
      if (this.kafkaProducer) {
        try {
          if (typeof this.kafkaProducer.publishMessage === "function") {
            await this._publishFileEvent("FILE_SAVED", finalFile, {
              processingTime,
              isNew: true,
            });
          } else {
            console.warn("⚠️ KafkaProducer n'a pas la méthode publishMessage");
            console.warn(
              "⚠️ Méthodes disponibles:",
              Object.getOwnPropertyNames(this.kafkaProducer),
            );
          }
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication fichier:", kafkaError.message);
        }
      }

      console.log(
        `💾 Fichier sauvegardé: ${finalFile._id || savedFile._id} (${processingTime}ms)`,
      );
      return finalFile;
    } catch (error) {
      this.metrics.errors++;
      console.error("❌ Erreur sauvegarde fichier:", error);
      throw error;
    }
  }

  async findById(fileId, useCache = true) {
    const startTime = Date.now();

    try {
      this.metrics.dbQueries++;
      const file = await FileModel.findById(fileId).lean();

      if (!file) {
        throw new Error(`Fichier ${fileId} non trouvé`);
      }

      const processingTime = Date.now() - startTime;

      console.log(`🔍 Fichier trouvé: ${fileId} (${processingTime}ms)`);
      return file;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur recherche fichier ${fileId}:`, error);
      throw error;
    }
  }

  async findByConversation(conversationId, options = {}) {
    const { page = 1, limit = 20, type = null, useCache = true } = options;
    const startTime = Date.now();

    try {
      const filter = {
        conversationId,
        status: "COMPLETED",
      };

      if (type) {
        filter["metadata.technical.fileType"] = type;
      }

      const skip = (page - 1) * limit;

      this.metrics.dbQueries += 2; // count + find
      const [files, totalCount] = await Promise.all([
        FileModel.find(filter)
          .sort({ createdAt: -1 })
          .skip(skip)
          .limit(limit)
          .lean(),
        FileModel.countDocuments(filter),
      ]);

      const result = {
        files: files.map((file) => ({
          ...file,
          displayUrl: file.metadata?.processing?.thumbnailUrl || file.url,
          formattedSize: this._formatFileSize(file.size),
        })),
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalCount / limit),
          totalCount,
          hasNext: page * limit < totalCount,
          hasPrevious: page > 1,
        },
        fromCache: false,
      };

      const processingTime = Date.now() - startTime;

      console.log(
        `🔍 Fichiers conversation: ${conversationId} (${files.length} files, ${processingTime}ms)`,
      );
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error(
        `❌ Erreur fichiers conversation ${conversationId}:`,
        error,
      );
      throw error;
    }
  }

  async findByUploader(uploaderId, options = {}) {
    const { page = 1, limit = 20, type = null, useCache = true } = options;
    const startTime = Date.now();
    const cacheKey = `${this.cachePrefix}uploader:${uploaderId}:p${page}:l${limit}:t${type}`;

    try {
      const filter = {
        uploadedBy: uploaderId,
        status: { $ne: "DELETED" },
      };

      if (type) {
        filter["metadata.technical.fileType"] = type;
      }

      const skip = (page - 1) * limit;

      this.metrics.dbQueries += 2;
      const [files, totalCount] = await Promise.all([
        FileModel.find(filter)
          .sort({ createdAt: -1 })
          .skip(skip)
          .limit(limit)
          .lean(),
        FileModel.countDocuments(filter),
      ]);

      const result = {
        files: files.map((file) => ({
          ...file,
          displayUrl: file.metadata?.processing?.thumbnailUrl || file.url,
          formattedSize: this._formatFileSize(file.size),
          canDelete: true,
        })),
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalCount / limit),
          totalCount,
          hasNext: page * limit < totalCount,
          hasPrevious: page > 1,
        },
        statistics: {
          totalSize: files.reduce((sum, file) => sum + file.size, 0),
          fileTypes: this._groupByType(files),
        },
        fromCache: false,
      };

      const processingTime = Date.now() - startTime;

      console.log(
        `🔍 Fichiers utilisateur: ${uploaderId} (${files.length} files, ${processingTime}ms)`,
      );
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur fichiers utilisateur ${uploaderId}:`, error);
      throw error;
    }
  }

  async incrementDownloadCount(fileId, userId = null, metadata = {}) {
    const startTime = Date.now();

    try {
      const updateData = {
        $inc: { downloadCount: 1 },
        $set: {
          "metadata.usage.lastDownload": new Date(),
          updatedAt: new Date(),
        },
      };

      // Ajouter à l'historique si userId fourni
      if (userId) {
        updateData.$push = {
          "metadata.usage.downloadHistory": {
            $each: [
              {
                userId,
                timestamp: new Date(),
                ip: metadata.ip || null,
                userAgent: metadata.userAgent || null,
              },
            ],
            $slice: -100, // Garder seulement les 100 derniers
          },
        };

        // Définir firstDownload si c'est le premier
        updateData.$setOnInsert = {
          "metadata.usage.firstDownload": new Date(),
        };
      }

      this.metrics.dbQueries++;
      const file = await FileModel.findByIdAndUpdate(fileId, updateData, {
        new: true,
        upsert: false,
      });

      if (!file) {
        throw new Error(`Fichier ${fileId} non trouvé`);
      }

      const processingTime = Date.now() - startTime;

      // 🚀 KAFKA
      if (this.kafkaProducer) {
        try {
          await this._publishFileEvent("FILE_DOWNLOADED", file, {
            downloadedBy: userId,
            downloadCount: file.downloadCount,
            processingTime,
            metadata,
          });
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication download:", kafkaError.message);
        }
      }

      console.log(
        `📥 Téléchargement compté: ${fileId} (count: ${file.downloadCount}, ${processingTime}ms)`,
      );
      return file;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur compteur téléchargement ${fileId}:`, error);
      throw error;
    }
  }

  async markAsCompleted(fileId, metadata = {}) {
    const startTime = Date.now();

    try {
      const updateData = {
        status: "COMPLETED",
        "metadata.processing.status": "completed",
        "metadata.processing.processed": true,
        "metadata.processing.processedAt": new Date(),
        updatedAt: new Date(),
      };

      // Ajouter les métadonnées de traitement
      if (metadata.thumbnailPath) {
        updateData["metadata.processing.thumbnailPath"] =
          metadata.thumbnailPath;
        updateData["metadata.processing.thumbnailGenerated"] = true;
      }
      if (metadata.thumbnailUrl) {
        updateData["metadata.processing.thumbnailUrl"] = metadata.thumbnailUrl;
      }
      if (metadata.compressionRatio) {
        updateData["metadata.processing.compressionRatio"] =
          metadata.compressionRatio;
        updateData["metadata.processing.compressed"] = true;
      }

      this.metrics.dbQueries++;
      const file = await FileModel.findByIdAndUpdate(
        fileId,
        { $set: updateData },
        { new: true },
      );

      if (!file) {
        throw new Error(`Fichier ${fileId} non trouvé`);
      }

      const processingTime = Date.now() - startTime;

      // 🚀 KAFKA
      if (this.kafkaProducer) {
        try {
          await this._publishFileEvent("FILE_PROCESSING_COMPLETED", file, {
            processingTime,
            metadata,
          });
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication completed:", kafkaError.message);
        }
      }

      console.log(
        `✅ Fichier marqué comme complété: ${fileId} (${processingTime}ms)`,
      );
      return file;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur mark completed ${fileId}:`, error);
      throw error;
    }
  }

  async markAsFailed(fileId, error) {
    const startTime = Date.now();

    try {
      const updateData = {
        status: "FAILED",
        "metadata.processing.status": "failed",
        "metadata.processing.processingErrors": [error.message],
        updatedAt: new Date(),
      };

      this.metrics.dbQueries++;
      const file = await FileModel.findByIdAndUpdate(
        fileId,
        {
          $set: updateData,
          $push: {
            "metadata.processing.processingErrors": {
              $each: [error.message],
              $slice: -10, // Garder les 10 dernières erreurs
            },
          },
        },
        { new: true },
      );

      if (!file) {
        throw new Error(`Fichier ${fileId} non trouvé`);
      }

      const processingTime = Date.now() - startTime;

      // 🚀 KAFKA
      if (this.kafkaProducer) {
        try {
          await this._publishFileEvent("FILE_PROCESSING_FAILED", file, {
            processingTime,
            error: error.message,
          });
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication failed:", kafkaError.message);
        }
      }

      console.log(
        `❌ Fichier marqué comme échoué: ${fileId} (${processingTime}ms)`,
      );
      return file;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur mark failed ${fileId}:`, error);
      throw error;
    }
  }

  async deleteFile(fileId, softDelete = true) {
    const startTime = Date.now();

    try {
      let file;

      if (softDelete) {
        // Soft delete - marquer comme supprimé
        this.metrics.dbQueries++;
        file = await FileModel.findByIdAndUpdate(
          fileId,
          {
            $set: {
              status: "DELETED",
              deletedAt: new Date(),
              updatedAt: new Date(),
            },
          },
          { new: true },
        );
      } else {
        // Hard delete - supprimer complètement
        this.metrics.dbQueries++;
        file = await FileModel.findByIdAndDelete(fileId);
      }

      if (!file) {
        throw new Error(`Fichier ${fileId} non trouvé`);
      }

      const processingTime = Date.now() - startTime;

      // 🚀 KAFKA
      if (this.kafkaProducer) {
        try {
          await this._publishFileEvent("FILE_DELETED", file, {
            softDelete,
            processingTime,
          });
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication delete:", kafkaError.message);
        }
      }

      console.log(
        `🗑️ Fichier supprimé: ${fileId} (soft: ${softDelete}, ${processingTime}ms)`,
      );
      return file;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur suppression fichier ${fileId}:`, error);
      throw error;
    }
  }

  // ================================
  // NOUVELLES MÉTHODES AVANCÉES
  // ================================

  async searchFiles(query, options = {}) {
    const {
      page = 1,
      limit = 20,
      sortBy = "createdAt",
      sortOrder = -1,
      useCache = true,
      useLike = true, // Ajout d'une option pour activer %like%
    } = options;

    const startTime = Date.now();

    try {
      let filter = { status: { $ne: "DELETED" } };
      if (query && typeof query === "string") {
        filter.$text = { $search: query };
      }

      let files = await File.find(filter)
        .sort({ score: { $meta: "textScore" }, [sortBy]: sortOrder })
        .skip((page - 1) * limit)
        .limit(limit)
        .lean();

      // Si aucun résultat et option %like% activée, faire une recherche regex
      if (useLike && files.length === 0 && query && query.length >= 2) {
        filter = { status: { $ne: "DELETED" } };
        filter.$or = [
          { originalName: { $regex: query, $options: "i" } },
          { "metadata.content.title": { $regex: query, $options: "i" } },
          { "metadata.content.artist": { $regex: query, $options: "i" } },
          { tags: { $regex: query, $options: "i" } },
        ];

        files = await File.find(filter)
          .sort({ [sortBy]: sortOrder })
          .skip((page - 1) * limit)
          .limit(limit)
          .lean();
      }

      const result = {
        files,
        totalFound: files.length,
        query,
        searchTime: Date.now() - startTime,
      };

      console.log(
        `🔍 Recherche fichiers: ${files.length} résultats (${result.searchTime}ms)`,
      );
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error("❌ Erreur recherche fichiers:", error);
      throw error;
    }
  }

  async getFileStatistics(filter = {}) {
    const startTime = Date.now();

    try {
      this.metrics.dbQueries++;
      const stats = await FileModel.aggregate([
        { $match: { status: { $ne: "DELETED" }, ...filter } },
        {
          $group: {
            _id: null,
            totalFiles: { $sum: 1 },
            totalSize: { $sum: "$size" },
            averageSize: { $avg: "$size" },
            totalDownloads: { $sum: "$downloadCount" },
            filesByType: {
              $push: {
                type: "$metadata.technical.fileType",
                size: "$size",
              },
            },
            newestFile: { $max: "$createdAt" },
            oldestFile: { $min: "$createdAt" },
          },
        },
        {
          $project: {
            _id: 0,
            totalFiles: 1,
            totalSize: 1,
            averageSize: { $round: ["$averageSize", 2] },
            totalDownloads: 1,
            filesByType: 1,
            newestFile: 1,
            oldestFile: 1,
            averageSizeFormatted: {
              $concat: [
                {
                  $toString: {
                    $round: [{ $divide: ["$averageSize", 1024] }, 2],
                  },
                },
                " KB",
              ],
            },
            totalSizeFormatted: {
              $concat: [
                {
                  $toString: {
                    $round: [{ $divide: ["$totalSize", 1048576] }, 2],
                  },
                },
                " MB",
              ],
            },
          },
        },
      ]);

      const result = stats[0] || {
        totalFiles: 0,
        totalSize: 0,
        averageSize: 0,
        totalDownloads: 0,
        filesByType: [],
        newestFile: null,
        oldestFile: null,
      };

      // Traiter les types de fichiers
      if (result.filesByType) {
        const typeStats = {};
        result.filesByType.forEach((item) => {
          const type = item.type || "UNKNOWN";
          if (!typeStats[type]) {
            typeStats[type] = { count: 0, totalSize: 0 };
          }
          typeStats[type].count++;
          typeStats[type].totalSize += item.size;
        });

        result.typeBreakdown = Object.entries(typeStats).map(
          ([type, data]) => ({
            type,
            count: data.count,
            totalSize: data.totalSize,
            percentage: ((data.count / result.totalFiles) * 100).toFixed(2),
            averageSize: (data.totalSize / data.count).toFixed(2),
          }),
        );
      }

      const processingTime = Date.now() - startTime;
      result.generatedAt = new Date().toISOString();
      result.processingTime = `${processingTime}ms`;

      console.log(`📊 Statistiques calculées (${processingTime}ms)`);
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error("❌ Erreur statistiques fichiers:", error);
      throw error;
    }
  }

  async bulkUpdateStatus(fileIds, status, metadata = {}) {
    const startTime = Date.now();

    try {
      const updateData = {
        status,
        updatedAt: new Date(),
        ...metadata,
      };

      this.metrics.dbQueries++;
      const result = await FileModel.updateMany(
        { _id: { $in: fileIds } },
        { $set: updateData },
      );

      const processingTime = Date.now() - startTime;

      // 🚀 KAFKA
      if (this.kafkaProducer) {
        try {
          await this._publishFileEvent("FILES_BULK_UPDATED", null, {
            fileIds,
            status,
            modifiedCount: result.modifiedCount,
            processingTime,
          });
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication bulk:", kafkaError.message);
        }
      }

      console.log(
        `📦 Mise à jour bulk: ${result.modifiedCount}/${fileIds.length} fichiers (${processingTime}ms)`,
      );
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error("❌ Erreur bulk update:", error);
      throw error;
    }
  }

  // ================================
  // MÉTHODES PRIVÉES (UTILITAIRES)
  // ================================

  _formatFileSize(bytes) {
    if (bytes === 0) return "0 B";

    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  }

  _groupByType(files) {
    const grouped = {};

    files.forEach((file) => {
      const type = file.metadata?.technical?.fileType || "UNKNOWN";

      if (!grouped[type]) {
        grouped[type] = {
          count: 0,
          totalSize: 0,
          files: [],
        };
      }

      grouped[type].count++;
      grouped[type].totalSize += file.size;
      grouped[type].files.push({
        id: file._id,
        name: file.originalName,
        size: file.size,
      });
    });

    // Ajouter des statistiques calculées
    Object.keys(grouped).forEach((type) => {
      grouped[type].averageSize = grouped[type].totalSize / grouped[type].count;
      grouped[type].formattedTotalSize = this._formatFileSize(
        grouped[type].totalSize,
      );
      grouped[type].formattedAverageSize = this._formatFileSize(
        grouped[type].averageSize,
      );
    });

    return grouped;
  }

  // ================================
  // MÉTHODES DE MONITORING
  // ================================

  getMetrics() {
    return {
      ...this.metrics,
      kafkaSuccessRate:
        this.metrics.kafkaEvents + this.metrics.kafkaErrors > 0
          ? (
              (this.metrics.kafkaEvents /
                (this.metrics.kafkaEvents + this.metrics.kafkaErrors)) *
              100
            ).toFixed(2) + "%"
          : "0%",
      generatedAt: new Date().toISOString(),
    };
  }

  resetMetrics() {
    this.metrics = {
      kafkaEvents: 0,
      kafkaErrors: 0,
      dbQueries: 0,
      errors: 0,
    };
  }

  async healthCheck() {
    const health = {
      status: "healthy",
      timestamp: new Date().toISOString(),
      checks: {},
    };

    // Test MongoDB
    try {
      await FileModel.findOne().limit(1);
      health.checks.mongodb = "healthy";
    } catch (error) {
      health.checks.mongodb = "unhealthy";
      health.status = "unhealthy";
    }

    health.metrics = this.getMetrics();

    return health;
  }

  // ✅ NOUVELLE MÉTHODE POUR TRAITEMENT ASYNCHRONE
  async processThumbnailsAsync(savedFile) {
    try {
      console.log(
        `🖼️ Début génération thumbnails pour ${savedFile.originalName}`,
      );

      // Télécharger l'image depuis MinIO/SFTP
      const tempImagePath =
        await this.thumbnailService.downloadImageForProcessing(savedFile.path);

      // Générer les thumbnails
      const thumbnails = await this.thumbnailService.generateThumbnails(
        tempImagePath,
        savedFile.originalName,
        savedFile._id,
      );

      // Mettre à jour le fichier en base avec les thumbnails
      await this.updateThumbnails(savedFile._id, thumbnails);

      // Nettoyer le fichier temporaire
      await fs.unlink(tempImagePath);

      console.log(`✅ Thumbnails générés et sauvegardés pour ${savedFile._id}`);
    } catch (error) {
      // Marquer le traitement comme échoué
      await this.markThumbnailProcessingFailed(savedFile._id, error);
      console.error(`❌ Échec génération thumbnails ${savedFile._id}:`, error);
    }
  }

  /**
   * ✅ SAUVEGARDER LES MINIATURES VIDÉO (buffers déjà générés par MediaProcessingService)
   * Upload chaque buffer de miniature vers le stockage puis met à jour la BDD
   */
  async processVideoThumbnailsAsync(savedFile, thumbnailData) {
    try {
      console.log(
        `🎬 Sauvegarde miniatures vidéo pour ${savedFile.originalName}`,
      );

      const uploadedThumbnails = [];
      const fileId = savedFile._id.toString();
      const baseName = path.basename(
        savedFile.originalName,
        path.extname(savedFile.originalName),
      );

      for (const thumb of thumbnailData.thumbnails) {
        if (!thumb.buffer || thumb.buffer.length === 0) continue;

        const thumbnailFileName = `thumbnail_${thumb.name}_${fileId}_${baseName}.webp`;
        const remotePath = `thumbnails/${thumbnailFileName}`;

        // Upload vers MinIO/SFTP si le service de stockage est disponible
        if (
          this.thumbnailService?.fileStorageService &&
          typeof this.thumbnailService.fileStorageService.uploadFromBuffer ===
            "function"
        ) {
          const uploadedPath =
            await this.thumbnailService.fileStorageService.uploadFromBuffer(
              thumb.buffer,
              remotePath,
              "image/webp",
            );

          uploadedThumbnails.push({
            size: thumb.name,
            width: thumb.width,
            height: thumb.height,
            path: uploadedPath,
            url: this.thumbnailService.generateThumbnailUrl(uploadedPath),
            fileName: thumbnailFileName,
          });
        } else {
          // Fallback: sauvegarder en local
          const localDir = "./storage/thumbnails";
          await fs.ensureDir(localDir);
          const localPath = path.join(localDir, thumbnailFileName);
          await fs.writeFile(localPath, thumb.buffer);

          uploadedThumbnails.push({
            size: thumb.name,
            width: thumb.width,
            height: thumb.height,
            path: localPath,
            url: `/api/files/thumbnail/${thumbnailFileName}`,
            fileName: thumbnailFileName,
          });
        }
      }

      if (uploadedThumbnails.length > 0) {
        await this.updateThumbnails(savedFile._id, uploadedThumbnails);
        console.log(
          `✅ ${uploadedThumbnails.length} miniatures vidéo sauvegardées pour ${fileId}`,
        );
      }
    } catch (error) {
      await this.markThumbnailProcessingFailed(savedFile._id, error);
      console.error(
        `❌ Échec sauvegarde miniatures vidéo ${savedFile._id}:`,
        error,
      );
    }
  }

  // ✅ NOUVELLE MÉTHODE POUR METTRE À JOUR LES THUMBNAILS
  async updateThumbnails(fileId, thumbnails) {
    try {
      const updateData = {
        "metadata.processing.thumbnailGenerated": true,
        "metadata.processing.thumbnails": thumbnails,
        "metadata.processing.status": "completed",
        "metadata.processing.processed": true,
        "metadata.processing.processedAt": new Date(),
        // URL du thumbnail principal (medium par défaut)
        "metadata.processing.thumbnailUrl":
          thumbnails.find((t) => t.size === "medium")?.url ||
          thumbnails[0]?.url,
        updatedAt: new Date(),
      };

      const updatedFile = await FileModel.findByIdAndUpdate(
        fileId,
        { $set: updateData },
        { new: true },
      );

      // Publier événement Kafka
      if (this.kafkaProducer) {
        await this._publishFileEvent("THUMBNAILS_GENERATED", updatedFile, {
          thumbnailCount: thumbnails.length,
          sizes: thumbnails.map((t) => t.size),
        });
      }

      return updatedFile;
    } catch (error) {
      console.error(`❌ Erreur mise à jour thumbnails ${fileId}:`, error);
      throw error;
    }
  }

  // ✅ NOUVELLE MÉTHODE POUR MARQUER L'ÉCHEC
  async markThumbnailProcessingFailed(fileId, error) {
    try {
      await FileModel.findByIdAndUpdate(fileId, {
        $set: {
          "metadata.processing.status": "failed",
          "metadata.processing.processingErrors": [error.message],
          updatedAt: new Date(),
        },
      });
    } catch (updateError) {
      console.error(
        `❌ Erreur marking thumbnail failed ${fileId}:`,
        updateError,
      );
    }
  }

  // Ajoute cette méthode privée dans la classe MongoFileRepository
  async _publishFileEvent(eventType, file, additionalData = {}) {
    if (
      !this.kafkaProducer ||
      typeof this.kafkaProducer.publishMessage !== "function"
    ) {
      console.warn(
        "⚠️ KafkaProducer non disponible ou méthode publishMessage absente",
      );
      return false;
    }
    try {
      await this.kafkaProducer.publishMessage({
        eventType,
        fileId: file?._id?.toString() || null,
        fileName: file?.originalName || null,
        userId: file?.uploadedBy || null,
        ...additionalData,
        timestamp: new Date().toISOString(),
      });
      this.metrics.kafkaEvents++;
      return true;
    } catch (err) {
      this.metrics.kafkaErrors++;
      console.warn(`⚠️ Erreur publication Kafka [${eventType}]:`, err.message);
      return false;
    }
  }
}

module.exports = MongoFileRepository;
