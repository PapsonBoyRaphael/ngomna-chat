const mongoose = require("mongoose");

const fileSchema = new mongoose.Schema(
  {
    _id: {
      type: String,
      required: true,
      trim: true,
    },
    originalName: {
      type: String,
      required: true,
      maxlength: 255,
      trim: true,
    },
    fileName: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
    mimeType: {
      type: String,
      required: true,
      index: true,
    },
    size: {
      type: Number,
      required: true,
      min: 0,
      max: 100 * 1024 * 1024, // 100MB max
    },
    path: {
      type: String,
      required: true,
    },
    url: {
      type: String,
      required: true,
    },
    uploadedBy: {
      type: String,
      required: true,
      index: true,
    },
    conversationId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Conversation",
      default: null,
      index: true,
    },
    messageId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Message",
      default: null,
      index: true,
    },

    // Statut du fichier
    status: {
      type: String,
      enum: ["UPLOADING", "PROCESSING", "COMPLETED", "FAILED", "DELETED"],
      default: "UPLOADING",
      index: true,
    },

    // Métadonnées enrichies
    metadata: {
      // Informations techniques
      technical: {
        extension: String,
        fileType: {
          type: String,
          // enum: ["IMAGE", "VIDEO", "AUDIO", "DOCUMENT", "OTHER"],
          index: true,
        },
        category: String,
        encoding: {
          type: String,
          default: "binary",
        },
        hash: String,
        checksums: {
          md5: String,
          sha1: String,
          sha256: String,
        },
      },

      // Métadonnées du contenu (variant selon le type)
      content: {
        duration: Number, // Pour audio/vidéo
        dimensions: {
          width: Number,
          height: Number,
        },
        bitrate: Number,
        sampleRate: Number,
        channels: Number,
        codec: String,
        quality: String,

        // Pour images
        format: String,
        colorSpace: String,
        hasAlpha: Boolean,
        exif: mongoose.Schema.Types.Mixed,
        location: {
          latitude: Number,
          longitude: Number,
        },

        // Pour vidéos
        fps: Number,
        aspectRatio: String,
        videoCodec: String,
        audioCodec: String,
        audioChannels: Number,
        audioSampleRate: Number,
        hasSubtitles: Boolean,

        // ✅ Miniature vidéo (frame extraite par MediaProcessingService)
        thumbnail: {
          generated: { type: Boolean, default: false },
          extractedAtSecond: Number,
          frameWidth: Number,
          frameHeight: Number,
          error: String,
        },

        // Pour audio
        artist: String,
        album: String,
        title: String,
        genre: String,
        year: Number,

        // Pour documents
        pageCount: Number,
        wordCount: Number,
        hasImages: Boolean,
        language: String,
      },

      // Métadonnées Kafka
      kafkaMetadata: {
        topic: {
          type: String,
          default: "chat.files",
        },
        events: [
          {
            type: String,
            timestamp: Date,
            success: Boolean,
            error: String,
            offset: Number,
            data: mongoose.Schema.Types.Mixed, // ✅ AJOUTER CE CHAMP MANQUANT
          },
        ],
        lastPublished: Date,
      },

      // Métadonnées Redis
      redisMetadata: {
        cacheKey: String,
        ttl: {
          type: Number,
          default: 7200, // 2 heures
        },
        cachedAt: Date,
        cacheHits: {
          type: Number,
          default: 0,
        },
      },

      // Traitement et optimisation
      processing: {
        status: {
          type: String,
          enum: ["pending", "processing", "completed", "failed"],
          default: "pending",
        },
        thumbnailGenerated: {
          type: Boolean,
          default: false,
        },
        thumbnailPath: String, // Chemin du thumbnail principal
        thumbnailUrl: String, // URL du thumbnail principal
        // ✅ NOUVEAU : Stocker tous les thumbnails
        thumbnails: [
          {
            size: {
              type: String,
              enum: ["small", "medium", "large"],
            },
            width: Number,
            height: Number,
            path: String,
            url: String,
            fileName: String,
          },
        ],
        compressed: {
          type: Boolean,
          default: false,
        },
        compressionRatio: Number,
        processed: {
          type: Boolean,
          default: false,
        },
        processingErrors: [String],
        processedAt: Date,
      },

      // Sécurité
      security: {
        encrypted: {
          type: Boolean,
          default: false,
        },
        encryptionAlgorithm: String,
        accessLevel: {
          type: String,
          enum: ["public", "private", "restricted"],
          default: "private",
        },
        allowedUsers: [String],
        restrictedUsers: [String],
        scanStatus: {
          type: String,
          enum: ["pending", "clean", "infected", "quarantined"],
          default: "pending",
        },
        scanResults: mongoose.Schema.Types.Mixed,
      },

      // Stockage
      storage: {
        provider: {
          type: String,
          enum: ["local", "s3", "gcs", "azure", "sftp"],
          default: "local",
        },
        bucket: String,
        region: String,
        storageClass: {
          type: String,
          default: "standard",
        },
        backupStatus: {
          type: String,
          enum: ["pending", "completed", "failed"],
          default: "pending",
        },
        backupPath: String,
      },

      // Statistiques d'utilisation
      usage: {
        firstDownload: Date,
        lastDownload: Date,
        downloadHistory: [
          {
            userId: String,
            timestamp: Date,
            ip: String,
            userAgent: String,
          },
        ],
        shareCount: {
          type: Number,
          default: 0,
        },
        viewCount: {
          type: Number,
          default: 0,
        },
        favoriteCount: {
          type: Number,
          default: 0,
        },
        reportCount: {
          type: Number,
          default: 0,
        },
      },
    },

    // Compteurs
    downloadCount: {
      type: Number,
      default: 0,
      min: 0,
    },

    // Visibilité
    isPublic: {
      type: Boolean,
      default: false,
    },

    // Expiration
    expiresAt: {
      type: Date,
      default: null,
    },

    // Tags
    tags: [String],

    isClientRecorded: {
      type: Boolean,
      default: false,
    },
  },
  {
    timestamps: true,
    versionKey: false,
    collection: "files",
  },
);

// ===============================
// INDEXATION
// ===============================

// Index composés pour performance
fileSchema.index({ uploadedBy: 1, createdAt: -1 }); // Fichiers par utilisateur
fileSchema.index({ conversationId: 1, createdAt: -1 }); // Fichiers par conversation
fileSchema.index({ "metadata.technical.fileType": 1, createdAt: -1 }); // Par type
fileSchema.index({ status: 1, createdAt: -1 }); // Par statut
fileSchema.index({ size: 1 }); // Pour les requêtes de taille
fileSchema.index({ downloadCount: -1 }); // Fichiers populaires
fileSchema.index({ "metadata.security.scanStatus": 1 }); // Statut de scan

// Index de recherche textuelle
fileSchema.index({
  originalName: "text",
  "metadata.content.title": "text",
  "metadata.content.artist": "text",
  tags: "text",
});

// Index TTL pour les fichiers temporaires
fileSchema.index(
  {
    expiresAt: 1,
  },
  {
    expireAfterSeconds: 0,
  },
);

// ===============================
// MÉTHODES D'INSTANCE
// ===============================

// Générer clé de cache
fileSchema.methods.generateCacheKey = function () {
  return `file:${this._id}`;
};

// Générer hash du fichier
fileSchema.methods.generateFileHash = function () {
  const crypto = require("crypto");
  const data = `${this.originalName}:${this.size}:${this.mimeType}:${this.uploadedBy}`;
  return crypto
    .createHash("sha256")
    .update(data)
    .digest("hex")
    .substring(0, 16);
};

// Déterminer le type de fichier
fileSchema.methods.getFileType = function () {
  if (this.mimeType.startsWith("image/")) return "IMAGE";
  if (this.mimeType.startsWith("video/")) return "VIDEO";
  if (this.mimeType.startsWith("audio/")) return "AUDIO";
  if (this.mimeType.includes("pdf")) return "DOCUMENT";
  if (this.mimeType.includes("text/") || this.mimeType.includes("application/"))
    return "DOCUMENT";
  return "OTHER";
};

// Marquer comme complété
fileSchema.methods.markAsCompleted = async function () {
  this.status = "COMPLETED";
  this.metadata.processing.status = "completed";
  this.metadata.processing.processed = true;
  this.metadata.processing.processedAt = new Date();

  await this.publishKafkaEvent("FILE_UPLOAD_COMPLETED");

  return this.save();
};

// Incrémenter téléchargements
fileSchema.methods.incrementDownloadCount = async function (
  userId = null,
  userAgent = null,
  ip = null,
) {
  this.downloadCount++;
  this.metadata.usage.lastDownload = new Date();

  if (!this.metadata.usage.firstDownload) {
    this.metadata.usage.firstDownload = new Date();
  }

  if (userId) {
    this.metadata.usage.downloadHistory.push({
      userId,
      timestamp: new Date(),
      ip,
      userAgent,
    });

    // Garder seulement les 100 derniers téléchargements
    if (this.metadata.usage.downloadHistory.length > 100) {
      this.metadata.usage.downloadHistory =
        this.metadata.usage.downloadHistory.slice(-100);
    }
  }

  await this.publishKafkaEvent("FILE_DOWNLOADED", {
    userId,
    downloadCount: this.downloadCount,
  });

  return this.save();
};

// Définir miniature
fileSchema.methods.setThumbnail = async function (thumbnailPath, thumbnailUrl) {
  this.metadata.processing.thumbnailGenerated = true;
  this.metadata.processing.thumbnailPath = thumbnailPath;
  this.metadata.processing.thumbnailUrl = thumbnailUrl;

  await this.publishKafkaEvent("FILE_THUMBNAIL_GENERATED", { thumbnailUrl });

  return this.save();
};

// Vérifier permissions de téléchargement
fileSchema.methods.canBeDownloadedBy = function (userId) {
  if (this.status !== "COMPLETED") return false;
  if (this.isPublic) return true;
  if (this.uploadedBy === userId) return true;

  const allowedUsers = this.metadata.security.allowedUsers || [];
  const restrictedUsers = this.metadata.security.restrictedUsers || [];

  if (restrictedUsers.includes(userId)) return false;
  if (allowedUsers.length > 0 && !allowedUsers.includes(userId)) return false;

  return true;
};

// Formater la taille
fileSchema.methods.formatFileSize = function () {
  const units = ["B", "KB", "MB", "GB"];
  let size = this.size;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(1)} ${units[unitIndex]}`;
};

// Publier événement Kafka - CORRECTION
fileSchema.methods.publishKafkaEvent = async function (
  eventType,
  additionalData = {},
  saveDocument = false, // ✅ NOUVEAU PARAMÈTRE
) {
  try {
    // ✅ CRÉER L'OBJET AVEC LES PROPRIÉTÉS SÉPARÉES
    const eventEntry = {
      type: eventType,
      timestamp: new Date(),
      success: true,
      error: null,
      offset: null,
      data: additionalData,
    };

    // Ajouter à la liste des événements
    if (!this.metadata.kafkaMetadata.events) {
      this.metadata.kafkaMetadata.events = [];
    }
    this.metadata.kafkaMetadata.events.push(eventEntry);

    // Limiter le nombre d'événements stockés
    if (this.metadata.kafkaMetadata.events.length > 50) {
      this.metadata.kafkaMetadata.events =
        this.metadata.kafkaMetadata.events.slice(-50);
    }

    // Mettre à jour lastPublished
    this.metadata.kafkaMetadata.lastPublished = new Date();

    // ✅ SAUVEGARDER SEULEMENT SI EXPLICITEMENT DEMANDÉ
    if (saveDocument) {
      await this.save();
    }

    console.log(`📤 Événement Kafka: ${eventType} pour fichier ${this._id}`);
    return true;
  } catch (error) {
    console.error(`❌ Erreur Kafka ${eventType}:`, error.message);
    return false;
  }
};

// Ajouter après les autres méthodes
fileSchema.methods.setAudioMetadata = async function (metadata) {
  try {
    this.metadata.content.duration = metadata.duration;
    this.metadata.content.bitrate = metadata.bitrate;
    this.metadata.content.sampleRate = metadata.sampleRate;
    this.metadata.content.channels = metadata.channels;

    // Métadonnées optionnelles
    if (metadata.title) this.metadata.content.title = metadata.title;
    if (metadata.artist) this.metadata.content.artist = metadata.artist;
    if (metadata.album) this.metadata.content.album = metadata.album;
    if (metadata.genre) this.metadata.content.genre = metadata.genre;
    if (metadata.year) this.metadata.content.year = metadata.year;

    await this.save();
    return true;
  } catch (error) {
    console.error("❌ Erreur définition métadonnées audio:", error);
    return false;
  }
};

// ===============================
// MÉTHODES STATIQUES
// ===============================

// Statistiques des fichiers
fileSchema.statics.getStatistics = async function (filter = {}) {
  return this.aggregate([
    { $match: filter },
    {
      $group: {
        _id: null,
        totalFiles: { $sum: 1 },
        totalSize: { $sum: "$size" },
        averageSize: { $avg: "$size" },
        filesByType: {
          $push: {
            type: "$metadata.technical.fileType",
            count: 1,
          },
        },
        totalDownloads: { $sum: "$downloadCount" },
      },
    },
  ]);
};

// ===============================
// MIDDLEWARE (HOOKS)
// ===============================

// Avant sauvegarde
fileSchema.pre("save", function (next) {
  if (this.isNew) {
    this.metadata.technical.hash = this.generateFileHash();
    this.metadata.technical.fileType = this.getFileType();
    this.metadata.redisMetadata.cacheKey = this.generateCacheKey();
  }

  next();
});

// Après sauvegarde
fileSchema.post("save", async function (doc, next) {
  try {
    if (doc.isNew) {
      console.log(`💾 Nouveau fichier créé: ${doc._id}`);
    } else {
      console.log(`🔄 Fichier mis à jour: ${doc._id}`);
    }
  } catch (error) {
    console.warn("⚠️ Erreur post-save fichier:", error.message);
  }
  next();
});

// ===============================
// MÉTHODES VIRTUELLES
// ===============================

// Indicateur de fichier image
fileSchema.virtual("isImage").get(function () {
  return this.metadata.technical.fileType === "IMAGE";
});

// URL de miniature ou fichier original
fileSchema.virtual("displayUrl").get(function () {
  return this.metadata.processing.thumbnailUrl || this.url;
});

// Configuration JSON
fileSchema.set("toJSON", {
  virtuals: true,
  transform: function (doc, ret) {
    delete ret.__v;
    delete ret.path; // Ne pas exposer le chemin physique
    return ret;
  },
});

const File = mongoose.model("File", fileSchema);

module.exports = File;
