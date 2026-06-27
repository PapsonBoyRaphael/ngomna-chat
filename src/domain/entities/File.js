const crypto = require("crypto");
const path = require("path");

class File {
  constructor({
    _id,
    originalName,
    fileName,
    mimeType,
    size,
    path: filePath,
    url,
    uploadedBy,
    conversationId = null,
    messageId = null,
    metadata = {},
    status = "UPLOADING",
    createdAt,
    updatedAt,
    downloadCount = 0,
    isPublic = false,
    expiresAt = null,
    tags = [],
    isClientRecorded = false,
  }) {
    this._id = _id;
    this.originalName = originalName;
    this.fileName = fileName;
    this.mimeType = mimeType;
    this.size = size;
    this.path = filePath;
    this.url = url;
    this.uploadedBy = uploadedBy;
    this.conversationId = conversationId;
    this.messageId = messageId;
    this.metadata = this.enrichMetadata(metadata);
    this.status = status; // UPLOADING, COMPLETED, FAILED, DELETED
    this.createdAt = createdAt || new Date();
    this.updatedAt = updatedAt || new Date();
    this.downloadCount = downloadCount;
    this.isPublic = isPublic;
    this.expiresAt = expiresAt;
    this.tags = tags;
    this.isClientRecorded =
      isClientRecorded === true || isClientRecorded === "true";
  }

  // Enrichir les métadonnées
  enrichMetadata(metadata) {
    const fileType = this.getFileType();
    const category = this.getFileCategory();

    return {
      // Informations techniques du fichier
      technical: {
        extension: path.extname(this.originalName),
        fileType,
        category,
        encoding: metadata.technical?.encoding || "binary",
        hash: this.generateFileHash(),
        checksums: {
          md5: metadata.technical?.checksums?.md5,
          sha1: metadata.technical?.checksums?.sha1,
          sha256: metadata.technical?.checksums?.sha256,
        },
        ...metadata.technical,
      },

      // Métadonnées spécifiques selon le type
      content: this.getContentMetadata(metadata.content || {}),

      // Métadonnées Kafka
      kafkaMetadata: {
        topic: "chat.files",
        partition: null,
        offset: null,
        events: [], // Historique des événements Kafka
        lastPublished: null,
      },

      // Métadonnées Redis
      redisMetadata: {
        cacheKey: this.generateCacheKey(),
        ttl: this.getCacheTTL(),
        cacheStrategy: "write-through",
        cachedAt: null,
        cacheHits: 0,
      },

      // Traitement et optimisation
      processing: {
        status: "pending", // pending, processing, completed, failed
        thumbnailGenerated: false,
        thumbnailPath: null,
        thumbnailUrl: null,
        compressed: false,
        compressionRatio: null,
        processed: false,
        processingErrors: [],
        ...metadata.processing,
      },

      // Sécurité
      security: {
        encrypted: false,
        encryptionAlgorithm: null,
        accessLevel: "private", // public, private, restricted
        allowedUsers: [],
        restrictedUsers: [],
        scanStatus: "pending", // pending, clean, infected, quarantined
        scanResults: null,
        ...metadata.security,
      },

      // Performance et stockage
      storage: {
        provider: "local", // local, s3, gcs, azure
        bucket: null,
        region: null,
        storageClass: "standard",
        backupStatus: "pending",
        backupPath: null,
        ...metadata.storage,
      },

      // Statistiques d'utilisation
      usage: {
        firstDownload: null,
        lastDownload: null,
        downloadHistory: [],
        shareCount: 0,
        viewCount: 0,
        favoriteCount: 0,
        reportCount: 0,
        ...metadata.usage,
      },

      ...metadata,
    };
  }

  // Déterminer le type de fichier
  getFileType() {
    if (this.mimeType.startsWith("image/")) return "IMAGE";
    if (this.mimeType.startsWith("video/")) return "VIDEO";
    if (this.mimeType.startsWith("audio/")) return "AUDIO";
    if (this.mimeType.includes("pdf")) return "PDF";
    if (
      this.mimeType.includes("text/") ||
      this.mimeType.includes("application/")
    )
      return "DOCUMENT";
    return "OTHER";
  }

  // Déterminer la catégorie
  getFileCategory() {
    const categories = {
      "image/jpeg": "photo",
      "image/png": "photo",
      "image/gif": "animation",
      "video/mp4": "video",
      "video/avi": "video",
      "audio/mp3": "music",
      "audio/wav": "audio",
      "application/pdf": "document",
      "application/msword": "document",
      "application/zip": "archive",
    };

    return categories[this.mimeType] || "other";
  }

  // Métadonnées spécifiques au contenu
  getContentMetadata(existing = {}) {
    const fileType = this.getFileType();

    const baseMetadata = {
      duration: null,
      dimensions: null,
      bitrate: null,
      sampleRate: null,
      channels: null,
      codec: null,
      quality: null,
      ...existing,
    };

    switch (fileType) {
      case "IMAGE":
        return {
          ...baseMetadata,
          format:
            existing.format || path.extname(this.originalName).toLowerCase(),
          colorSpace: existing.colorSpace,
          hasAlpha: existing.hasAlpha || false,
          exif: existing.exif || {},
          location: existing.location,
        };

      case "VIDEO":
        return {
          ...baseMetadata,
          fps: existing.fps,
          aspectRatio: existing.aspectRatio,
          videoCodec: existing.videoCodec,
          audioCodec: existing.audioCodec,
          audioChannels: existing.audioChannels,
          audioSampleRate: existing.audioSampleRate,
          hasSubtitles: existing.hasSubtitles || false,
          // ✅ Miniature vidéo (frame extraite + thumbnails générés)
          thumbnail: existing.thumbnail || null,
        };

      case "AUDIO":
        return {
          ...baseMetadata,
          artist: existing.artist,
          album: existing.album,
          title: existing.title,
          genre: existing.genre,
          year: existing.year,
        };

      case "DOCUMENT":
        return {
          ...baseMetadata,
          pageCount: existing.pageCount,
          wordCount: existing.wordCount,
          hasImages: existing.hasImages || false,
          language: existing.language,
        };

      default:
        return baseMetadata;
    }
  }

  // Générer un hash du fichier
  generateFileHash() {
    const data = `${this.originalName}:${this.size}:${this.mimeType}:${this.uploadedBy}`;
    return crypto
      .createHash("sha256")
      .update(data)
      .digest("hex")
      .substring(0, 16);
  }

  // Générer une clé de cache
  generateCacheKey() {
    return `file:${this._id}`;
  }

  // TTL du cache selon le type
  getCacheTTL() {
    const ttlMap = {
      IMAGE: 7200, // 2 heures
      VIDEO: 3600, // 1 heure
      AUDIO: 7200, // 2 heures
      DOCUMENT: 14400, // 4 heures
      OTHER: 3600, // 1 heure
    };

    const fileType = this.getFileType();
    return ttlMap[fileType] || 3600;
  }

  // Validation
  validate() {
    const errors = [];

    if (!this.originalName || this.originalName.trim().length === 0) {
      errors.push("originalName est requis");
    }

    if (!this.fileName || this.fileName.trim().length === 0) {
      errors.push("fileName est requis");
    }

    if (!this.mimeType) {
      errors.push("mimeType est requis");
    }

    if (!this.size || this.size <= 0) {
      errors.push("size doit être supérieur à 0");
    }

    if (this.size > 100 * 1024 * 1024) {
      // 100MB max
      errors.push("size ne peut pas dépasser 100MB");
    }

    if (!this.uploadedBy) {
      errors.push("uploadedBy est requis");
    }

    const validStatuses = [
      "UPLOADING",
      "PROCESSING",
      "COMPLETED",
      "FAILED",
      "DELETED",
    ];
    if (!validStatuses.includes(this.status)) {
      errors.push(`status doit être un de: ${validStatuses.join(", ")}`);
    }

    if (errors.length > 0) {
      throw new Error(`Validation File échouée: ${errors.join(", ")}`);
    }

    return true;
  }

  // Marquer comme complété
  markAsCompleted() {
    this.status = "COMPLETED";
    this.metadata.processing.status = "completed";
    this.metadata.processing.processed = true;
    this.updatedAt = new Date();
    return this;
  }

  // Marquer comme échoué
  markAsFailed(error) {
    this.status = "FAILED";
    this.metadata.processing.status = "failed";
    this.metadata.processing.processingErrors.push({
      error: error.message,
      timestamp: new Date().toISOString(),
      stack: error.stack,
    });
    this.updatedAt = new Date();
    return this;
  }

  // Incrémenter le compteur de téléchargements
  incrementDownloadCount(userId = null) {
    this.downloadCount++;
    this.metadata.usage.lastDownload = new Date().toISOString();

    if (!this.metadata.usage.firstDownload) {
      this.metadata.usage.firstDownload = new Date().toISOString();
    }

    if (userId) {
      this.metadata.usage.downloadHistory.push({
        userId,
        timestamp: new Date().toISOString(),
        ip: null, // À ajouter via middleware
      });
    }

    this.updatedAt = new Date();
    return this;
  }

  // Ajouter une miniature
  setThumbnail(thumbnailPath, thumbnailUrl) {
    this.metadata.processing.thumbnailGenerated = true;
    this.metadata.processing.thumbnailPath = thumbnailPath;
    this.metadata.processing.thumbnailUrl = thumbnailUrl;
    this.updatedAt = new Date();
    return this;
  }

  // Marquer comme supprimé (soft delete)
  softDelete() {
    this.status = "DELETED";
    this.updatedAt = new Date();
    return this;
  }

  // Vérifier si le fichier peut être téléchargé par un utilisateur
  canBeDownloadedBy(userId) {
    if (this.status !== "COMPLETED") return false;
    if (this.isPublic) return true;
    if (this.uploadedBy === userId) return true;

    // Vérifier l'accès basé sur la conversation
    if (this.conversationId) {
      // Cette logique devrait être implémentée via un service
      return true; // Simplifié pour l'exemple
    }

    // Vérifier les permissions spécifiques
    const allowedUsers = this.metadata.security.allowedUsers || [];
    const restrictedUsers = this.metadata.security.restrictedUsers || [];

    if (restrictedUsers.includes(userId)) return false;
    if (allowedUsers.length > 0 && !allowedUsers.includes(userId)) return false;

    return true;
  }

  // Obtenir des informations de présentation
  getPresentationInfo() {
    const fileType = this.getFileType();
    const sizeFormatted = this.formatFileSize();

    return {
      name: this.originalName,
      size: sizeFormatted,
      type: fileType,
      category: this.metadata.technical.category,
      icon: this.getFileIcon(),
      canPreview: this.canBePreviewedInBrowser(),
      thumbnail: this.metadata.processing.thumbnailUrl,
      downloadUrl: this.url,
      uploadedAt: this.createdAt,
      uploadedBy: this.uploadedBy,
    };
  }

  // Formater la taille du fichier
  formatFileSize() {
    const units = ["B", "KB", "MB", "GB"];
    let size = this.size;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    return `${size.toFixed(1)} ${units[unitIndex]}`;
  }

  // Obtenir l'icône appropriée
  getFileIcon() {
    const icons = {
      IMAGE: "🖼️",
      VIDEO: "🎥",
      AUDIO: "🎵",
      PDF: "📄",
      DOCUMENT: "📝",
      OTHER: "📎",
    };

    return icons[this.getFileType()] || icons["OTHER"];
  }

  // Vérifier si le fichier peut être prévisualisé
  canBePreviewedInBrowser() {
    const previewableMimes = [
      "image/jpeg",
      "image/png",
      "image/gif",
      "image/webp",
      "video/mp4",
      "video/webm",
      "audio/mp3",
      "audio/wav",
      "audio/ogg",
      "application/pdf",
      "text/plain",
    ];

    return previewableMimes.includes(this.mimeType);
  }

  // Sérialisation pour Kafka
  toKafkaPayload() {
    return {
      fileId: this._id,
      originalName: this.originalName,
      fileName: this.fileName,
      mimeType: this.mimeType,
      size: this.size,
      uploadedBy: this.uploadedBy,
      conversationId: this.conversationId,
      messageId: this.messageId,
      status: this.status,
      fileType: this.getFileType(),
      category: this.metadata.technical.category,
      downloadCount: this.downloadCount,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      metadata: {
        kafkaMetadata: {
          ...this.metadata.kafkaMetadata,
          serializedAt: new Date().toISOString(),
        },
      },
    };
  }

  // Sérialisation pour Redis
  toRedisPayload() {
    return {
      _id: this._id,
      originalName: this.originalName,
      fileName: this.fileName,
      mimeType: this.mimeType,
      size: this.size,
      path: this.path,
      url: this.url,
      uploadedBy: this.uploadedBy,
      conversationId: this.conversationId,
      messageId: this.messageId,
      status: this.status,
      downloadCount: this.downloadCount,
      isPublic: this.isPublic,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      metadata: {
        ...this.metadata,
        redisMetadata: {
          ...this.metadata.redisMetadata,
          cachedAt: new Date().toISOString(),
        },
      },
    };
  }

  // Conversion vers objet simple
  toObject() {
    return {
      _id: this._id,
      originalName: this.originalName,
      fileName: this.fileName,
      mimeType: this.mimeType,
      size: this.size,
      path: this.path,
      url: this.url,
      uploadedBy: this.uploadedBy,
      conversationId: this.conversationId,
      messageId: this.messageId,
      metadata: this.metadata,
      status: this.status,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      downloadCount: this.downloadCount,
      isPublic: this.isPublic,
      expiresAt: this.expiresAt,
      tags: this.tags,
      isClientRecorded: this.isClientRecorded,
    };
  }

  // Méthodes statiques
  static fromObject(obj) {
    return new File(obj);
  }

  static fromRedisPayload(payload) {
    const parsed = typeof payload === "string" ? JSON.parse(payload) : payload;
    return new File(parsed);
  }

  // Créer un fichier pour upload
  static createForUpload(
    originalName,
    mimeType,
    size,
    uploadedBy,
    conversationId = null,
  ) {
    const fileName = `${Date.now()}_${originalName}`;

    return new File({
      originalName,
      fileName,
      mimeType,
      size,
      uploadedBy,
      conversationId,
      status: "UPLOADING",
    });
  }
}

module.exports = File;
