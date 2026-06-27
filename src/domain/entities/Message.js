const crypto = require("crypto");

class Message {
  constructor({
    _id,
    conversationId,
    senderId,
    receiverId,
    content,
    type = "TEXT",
    status = "SENT",
    metadata = {},
    createdAt,
    updatedAt,
    deletedAt = null,
    editedAt = null,
    replyTo = null,
    reactions = [],
  }) {
    this._id = _id;
    this.conversationId = conversationId;
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.content = content;
    this.type = type; // TEXT, IMAGE, VIDEO, AUDIO, FILE, LOCATION, CONTACT
    this.status = status; // SENT, DELIVERED, READ, FAILED
    this.metadata = this.enrichMetadata(metadata);
    this.createdAt = createdAt || new Date();
    this.updatedAt = updatedAt || new Date();
    this.deletedAt = deletedAt;
    this.editedAt = editedAt;
    this.replyTo = replyTo; // ID du message auquel on répond
    this.reactions = reactions; // Array des réactions
  }

  // Enrichir les métadonnées avec des informations contextuelles
  enrichMetadata(metadata) {
    const baseMetadata = {
      serverId: process.env.SERVER_ID || "default",
      clientVersion: metadata.clientVersion || "1.0.0",
      platform: metadata.platform || "web",
      deviceInfo: metadata.deviceInfo || {},

      // Métadonnées pour Kafka
      kafkaMetadata: {
        topic: this.getKafkaTopic(),
        partition: null,
        offset: null,
        timestamp: new Date().toISOString(),
        producerId: process.env.KAFKA_CLIENT_ID || "chat-file-service",
      },

      // Métadonnées pour Redis
      redisMetadata: {
        cacheKey: this.generateCacheKey(),
        ttl: this.getCacheTTL(),
        cacheStrategy: this.getCacheStrategy(),
      },

      // Métadonnées de délivrance
      deliveryMetadata: {
        attempts: 0,
        lastAttempt: null,
        deliveredAt: null,
        readAt: null,
        failureReason: null,
      },

      // Métadonnées de sécurité
      securityMetadata: {
        encrypted: false,
        signature: null,
        checksum: this.generateChecksum(),
      },

      ...metadata,
    };

    return baseMetadata;
  }

  // Générer une clé de cache Redis
  generateCacheKey() {
    return `message:${this.conversationId}:${this._id}`;
  }

  // Déterminer le TTL du cache selon le type de message
  getCacheTTL() {
    const ttlMap = {
      TEXT: 3600, // 1 heure
      IMAGE: 7200, // 2 heures
      VIDEO: 1800, // 30 minutes
      AUDIO: 3600, // 1 heure
      FILE: 7200, // 2 heures
      LOCATION: 1800, // 30 minutes
      CONTACT: 3600, // 1 heure
    };

    return ttlMap[this.type] || 3600;
  }

  // Stratégie de cache
  getCacheStrategy() {
    const strategies = {
      TEXT: "cache-aside",
      IMAGE: "write-through",
      VIDEO: "write-behind",
      AUDIO: "cache-aside",
      FILE: "write-through",
      LOCATION: "cache-aside",
      CONTACT: "cache-aside",
    };

    return strategies[this.type] || "cache-aside";
  }

  // Déterminer le topic Kafka
  getKafkaTopic() {
    const topicMap = {
      TEXT: "chat.messages",
      IMAGE: "chat.files",
      VIDEO: "chat.files",
      AUDIO: "chat.files",
      FILE: "chat.files",
      LOCATION: "chat.messages",
      CONTACT: "chat.messages",
    };

    return topicMap[this.type] || "chat.messages";
  }

  // Générer un checksum pour l'intégrité
  generateChecksum() {
    const data = `${this.senderId}:${this.receiverId}:${this.content}:${this.createdAt}`;
    return crypto
      .createHash("sha256")
      .update(data)
      .digest("hex")
      .substring(0, 16);
  }

  // Validation de l'entité
  validate() {
    const errors = [];

    if (!this.conversationId) {
      errors.push("conversationId est requis");
    }

    if (!this.senderId) {
      errors.push("senderId est requis");
    }

    if (!this.content || this.content.trim().length === 0) {
      errors.push("content ne peut pas être vide");
    }

    if (this.content.length > 10000) {
      errors.push("content ne peut pas dépasser 10000 caractères");
    }

    const validTypes = [
      "TEXT",
      "IMAGE",
      "VIDEO",
      "AUDIO",
      "FILE",
      "LOCATION",
      "CONTACT",
    ];
    if (!validTypes.includes(this.type)) {
      errors.push(`type doit être un de: ${validTypes.join(", ")}`);
    }

    const validStatuses = ["SENT", "DELIVERED", "READ", "FAILED"];
    if (!validStatuses.includes(this.status)) {
      errors.push(`status doit être un de: ${validStatuses.join(", ")}`);
    }

    if (errors.length > 0) {
      throw new Error(`Validation Message échouée: ${errors.join(", ")}`);
    }

    return true;
  }

  // Marquer comme lu
  markAsRead() {
    this.status = "READ";
    this.metadata.deliveryMetadata.readAt = new Date().toISOString();
    this.updatedAt = new Date();
    this.readAt = new Date();
    return this;
  }

  // Marquer comme livré
  markAsDelivered() {
    this.status = "DELIVERED";
    this.metadata.deliveryMetadata.deliveredAt = new Date().toISOString();
    this.updatedAt = new Date();
    this.receivedAt = new Date();
    return this;
  }

  // Ajouter une réaction
  addReaction(userId, emoji) {
    // Supprimer réaction existante du même utilisateur
    this.reactions = this.reactions.filter((r) => r.userId !== userId);

    // Ajouter nouvelle réaction
    this.reactions.push({
      userId,
      emoji,
      timestamp: new Date().toISOString(),
    });

    this.updatedAt = new Date();
    return this;
  }

  // Supprimer une réaction
  removeReaction(userId) {
    this.reactions = this.reactions.filter((r) => r.userId !== userId);
    this.updatedAt = new Date();
    return this;
  }

  // Éditer le message
  edit(newContent) {
    this.content = newContent;
    this.editedAt = new Date();
    this.updatedAt = new Date();
    return this;
  }

  // Marquer comme supprimé (soft delete)
  softDelete() {
    this.deletedAt = new Date();
    this.updatedAt = new Date();
    return this;
  }

  // Sérialisation pour Kafka
  toKafkaPayload() {
    return {
      messageId: this._id,
      conversationId: this.conversationId,
      senderId: this.senderId,
      receiverId: this.receiverId,
      content: this.content,
      type: this.type,
      status: this.status,
      timestamp: this.createdAt,
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
      conversationId: this.conversationId,
      senderId: this.senderId,
      receiverId: this.receiverId,
      content: this.content,
      type: this.type,
      status: this.status,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      reactions: this.reactions,
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
      conversationId: this.conversationId,
      senderId: this.senderId,
      receiverId: this.receiverId,
      content: this.content,
      type: this.type,
      status: this.status,
      metadata: this.metadata,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      deletedAt: this.deletedAt,
      editedAt: this.editedAt,
      replyTo: this.replyTo,
      reactions: this.reactions,
    };
  }

  // Méthode statique pour créer depuis un objet
  static fromObject(obj) {
    return new Message(obj);
  }

  // Méthode statique pour créer depuis Redis
  static fromRedisPayload(payload) {
    const parsed = typeof payload === "string" ? JSON.parse(payload) : payload;
    return new Message(parsed);
  }
}

module.exports = Message;
