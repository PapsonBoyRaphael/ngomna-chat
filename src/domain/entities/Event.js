class Event {
  constructor({
    _id,
    eventType,
    entityType,
    entityId,
    userId,
    data = {},
    metadata = {},
    timestamp,
    processed = false,
    error = null,
  }) {
    this._id = _id;
    this.eventType = eventType; // USER_CONNECTED, MESSAGE_SENT, FILE_UPLOADED, etc.
    this.entityType = entityType; // Message, Conversation, File, User
    this.entityId = entityId;
    this.userId = userId;
    this.data = data;
    this.metadata = this.enrichMetadata(metadata);
    this.timestamp = timestamp || new Date();
    this.processed = processed;
    this.error = error;
  }

  // Enrichir les métadonnées
  enrichMetadata(metadata) {
    return {
      source: process.env.SERVICE_NAME || "chat-file-service",
      version: "1.0.0",
      environment: process.env.NODE_ENV || "development",

      // Métadonnées Kafka
      kafkaMetadata: {
        topic: this.getKafkaTopic(),
        partition: null,
        offset: null,
        key: this.generateEventKey(),
        ...metadata.kafkaMetadata,
      },

      // Métadonnées de traitement
      processing: {
        attempts: 0,
        maxAttempts: 3,
        nextRetry: null,
        ...metadata.processing,
      },

      ...metadata,
    };
  }

  // Déterminer le topic Kafka selon le type d'événement
  getKafkaTopic() {
    const topicMap = {
      MESSAGE_SENT: "chat.messages",
      MESSAGE_DELIVERED: "chat.messages",
      MESSAGE_READ: "chat.messages",
      FILE_UPLOADED: "chat.files",
      FILE_DOWNLOADED: "chat.files",
      USER_CONNECTED: "chat.events",
      USER_DISCONNECTED: "chat.events",
      CONVERSATION_CREATED: "chat.conversations",
      CONVERSATION_UPDATED: "chat.conversations",
    };

    return topicMap[this.eventType] || "chat.events";
  }

  // Générer une clé pour l'événement
  generateEventKey() {
    return `${this.entityType}:${this.entityId}:${this.eventType}`;
  }

  // Marquer comme traité
  markAsProcessed() {
    this.processed = true;
    this.metadata.processing.processedAt = new Date().toISOString();
    return this;
  }

  // Marquer comme échoué
  markAsFailed(error) {
    this.processed = false;
    this.error = {
      message: error.message,
      stack: error.stack,
      timestamp: new Date().toISOString(),
    };
    this.metadata.processing.attempts++;
    return this;
  }

  // Validation
  validate() {
    const errors = [];

    if (!this.eventType) {
      errors.push("eventType est requis");
    }

    if (!this.entityType) {
      errors.push("entityType est requis");
    }

    if (!this.entityId) {
      errors.push("entityId est requis");
    }

    if (errors.length > 0) {
      throw new Error(`Validation Event échouée: ${errors.join(", ")}`);
    }

    return true;
  }

  // Sérialisation pour Kafka
  toKafkaPayload() {
    return {
      eventId: this._id,
      eventType: this.eventType,
      entityType: this.entityType,
      entityId: this.entityId,
      userId: this.userId,
      data: this.data,
      timestamp: this.timestamp,
      metadata: {
        kafkaMetadata: {
          ...this.metadata.kafkaMetadata,
          serializedAt: new Date().toISOString(),
        },
      },
    };
  }

  // Conversion vers objet simple
  toObject() {
    return {
      _id: this._id,
      eventType: this.eventType,
      entityType: this.entityType,
      entityId: this.entityId,
      userId: this.userId,
      data: this.data,
      metadata: this.metadata,
      timestamp: this.timestamp,
      processed: this.processed,
      error: this.error,
    };
  }

  // Méthodes statiques
  static fromObject(obj) {
    return new Event(obj);
  }

  // Créer un événement de message
  static createMessageEvent(eventType, messageId, userId, data = {}) {
    return new Event({
      eventType,
      entityType: "Message",
      entityId: messageId,
      userId,
      data,
    });
  }

  // Créer un événement de fichier
  static createFileEvent(eventType, fileId, userId, data = {}) {
    return new Event({
      eventType,
      entityType: "File",
      entityId: fileId,
      userId,
      data,
    });
  }

  // Créer un événement de conversation
  static createConversationEvent(eventType, conversationId, userId, data = {}) {
    return new Event({
      eventType,
      entityType: "Conversation",
      entityId: conversationId,
      userId,
      data,
    });
  }
}

module.exports = Event;
