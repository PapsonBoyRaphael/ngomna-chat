const mongoose = require("mongoose");
const { duplexPair } = require("stream");

// Schéma enrichi pour les messages
const messageSchema = new mongoose.Schema(
  {
    conversationId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Conversation",
      required: true,
      index: true,
    },
    senderId: {
      type: String,
      required: true,
      index: true,
    },
    receiverId: {
      type: String,
      required: false,
      index: true,
    },
    content: {
      type: String,
      required: function () {
        return this.type === "TEXT";
      },
      maxlength: 10000,
      trim: true,
    },
    type: {
      type: String,
      enum: [
        "TEXT",
        "IMAGE",
        "VIDEO",
        "AUDIO",
        "FILE",
        "LOCATION",
        "CONTACT",
        "SYSTEM",
        "CALL",
        "VIDEO_CALL",
      ],
      default: "TEXT",
      index: true,
    },
    status: {
      type: String,
      enum: ["SENT", "DELIVERED", "READ", "FAILED", "DELETED"],
      default: "SENT",
      index: true,
    },

    // ✅ COMPTEURS POUR GROUPES ET BROADCASTS
    // Pour PRIVATE: totalRecipients = 1
    // Pour GROUP/BROADCAST: totalRecipients = participants.length - 1 (exclut l'expéditeur)
    deliveredCount: {
      type: Number,
      default: 0,
    },
    readCount: {
      type: Number,
      default: 0,
    },
    totalRecipients: {
      type: Number,
      default: 1, // Par défaut pour PRIVATE
    },

    // ✅ LISTE DES UTILISATEURS QUI ONT REÇU/LU (pour éviter les doublons)
    deliveredBy: [
      {
        type: String, // userId
      },
    ],
    readBy: [
      {
        type: String, // userId
      },
    ],

    // Dates de réception et de lecture (ajout explicite)
    receivedAt: {
      type: Date,
      default: null,
      index: true,
    },
    readAt: {
      type: Date,
      default: null,
      index: true,
    },

    // Métadonnées enrichies
    metadata: {
      // Métadonnées techniques
      technical: {
        serverId: {
          type: String,
          default: () => process.env.SERVER_ID || "default",
        },
        clientVersion: String,
        platform: {
          type: String,
          enum: ["web", "android", "ios", "desktop"],
          default: "web",
        },
        deviceInfo: {
          userAgent: String,
          ip: String,
          browser: String,
          os: String,
        },
        messageId: String,
        checksum: String,
      },

      // Métadonnées Kafka
      kafkaMetadata: {
        topic: {
          type: String,
          default: "chat.messages",
        },
        partition: Number,
        offset: Number,
        publishedAt: Date,
        events: [
          {
            type: String,
            timestamp: Date,
            success: Boolean,
            error: String,
          },
        ],
      },

      // Métadonnées Redis
      redisMetadata: {
        cacheKey: String,
        cachedAt: Date,
        ttl: {
          type: Number,
          default: 3600,
        },
        cacheStrategy: {
          type: String,
          enum: ["cache-aside", "write-through", "write-behind"],
          default: "cache-aside",
        },
        cacheHits: {
          type: Number,
          default: 0,
        },
      },

      // Métadonnées de délivrance
      deliveryMetadata: {
        attempts: {
          type: Number,
          default: 0,
        },
        lastAttempt: Date,
        deliveredAt: Date,
        readAt: Date,
        failureReason: String,
        retryCount: {
          type: Number,
          default: 0,
        },
      },

      // Métadonnées spécifiques au contenu
      contentMetadata: {
        originalContent: String, // Pour les messages édités
        mentions: [String], // @utilisateur
        hashtags: [String], // #topic
        urls: [
          {
            url: String,
            title: String,
            description: String,
            image: String,
          },
        ],
        file: {
          fileId: String,
          fileName: String,
          fileSize: Number,
          duration: Number, // Pour les médias
          mimeType: String,
          url: String,
          thumbnailUrl: String,
          uploadedAt: Date,
          status: String,
          isClientRecorded: {
            type: Boolean,
            default: false,
          },
        },

        // ✅ Métadonnées pour les messages broadcast (lien vers les conversations privées)
        broadcast: {
          // ID de la conversation broadcast source (présent dans les messages des conv privées)
          broadcastConversationId: {
            type: String,
            default: null,
          },
          // Liste des conversations privées créées/utilisées pour dispatch (présent dans le message broadcast)
          privateConversations: [
            {
              recipientId: String,
              conversationId: String,
              messageId: String,
            },
          ],
        },

        // ✅ Métadonnées pour les appels (CALL / VIDEO_CALL)
        call: {
          callId: String, // Identifiant unique de l'appel
          callType: {
            type: String,
            enum: ["AUDIO", "VIDEO"],
          },
          status: {
            type: String,
            enum: [
              "INITIATED", // Appel lancé
              "RINGING", // En train de sonner
              "ANSWERED", // Décroché
              "ENDED", // Terminé normalement
              "MISSED", // Manqué
              "DECLINED", // Refusé
              "CANCELLED", // Annulé par l'appelant
              "FAILED", // Échec technique
              "BUSY", // Occupé
            ],
          },
          initiatorId: String, // Matricule de l'appelant
          receiverIds: [String], // Matricule(s) du/des destinataire(s)
          startedAt: Date, // Début de l'appel (quand décroché)
          endedAt: Date, // Fin de l'appel
          duration: Number, // Durée en secondes (0 si manqué/refusé)
          endReason: String, // Raison de fin ("user_hangup", "timeout", "error"...)
        },

        // ✅ Métadonnées de chiffrement E2EE
        encryptionMetadata: {
          // Mode actif au moment de l'envoi : 'none' | 'e2ee'
          mode: {
            type: String,
            enum: ["none", "e2ee"],
            default: "none",
          },
          // Vecteur d'initialisation AES-256-GCM (base64, 16 bytes)
          iv: { type: String, default: null },
          // Auth tag GCM (base64, 16 bytes) — intégrité du contenu
          tag: { type: String, default: null },
          // Clé symétrique AES chiffrée avec la clé publique RSA du destinataire (base64)
          encryptedKey: { type: String, default: null },
          // Version de la clé RSA utilisée (pour rotation / déchiffrement des anciens messages)
          keyVersion: { type: String, default: null },
        },
      },
    },

    // Gestion des réponses et réactions
    replyTo: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Message",
      default: null,
    },

    // Gestion du transfert de messages
    isForwarded: {
      type: Boolean,
      default: false,
    },
    forwardedFrom: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Message",
      default: null,
    },
    originalSenderId: {
      type: String,
      default: null,
    },

    reactions: [
      {
        userId: {
          type: String,
          required: true,
        },
        emoji: {
          type: String,
          required: true,
        },
        timestamp: {
          type: Date,
          default: Date.now,
        },
      },
    ],

    // Gestion des modifications
    editedAt: {
      type: Date,
      default: null,
    },
    deletedAt: {
      type: Date,
      default: null,
    },

    // Champs de suppression
    isDeleted: {
      type: Boolean,
      default: false,
    },
    deletedBy: {
      type: String, // userId de celui qui a supprimé
      default: null,
    },
    deletedFor: {
      type: String, // "EVERYONE" ou null (pour FOR_ME on utilise deletedForUsers)
      enum: ["EVERYONE", null],
      default: null,
    },
    deletedForUsers: {
      type: [String], // liste de userIds pour suppression "pour moi uniquement"
      default: [],
    },

    // Champs de système
    isSystemMessage: {
      type: Boolean,
      default: false,
    },
    priority: {
      type: String,
      enum: ["LOW", "NORMAL", "HIGH", "URGENT"],
      default: "NORMAL",
    },
  },
  {
    timestamps: true,
    versionKey: false,
    collection: "messages",
  },
);

// ===============================
// INDEXATION POUR PERFORMANCE
// ===============================

// Index composés pour les requêtes courantes
messageSchema.index({ conversationId: 1, createdAt: -1 }); // Messages par conversation
messageSchema.index({ senderId: 1, createdAt: -1 }); // Messages par expéditeur
messageSchema.index({ receiverId: 1, status: 1 }); // Messages non lus
messageSchema.index({ "metadata.kafkaMetadata.offset": 1 }); // Événements Kafka
messageSchema.index({ type: 1, createdAt: -1 }); // Filtrage par type
messageSchema.index({ status: 1, "metadata.deliveryMetadata.attempts": 1 }); // Messages en échec

// Index de recherche textuelle
messageSchema.index({
  content: "text",
  "metadata.contentMetadata.mentions": "text",
  "metadata.contentMetadata.hashtags": "text",
});

// Index TTL pour les messages temporaires (optionnel)
messageSchema.index(
  {
    createdAt: 1,
  },
  {
    expireAfterSeconds: 7776000, // 90 jours
    partialFilterExpression: {
      isSystemMessage: true,
    },
  },
);

// ===============================
// MÉTHODES D'INSTANCE
// ===============================

// Générer une clé de cache Redis
messageSchema.methods.generateCacheKey = function () {
  return `message:${this.conversationId}:${this._id}`;
};

// Générer un checksum pour l'intégrité
messageSchema.methods.generateChecksum = function () {
  const crypto = require("crypto");
  const data = `${this.senderId}:${this.receiverId}:${this.content}:${this.createdAt}`;
  return crypto
    .createHash("sha256")
    .update(data)
    .digest("hex")
    .substring(0, 16);
};

// Marquer comme lu avec événement Kafka
messageSchema.methods.markAsRead = async function () {
  this.status = "READ";
  this.readAt = new Date(); // <-- Ajout explicite
  this.metadata.deliveryMetadata.readAt = new Date();

  // Publier événement Kafka
  await this.publishKafkaEvent("MESSAGE_READ");

  return this.save();
};

// Marquer comme livré
messageSchema.methods.markAsDelivered = async function () {
  this.status = "DELIVERED";
  this.receivedAt = new Date(); // <-- Ajout explicite
  this.metadata.deliveryMetadata.deliveredAt = new Date();

  await this.publishKafkaEvent("MESSAGE_DELIVERED");

  return this.save();
};

// Ajouter une réaction
messageSchema.methods.addReaction = async function (userId, emoji) {
  const existingReaction = this.reactions.find((r) => r.userId === userId);

  if (existingReaction) {
    existingReaction.emoji = emoji;
    existingReaction.timestamp = new Date();
  } else {
    this.reactions.push({
      userId,
      emoji,
      timestamp: new Date(),
    });
  }

  await this.publishKafkaEvent("MESSAGE_REACTION_ADDED", { userId, emoji });

  return this.save();
};

// Éditer le message
messageSchema.methods.editContent = async function (newContent) {
  if (!this.metadata.contentMetadata.originalContent) {
    this.metadata.contentMetadata.originalContent = this.content;
  }

  this.content = newContent;
  this.editedAt = new Date();

  await this.publishKafkaEvent("MESSAGE_EDITED", {
    originalContent: this.metadata.contentMetadata.originalContent,
    newContent,
  });

  return this.save();
};

// Publier un événement Kafka
messageSchema.methods.publishKafkaEvent = async function (
  eventType,
  additionalData = {},
) {
  try {
    // Vérifier si Kafka est disponible
    const indexModule = require("../../../index");
    const kafkaProducers = indexModule.kafkaProducers
      ? indexModule.kafkaProducers()
      : null;

    if (!kafkaProducers?.messageProducer) {
      console.warn("⚠️ Kafka producer non disponible pour", eventType);
      return false;
    }

    const eventData = {
      eventType,
      messageId: this._id,
      conversationId: this.conversationId,
      senderId: this.senderId,
      receiverId: this.receiverId,
      content: this.content,
      type: this.type,
      status: this.status,
      timestamp: new Date().toISOString(),
      serverId: this.metadata.technical.serverId,
      ...additionalData,
    };

    await kafkaProducers.messageProducer.publishMessage(eventData);

    // Enregistrer l'événement dans les métadonnées
    this.metadata.kafkaMetadata.events.push({
      type: eventType,
      timestamp: new Date(),
      success: true,
    });

    console.log(
      `📤 Événement Kafka publié: ${eventType} pour message ${this._id}`,
    );
    return true;
  } catch (error) {
    console.error(`❌ Erreur publication Kafka ${eventType}:`, error.message);

    // Enregistrer l'erreur
    this.metadata.kafkaMetadata.events.push({
      type: eventType,
      timestamp: new Date(),
      success: false,
      error: error.message,
    });

    return false;
  }
};

// Invalider le cache Redis - CORRECTION
messageSchema.methods.invalidateCache = async function () {
  // ✅ L'invalidation de cache est gérée par CachedMessageRepository
  // Ce stub est conservé pour compatibilité avec le middleware post("save")
  return true;
};

// ===============================
// MÉTHODES STATIQUES
// ===============================

// Recherche avec mise en cache
messageSchema.statics.findWithCache = async function (query, options = {}) {
  const redisClient = require("../../../index").redisClient;
  const cacheKey = `query:${JSON.stringify(query)}:${JSON.stringify(options)}`;

  if (redisClient) {
    try {
      const cached = await redisClient.get(cacheKey);
      if (cached) {
        console.log(`📦 Résultats récupérés depuis Redis: ${cacheKey}`);
        return JSON.parse(cached);
      }
    } catch (error) {
      console.warn("⚠️ Erreur lecture cache:", error.message);
    }
  }

  const results = await this.find(query, null, options).lean();

  // Mettre en cache si Redis disponible
  if (redisClient && results.length > 0) {
    try {
      await redisClient.setex(cacheKey, 300, JSON.stringify(results)); // 5 minutes
    } catch (error) {
      console.warn("⚠️ Erreur mise en cache:", error.message);
    }
  }

  return results;
};

// Statistiques des messages
messageSchema.statics.getStatistics = async function (conversationId) {
  return this.aggregate([
    { $match: { conversationId: mongoose.Types.ObjectId(conversationId) } },
    {
      $group: {
        _id: null,
        totalMessages: { $sum: 1 },
        messagesByType: {
          $push: {
            type: "$type",
            count: 1,
          },
        },
        messagesByStatus: {
          $push: {
            status: "$status",
            count: 1,
          },
        },
        lastMessage: { $max: "$createdAt" },
        firstMessage: { $min: "$createdAt" },
      },
    },
  ]);
};

// ✅ Mise à jour du statut d'appel (CALL / VIDEO_CALL)
messageSchema.statics.updateCallStatus = async function (messageId, updates) {
  const updateFields = {};

  if (updates.status) {
    updateFields["metadata.contentMetadata.call.status"] = updates.status;
  }
  if (updates.startedAt) {
    updateFields["metadata.contentMetadata.call.startedAt"] = updates.startedAt;
  }
  if (updates.endedAt) {
    updateFields["metadata.contentMetadata.call.endedAt"] = updates.endedAt;
  }
  if (updates.duration !== undefined) {
    updateFields["metadata.contentMetadata.call.duration"] = updates.duration;
  }
  if (updates.endReason) {
    updateFields["metadata.contentMetadata.call.endReason"] = updates.endReason;
  }

  if (Object.keys(updateFields).length === 0) {
    console.warn("⚠️ updateCallStatus: aucun champ à mettre à jour");
    return null;
  }

  const result = await this.findByIdAndUpdate(
    messageId,
    { $set: updateFields },
    { new: true, lean: true },
  );

  if (!result) {
    throw new Error(
      `Message ${messageId} introuvable pour mise à jour call status`,
    );
  }

  console.log(
    `✅ Call status mis à jour: ${messageId} → ${updates.status || "update"}`,
  );
  return result;
};

// ===============================
// MIDDLEWARE (HOOKS)
// ===============================

// Avant sauvegarde - Générer métadonnées
messageSchema.pre("save", function (next) {
  // Générer checksum si nouveau message
  if (this.isNew) {
    this.metadata.technical.checksum = this.generateChecksum();
    this.metadata.technical.messageId = `msg_${Date.now()}_${this.senderId}`;
    this.metadata.redisMetadata.cacheKey = this.generateCacheKey();
  }

  next();
});

// Après sauvegarde - Publier événement Kafka
messageSchema.post("save", async function (doc, next) {
  try {
    // Publier événement selon l'action
    if (doc.isNew) {
      // await doc.publishKafkaEvent("MESSAGE_SENT"); // Désactiver temporairement
      console.log(`📤 Message créé: ${doc._id}`);
    } else {
      // await doc.publishKafkaEvent("MESSAGE_UPDATED"); // Désactiver temporairement
      console.log(`🔄 Message mis à jour: ${doc._id}`);
    }

    // Invalider les caches
    await doc.invalidateCache();
  } catch (error) {
    console.warn("⚠️ Erreur post-save message:", error.message);
  }

  next();
});

// Après suppression - Publier événement
messageSchema.post("findOneAndDelete", async function (doc) {
  if (doc) {
    try {
      await doc.publishKafkaEvent("MESSAGE_DELETED");
      await doc.invalidateCache();
    } catch (error) {
      console.warn("⚠️ Erreur post-delete message:", error.message);
    }
  }
});

// ===============================
// MÉTHODES VIRTUELLES
// ===============================

// Indicateur de message lu
messageSchema.virtual("isRead").get(function () {
  return this.status === "read";
});

// Indicateur de message livré
messageSchema.virtual("isDelivered").get(function () {
  return ["delivered", "read"].includes(this.status);
});

// Temps depuis l'envoi
messageSchema.virtual("timeSinceCreated").get(function () {
  return Date.now() - this.createdAt.getTime();
});

// Configuration de transformation JSON
messageSchema.set("toJSON", {
  virtuals: true,
  transform: function (doc, ret) {
    delete ret.__v;
    delete ret.metadata.technical.checksum; // Masquer le checksum
    return ret;
  },
});

const Message = mongoose.model("Message", messageSchema);

module.exports = Message;
