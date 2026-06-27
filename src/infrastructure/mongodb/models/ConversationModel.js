const mongoose = require("mongoose");
const { Schema } = mongoose;

// ✅ SCHÉMA AUDIT LOG AVEC ENUM ÉTENDU
const auditLogEntrySchema = new Schema(
  {
    action: {
      type: String,
      enum: [
        // ✅ ACTIONS STANDARDS
        "CREATED",
        "UPDATED",
        "DELETED",

        // ✅ ACTIONS PARTICIPANTS
        "PARTICIPANT_ADDED",
        "PARTICIPANT_REMOVED",
        "PARTICIPANT_INVITED",
        "PARTICIPANT_LEFT",

        // ✅ ACTIONS GESTION
        "ARCHIVED",
        "UNARCHIVED",
        "MUTED",
        "UNMUTED",
        "PINNED",
        "UNPINNED",

        // ✅ ACTIONS AUTOMATIQUES
        "AUTO_CREATED", // ✅ AJOUTÉ
        "AUTO_ARCHIVED", // ✅ AJOUTÉ
        "AUTO_DELETED", // ✅ AJOUTÉ
        "AUTO_PARTICIPANT_REMOVED", // ✅ AJOUTÉ

        // ✅ ACTIONS MESSAGES
        "MESSAGE_SENT",
        "MESSAGE_DELETED",
        "MESSAGE_EDITED",

        // ✅ ACTIONS STATUT
        "STATUS_CHANGED",
        "SETTINGS_UPDATED",
        "PERMISSIONS_CHANGED",
      ],
      required: true,
    },
    userId: {
      type: String,
      required: true,
    },
    timestamp: {
      type: Date,
      default: Date.now,
      required: true,
    },
    details: {
      type: Schema.Types.Mixed,
      default: {},
    },
    metadata: {
      type: Schema.Types.Mixed,
      default: {},
    },
  },
  {
    _id: true,
    timestamps: false,
  },
);

// ✅ SCHÉMA USER METADATA
const userMetadataSchema = new Schema(
  {
    userId: {
      type: String,
      required: true,
    },
    unreadCount: {
      type: Number,
      default: 0,
    },
    lastReadAt: {
      type: Date,
      default: null,
    },
    isMuted: {
      type: Boolean,
      default: false,
    },
    isPinned: {
      type: Boolean,
      default: false,
    },
    customName: {
      type: String,
      default: null,
    },
    notificationSettings: {
      enabled: { type: Boolean, default: true },
      sound: { type: Boolean, default: true },
      vibration: { type: Boolean, default: true },
    },
    // ✅ LAST SEEN - Mise à jour lors de la déconnexion
    lastSeen: {
      type: Date,
      default: null,
    },
    // ✅ AJOUT DES INFOS UTILISATEUR
    nom: {
      type: String,
      default: null,
    },
    prenom: {
      type: String,
      default: null,
    },
    sexe: {
      type: String,
      enum: ["M", "F", "O", null],
      default: null,
    },
    departement: {
      type: String,
      default: null,
    },
    ministere: {
      type: String,
      default: null,
    },
    avatar: {
      type: String,
      default: null,
    },
  },
  {
    _id: false,
    timestamps: false,
  },
);

// ✅ SCHÉMA LAST MESSAGE
const lastMessageSchema = new Schema(
  {
    _id: {
      type: Schema.Types.ObjectId,
      required: true,
    },
    content: {
      type: String,
      maxlength: 200, // Tronqué pour performance
      required: true,
    },
    type: {
      type: String,
      enum: ["TEXT", "IMAGE", "VIDEO", "AUDIO", "FILE", "SYSTEM", "CALL"],
      default: "TEXT",
    },
    senderId: {
      type: String,
      required: true,
    },
    senderName: {
      type: String,
      default: null,
    },
    status: {
      type: String,
      enum: ["SENDING", "SENT", "DELIVERED", "READ", "FAILED"],
      default: "SENT",
    },
    timestamp: {
      type: Date,
      required: true,
    },
  },
  {
    _id: false,
    timestamps: false,
  },
);

// ✅ SCHÉMA PRINCIPAL CONVERSATION
const conversationSchema = new Schema(
  {
    name: {
      type: String,
      required: true,
      trim: true,
      maxlength: 100,
    },
    type: {
      type: String,
      enum: ["PRIVATE", "GROUP", "CHANNEL", "SUPPORT", "BROADCAST"],
      default: "PRIVATE",
      required: true,
    },
    description: {
      type: String,
      maxlength: 500,
      default: null,
    },
    // Ajout du code structure pour les groupes auto/structurés
    code_structure: {
      type: String,
      default: null,
      index: true,
      description: "Code structure institutionnelle (groupes auto, etc.)",
    },
    participants: [
      {
        type: String,
        required: false, // Permettre groupes vides
      },
    ],
    createdBy: {
      type: String,
      required: true,
    },
    totalRecipients: {
      type: Number,
      default: 1,
      required: true,
    },
    isActive: {
      type: Boolean,
      default: true,
    },

    // ✅ ARCHIVAGE PAR UTILISATEUR (par défaut exclues des listes normales)
    // Tableau simple de userId — cohérent avec l'entité domaine Conversation.js
    archivedBy: [
      {
        type: String, // userId
      },
    ],

    // ✅ DATE D'ARCHIVAGE PAR UTILISATEUR { userId: Date }
    archivedAt: {
      type: Map,
      of: Date,
      default: {},
    },

    // ✅ MÉTADONNÉES UTILISATEUR
    userMetadata: [userMetadataSchema],

    // ✅ DERNIER MESSAGE
    lastMessage: {
      type: lastMessageSchema,
      default: null,
    },
    lastMessageAt: {
      type: Date,
      default: null,
    },

    // ✅ PARAMÈTRES
    settings: {
      allowInvites: { type: Boolean, default: true },
      isPublic: { type: Boolean, default: false },
      maxParticipants: { type: Number, default: 200 },
      messageRetention: { type: Number, default: 0 }, // 0 = illimité
      autoDeleteAfter: { type: Number, default: 0 }, // 0 = jamais
      // Pour BROADCAST
      broadcastAdmins: [{ type: String }], // IDs des admins/envoyeurs
      broadcastRecipients: [{ type: String }], // IDs des destinataires
    },

    // ✅ MÉTADONNÉES BROADCAST (uniquement pour type === "BROADCAST")
    // Stocke le lien entre la conversation broadcast et les conversations privées créées par destinataire
    broadcastMetadata: {
      // Mapping destinataire → conversation privée
      privateConversations: [
        {
          recipientId: { type: String, required: true },
          conversationId: {
            type: Schema.Types.ObjectId,
            ref: "Conversation",
            required: true,
          },
          _id: false,
        },
      ],
      // Statistiques de diffusion
      totalRecipients: { type: Number, default: 0 },
      totalMessagesSent: { type: Number, default: 0 },
      lastBroadcastAt: { type: Date, default: null },
    },

    // ✅ MÉTADONNÉES ET AUDIT - AVEC ENUM CORRIGÉ
    metadata: {
      autoCreated: { type: Boolean, default: false },
      createdFrom: { type: String, default: null },
      version: { type: Number, default: 1 },
      tags: [{ type: String }],

      // ✅ AUDIT LOG AVEC ENUM ÉTENDU
      auditLog: {
        type: [auditLogEntrySchema],
        default: [],
      },

      // Statistiques
      stats: {
        totalMessages: { type: Number, default: 0 },
        totalFiles: { type: Number, default: 0 },
        totalParticipants: { type: Number, default: 0 },
        lastActivity: { type: Date, default: null },
      },
    },

    // ✅ INTÉGRATIONS EXTERNES
    integrations: {
      webhooks: [
        {
          url: String,
          events: [String],
          isActive: { type: Boolean, default: true },
        },
      ],
      bots: [
        {
          botId: String,
          permissions: [String],
          isActive: { type: Boolean, default: true },
        },
      ],
    },
  },
  {
    timestamps: true,
    collection: "conversations",
    // ✅ OPTIONS POUR GÉRER LES OBJETS MIXTES
    minimize: false, // ✅ NE PAS SUPPRIMER LES OBJETS VIDES
    strict: false, // ✅ PERMETTRE LES CHAMPS NON DÉFINIS DANS LE SCHÉMA
  },
);

// ✅ INDEX COMPOSITES
conversationSchema.index({ participants: 1, lastMessageAt: -1 });
conversationSchema.index({ participants: 1, type: 1 });
conversationSchema.index({ createdBy: 1, createdAt: -1 });
conversationSchema.index({ "userMetadata.userId": 1 });
conversationSchema.index({ isActive: 1, archivedBy: 1 });
conversationSchema.index({ archivedBy: 1, lastMessageAt: -1 }); // ✅ Requêtes getArchivedConversations

// ✅ MÉTHODES VIRTUELLES
conversationSchema.virtual("participantCount").get(function () {
  return this.participants ? this.participants.length : 0;
});

conversationSchema.virtual("isPrivate").get(function () {
  return this.type === "PRIVATE";
});

conversationSchema.virtual("isGroup").get(function () {
  return this.type === "GROUP";
});

// ✅ MÉTHODES D'INSTANCE
conversationSchema.methods.addParticipant = function (userId, addedBy = null) {
  if (!this.participants.includes(userId)) {
    this.participants.push(userId);

    // Ajouter métadonnées utilisateur
    this.userMetadata.push({
      userId: userId,
      unreadCount: 0,
      lastReadAt: null,
      isMuted: false,
      isPinned: false,
    });

    // ✅ AUDIT LOG AVEC ACTION VALIDE
    this.metadata.auditLog.push({
      action: "PARTICIPANT_ADDED",
      userId: addedBy || userId,
      timestamp: new Date(),
      details: {
        participantId: userId,
        addedBy: addedBy,
      },
    });

    this.metadata.stats.totalParticipants = this.participants.length;
    this.updatedAt = new Date();
  }
};

conversationSchema.methods.removeParticipant = function (
  userId,
  removedBy = null,
) {
  const index = this.participants.indexOf(userId);
  if (index > -1) {
    this.participants.splice(index, 1);

    // Supprimer métadonnées utilisateur
    this.userMetadata = this.userMetadata.filter(
      (meta) => meta.userId !== userId,
    );

    // ✅ AUDIT LOG AVEC ACTION VALIDE
    this.metadata.auditLog.push({
      action: "PARTICIPANT_REMOVED",
      userId: removedBy || userId,
      timestamp: new Date(),
      details: {
        participantId: userId,
        removedBy: removedBy,
      },
    });

    this.metadata.stats.totalParticipants = this.participants.length;
    this.updatedAt = new Date();
  }
};

conversationSchema.methods.updateLastMessage = function (messageData) {
  this.lastMessage = {
    _id: messageData._id || messageData.id,
    content: messageData.content.substring(0, 200),
    type: messageData.type || "TEXT",
    senderId: messageData.senderId,
    senderName: messageData.senderName || null,
    status: messageData.status || "SENT",
    timestamp: messageData.timestamp || new Date(),
  };

  this.lastMessageAt = this.lastMessage.timestamp;
  this.metadata.stats.lastActivity = new Date();
  this.metadata.stats.totalMessages += 1;
  this.updatedAt = new Date();
};

/**
 * ✅ METTRE À JOUR UNIQUEMENT LE STATUT DU DERNIER MESSAGE
 * À appeler quand le statut du dernier message change (DELIVERED, READ)
 */
conversationSchema.methods.updateLastMessageStatus = function (newStatus) {
  if (this.lastMessage && this.lastMessage._id) {
    this.lastMessage.status = newStatus;
    this.updatedAt = new Date();
    return true;
  }
  return false;
};

conversationSchema.methods.getUserMetadata = function (userId) {
  return (
    this.userMetadata.find((meta) => meta.userId === userId) || {
      userId: userId,
      unreadCount: 0,
      lastReadAt: null,
      isMuted: false,
      isPinned: false,
    }
  );
};

conversationSchema.methods.updateUserMetadata = function (userId, updates) {
  let userMeta = this.userMetadata.find((meta) => meta.userId === userId);

  if (!userMeta) {
    userMeta = {
      userId: userId,
      unreadCount: 0,
      lastReadAt: null,
      isMuted: false,
      isPinned: false,
    };
    this.userMetadata.push(userMeta);
  }

  Object.assign(userMeta, updates);
  this.updatedAt = new Date();
};

/**
 * ✅ ARCHIVER LA CONVERSATION POUR UN UTILISATEUR
 * L'archivage est individuel : chaque userId a son propre état
 */
conversationSchema.methods.archiveForUser = function (userId) {
  if (!this.archivedBy.includes(userId)) {
    this.archivedBy.push(userId);
    if (!this.archivedAt) this.archivedAt = new Map();
    this.archivedAt.set(userId, new Date());
    this.markModified("archivedBy");
    this.markModified("archivedAt");

    this.metadata.auditLog.push({
      action: "ARCHIVED",
      userId,
      timestamp: new Date(),
      details: { archivedBy: userId },
    });
  }
  return this;
};

/**
 * ✅ DÉSARCHIVER LA CONVERSATION POUR UN UTILISATEUR
 */
conversationSchema.methods.unarchiveForUser = function (userId) {
  const idx = this.archivedBy.indexOf(userId);
  if (idx > -1) {
    this.archivedBy.splice(idx, 1);
    if (this.archivedAt) this.archivedAt.delete(userId);
    this.markModified("archivedBy");
    this.markModified("archivedAt");

    this.metadata.auditLog.push({
      action: "UNARCHIVED",
      userId,
      timestamp: new Date(),
      details: { unarchivedBy: userId },
    });
  }
  return this;
};

/**
 * ✅ VÉRIFIER SI LA CONVERSATION EST ARCHIVÉE POUR UN UTILISATEUR
 */
conversationSchema.methods.isArchivedForUser = function (userId) {
  return this.archivedBy.includes(userId);
};

// ✅ MÉTHODES STATIQUES
conversationSchema.statics.findByParticipant = function (userId, options = {}) {
  const {
    includeArchived = false,
    type = null,
    limit = 50,
    page = 1,
  } = options;

  const filter = {
    participants: {
      $in: [
        userId,
        typeof userId === "string" ? Number(userId) : String(userId),
      ],
    },
    isActive: true,
  };

  // ✅ Exclure les conversations archivées par cet utilisateur
  if (!includeArchived) {
    filter.archivedBy = { $nin: [userId] };
  }

  if (type) {
    filter.type = type;
  }

  const skip = (page - 1) * limit;

  return this.find(filter)
    .sort({ lastMessageAt: -1 })
    .skip(skip)
    .limit(limit)
    .lean();
};

/**
 * ✅ RÉCUPÉRER LES CONVERSATIONS ARCHIVÉES D'UN UTILISATEUR
 */
conversationSchema.statics.findArchivedByParticipant = function (
  userId,
  options = {},
) {
  const { type = null, limit = 50, page = 1 } = options;

  const filter = {
    participants: {
      $in: [
        userId,
        typeof userId === "string" ? Number(userId) : String(userId),
      ],
    },
    isActive: true,
    archivedBy: userId, // Seulement celles archivées par cet utilisateur
  };

  if (type) filter.type = type;

  const skip = (page - 1) * limit;

  return this.find(filter)
    .sort({ [`archivedAt.${userId}`]: -1, lastMessageAt: -1 })
    .skip(skip)
    .limit(limit)
    .lean();
};

conversationSchema.statics.findPrivateConversation = function (
  participant1,
  participant2,
) {
  return this.findOne({
    type: "PRIVATE",
    participants: {
      $all: [participant1, participant2],
      $size: 2,
    },
    isActive: true,
  });
};

// ✅ HOOKS PRE/POST
conversationSchema.pre("save", function (next) {
  try {
    // ✅ INITIALISER LES COMPTEURS NON-LUS SI NÉCESSAIRE
    if (typeof this.initializeUnreadCounts === "function") {
      this.initializeUnreadCounts();
    } else {
      // ✅ Fallback manuel si la méthode n'existe pas
      if (!this.unreadCounts) {
        this.unreadCounts = {};
      }
      if (this.participants && Array.isArray(this.participants)) {
        this.participants.forEach((participantId) => {
          if (!(participantId in this.unreadCounts)) {
            this.unreadCounts[participantId] = 0;
          }
        });
        this.markModified("unreadCounts");
      }
    }

    // ✅ VALIDER ET NETTOYER LES COMPTEURS - UTILISER 'this' AU LIEU DE 'doc'
    if (typeof this.validateAndCleanUnreadCounts === "function") {
      this.validateAndCleanUnreadCounts();
    } else {
      // ✅ Fallback manuel pour la validation
      if (!this.unreadCounts || typeof this.unreadCounts !== "object") {
        this.unreadCounts = {};
      }

      // Nettoyer les valeurs invalides
      for (const [userId, count] of Object.entries(this.unreadCounts)) {
        if (typeof count !== "number" || count < 0 || isNaN(count)) {
          this.unreadCounts[userId] = 0;
        }
      }

      this.markModified("unreadCounts");
    }

    // ✅ METTRE À JOUR LES STATISTIQUES
    if (this.isNew) {
      if (!this.metadata) {
        this.metadata = { stats: {} };
      }
      if (!this.metadata.stats) {
        this.metadata.stats = {};
      }
      this.metadata.stats.totalParticipants = this.participants?.length || 0;
      this.metadata.stats.lastActivity = new Date();
    }

    // ✅ VALIDER LE NOMBRE DE PARTICIPANTS SELON LE TYPE
    if (this.type === "PRIVATE" && this.participants.length > 2) {
      return next(
        new Error(
          "Une conversation privée ne peut avoir que 2 participants maximum",
        ),
      );
    }

    if (this.participants.length > (this.settings?.maxParticipants || 200)) {
      return next(new Error(`Nombre maximum de participants dépassé`));
    }

    // ✅ VALIDATION DES CHAMPS REQUIS
    if (!this.name || this.name.trim() === "") {
      return next(new Error("Le nom de la conversation est requis"));
    }

    if (!this.participants || this.participants.length === 0) {
      // Autoriser les groupes vides pour les conversations auto-créées
      const autoCreated = (this.metadata && this.metadata.autoCreated) || false;
      const createdBySystem = this.createdBy === "SYSTEM";
      if (!autoCreated && !createdBySystem) {
        return next(new Error("Au moins un participant est requis"));
      }

      // Normaliser participants en tableau vide si nécessaire
      this.participants = this.participants || [];
    }

    if (!this.createdBy) {
      // Pour les groupes auto-créés, attribuer SYSTEM si absent
      if (this.metadata && this.metadata.autoCreated) {
        this.createdBy = "SYSTEM";
      } else {
        return next(new Error("Le créateur de la conversation est requis"));
      }
    }

    console.log(
      `📝 Validation pre-save réussie pour conversation: ${this._id}`,
    );
    next();
  } catch (error) {
    console.error(`❌ Erreur pre-save conversation ${this._id}:`, {
      error: error.message,
      stack: error.stack,
    });
    next(error);
  }
});

conversationSchema.post("save", function (doc) {
  try {
    // Log de création/modification avec plus de détails
    const stats =
      typeof doc.getUnreadCountsStats === "function"
        ? doc.getUnreadCountsStats()
        : {
            totalUsers: 0,
            usersWithUnread: 0,
            totalUnreadMessages: 0,
            averageUnreadPerUser: 0,
          };

    console.log(`📝 Conversation sauvegardée: ${doc._id} (${doc.type})`, {
      participants: doc.participants?.length || 0,
      unreadStats: stats,
      hasMetadata: !!doc.metadata,
      isNew: !!doc.isNew,
    });
  } catch (error) {
    console.warn("⚠️ Erreur post-save conversation:", error.message);
  }
});

// ✅ HOOK POST-INIT CORRIGÉ
conversationSchema.post("init", function (doc) {
  try {
    // Valider et nettoyer les données chargées depuis la DB
    if (doc && typeof doc.validateAndCleanUnreadCounts === "function") {
      doc.validateAndCleanUnreadCounts();
    }
  } catch (error) {
    console.warn("⚠️ Erreur post-init conversation:", error.message);
  }
});

const Conversation = mongoose.model("Conversation", conversationSchema);

module.exports = Conversation;
