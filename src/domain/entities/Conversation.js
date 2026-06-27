class Conversation {
  constructor({
    _id,
    participants = [],
    type = "PRIVATE",
    name = null,
    description = null,
    avatar = null,
    lastMessage = null,
    lastMessageAt = null,
    unreadCounts = {},
    settings = {},
    metadata = {},
    createdAt,
    updatedAt,
    archivedBy = [],
    mutedBy = [],
    pinnedBy = [],
  }) {
    this._id = _id;
    this.participants = participants;
    this.type = type; // PRIVATE, GROUP, CHANNEL
    this.name = name;
    this.description = description;
    this.avatar = avatar;
    this.lastMessage = lastMessage;
    this.lastMessageAt = lastMessageAt;
    this.unreadCounts = unreadCounts; // { userId: count }
    this.settings = this.enrichSettings(settings);
    this.metadata = this.enrichMetadata(metadata);
    this.createdAt = createdAt || new Date();
    this.updatedAt = updatedAt || new Date();
    this.archivedBy = archivedBy; // Array des userId qui ont archivé
    this.mutedBy = mutedBy; // Array des userId qui ont mis en sourdine
    this.pinnedBy = pinnedBy; // Array des userId qui ont épinglé
  }

  // Enrichir les paramètres de conversation
  enrichSettings(settings) {
    return {
      // Paramètres de notifications
      notifications: {
        enabled: true,
        sound: true,
        vibration: true,
        preview: true,
        ...settings.notifications,
      },

      // Paramètres de confidentialité
      privacy: {
        readReceipts: true,
        lastSeen: true,
        profilePhoto: true,
        ...settings.privacy,
      },

      // Paramètres de groupe (si applicable)
      group: {
        allowMembersToAddOthers: true,
        allowMembersToEditInfo: false,
        allowMembersToSendMessages: true,
        ...settings.group,
      },

      // Paramètres de rétention
      retention: {
        autoDeleteAfterDays: null,
        backupEnabled: true,
        ...settings.retention,
      },

      ...settings,
    };
  }

  // Enrichir les métadonnées
  enrichMetadata(metadata) {
    return {
      // Métadonnées Kafka
      kafkaMetadata: {
        topic: "chat.conversations",
        partition: null,
        lastEventOffset: null,
        lastEventTimestamp: null,
      },

      // Métadonnées Redis
      redisMetadata: {
        cacheKey: this.generateCacheKey(),
        ttl: 1800, // 30 minutes
        cacheStrategy: "write-through",
        lastCached: null,
      },

      // Statistiques
      statistics: {
        totalMessages: 0,
        totalFiles: 0,
        totalImages: 0,
        totalVideos: 0,
        lastActivity: null,
        mostActiveUser: null,
        ...metadata.statistics,
      },

      // Métadonnées de performance
      performance: {
        avgResponseTime: null,
        peakConcurrentUsers: 0,
        lastPerformanceCheck: null,
      },

      // Historique des modifications
      auditLog: metadata.auditLog || [],

      ...metadata,
    };
  }

  // Générer une clé de cache
  generateCacheKey() {
    return `conversation:${this._id}`;
  }

  // Validation
  validate() {
    const errors = [];

    if (!this.participants || this.participants.length === 0) {
      errors.push("Une conversation doit avoir au moins un participant");
    }

    if (this.participants.length < 2 && this.type === "PRIVATE") {
      errors.push(
        "Une conversation privée doit avoir exactement 2 participants"
      );
    }

    if (
      this.type === "GROUP" &&
      (!this.name || this.name.trim().length === 0)
    ) {
      errors.push("Un groupe doit avoir un nom");
    }

    const validTypes = ["PRIVATE", "GROUP", "CHANNEL"];
    if (!validTypes.includes(this.type)) {
      errors.push(`Type doit être un de: ${validTypes.join(", ")}`);
    }

    if (errors.length > 0) {
      throw new Error(`Validation Conversation échouée: ${errors.join(", ")}`);
    }

    return true;
  }

  // Ajouter un participant
  addParticipant(userId) {
    if (!this.participants.includes(userId)) {
      this.participants.push(userId);
      this.unreadCounts[userId] = 0;
      this.updatedAt = new Date();

      // Ajouter à l'audit log
      this.metadata.auditLog.push({
        action: "PARTICIPANT_ADDED",
        userId,
        timestamp: new Date().toISOString(),
        metadata: { addedBy: "system" },
      });
    }
    return this;
  }

  // Supprimer un participant
  removeParticipant(userId) {
    this.participants = this.participants.filter((id) => id !== userId);
    delete this.unreadCounts[userId];
    this.archivedBy = this.archivedBy.filter((id) => id !== userId);
    this.mutedBy = this.mutedBy.filter((id) => id !== userId);
    this.pinnedBy = this.pinnedBy.filter((id) => id !== userId);
    this.updatedAt = new Date();

    // Ajouter à l'audit log
    this.metadata.auditLog.push({
      action: "PARTICIPANT_REMOVED",
      userId,
      timestamp: new Date().toISOString(),
      metadata: { removedBy: "system" },
    });

    return this;
  }

  // Mettre à jour le dernier message
  updateLastMessage(messageId, messageContent, messageType, senderId) {
    this.lastMessage = {
      _id: messageId,
      content: messageContent,
      type: messageType,
      senderId,
      timestamp: new Date().toISOString(),
    };
    this.lastMessageAt = new Date();
    this.updatedAt = new Date();

    // Incrémenter les compteurs non lus pour tous sauf l'expéditeur
    this.participants.forEach((participantId) => {
      if (participantId !== senderId) {
        this.unreadCounts[participantId] =
          (this.unreadCounts[participantId] || 0) + 1;
      }
    });

    // Mettre à jour les statistiques
    this.metadata.statistics.totalMessages++;
    if (["IMAGE", "VIDEO", "AUDIO", "FILE"].includes(messageType)) {
      this.metadata.statistics.totalFiles++;

      switch (messageType) {
        case "IMAGE":
          this.metadata.statistics.totalImages++;
          break;
        case "VIDEO":
          this.metadata.statistics.totalVideos++;
          break;
      }
    }

    this.metadata.statistics.lastActivity = new Date().toISOString();

    return this;
  }

  // Marquer les messages comme lus
  markAsRead(userId) {
    this.unreadCounts[userId] = 0;
    this.updatedAt = new Date();
    return this;
  }

  // Archiver pour un utilisateur
  archive(userId) {
    if (!this.archivedBy.includes(userId)) {
      this.archivedBy.push(userId);
      this.updatedAt = new Date();
    }
    return this;
  }

  // Désarchiver pour un utilisateur
  unarchive(userId) {
    this.archivedBy = this.archivedBy.filter((id) => id !== userId);
    this.updatedAt = new Date();
    return this;
  }

  // Mettre en sourdine pour un utilisateur
  mute(userId) {
    if (!this.mutedBy.includes(userId)) {
      this.mutedBy.push(userId);
      this.updatedAt = new Date();
    }
    return this;
  }

  // Enlever la sourdine pour un utilisateur
  unmute(userId) {
    this.mutedBy = this.mutedBy.filter((id) => id !== userId);
    this.updatedAt = new Date();
    return this;
  }

  // Épingler pour un utilisateur
  pin(userId) {
    if (!this.pinnedBy.includes(userId)) {
      this.pinnedBy.push(userId);
      this.updatedAt = new Date();
    }
    return this;
  }

  // Désépingler pour un utilisateur
  unpin(userId) {
    this.pinnedBy = this.pinnedBy.filter((id) => id !== userId);
    this.updatedAt = new Date();
    return this;
  }

  // Obtenir le nombre total de messages non lus
  getTotalUnreadCount() {
    return Object.values(this.unreadCounts).reduce(
      (sum, count) => sum + count,
      0
    );
  }

  // Vérifier si un utilisateur est participant
  hasParticipant(userId) {
    return this.participants.includes(userId);
  }

  // Obtenir les métadonnées pour un utilisateur spécifique
  getMetadataForUser(userId) {
    return {
      unreadCount: this.unreadCounts[userId] || 0,
      isArchived: this.archivedBy.includes(userId),
      isMuted: this.mutedBy.includes(userId),
      isPinned: this.pinnedBy.includes(userId),
      isParticipant: this.hasParticipant(userId),
    };
  }

  // Conversion vers objet simple
  toObject() {
    return {
      _id: this._id,
      type: this.type,
      name: this.name,
      description: this.description,
      participants: this.participants,
      createdBy: this.createdBy,
      lastMessage: this.lastMessage,
      lastActivity: this.lastActivity,
      unreadCounts: this.unreadCounts,
      archivedBy: this.archivedBy,
      mutedBy: this.mutedBy,
      pinnedBy: this.pinnedBy,
      settings: this.settings,
      metadata: this.metadata,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
    };
  }

  // Méthode statique pour créer depuis un objet
  static fromObject(obj) {
    return new Conversation(obj);
  }

  // Sérialisation pour Kafka
  toKafkaPayload() {
    return {
      conversationId: this._id,
      type: this.type,
      name: this.name,
      participants: this.participants,
      createdBy: this.createdBy,
      lastActivity: this.lastActivity,
      participantCount: this.participants.length,
      timestamp: this.updatedAt,
      metadata: {
        kafkaMetadata: {
          topic: "chat.conversations",
          serializedAt: new Date().toISOString(),
        },
      },
    };
  }

  // Sérialisation pour Redis
  toRedisPayload() {
    return {
      _id: this._id,
      type: this.type,
      name: this.name,
      participants: this.participants,
      createdBy: this.createdBy,
      lastMessage: this.lastMessage,
      lastActivity: this.lastActivity,
      unreadCounts: this.unreadCounts,
      settings: this.settings,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      metadata: {
        ...this.metadata,
        redisMetadata: {
          cacheKey: `conversation:${this._id}`,
          cachedAt: new Date().toISOString(),
        },
      },
    };
  }

  // Créer une conversation privée
  static createPrivateConversation(userId1, userId2) {
    return new Conversation({
      participants: [userId1, userId2],
      type: "PRIVATE",
      unreadCounts: {
        [userId1]: 0,
        [userId2]: 0,
      },
    });
  }

  // Créer un groupe
  static createGroup(name, adminId, members = []) {
    const allMembers = [adminId, ...members.filter((id) => id !== adminId)];
    const unreadCounts = {};
    allMembers.forEach((memberId) => {
      unreadCounts[memberId] = 0;
    });

    return new Conversation({
      participants: allMembers,
      type: "GROUP",
      name,
      unreadCounts,
      settings: {
        group: {
          admins: [adminId],
          allowMembersToAddOthers: true,
          allowMembersToEditInfo: false,
        },
      },
    });
  }

  // Dans handleDeleteMessage ou dans le repository
  async deleteMessage(messageId, userId) {
    // 1. Supprimer le message
    const deletedMessage = await this.messageRepository.deleteById(messageId);

    // 2. Vérifier si c'était le lastMessage de la conversation
    const conversation = await this.conversationRepository.findById(
      deletedMessage.conversationId
    );

    if (conversation.lastMessage?._id?.toString() === messageId) {
      // 3. Récupérer le message précédent non supprimé
      const previousMessage =
        await this.messageRepository.findLastNonDeletedMessage(
          deletedMessage.conversationId
        );

      // 4. Mettre à jour la conversation
      if (previousMessage) {
        await this.conversationRepository.updateLastMessage(
          deletedMessage.conversationId,
          previousMessage
        );
      } else {
        // Aucun message restant
        await this.conversationRepository.clearLastMessage(
          deletedMessage.conversationId
        );
      }
    }

    return deletedMessage;
  }
}

module.exports = Conversation;
