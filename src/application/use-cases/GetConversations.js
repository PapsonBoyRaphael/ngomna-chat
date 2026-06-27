class GetConversations {
  constructor(
    conversationRepository,
    messageRepository,
    cacheService = null,
    onlineUserManager = null,
  ) {
    this.conversationRepository = conversationRepository;
    this.messageRepository = messageRepository;
    this.cacheService = cacheService;
    this.onlineUserManager = onlineUserManager;
  }

  // ✅ HELPER: Extract unread count from userMetadata (authoritative source)
  _getUnreadCountFromUserMetadata(conversation, userId) {
    if (Array.isArray(conversation.userMetadata)) {
      const userMeta = conversation.userMetadata.find(
        (meta) => meta.userId === userId,
      );
      return userMeta?.unreadCount || 0;
    }
    // Fallback to legacy unreadCounts if userMetadata unavailable
    return conversation.unreadCounts?.[userId] || 0;
  }

  async execute(userId, options = {}) {
    const startTime = Date.now();

    const {
      page = 1,
      limit = 20,
      cursor = null,
      direction = "newer",
      includeArchived = false,
      useCache = true,
      userDepartement = null,
      userMinistere = null,
    } = options;

    try {
      console.log(
        `🔍 GetConversations: userId=${userId}, page=${page}, limit=${limit}, cursor=${cursor}, useCache=${useCache}`,
      );

      // ✅ APPEL REPOSITORY avec cursor ET cache
      const result = await this.conversationRepository.findByParticipant(
        userId,
        {
          page: parseInt(page),
          limit: parseInt(limit),
          cursor,
          direction,
          includeArchived,
          useCache,
        },
      );

      if (!result || !Array.isArray(result.conversations)) {
        throw new Error("Format de données invalide depuis le repository");
      }

      // ✅ EXCLURE LES BROADCAST POUR LES NON-CRÉATEURS
      // Un broadcast n'est visible que par son créateur
      const allConversations = result.conversations || [];
      const conversations = allConversations.filter((c) => {
        if (c.type !== "BROADCAST") return true;
        return String(c.createdBy) === String(userId);
      });
      const totalCount =
        result.totalCount || result.pagination?.totalCount || 0;

      console.log(
        `📋 ${
          conversations.length
        } conversations trouvées sur ${totalCount} total pour la page ${page} (${
          result.fromCache ? "cache" : "MongoDB"
        })`,
      );

      // Trier par dernière activité
      const sortedConversations = conversations.sort(
        (a, b) =>
          new Date(b.lastMessageAt || b.updatedAt) -
          new Date(a.lastMessageAt || a.updatedAt),
      );

      // ✅ ENRICHIR LES CONVERSATIONS AVEC LE STATUT DE PRÉSENCE DES PARTICIPANTS
      const enrichedConversations = await this._enrichConversationsWithPresence(
        sortedConversations,
        userId,
      );

      // ✅ SÉPARER LES CONVERSATIONS PAR CATÉGORIE
      // Conversations non lues
      const unreadConversations = enrichedConversations.filter(
        (c) => this._getUnreadCountFromUserMetadata(c, userId) > 0,
      );

      // Conversations de groupe
      const groupConversations = enrichedConversations.filter(
        (c) => c.type === "GROUP",
      );

      // Conversations de diffusion
      const broadcastConversations = enrichedConversations.filter(
        (c) => c.type === "BROADCAST",
      );

      // Conversations privées
      const privateConversations = enrichedConversations.filter(
        (c) => c.type === "PRIVATE",
      );

      const departementConversations = [];

      // ✅ CALCULS DE PAGINATION CORRECTS
      const totalPages = Math.ceil(totalCount / limit);
      const hasNext = page < totalPages;
      const hasPrevious = page > 1;

      const finalResult = {
        conversations: enrichedConversations,

        // ✅ CONVERSATIONS PAR CATÉGORIE
        categorized: {
          unread: unreadConversations,
          groups: groupConversations,
          broadcasts: broadcastConversations,
          departement: departementConversations,
          private: privateConversations,
        },

        // ✅ STATISTIQUES PAR CATÉGORIE
        stats: {
          total: enrichedConversations.length,
          unread: unreadConversations.length,
          groups: groupConversations.length,
          broadcasts: broadcastConversations.length,
          departement: 0,
          private: privateConversations.length,
          unreadMessagesInGroups: groupConversations.reduce(
            (sum, c) => sum + this._getUnreadCountFromUserMetadata(c, userId),
            0,
          ),
          unreadMessagesInBroadcasts: broadcastConversations.reduce(
            (sum, c) => sum + this._getUnreadCountFromUserMetadata(c, userId),
            0,
          ),
          unreadMessagesInDepartement: 0,
          unreadMessagesInPrivate: privateConversations.reduce(
            (sum, c) => sum + this._getUnreadCountFromUserMetadata(c, userId),
            0,
          ),
        },

        // ✅ CONTEXTE UTILISATEUR
        userContext: {
          userId,
          departement: userDepartement,
          ministere: userMinistere,
        },

        pagination: {
          currentPage: parseInt(page),
          totalPages: totalPages,
          totalCount: totalCount,
          hasNext: hasNext,
          hasPrevious: hasPrevious,
          limit: parseInt(limit),
          offset: (page - 1) * limit,
          nextPage: hasNext ? parseInt(page) + 1 : null,
          previousPage: hasPrevious ? parseInt(page) - 1 : null,
        },
        totalCount: totalCount,
        unreadConversations: unreadConversations.length,
        totalUnreadMessages: enrichedConversations.reduce(
          (sum, c) => sum + this._getUnreadCountFromUserMetadata(c, userId),
          0,
        ),
        fromCache: result.fromCache || false,
        nextCursor: result.nextCursor || null,
        hasMore: result.hasMore || false,
        processingTime: Date.now() - startTime,
      };

      console.log(
        `✅ Page ${page}: ${
          finalResult.conversations.length
        } conversations récupérées (${finalResult.processingTime}ms) - ${
          result.fromCache ? "CACHE" : "DB"
        }`,
      );
      console.log(
        `📊 Catégories: ${finalResult.stats.unread} non-lues, ${finalResult.stats.groups} groupes, ${finalResult.stats.broadcasts} broadcasts, ${finalResult.stats.private} privées`,
      );

      return finalResult;
    } catch (error) {
      const processingTime = Date.now() - startTime;
      console.error(
        `❌ Erreur GetConversations: ${error.message} (${processingTime}ms)`,
      );
      throw error;
    }
  }

  /**
   * ✅ ENRICHIR LES CONVERSATIONS AVEC LE STATUT DE PRÉSENCE DES PARTICIPANTS
   */
  async _enrichConversationsWithPresence(conversations, currentUserId) {
    if (
      !this.onlineUserManager ||
      !conversations ||
      conversations.length === 0
    ) {
      return conversations;
    }

    try {
      // Collecter tous les participants uniques de toutes les conversations
      const allParticipantIds = new Set();

      for (const conv of conversations) {
        if (Array.isArray(conv.participants)) {
          conv.participants.forEach((p) => allParticipantIds.add(String(p)));
        }
      }

      // Récupérer le statut de présence de tous les participants en batch
      const presenceMap = new Map();

      for (const participantId of allParticipantIds) {
        try {
          const isOnline =
            await this.onlineUserManager.isUserOnline(participantId);

          let presenceData = null;

          if (isOnline) {
            // Utilisateur en ligne : récupérer depuis user_data
            const userData =
              await this.onlineUserManager.getUserData(participantId);

            presenceData = {
              isOnline: true,
              status: userData?.status || "online",
              lastActivity: userData?.lastActivity || null,
            };
          } else {
            // Utilisateur offline : récupérer depuis last_seen
            const lastSeenData =
              await this.onlineUserManager.getLastSeen(participantId);

            presenceData = {
              isOnline: false,
              status: "offline",
              lastActivity: lastSeenData?.lastActivity || null,
              disconnectedAt: lastSeenData?.disconnectedAt || null,
            };
          }

          presenceMap.set(String(participantId), presenceData);
        } catch (err) {
          // En cas d'erreur pour un participant, on met offline par défaut
          presenceMap.set(String(participantId), {
            isOnline: false,
            status: "offline",
            lastActivity: null,
          });
        }
      }

      // Enrichir chaque conversation avec les infos de présence
      return conversations.map((conv) => {
        // Enrichir userMetadata avec le statut de présence
        if (Array.isArray(conv.userMetadata)) {
          conv.userMetadata = conv.userMetadata.map((meta) => {
            const participantPresence = presenceMap.get(String(meta.userId));

            return {
              ...meta,
              presence: participantPresence || {
                isOnline: false,
                status: "offline",
                lastActivity: null,
              },
            };
          });
        }

        // Ajouter des statistiques de présence au niveau de la conversation
        const onlineParticipants =
          conv.participants?.filter(
            (p) => presenceMap.get(String(p))?.isOnline,
          ) || [];

        conv.presenceStats = {
          totalParticipants: conv.participants?.length || 0,
          onlineCount: onlineParticipants.length,
          offlineCount:
            (conv.participants?.length || 0) - onlineParticipants.length,
          onlineParticipants: onlineParticipants,
        };

        // Pour les conversations privées (1-à-1), déterminer si l'autre utilisateur est en ligne
        if (
          conv.type === "PRIVATE" &&
          Array.isArray(conv.participants) &&
          conv.participants.length === 2
        ) {
          const otherUserId = conv.participants.find(
            (p) => String(p) !== String(currentUserId),
          );
          if (otherUserId) {
            const otherUserPresence = presenceMap.get(String(otherUserId));
            conv.otherUserPresence = otherUserPresence || {
              isOnline: false,
              status: "offline",
              lastActivity: null,
            };
          }
        }

        return conv;
      });
    } catch (error) {
      console.error("❌ Erreur enrichissement présence:", error.message);
      // En cas d'erreur, retourner les conversations sans enrichissement
      return conversations;
    }
  }
}

module.exports = GetConversations;
