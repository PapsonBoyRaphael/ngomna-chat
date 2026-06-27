/**
 * GetArchivedConversations — Use Case
 *
 * Récupère la liste paginée des conversations archivées par un utilisateur.
 * Inspiré de GetConversations.js — même structure de retour, même enrichissement.
 */
class GetArchivedConversations {
  constructor(conversationRepository, onlineUserManager = null) {
    this.conversationRepository = conversationRepository;
    this.onlineUserManager = onlineUserManager;
  }

  // ✅ Helper : unread count depuis userMetadata (source de vérité)
  _getUnreadCountFromUserMetadata(conversation, userId) {
    if (Array.isArray(conversation.userMetadata)) {
      const userMeta = conversation.userMetadata.find(
        (meta) => meta.userId === userId,
      );
      return userMeta?.unreadCount || 0;
    }
    return conversation.unreadCounts?.[userId] || 0;
  }

  /**
   * @param {string} userId
   * @param {object} options - { page, limit, type }
   */
  async execute(userId, options = {}) {
    const startTime = Date.now();

    const { page = 1, limit = 20, type = null } = options;

    console.log(
      `🗄️  GetArchivedConversations: userId=${userId}, page=${page}, limit=${limit}`,
    );

    try {
      const result = await this.conversationRepository.findArchivedByUser(
        userId,
        { page: parseInt(page), limit: parseInt(limit), type },
      );

      if (!result || !Array.isArray(result.conversations)) {
        throw new Error("Format de données invalide depuis le repository");
      }

      const conversations = result.conversations;
      const totalCount = result.totalCount || 0;

      console.log(
        `📋 ${conversations.length} conversation(s) archivée(s) trouvée(s) pour ${userId}`,
      );

      // Trier par date d'archivage descendante
      const sorted = conversations.sort(
        (a, b) =>
          new Date(b.updatedAt || b.lastMessageAt) -
          new Date(a.updatedAt || a.lastMessageAt),
      );

      // Enrichir avec le statut de présence (même pattern que GetConversations)
      const enriched = await this._enrichConversationsWithPresence(
        sorted,
        userId,
      );

      // Catégoriser
      const privateConvs = enriched.filter((c) => c.type === "PRIVATE");
      const groupConvs = enriched.filter((c) => c.type === "GROUP");
      const channelConvs = enriched.filter((c) => c.type === "CHANNEL");
      const otherConvs = enriched.filter(
        (c) => !["PRIVATE", "GROUP", "CHANNEL"].includes(c.type),
      );

      const totalPages = Math.ceil(totalCount / limit);

      const finalResult = {
        conversations: enriched,

        categorized: {
          private: privateConvs,
          groups: groupConvs,
          channels: channelConvs,
          other: otherConvs,
        },

        stats: {
          total: enriched.length,
          private: privateConvs.length,
          groups: groupConvs.length,
          channels: channelConvs.length,
        },

        pagination: {
          currentPage: parseInt(page),
          totalPages,
          totalCount,
          hasNext: page < totalPages,
          hasPrevious: parseInt(page) > 1,
          limit: parseInt(limit),
          nextPage: page < totalPages ? parseInt(page) + 1 : null,
          previousPage: parseInt(page) > 1 ? parseInt(page) - 1 : null,
        },

        processingTime: Date.now() - startTime,
      };

      console.log(
        `✅ ${enriched.length} conversation(s) archivée(s) retournée(s) ` +
          `(${finalResult.processingTime}ms)`,
      );

      return finalResult;
    } catch (error) {
      console.error(
        `❌ Erreur GetArchivedConversations: ${error.message} (${Date.now() - startTime}ms)`,
      );
      throw error;
    }
  }

  /**
   * Enrichissement présence — même logique que GetConversations
   */
  async _enrichConversationsWithPresence(conversations, currentUserId) {
    if (!this.onlineUserManager || conversations.length === 0) {
      return conversations;
    }

    try {
      const allParticipantIds = new Set();
      for (const conv of conversations) {
        if (Array.isArray(conv.participants)) {
          conv.participants.forEach((p) => allParticipantIds.add(String(p)));
        }
      }

      const presenceMap = new Map();

      for (const participantId of allParticipantIds) {
        try {
          const isOnline =
            await this.onlineUserManager.isUserOnline(participantId);

          if (isOnline) {
            const userData =
              await this.onlineUserManager.getUserData(participantId);
            presenceMap.set(String(participantId), {
              isOnline: true,
              status: userData?.status || "online",
              lastActivity: userData?.lastActivity || null,
            });
          } else {
            const lastSeenData =
              await this.onlineUserManager.getLastSeen(participantId);
            presenceMap.set(String(participantId), {
              isOnline: false,
              status: "offline",
              lastActivity: lastSeenData?.lastActivity || null,
              disconnectedAt: lastSeenData?.disconnectedAt || null,
            });
          }
        } catch {
          presenceMap.set(String(participantId), {
            isOnline: false,
            status: "offline",
            lastActivity: null,
          });
        }
      }

      return conversations.map((conv) => {
        if (Array.isArray(conv.userMetadata)) {
          conv.userMetadata = conv.userMetadata.map((meta) => ({
            ...meta,
            presence: presenceMap.get(String(meta.userId)) || {
              isOnline: false,
              status: "offline",
              lastActivity: null,
            },
          }));
        }

        const onlineParticipants =
          conv.participants?.filter(
            (p) => presenceMap.get(String(p))?.isOnline,
          ) || [];

        conv.presenceStats = {
          totalParticipants: conv.participants?.length || 0,
          onlineCount: onlineParticipants.length,
          offlineCount:
            (conv.participants?.length || 0) - onlineParticipants.length,
        };

        if (
          conv.type === "PRIVATE" &&
          Array.isArray(conv.participants) &&
          conv.participants.length === 2
        ) {
          const otherUserId = conv.participants.find(
            (p) => String(p) !== String(currentUserId),
          );
          if (otherUserId) {
            conv.otherUserPresence = presenceMap.get(String(otherUserId)) || {
              isOnline: false,
              status: "offline",
              lastActivity: null,
            };
          }
        }

        return conv;
      });
    } catch (error) {
      console.error(
        "❌ Erreur enrichissement présence (archivées):",
        error.message,
      );
      return conversations;
    }
  }
}

module.exports = GetArchivedConversations;
