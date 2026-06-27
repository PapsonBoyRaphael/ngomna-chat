class SearchOccurrences {
  constructor({ fileRepository, conversationRepository, messageRepository }) {
    this.fileRepository = fileRepository;
    this.conversationRepository = conversationRepository;
    this.messageRepository = messageRepository;
  }

  /**
   * Recherche un texte dans les fichiers, conversations et messages
   * @param {string} query - Mot-clé à rechercher
   * @param {object} options - Options de recherche (userId, pagination, etc.)
   * @returns {Promise<object>} - Résultats groupés
   */
  async execute(query, options = {}) {
    if (!query || typeof query !== "string" || query.length < 2) {
      throw new Error(
        "Le mot-clé de recherche doit contenir au moins 2 caractères",
      );
    }

    const {
      userId,
      page = 1,
      limit = 20,
      useCache = true,
      useLike = true,
      includeArchived = false,
      scope = "all", // Ajout du paramètre scope
    } = options;

    // Déterminer les entités à rechercher
    let scopes = [];
    if (typeof scope === "string") {
      scopes =
        scope === "all"
          ? [
              "messages",
              "conversations",
              "files",
              "groups",
              "broadcast",
              "channels",
            ]
          : scope.split(",").map((s) => s.trim().toLowerCase());
    } else if (Array.isArray(scope)) {
      scopes = scope;
    } else {
      scopes = [
        "messages",
        "conversations",
        "files",
        "groups",
        "broadcast",
        "channels",
      ];
    }

    let filesResult = { files: [], totalFound: 0, searchTime: 0 };
    let conversationsResult = {
      conversations: [],
      totalFound: 0,
      searchTime: 0,
    };
    let groupsResult = { conversations: [], totalFound: 0, searchTime: 0 };
    let broadcastResult = { conversations: [], totalFound: 0, searchTime: 0 };
    let channelsResult = { conversations: [], totalFound: 0, searchTime: 0 };
    let messagesResult = { messages: [], totalFound: 0, searchTime: 0 };

    if (scopes.includes("files")) {
      filesResult = await this.fileRepository.searchFiles(query, {
        page,
        limit,
        useCache,
        useLike,
      });
    }
    if (
      scopes.includes("conversations") ||
      scopes.includes("groups") ||
      scopes.includes("broadcast") ||
      scopes.includes("channels")
    ) {
      // Recherche toutes les conversations, puis filtre par type si demandé
      const convResult = await this.conversationRepository.searchConversations(
        query,
        {
          userId,
          limit,
          useCache,
          useLike,
          includeArchived,
        },
      );

      if (scopes.includes("conversations")) {
        conversationsResult.conversations = convResult.conversations.filter(
          (c) => c.type === "PRIVATE",
        );
        conversationsResult.totalFound =
          conversationsResult.conversations.length;
        conversationsResult.searchTime = convResult.searchTime;
      }
      if (scopes.includes("groups")) {
        groupsResult.conversations = convResult.conversations.filter(
          (c) => c.type === "GROUP",
        );
        groupsResult.totalFound = groupsResult.conversations.length;
        groupsResult.searchTime = convResult.searchTime;
      }
      if (scopes.includes("broadcast")) {
        broadcastResult.conversations = convResult.conversations.filter(
          (c) => c.type === "BROADCAST",
        );
        broadcastResult.totalFound = broadcastResult.conversations.length;
        broadcastResult.searchTime = convResult.searchTime;
      }
      if (scopes.includes("channels")) {
        channelsResult.conversations = convResult.conversations.filter(
          (c) => c.type === "CHANNEL",
        );
        channelsResult.totalFound = channelsResult.conversations.length;
        channelsResult.searchTime = convResult.searchTime;
      }
    }
    if (scopes.includes("messages")) {
      messagesResult = await this.messageRepository.searchMessages(query, {
        userId,
        limit,
        useCache,
        useLike,
      });
    }

    return {
      query,
      files: filesResult.files || [],
      totalFiles: filesResult.totalFound || 0,
      conversations: conversationsResult.conversations || [],
      totalConversations: conversationsResult.totalFound || 0,
      groups: groupsResult.conversations || [],
      totalGroups: groupsResult.totalFound || 0,
      broadcast: broadcastResult.conversations || [],
      totalBroadcast: broadcastResult.totalFound || 0,
      messages: messagesResult.messages || [],
      totalChannels: channelsResult.totalFound || 0,
      totalMessages: messagesResult.totalFound || 0,
      searchTime: Math.max(
        filesResult.searchTime || 0,
        conversationsResult.searchTime || 0,
        groupsResult.searchTime || 0,
        broadcastResult.searchTime || 0,
        channelsResult.searchTime || 0,
        messagesResult.searchTime || 0,
      ),
      scope: scopes,
    };
  }
}

module.exports = SearchOccurrences;
