class GetMessages {
  constructor(messageRepository) {
    this.messageRepository = messageRepository;
  }

  async execute(conversationId, options = {}) {
    try {
      const {
        cursor = null,
        limit = 50,
        direction = "older",
        userId,
        useCache = true,
      } = options;

      if (!conversationId) {
        throw new Error("ID de conversation requis");
      }

      console.log(
        `🔍 GetMessages: conversation=${conversationId}, cursor=${cursor}, direction=${direction}, useCache=${useCache}`
      );

      // ✅ APPEL REPOSITORY avec cursor
      const result = await this.messageRepository.findByConversation(
        conversationId,
        {
          cursor,
          limit: parseInt(limit),
          direction,
          userId,
          useCache,
        }
      );

      console.log(
        `✅ Messages récupérés: ${result.messages?.length || 0} (${
          result.fromCache ? "cache" : "MongoDB"
        })`
      );

      return {
        messages: result.messages || [],
        nextCursor: result.nextCursor || null,
        hasMore: result.hasMore || false,
        fromCache: result.fromCache || false,
        totalCount: result.totalCount || 0,
      };
    } catch (error) {
      console.error("❌ Erreur GetMessages use case:", error);
      throw error;
    }
  }
}

module.exports = GetMessages;
