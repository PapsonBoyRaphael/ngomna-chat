class GetConversation {
  constructor(conversationRepository, messageRepository) {
    this.conversationRepository = conversationRepository;
    this.messageRepository = messageRepository;
  }

  async execute(conversationId, userId, useCache = false) {
    try {
      if (!conversationId || !userId) {
        throw new Error("conversationId et userId sont requis");
      }

      const conversation =
        await this.conversationRepository.findById(conversationId);

      if (!conversation) {
        throw new Error("Conversation non trouvée");
      }

      // Vérifier les permissions
      if (!conversation.participants.includes(userId)) {
        throw new Error("Accès non autorisé à cette conversation");
      }

      // ✅ Un broadcast n'est accessible qu'au créateur
      if (
        conversation.type === "BROADCAST" &&
        String(conversation.createdBy) !== String(userId)
      ) {
        throw new Error("Accès non autorisé à cette conversation");
      }

      // Enrichir avec métadonnées
      const [unreadCount, lastMessage, messageCount] = await Promise.all([
        this.messageRepository.getUnreadCount(userId, conversationId),
        this.messageRepository.getLastMessage(conversationId),
        this.messageRepository.getMessageCount(conversationId),
      ]);

      const result = {
        ...conversation,
        unreadCount,
        lastMessage,
        messageCount,
        isActive: messageCount > 0,
        retrievedAt: new Date().toISOString(),
      };

      console.log(`✅ Conversation récupérée: ${conversation.participants}`);

      return result;
    } catch (error) {
      console.error("❌ Erreur GetConversation:", error);
      throw error;
    }
  }
}

module.exports = GetConversation;
