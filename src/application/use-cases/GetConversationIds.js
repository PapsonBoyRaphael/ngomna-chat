class GetConversationIds {
  constructor(conversationRepository) {
    this.conversationRepository = conversationRepository;
  }

  /**
   * Retourne la liste des IDs de conversations où l'utilisateur est participant
   * @param {string} userId
   * @returns {Promise<string[]>}
   */
  async execute(userId) {
    if (!userId) throw new Error("userId requis");
    // Utilise findByParticipant du repository
    const result = await this.conversationRepository.findByParticipant(userId, {
      page: 1,
      limit: 1000, // ou plus selon besoin
      useCache: false,
    });
    // result peut être { conversations: [...] }
    const conversations = result.conversations || result || [];
    // ✅ Exclure les BROADCAST dont l'utilisateur n'est pas le créateur
    const visibleConversations = conversations.filter((conv) => {
      if (conv.type !== "BROADCAST") return true;
      return String(conv.createdBy) === String(userId);
    });
    return visibleConversations.map((conv) =>
      conv._id ? conv._id.toString() : conv.id?.toString(),
    );
  }
}

module.exports = GetConversationIds;
