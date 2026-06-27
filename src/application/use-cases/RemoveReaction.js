/**
 * RemoveReaction - Supprimer la réaction d'un utilisateur sur un message
 * ✅ Supprime en base via le repository
 * ✅ Publie l'événement dans Redis Streams pour distribution MDS
 */
class RemoveReaction {
  constructor(messageRepository, resilientMessageService = null) {
    this.messageRepository = messageRepository;
    this.resilientMessageService = resilientMessageService;
  }

  /**
   * Supprimer une réaction d'un message
   * @param {Object} params
   * @param {string} params.messageId - ID du message
   * @param {string} params.userId - ID de l'utilisateur
   * @param {string} [params.conversationId] - ID de la conversation (optionnel, résolu depuis le message)
   * @param {string} [params.senderSocketId] - Socket ID de l'émetteur (pour exclusion MDS)
   * @returns {Promise<Object>} Résultat avec messageId, conversationId, userId, action, timestamp
   */
  async execute({
    messageId,
    userId,
    conversationId = null,
    senderSocketId = null,
  }) {
    // ✅ VALIDATION
    if (!messageId || !userId) {
      throw new Error("messageId et userId sont requis");
    }

    // ✅ SUPPRIMER EN BASE VIA LE REPOSITORY
    const result = await this.messageRepository.removeReaction(
      messageId,
      String(userId),
    );

    const convId = conversationId || result.conversationId;
    const timestamp = new Date().toISOString();

    // ✅ PUBLIER DANS REDIS STREAM POUR DISTRIBUTION MDS
    if (this.resilientMessageService && this.resilientMessageService.redis) {
      try {
        await this.resilientMessageService.redis.xAdd(
          "chat:stream:events:reactions",
          "*",
          {
            messageId: String(messageId),
            conversationId: String(convId),
            userId: String(userId),
            senderSocketId: String(senderSocketId || ""),
            reaction: "",
            action: "remove",
            timestamp,
          },
        );
      } catch (err) {
        console.error(
          "❌ Erreur publication suppression réaction:",
          err.message,
        );
      }
    }

    console.log(
      `🚫 Réaction supprimée: ${userId} sur ${messageId} (removed: ${result.removed})`,
    );

    return {
      success: true,
      messageId,
      conversationId: convId,
      userId: String(userId),
      reaction: "",
      action: "remove",
      timestamp,
    };
  }
}

module.exports = RemoveReaction;
