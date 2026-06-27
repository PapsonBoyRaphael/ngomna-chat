/**
 * AddReaction - Ajouter/mettre à jour une réaction sur un message
 * ✅ Sauvegarde en base via le repository
 * ✅ Publie l'événement dans Redis Streams pour distribution MDS
 */
class AddReaction {
  constructor(messageRepository, resilientMessageService = null) {
    this.messageRepository = messageRepository;
    this.resilientMessageService = resilientMessageService;
  }

  /**
   * Ajouter une réaction à un message
   * @param {Object} params
   * @param {string} params.messageId - ID du message
   * @param {string} params.userId - ID de l'utilisateur qui réagit
   * @param {string} params.emoji - Emoji de la réaction
   * @param {string} [params.conversationId] - ID de la conversation (optionnel, résolu depuis le message)
   * @param {string} [params.senderSocketId] - Socket ID de l'émetteur (pour exclusion MDS)
   * @returns {Promise<Object>} Résultat avec messageId, conversationId, userId, reaction, action, timestamp
   */
  async execute({
    messageId,
    userId,
    emoji,
    conversationId = null,
    senderSocketId = null,
  }) {
    // ✅ VALIDATION
    if (!messageId || !userId) {
      throw new Error("messageId et userId sont requis");
    }

    if (!emoji) {
      throw new Error("emoji est requis");
    }

    // ✅ SAUVEGARDER EN BASE VIA LE REPOSITORY
    const result = await this.messageRepository.addReaction(
      messageId,
      String(userId),
      emoji,
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
            reaction: emoji,
            action: "add",
            timestamp,
          },
        );
      } catch (err) {
        console.error("❌ Erreur publication réaction:", err.message);
      }
    }

    console.log(
      `😀 Réaction ajoutée: ${emoji} par ${userId} sur ${messageId} (${result.action})`,
    );

    return {
      success: true,
      messageId,
      conversationId: convId,
      userId: String(userId),
      reaction: emoji,
      action: "add",
      timestamp,
    };
  }
}

module.exports = AddReaction;
