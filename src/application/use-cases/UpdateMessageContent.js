class UpdateMessageContent {
  constructor(
    messageRepository,
    conversationRepository = null,
    kafkaProducer = null,
    resilientMessageService = null,
  ) {
    this.messageRepository = messageRepository;
    this.conversationRepository = conversationRepository;
    this.kafkaProducer = kafkaProducer;
    this.resilientMessageService = resilientMessageService;
  }

  /**
   * Met à jour le contenu d'un message (texte uniquement)
   * @param {Object} params
   * @param {string} params.messageId
   * @param {string} params.newContent
   * @param {string} params.userId
   * @returns {Promise<Object>} message mis à jour
   */
  async execute({ messageId, newContent, userId, senderSocketId = null }) {
    if (!messageId || !newContent || !userId) {
      throw new Error("messageId, newContent et userId sont requis");
    }

    // Récupérer le message
    const message = await this.messageRepository.findById(messageId);
    if (!message) {
      throw new Error("Message introuvable");
    }

    // Vérifier que l'utilisateur est bien l'auteur
    if (String(message.senderId) !== String(userId)) {
      throw new Error("Modification non autorisée");
    }

    // Mettre à jour le contenu et la date d'édition
    message.content = newContent;
    message.editedAt = new Date();
    message.updatedAt = new Date();

    // Historiser l'ancien contenu si besoin
    if (
      !message.metadata?.contentMetadata?.originalContent &&
      message.metadata?.contentMetadata
    ) {
      message.metadata.contentMetadata.originalContent = message.content;
    }

    // Sauvegarder la modification
    const updated = await this.messageRepository.save(message);

    // ✅ PUBLIER DANS REDIS STREAMS - STATUT EDITED
    // EDITED doit être envoyé à TOUS les participants de la conversation
    if (this.resilientMessageService) {
      try {
        // ✅ RÉCUPÉRER LES PARTICIPANTS DE LA CONVERSATION
        let conversationParticipants = [];
        if (message.conversationId && this.conversationRepository) {
          try {
            const conversation = await this.conversationRepository.findById(
              message.conversationId,
            );
            if (conversation) {
              conversationParticipants = conversation.participants || [];
              console.log(
                `👥 [EDITED] Participants trouvés: ${conversationParticipants
                  .map((p) => p.userId || p)
                  .join(", ")}`,
              );
            }
          } catch (convError) {
            console.warn(
              "⚠️ [EDITED] Erreur récupération participants:",
              convError.message,
            );
          }
        }

        // ✅ ENVOYER L'EDITED À TOUS LES PARTICIPANTS AVEC LE NOUVEAU CONTENU
        await this.resilientMessageService.publishEditedMessageToAllParticipants(
          messageId,
          message.conversationId,
          newContent, // ✅ PASSER LE NOUVEAU CONTENU
          conversationParticipants,
          senderSocketId, // ✅ PROPAGER senderSocketId pour exclusion MDS
        );
        console.log(`📤 [EDITED] événement publié pour message ${messageId}`);
      } catch (streamErr) {
        console.error(
          "❌ Erreur publication statut EDITED:",
          streamErr.message,
        );
      }
    }

    return updated;
  }
}

module.exports = UpdateMessageContent;
