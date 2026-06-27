/**
 * LeaveConversation - Un participant quitte volontairement une conversation
 * Publie l'événement conversation.participant.left dans Redis Streams
 */
class LeaveConversation {
  constructor(
    conversationRepository,
    resilientMessageService = null,
    userCacheService = null,
  ) {
    this.conversationRepository = conversationRepository;
    this.resilientMessageService = resilientMessageService;
    this.userCacheService = userCacheService;
  }

  async execute({ conversationId, userId }) {
    if (!conversationId || !userId) {
      throw new Error("conversationId et userId requis");
    }

    // Récupérer la conversation
    const conversation =
      await this.conversationRepository.findById(conversationId);
    if (!conversation) {
      throw new Error("Conversation introuvable");
    }

    // Vérifier que c'est un groupe
    if (conversation.type !== "GROUP") {
      throw new Error("Seuls les groupes peuvent être quittés");
    }

    // Vérifier que l'utilisateur est bien membre
    if (!conversation.participants.includes(userId)) {
      throw new Error("Vous n'êtes pas membre de ce groupe");
    }

    // Ne pas permettre au créateur de quitter (il doit transférer d'abord)
    if (userId === conversation.createdBy) {
      throw new Error(
        "Le créateur doit transférer la propriété avant de quitter",
      );
    }

    // Récupérer les infos du participant
    let participantInfo = null;
    if (this.userCacheService) {
      try {
        const users = await this.userCacheService.fetchUsersInfo([userId]);
        participantInfo = users[0];
      } catch (err) {
        console.warn(
          "⚠️ Impossible de récupérer les infos utilisateur:",
          err.message,
        );
      }
    }

    // Retirer le participant
    conversation.participants = conversation.participants.filter(
      (id) => id !== userId,
    );

    // Supprimer les unreadCounts
    if (conversation.unreadCounts) {
      delete conversation.unreadCounts[userId];
    }

    // Supprimer les métadonnées utilisateur
    if (conversation.userMetadata) {
      conversation.userMetadata = conversation.userMetadata.filter(
        (meta) => meta.userId !== userId,
      );
    }

    // Mettre à jour les stats
    if (conversation.metadata?.stats) {
      conversation.metadata.stats.totalParticipants =
        conversation.participants.length;
    }

    // Ajouter dans l'audit log
    if (conversation.metadata?.auditLog) {
      conversation.metadata.auditLog.push({
        action: "PARTICIPANT_LEFT",
        userId: userId,
        timestamp: new Date(),
        details: { voluntary: true },
        metadata: { source: "LeaveConversation-UseCase" },
      });
    }

    conversation.updatedAt = new Date();

    // Sauvegarder
    const updated = await this.conversationRepository.save(conversation);

    // ✅ PUBLIER DANS REDIS STREAMS chat:stream:events:conversations
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:conversations",
          {
            event: "conversation.participant.left",
            conversationId: conversationId.toString(),
            participantId: userId,
            participantName: participantInfo?.name || "Utilisateur inconnu",
            leftAt: new Date().toISOString(),
            totalParticipants: conversation.participants.length.toString(),
            timestamp: Date.now().toString(),
          },
        );
        console.log(
          `📤 [conversation.participant.left] publié dans chat:stream:events:conversations`,
        );
      } catch (streamErr) {
        console.error(
          "❌ Erreur publication stream participant.left:",
          streamErr.message,
        );
      }
    }

    // Publier notification système
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.publishSystemMessage({
          conversationId: conversationId.toString(),
          type: "SYSTEM",
          subType: "PARTICIPANT_LEFT",
          senderId: userId,
          senderName: participantInfo?.name || "Un membre",
          content: `${participantInfo?.name || userId} a quitté le groupe`,
          metadata: {
            participantId: userId,
            participantName: participantInfo?.name,
            voluntary: true,
          },
        });
      } catch (err) {
        console.warn("⚠️ Erreur publication notification:", err.message);
      }
    }

    return {
      success: true,
      conversationId,
      userId,
      remainingParticipants: conversation.participants.length,
    };
  }
}

module.exports = LeaveConversation;
