/**
 * ArchiveConversation — Use Case
 *
 * Permet à un utilisateur d'archiver ou de désarchiver une conversation.
 * L'archivage est INDIVIDUEL : il affecte uniquement l'utilisateur demandeur,
 * pas les autres participants.
 *
 * Actions supportées : 'archive' | 'unarchive'
 */
class ArchiveConversation {
  constructor(conversationRepository) {
    this.conversationRepository = conversationRepository;
  }

  /**
   * @param {string} userId        - L'utilisateur qui archive/désarchive
   * @param {string} conversationId
   * @param {string} action        - 'archive' | 'unarchive'
   */
  async execute(userId, conversationId, action = "archive") {
    const startTime = Date.now();

    if (!["archive", "unarchive"].includes(action)) {
      throw new Error(
        `Action invalide: "${action}". Utiliser 'archive' ou 'unarchive'`,
      );
    }

    console.log(
      `📦 ArchiveConversation: userId=${userId}, convId=${conversationId}, action=${action}`,
    );

    // 1. Récupérer la conversation
    const conversation =
      await this.conversationRepository.findById(conversationId);
    if (!conversation) {
      throw new Error(`Conversation ${conversationId} introuvable`);
    }

    // 2. Vérifier que userId est participant
    const isParticipant =
      Array.isArray(conversation.participants) &&
      conversation.participants.some((p) => String(p) === String(userId));

    if (!isParticipant) {
      throw new Error(
        `Accès refusé : vous n'êtes pas participant de cette conversation`,
      );
    }

    // 3. Vérifier si le changement est nécessaire
    const archivedBy = Array.isArray(conversation.archivedBy)
      ? conversation.archivedBy.map(String)
      : [];
    const isAlreadyArchived = archivedBy.includes(String(userId));

    if (action === "archive" && isAlreadyArchived) {
      console.log(
        `ℹ️  Conversation ${conversationId} déjà archivée pour ${userId}`,
      );
      return {
        conversationId,
        userId,
        action,
        alreadyInState: true,
        isArchived: true,
        processingTime: Date.now() - startTime,
      };
    }

    if (action === "unarchive" && !isAlreadyArchived) {
      console.log(
        `ℹ️  Conversation ${conversationId} déjà active pour ${userId}`,
      );
      return {
        conversationId,
        userId,
        action,
        alreadyInState: true,
        isArchived: false,
        processingTime: Date.now() - startTime,
      };
    }

    // 4. Appliquer la modification via le repository
    const updated = await this.conversationRepository.archiveForUser(
      conversationId,
      userId,
      action,
    );

    const processingTime = Date.now() - startTime;

    console.log(
      `✅ Conversation ${action === "archive" ? "archivée" : "désarchivée"}: ` +
        `${conversationId} pour ${userId} (${processingTime}ms)`,
    );

    return {
      conversationId,
      userId,
      action,
      alreadyInState: false,
      isArchived: action === "archive",
      conversation: updated,
      processingTime,
    };
  }
}

module.exports = ArchiveConversation;
