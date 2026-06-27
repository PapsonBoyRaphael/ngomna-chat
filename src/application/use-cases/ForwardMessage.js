/**
 * Use Case : Transférer un message vers une ou plusieurs conversations
 * ✅ Récupère le message original et délègue l'envoi à SendMessage
 * ✅ Chaque message transféré est un vrai "nouveau message" (ACK, stream, unreadCount)
 * ✅ Supporte le transfert vers 1-10 conversations en une seule opération
 */

class ForwardMessage {
  constructor(messageRepository, sendMessageUseCase) {
    this.messageRepository = messageRepository;
    this.sendMessageUseCase = sendMessageUseCase;
  }

  /**
   * Transfère un message vers une ou plusieurs conversations
   * @param {Object} params
   * @param {string} params.originalMessageId - ID du message à transférer
   * @param {string|string[]} params.targetConversationIds - ID(s) de la/les conversation(s) cible(s)
   * @param {string} params.senderId - ID de l'utilisateur qui transfère
   * @param {string} [params.senderSocketId] - Socket ID de l'expéditeur
   * @returns {Object} Résultat avec les messages créés
   */
  async execute({
    originalMessageId,
    targetConversationIds,
    senderId,
    senderSocketId = null,
  }) {
    const startTime = Date.now();

    try {
      // ✅ VALIDATION DES PARAMÈTRES
      if (!originalMessageId) {
        throw new Error("originalMessageId est requis");
      }
      if (!targetConversationIds) {
        throw new Error("targetConversationIds est requis");
      }
      if (!senderId) {
        throw new Error("senderId est requis");
      }

      // Normaliser en tableau
      const conversationIds = Array.isArray(targetConversationIds)
        ? targetConversationIds
        : [targetConversationIds];

      if (conversationIds.length === 0) {
        throw new Error("Au moins une conversation cible est requise");
      }

      if (conversationIds.length > 10) {
        throw new Error("Maximum 10 conversations cibles par transfert");
      }

      // ✅ ÉTAPE 1 : RÉCUPÉRER LE MESSAGE ORIGINAL
      const originalMessage =
        await this.messageRepository.findById(originalMessageId);

      if (!originalMessage) {
        throw new Error(`Message original introuvable: ${originalMessageId}`);
      }

      if (originalMessage.isDeleted) {
        throw new Error("Impossible de transférer un message supprimé");
      }

      console.log(
        `📤 Transfert message ${originalMessageId} par ${senderId} vers ${conversationIds.length} conversation(s)`,
      );

      // ✅ ÉTAPE 2 : PRÉPARER LE fileId ORIGINAL (si applicable)
      const originalFileId =
        originalMessage.metadata?.contentMetadata?.file?.fileId || null;

      // ✅ ÉTAPE 3 : TRANSFÉRER VERS CHAQUE CONVERSATION VIA SendMessage
      const results = [];
      const errors = [];

      for (const targetConvId of conversationIds) {
        try {
          const result = await this.sendMessageUseCase.execute({
            content: originalMessage.content,
            senderId,
            senderSocketId,
            conversationId: targetConvId,
            type: originalMessage.type,
            fileId: originalFileId,
            // ✅ CHAMPS DE TRANSFERT
            isForwarded: true,
            forwardedFrom: String(originalMessage._id || originalMessage.id),
            originalSenderId: originalMessage.senderId,
          });

          if (result && result.success && result.message) {
            results.push({
              messageId: result.message.id || result.message._id,
              conversationId: String(
                result.conversation?.id ||
                  result.conversation?._id ||
                  targetConvId,
              ),
              conversationType: result.conversation?.type || null,
              content: result.message.content,
              type: result.message.type,
              isForwarded: true,
              originalMessageId: String(
                originalMessage._id || originalMessage.id,
              ),
              originalSenderId: originalMessage.senderId,
              timestamp: result.message.timestamp || result.message.createdAt,
            });
          } else {
            errors.push({
              conversationId: targetConvId,
              error: "Résultat inattendu de SendMessage",
            });
          }
        } catch (convError) {
          console.error(
            `❌ Erreur transfert vers ${targetConvId}:`,
            convError.message,
          );
          errors.push({
            conversationId: targetConvId,
            error: convError.message,
          });
        }
      }

      const duration = Date.now() - startTime;
      console.log(
        `✅ Transfert terminé en ${duration}ms: ${results.length} succès, ${errors.length} erreurs`,
      );

      return {
        success: results.length > 0,
        forwarded: results,
        errors: errors.length > 0 ? errors : undefined,
        originalMessageId,
        count: results.length,
        duration,
      };
    } catch (error) {
      console.error("❌ Erreur ForwardMessage use case:", error);
      throw error;
    }
  }
}

module.exports = ForwardMessage;
