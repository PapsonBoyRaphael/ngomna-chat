/**
 * Use Case : Répondre à un message dans une conversation
 * ✅ Récupère le message parent et délègue l'envoi à SendMessage
 * ✅ Le message de réponse est un vrai "nouveau message" (ACK, stream, unreadCount)
 * ✅ Même pattern que ForwardMessage : composition via SendMessage
 */

class ReplyMessage {
  constructor(messageRepository, sendMessageUseCase) {
    this.messageRepository = messageRepository;
    this.sendMessageUseCase = sendMessageUseCase;
  }

  /**
   * Répond à un message dans une conversation
   * @param {Object} params
   * @param {string} params.messageId - ID du message parent auquel on répond
   * @param {string} params.content - Contenu de la réponse
   * @param {string} params.senderId - ID de l'utilisateur qui répond
   * @param {string} [params.conversationId] - ID de la conversation (optionnel, déduit du message parent)
   * @param {string} [params.senderSocketId] - Socket ID de l'expéditeur
   * @param {string} [params.type] - Type du message (défaut: TEXT)
   * @returns {Object} Résultat avec le message créé
   */
  async execute({
    messageId,
    content,
    senderId,
    conversationId = null,
    senderSocketId = null,
    type = "TEXT",
  }) {
    const startTime = Date.now();

    try {
      // ✅ VALIDATION DES PARAMÈTRES
      if (!messageId) {
        throw new Error("messageId est requis");
      }
      if (!content) {
        throw new Error("content est requis");
      }
      if (!senderId) {
        throw new Error("senderId est requis");
      }

      // ✅ ÉTAPE 1 : RÉCUPÉRER LE MESSAGE PARENT
      const parentMessage = await this.messageRepository.findById(messageId);

      if (!parentMessage) {
        throw new Error(`Message parent introuvable: ${messageId}`);
      }

      if (parentMessage.isDeleted) {
        throw new Error("Impossible de répondre à un message supprimé");
      }

      // ✅ ÉTAPE 2 : DÉTERMINER LA CONVERSATION
      const targetConversationId =
        conversationId || String(parentMessage.conversationId);

      if (!targetConversationId) {
        throw new Error(
          "Impossible de déterminer la conversation pour la réponse",
        );
      }

      console.log(
        `💬 Réponse au message ${messageId} par ${senderId} dans conversation ${targetConversationId}`,
      );

      // ✅ ÉTAPE 3 : CRÉER LE MESSAGE DE RÉPONSE VIA SendMessage
      const result = await this.sendMessageUseCase.execute({
        content,
        senderId,
        senderSocketId,
        conversationId: targetConversationId,
        type,
        // ✅ CHAMP DE RÉPONSE
        replyTo: String(parentMessage._id || parentMessage.id),
      });

      const duration = Date.now() - startTime;

      if (result && result.success && result.message) {
        console.log(
          `✅ Réponse créée en ${duration}ms: ${result.message.id || result.message._id}`,
        );

        return {
          success: true,
          message: result.message,
          conversation: result.conversation,
          replyTo: {
            messageId: String(parentMessage._id || parentMessage.id),
            senderId: parentMessage.senderId,
            content:
              parentMessage.content?.substring(0, 200) || "[contenu média]",
            type: parentMessage.type,
          },
          conversationId: String(
            result.conversation?.id ||
              result.conversation?._id ||
              targetConversationId,
          ),
          duration,
        };
      } else {
        throw new Error("Résultat inattendu de SendMessage");
      }
    } catch (error) {
      console.error("❌ Erreur ReplyMessage use case:", error);
      throw error;
    }
  }
}

module.exports = ReplyMessage;
