class MarkMessageDelivered {
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
   * params:
   *  - messageId (string) - un seul message
   *  - conversationId (string) - pour marquer tous les messages d'une conversation
   *  - messageIds (array) - pour marquer plusieurs messages spécifiques
   *  - userId (receiver) - REQUIS
   */
  async execute({
    messageId = null,
    conversationId = null,
    messageIds = null,
    userId,
  }) {
    const start = Date.now();
    try {
      // ✅ VALIDATION : userId EST REQUIS
      if (!userId) {
        throw new Error("userId (receiverId) requis");
      }

      console.log(`📬 MarkMessageDelivered.execute():`, {
        messageId,
        conversationId,
        messageIdsCount: messageIds?.length || 0,
        userId,
      });

      let result;

      // ✅ CAS 1 : UN SEUL MESSAGE
      if (messageId) {
        console.log(
          `📬 Marquage UN seul message: ${messageId} comme DELIVERED`,
        );
        result = await this.messageRepository.updateSingleMessageStatus(
          messageId,
          userId,
          "DELIVERED",
        );
      }
      // ✅ CAS 2 : TOUS LES MESSAGES D'UNE CONVERSATION
      // else if (conversationId && !messageIds) {
      //   console.log(
      //     `📬 Marquage TOUS messages conversation ${conversationId} comme DELIVERED`,
      //   );
      //   result = await this.messageRepository.updateMessageStatus(
      //     conversationId,
      //     userId,
      //     "DELIVERED",
      //     [], // messageIds vide = tous les messages
      //   );
      // }
      // ✅ CAS 3 : MESSAGES SPÉCIFIQUES
      else if (conversationId && messageIds) {
        console.log(
          `📬 Marquage ${messageIds.length} messages spécifiques comme DELIVERED`,
        );
        result = await this.messageRepository.updateMessageStatus(
          conversationId,
          userId,
          "DELIVERED",
          messageIds,
        );
      } else {
        throw new Error(
          "Doit avoir soit messageId, soit conversationId avec ou sans messageIds",
        );
      }

      console.log("✅ Mise à jour DELIVERED terminée:", {
        conversationId,
        messageId,
        userId,
        modifiedCount: result?.modifiedCount || 0,
        durationMs: Date.now() - start,
      });

      // ✅ METTRE À JOUR LE STATUT DU DERNIER MESSAGE SI LE STATUS GLOBAL A CHANGÉ
      // Uniquement si le message a atteint le statut DELIVERED (tous les destinataires ont reçu)
      if (this.conversationRepository && result && result.modifiedCount > 0) {
        try {
          const updatedMessage = result.message;
          const targetConvId = conversationId || updatedMessage?.conversationId;
          const targetMsgId =
            messageId || (messageIds && messageIds[messageIds.length - 1]);

          // ✅ VÉRIFIER SI LE STATUT GLOBAL EST MAINTENANT DELIVERED
          if (
            targetConvId &&
            targetMsgId &&
            updatedMessage?.status === "DELIVERED"
          ) {
            const updateResult =
              await this.conversationRepository.updateLastMessageStatus(
                targetConvId,
                targetMsgId,
                "DELIVERED",
              );
            if (updateResult) {
              console.log(
                `✅ lastMessage.status mis à jour → DELIVERED (tous ont reçu)`,
              );
            } else {
              console.log(
                `ℹ️ lastMessage.status non modifié (message ${targetMsgId} n'est pas le dernier message de la conversation)`,
              );
            }
          } else {
            console.log(
              `ℹ️ lastMessage.status non mis à jour (${updatedMessage?.deliveredCount || 0}/${updatedMessage?.totalRecipients || 1} ont reçu)`,
            );
          }
        } catch (lastMsgError) {
          console.warn(
            "⚠️ Erreur mise à jour lastMessage.status DELIVERED:",
            lastMsgError.message,
          );
        }
      }

      // ✅ PUBLIER DANS REDIS STREAMS - STATUT DELIVERED
      // L'accusé de réception est envoyé UNIQUEMENT à l'expéditeur du message
      if (this.resilientMessageService && result && result.modifiedCount > 0) {
        try {
          // Pour les messages individuels, publier un événement par message
          if (messageId) {
            await this.resilientMessageService.publishMessageStatus(
              messageId,
              result.message.senderId, // ✅ À l'EXPÉDITEUR du message
              "DELIVERED",
              result.message.receivedAt || result.message.receiveAt || null,
              null,
              null,
            );
          } else if (messageIds && messageIds.length > 0) {
            // Pour les messages spécifiques
            for (const msgId of messageIds) {
              const message = await this.messageRepository.findById(msgId);
              if (!message) continue;
              await this.resilientMessageService.publishMessageStatus(
                msgId,
                message.senderId, // ✅ À l'EXPÉDITEUR du message
                "DELIVERED",
                null,
                null,
                null,
              );
            }
          }
          console.log(`📤 [DELIVERED] événements publiés`);
        } catch (streamErr) {
          console.error(
            "❌ Erreur publication statuts DELIVERED:",
            streamErr.message,
          );
        }
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur MarkMessageDelivered use case:", error.message);
      throw error;
    }
  }
}

module.exports = MarkMessageDelivered;
