class MarkMessageRead {
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
   *  - messageId (string) OR (conversationId + messageIds)
   *  - userId (reader)
   *  - conversationId (optional)
   *  - messageIds (optional array)
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
        throw new Error("userId (reader) requis");
      }

      console.log(`📬 MarkMessageRead.execute():`, {
        messageId,
        conversationId,
        messageIdsCount: messageIds?.length || 0,
        userId,
      });

      let result;

      // ✅ CAS 1 : UN SEUL MESSAGE
      if (messageId) {
        console.log(`📬 Marquage UN seul message: ${messageId} comme READ`);
        result = await this.messageRepository.updateSingleMessageStatus(
          messageId,
          userId,
          "READ",
        );
      }
      // ✅ CAS 3 : MESSAGES SPÉCIFIQUES
      else if (conversationId && messageIds) {
        console.log(
          `📬 Marquage ${messageIds.length} messages spécifiques comme READ`,
        );
        result = await this.messageRepository.updateMessageStatus(
          conversationId,
          userId,
          "READ",
          messageIds,
        );
      } else {
        throw new Error(
          "Doit avoir soit messageId, soit conversationId avec ou sans messageIds",
        );
      }

      console.log("✅ Mise à jour READ terminée:", {
        conversationId,
        messageId,
        userId,
        modifiedCount: result?.modifiedCount || 0,
        durationMs: Date.now() - start,
      });

      // ✅ METTRE À JOUR LE STATUT DU DERNIER MESSAGE SI LE STATUS GLOBAL A CHANGÉ
      if (this.conversationRepository && result && result.modifiedCount > 0) {
        try {
          const targetConvId = conversationId || result.message?.conversationId;

          if (targetConvId && messageId && result.message?.status === "READ") {
            // ✅ CAS SINGLE : result.message disponible avec le statut mis à jour
            await this.conversationRepository.updateLastMessageStatus(
              targetConvId,
              messageId,
              "READ",
            );
            console.log(
              `✅ lastMessage.status mis à jour → READ (single, tous ont lu)`,
            );
          } else if (targetConvId && messageIds && messageIds.length > 0) {
            // ✅ CAS BATCH : result.message N'EST PAS disponible (updateMessageStatus ne le retourne pas)
            // Récupérer la conversation pour vérifier si son lastMessage est dans le batch
            const conversation =
              await this.conversationRepository.findById(targetConvId);
            const lastMsgId = conversation?.lastMessage?._id?.toString();

            if (
              lastMsgId &&
              messageIds.some((id) => id.toString() === lastMsgId)
            ) {
              // Le lastMessage de la conversation est dans le batch marqué READ
              const lastMsg = await this.messageRepository.findById(lastMsgId);
              if (lastMsg && lastMsg.status === "READ") {
                await this.conversationRepository.updateLastMessageStatus(
                  targetConvId,
                  lastMsgId,
                  "READ",
                );
                console.log(
                  `✅ lastMessage.status mis à jour → READ (batch, message ${lastMsgId})`,
                );
              } else {
                console.log(
                  `ℹ️ lastMessage.status non mis à jour (${lastMsg?.readCount || 0}/${lastMsg?.totalRecipients || 1} ont lu)`,
                );
              }
            } else {
              console.log(
                `ℹ️ lastMessage (${lastMsgId}) pas dans le batch de ${messageIds.length} messages`,
              );
            }
          } else if (!targetConvId) {
            console.log(
              `ℹ️ lastMessage.status non mis à jour: conversationId indisponible`,
            );
          }
        } catch (lastMsgError) {
          console.warn(
            "⚠️ Erreur mise à jour lastMessage.status READ:",
            lastMsgError.message,
          );
        }
      }

      // ✅ DÉCRÉMENTER LE COMPTEUR userMetadata.unreadCount DANS MONGODB
      if (result && result.modifiedCount > 0 && this.conversationRepository) {
        try {
          const targetConvId = conversationId || result.message?.conversationId;
          if (targetConvId) {
            const readCount = result.modifiedCount || 1;
            await this.conversationRepository.decrementUnreadCountInUserMetadata(
              targetConvId,
              userId,
              readCount,
            );
            console.log(
              `✅ Compteur userMetadata décrémenté de ${readCount} pour ${userId}`,
            );
          }
        } catch (decrementError) {
          console.error(
            `❌ Erreur décrémentation compteur userMetadata:`,
            decrementError.message,
          );
          // Ne pas faire échouer la mise à jour du statut si la décrémentation échoue
        }
      }

      // ✅ PUBLIER DANS REDIS STREAMS - STATUT READ
      // L'accusé de lecture est envoyé UNIQUEMENT à l'expéditeur du message
      if (this.resilientMessageService && result && result.modifiedCount > 0) {
        try {
          // Pour les messages individuels, publier un événement par message
          if (messageId && result.message) {
            await this.resilientMessageService.publishMessageStatus(
              messageId,
              result.message.senderId, // ✅ À l'EXPÉDITEUR du message
              "READ",
              result.message.readAt || result.message.receivedAt || null,
              null,
              null,
            );
          } else if (messageIds && messageIds.length > 0) {
            // Pour chaque message marqué comme lu, publier un événement séparé à l'expéditeur
            for (const msgId of messageIds) {
              const message = await this.messageRepository.findById(msgId);
              if (!message) continue;
              await this.resilientMessageService.publishMessageStatus(
                msgId,
                message.senderId, // ✅ À l'EXPÉDITEUR du message
                "READ",
                message.readAt || null,
                null,
                null,
              );
            }
          }

          console.log(`📤 [READ] événements publiés`);
        } catch (streamErr) {
          console.error(
            "❌ Erreur publication statuts READ:",
            streamErr.message,
          );
        }
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur MarkMessageRead use case:", error.message);
      throw error;
    }
  }
}

module.exports = MarkMessageRead;
