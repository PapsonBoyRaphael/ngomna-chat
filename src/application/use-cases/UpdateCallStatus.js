/**
 * ✅ Use Case : Mettre à jour le statut d'un appel
 * Responsabilité : Valider, déléguer la mise à jour au repository,
 *                  puis publier l'événement via Redis Stream pour tous les participants
 */
class UpdateCallStatus {
  constructor(messageRepository, resilientService = null) {
    this.messageRepository = messageRepository;
    this.resilientService = resilientService;
  }

  /**
   * @param {Object} params
   * @param {string} params.messageId - ID du message d'appel à mettre à jour
   * @param {Object} params.updates - Champs à mettre à jour
   * @param {string} [params.updates.status] - INITIATED | RINGING | ANSWERED | ENDED | MISSED | DECLINED | CANCELLED | FAILED | BUSY
   * @param {Date}   [params.updates.startedAt] - Début de l'appel (quand décroché)
   * @param {Date}   [params.updates.endedAt] - Fin de l'appel
   * @param {number} [params.updates.duration] - Durée en secondes
   * @param {string} [params.updates.endReason] - Raison de fin (user_hangup, timeout, no_answer, user_declined, error...)
   * @param {string} [params.conversationId] - ID de la conversation (pour publication stream)
   * @param {string} [params.userId] - ID de l'utilisateur ayant déclenché l'action
   * @param {string} [params.callId] - ID de l'appel
   * @param {string[]} [params.participants] - Liste des participants à notifier
   */
  async execute({
    messageId,
    updates,
    conversationId = null,
    userId = null,
    callId = null,
    participants = [],
    senderSocketId = null,
  }) {
    if (!messageId) {
      throw new Error(
        "messageId est requis pour mettre à jour le statut d'appel",
      );
    }

    if (!updates || typeof updates !== "object") {
      throw new Error("updates est requis et doit être un objet");
    }

    // ✅ Valider le statut si fourni
    const validStatuses = [
      "INITIATED",
      "RINGING",
      "ANSWERED",
      "ENDED",
      "MISSED",
      "DECLINED",
      "CANCELLED",
      "FAILED",
      "BUSY",
    ];
    if (updates.status && !validStatuses.includes(updates.status)) {
      throw new Error(
        `Statut d'appel invalide: ${updates.status}. Valides: ${validStatuses.join(", ")}`,
      );
    }

    console.log(
      `📞 UpdateCallStatus: ${messageId} → ${updates.status || "update"}`,
    );

    try {
      const result = await this.messageRepository.updateCallStatus(
        messageId,
        updates,
      );
      console.log(
        `✅ Statut d'appel mis à jour: ${messageId} → ${updates.status}`,
      );

      // ✅ PUBLIER L'ÉVÉNEMENT VIA REDIS STREAM POUR TOUS LES PARTICIPANTS
      if (this.resilientService && updates.status) {
        try {
          const eventData = {
            event: "call.status.updated",
            messageId: String(messageId),
            callId: String(callId || ""),
            conversationId: String(conversationId || ""),
            status: updates.status,
            userId: String(userId || ""),
            participants: JSON.stringify(participants),
            startedAt: updates.startedAt ? updates.startedAt.toISOString() : "",
            endedAt: updates.endedAt ? updates.endedAt.toISOString() : "",
            duration: String(updates.duration || 0),
            endReason: updates.endReason || "",
            senderSocketId: senderSocketId || "", // ✅ Propager senderSocketId pour exclusion MDS
            timestamp: new Date().toISOString(),
          };

          await this.resilientService.addToStream(
            "chat:stream:events:call",
            eventData,
          );

          console.log(
            `📤 Événement call.status.updated publié dans stream: ${updates.status} (${participants.length} participant(s))`,
          );
        } catch (streamErr) {
          console.error(
            `❌ Erreur publication stream call status:`,
            streamErr.message,
          );
          // Non-bloquant : la mise à jour en base a déjà réussi
        }
      }

      return result;
    } catch (error) {
      console.error(
        `❌ Erreur UpdateCallStatus pour ${messageId}:`,
        error.message,
      );
      throw error;
    }
  }
}

module.exports = UpdateCallStatus;
