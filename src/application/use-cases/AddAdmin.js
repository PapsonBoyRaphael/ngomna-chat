/**
 * AddAdmin - Promeut un ou plusieurs membres comme admins dans un groupe ou channel.
 *
 * Règles métier :
 *   - Seul le créateur ou un admin existant peut promouvoir.
 *   - Le membre cible doit déjà être participant.
 *   - Il ne peut pas être déjà admin.
 *   - Applicable uniquement aux types GROUP et CHANNEL.
 *
 * Publication Redis Streams : chat:stream:events:conversations
 */
class AddAdmin {
  /**
   * @param {Object} conversationRepository
   * @param {Object|null} resilientMessageService
   */
  constructor(conversationRepository, resilientMessageService = null) {
    this.conversationRepository = conversationRepository;
    this.resilientMessageService = resilientMessageService;
  }

  /**
   * @param {Object} params
   * @param {string}          params.conversationId  - ID du groupe / channel
   * @param {string|string[]} params.userIds         - ID(s) du ou des membres à promouvoir
   * @param {string}          params.promotedBy      - ID de l'utilisateur qui fait la promotion
   * @returns {Promise<Object>} La conversation mise à jour
   */
  async execute({ conversationId, userIds, promotedBy }) {
    // ── Validation des paramètres ──────────────────────────────────────────
    if (!conversationId || !userIds || !promotedBy) {
      throw new Error("conversationId, userIds et promotedBy sont requis");
    }

    const targetIds = Array.isArray(userIds) ? userIds : [userIds];
    if (targetIds.length === 0) {
      throw new Error("userIds ne peut pas être vide");
    }

    // ── Récupération de la conversation ───────────────────────────────────
    const conversation =
      await this.conversationRepository.findById(conversationId);

    if (!conversation) {
      throw new Error("Conversation introuvable");
    }

    // ── Vérification du type ──────────────────────────────────────────────
    if (!["GROUP", "CHANNEL"].includes(conversation.type)) {
      throw new Error(
        "La promotion d'admins n'est possible que dans les groupes et les channels",
      );
    }

    // ── Liste courante des admins ─────────────────────────────────────────
    // Pour GROUP et CHANNEL, les admins sont stockés dans settings.broadcastAdmins
    if (!conversation.settings) {
      conversation.settings = {};
    }
    if (!Array.isArray(conversation.settings.broadcastAdmins)) {
      conversation.settings.broadcastAdmins = [];
    }

    const currentAdmins = conversation.settings.broadcastAdmins;

    // ── Vérification des permissions du demandeur ────────────────────────
    const isCreator = String(conversation.createdBy) === String(promotedBy);
    const isExistingAdmin = currentAdmins
      .map(String)
      .includes(String(promotedBy));

    if (!isCreator && !isExistingAdmin) {
      throw new Error(
        "Seul le créateur ou un admin existant peut promouvoir des membres",
      );
    }

    // ── Traitement de chaque cible ────────────────────────────────────────
    const promoted = [];
    const skipped = [];

    for (const userId of targetIds) {
      // Doit être participant
      const isParticipant = (conversation.participants || [])
        .map(String)
        .includes(String(userId));

      if (!isParticipant) {
        throw new Error(
          `L'utilisateur ${userId} n'est pas membre de ce groupe`,
        );
      }

      // Déjà admin ?
      if (currentAdmins.map(String).includes(String(userId))) {
        skipped.push(userId);
        continue;
      }

      conversation.settings.broadcastAdmins.push(String(userId));
      promoted.push(userId);
    }

    if (promoted.length === 0) {
      return {
        conversation,
        promoted: [],
        skipped,
        message: "Tous les utilisateurs étaient déjà admins",
      };
    }

    // ── Audit log ─────────────────────────────────────────────────────────
    if (!conversation.metadata) {
      conversation.metadata = {};
    }
    if (!Array.isArray(conversation.metadata.auditLog)) {
      conversation.metadata.auditLog = [];
    }

    conversation.metadata.auditLog.push({
      action: "PERMISSIONS_CHANGED",
      userId: promotedBy,
      timestamp: new Date(),
      details: {
        operation: "ADD_ADMIN",
        promoted,
        skipped,
        adminCount: conversation.settings.broadcastAdmins.length,
      },
      metadata: { source: "AddAdmin-UseCase" },
    });

    conversation.updatedAt = new Date();

    // ── Sauvegarde ────────────────────────────────────────────────────────
    const updated = await this.conversationRepository.save(conversation);

    console.log(
      `✅ [AddAdmin] ${promoted.length} admin(s) ajouté(s) dans ${conversationId}:`,
      promoted,
    );

    // ── Publication Redis Streams ─────────────────────────────────────────
    if (this.resilientMessageService && promoted.length > 0) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:conversations",
          {
            event: "conversation.admins.updated",
            conversationId: conversationId.toString(),
            operation: "ADD_ADMIN",
            promoted: JSON.stringify(promoted),
            skipped: JSON.stringify(skipped),
            promotedBy,
            adminCount: conversation.settings.broadcastAdmins.length.toString(),
            participants: JSON.stringify(conversation.participants),
            timestamp: Date.now().toString(),
          },
        );
        console.log(
          `📤 [conversation.admins.updated] publié dans chat:stream:events:conversations`,
        );
      } catch (streamErr) {
        console.warn(
          "⚠️ Erreur publication stream conversation.admins.updated:",
          streamErr.message,
        );
      }
    }

    return {
      conversation: updated,
      promoted,
      skipped,
    };
  }
}

module.exports = AddAdmin;
