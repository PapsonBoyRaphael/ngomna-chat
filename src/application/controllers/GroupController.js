class GroupController {
  /**
   * @param {Object} useCases
   * @param {import("../use-cases/CreateGroup")}         useCases.createGroupUseCase
   * @param {import("../use-cases/GetConversation")}     useCases.getConversationUseCase
   * @param {import("../use-cases/AddParticipant")}      useCases.addParticipantUseCase
   * @param {import("../use-cases/RemoveParticipant")}   useCases.removeParticipantUseCase
   * @param {import("../use-cases/LeaveConversation")}   useCases.leaveConversationUseCase
   * @param {import("../use-cases/AddAdmin")}            useCases.addAdminUseCase
   * @param {import("../use-cases/SearchOccurrences")}   useCases.searchOccurrencesUseCase
   */
  constructor({
    createGroupUseCase,
    getConversationUseCase,
    addParticipantUseCase,
    removeParticipantUseCase,
    leaveConversationUseCase,
    addAdminUseCase = null,
    searchOccurrencesUseCase = null,
  }) {
    this.createGroupUseCase = createGroupUseCase;
    this.getConversationUseCase = getConversationUseCase;
    this.addParticipantUseCase = addParticipantUseCase;
    this.removeParticipantUseCase = removeParticipantUseCase;
    this.leaveConversationUseCase = leaveConversationUseCase;
    this.addAdminUseCase = addAdminUseCase;
    this.searchOccurrencesUseCase = searchOccurrencesUseCase;
  }

  // =========================================================
  // POST /groups
  // =========================================================
  async createGroup(req, res) {
    try {
      const { groupId, name, type, adminId, members, finalAdmins } = req.body;

      if (
        !name ||
        !adminId ||
        !Array.isArray(members) ||
        members.length === 0
      ) {
        return res.status(400).json({
          success: false,
          message: "name, adminId et members (tableau non vide) sont requis",
          code: "MISSING_REQUIRED_FIELDS",
        });
      }

      const group = await this.createGroupUseCase.execute({
        groupId: groupId || null,
        name,
        type,
        adminId,
        members,
        finalAdmins: finalAdmins || [],
      });

      return res.status(201).json({
        success: true,
        message: "Groupe créé avec succès",
        data: group,
      });
    } catch (error) {
      console.error("❌ Erreur createGroup:", error);
      const statusCode = error.message.includes("invalides") ? 400 : 500;
      return res.status(statusCode).json({
        success: false,
        message: "Erreur lors de la création du groupe",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // =========================================================
  // GET /groups/:groupId
  // =========================================================
  async getGroup(req, res) {
    try {
      const { groupId } = req.params;
      const userId = req.user?.id || req.user?.userId || req.headers["user-id"];

      if (!userId) {
        return res.status(400).json({
          success: false,
          message: "ID utilisateur requis",
          code: "MISSING_USER_ID",
        });
      }

      const group = await this.getConversationUseCase.execute(groupId, userId);

      if (!group) {
        return res.status(404).json({
          success: false,
          message: "Groupe introuvable",
          code: "GROUP_NOT_FOUND",
        });
      }

      if (group.type !== "GROUP") {
        return res.status(400).json({
          success: false,
          message: "Cette conversation n'est pas un groupe",
          code: "NOT_A_GROUP",
        });
      }

      return res.json({
        success: true,
        data: group,
      });
    } catch (error) {
      console.error("❌ Erreur getGroup:", error);
      if (
        error.message.includes("non autorisé") ||
        error.message.includes("Accès")
      ) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "ACCESS_DENIED",
        });
      }
      if (
        error.message.includes("non trouvée") ||
        error.message.includes("introuvable")
      ) {
        return res.status(404).json({
          success: false,
          message: error.message,
          code: "GROUP_NOT_FOUND",
        });
      }
      return res.status(500).json({
        success: false,
        message: "Erreur lors de la récupération du groupe",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // =========================================================
  // POST /groups/:groupId/participants
  // =========================================================
  async addParticipant(req, res) {
    try {
      const { groupId } = req.params;
      const { participantId, addedBy } = req.body;
      const requesterId =
        addedBy || req.user?.id || req.user?.userId || req.headers["user-id"];

      if (!participantId) {
        return res.status(400).json({
          success: false,
          message: "participantId est requis",
          code: "MISSING_PARTICIPANT_ID",
        });
      }

      const result = await this.addParticipantUseCase.execute({
        conversationId: groupId,
        participantId,
        addedBy: requesterId,
      });

      return res.status(200).json({
        success: true,
        message: "Participant ajouté avec succès",
        data: result,
      });
    } catch (error) {
      console.error("❌ Erreur addParticipant:", error);
      if (error.message.includes("déjà membre")) {
        return res.status(409).json({
          success: false,
          message: error.message,
          code: "ALREADY_MEMBER",
        });
      }
      if (
        error.message.includes("Accès") ||
        error.message.includes("autorisé") ||
        error.message.includes("admin")
      ) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "FORBIDDEN",
        });
      }
      return res.status(400).json({
        success: false,
        message: "Erreur lors de l'ajout du participant",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // =========================================================
  // DELETE /groups/:groupId/participants/:participantId
  // =========================================================
  async removeParticipant(req, res) {
    try {
      const { groupId, participantId } = req.params;
      const removedBy =
        req.body?.removedBy ||
        req.user?.id ||
        req.user?.userId ||
        req.headers["user-id"];

      const result = await this.removeParticipantUseCase.execute({
        conversationId: groupId,
        participantId,
        removedBy,
      });

      return res.status(200).json({
        success: true,
        message: "Participant retiré avec succès",
        data: result,
      });
    } catch (error) {
      console.error("❌ Erreur removeParticipant:", error);
      if (
        error.message.includes("autorisé") ||
        error.message.includes("admin") ||
        error.message.includes("Seul")
      ) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "FORBIDDEN",
        });
      }
      if (error.message.includes("n'est pas membre")) {
        return res.status(404).json({
          success: false,
          message: error.message,
          code: "NOT_A_MEMBER",
        });
      }
      return res.status(400).json({
        success: false,
        message: "Erreur lors du retrait du participant",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // =========================================================
  // POST /groups/:groupId/leave
  // =========================================================
  async leaveGroup(req, res) {
    try {
      const { groupId } = req.params;
      const userId =
        req.body?.userId ||
        req.user?.id ||
        req.user?.userId ||
        req.headers["user-id"];

      if (!userId) {
        return res.status(400).json({
          success: false,
          message: "ID utilisateur requis",
          code: "MISSING_USER_ID",
        });
      }

      const result = await this.leaveConversationUseCase.execute({
        conversationId: groupId,
        userId,
      });

      return res.status(200).json({
        success: true,
        message: "Vous avez quitté le groupe avec succès",
        data: result,
      });
    } catch (error) {
      console.error("❌ Erreur leaveGroup:", error);
      if (error.message.includes("créateur")) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "CREATOR_CANNOT_LEAVE",
        });
      }
      if (
        error.message.includes("pas membre") ||
        error.message.includes("n'êtes pas")
      ) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "NOT_A_MEMBER",
        });
      }
      return res.status(400).json({
        success: false,
        message: "Erreur lors de la sortie du groupe",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }

  // =========================================================
  // GET /groups/search?q=...
  // =========================================================
  async searchOccurrences(req, res) {
    const startTime = Date.now();

    if (!this.searchOccurrencesUseCase) {
      return res.status(503).json({
        success: false,
        message: "Service de recherche non disponible",
        code: "SEARCH_UNAVAILABLE",
      });
    }

    try {
      const { query, page = 1, limit = 20, useLike = true } = req.query;
      const userId = req.user?.id || req.user?.userId || req.headers["user-id"];

      if (!query || query.length < 2) {
        return res.status(400).json({
          success: false,
          message:
            "Le mot-clé de recherche doit contenir au moins 2 caractères",
          code: "INVALID_QUERY",
        });
      }

      const result = await this.searchOccurrencesUseCase.execute(query, {
        userId,
        page: parseInt(page),
        limit: parseInt(limit),
        useLike,
        scope: "groups",
      });

      return res.json({
        success: true,
        data: result,
        metadata: {
          processingTime: `${Date.now() - startTime}ms`,
          timestamp: new Date().toISOString(),
        },
      });
    } catch (error) {
      console.error("❌ Erreur searchOccurrences (groups):", error);
      return res.status(500).json({
        success: false,
        message: "Erreur lors de la recherche globale",
        error: error.message,
      });
    }
  }

  // =========================================================
  // POST /groups/:groupId/admins
  // =========================================================
  async addAdmin(req, res) {
    if (!this.addAdminUseCase) {
      return res.status(503).json({
        success: false,
        message: "Service de gestion des admins non disponible",
        code: "ADD_ADMIN_UNAVAILABLE",
      });
    }

    try {
      const { groupId } = req.params;
      const { userIds, promotedBy } = req.body;
      const requesterId =
        promotedBy ||
        req.user?.id ||
        req.user?.userId ||
        req.headers["user-id"];

      if (!userIds) {
        return res.status(400).json({
          success: false,
          message: "userIds est requis (string ou tableau de strings)",
          code: "MISSING_USER_IDS",
        });
      }

      if (!requesterId) {
        return res.status(400).json({
          success: false,
          message: "ID de l'utilisateur demandeur requis",
          code: "MISSING_PROMOTED_BY",
        });
      }

      const result = await this.addAdminUseCase.execute({
        conversationId: groupId,
        userIds,
        promotedBy: requesterId,
      });

      return res.status(200).json({
        success: true,
        message:
          result.promoted.length > 0
            ? `${result.promoted.length} admin(s) ajouté(s) avec succès`
            : result.message || "Aucun changement effectué",
        data: {
          promoted: result.promoted,
          skipped: result.skipped,
          adminCount: result.conversation?.settings?.broadcastAdmins?.length,
        },
      });
    } catch (error) {
      console.error("❌ Erreur addAdmin:", error);
      if (
        error.message.includes("autorisé") ||
        error.message.includes("créateur") ||
        error.message.includes("admin existant")
      ) {
        return res.status(403).json({
          success: false,
          message: error.message,
          code: "FORBIDDEN",
        });
      }
      if (
        error.message.includes("n'est pas membre") ||
        error.message.includes("introuvable")
      ) {
        return res.status(404).json({
          success: false,
          message: error.message,
          code: "NOT_FOUND",
        });
      }
      if (error.message.includes("groupes et les channels")) {
        return res.status(400).json({
          success: false,
          message: error.message,
          code: "INVALID_CONVERSATION_TYPE",
        });
      }
      return res.status(500).json({
        success: false,
        message: "Erreur lors de l'ajout d'admin",
        error:
          process.env.NODE_ENV === "development"
            ? error.message
            : "Erreur interne",
      });
    }
  }
}

module.exports = GroupController;
