const axios = require("axios");

/**
 * AutoGroupSyncService
 *
 * Service qui synchronise automatiquement les groupes de structure lors de la connexion d'un utilisateur.
 *
 * Fonctionnalités :
 * - Récupère les structures depuis le service de visibilité
 * - Crée les groupes de structure s'ils n'existent pas
 * - Ajoute l'utilisateur aux groupes dont il est membre
 * - Met à jour les métadonnées utilisateur dans les groupes
 */
class AutoGroupSyncService {
  constructor({
    conversationRepository,
    createGroupUseCase,
    addParticipantUseCase,
    userCacheService,
    visibilityServiceUrl = process.env.VISIBILITY_SERVICE_URL,
    includeHierarchy = process.env.AUTO_GROUP_SYNC_INCLUDE_HIERARCHY !==
      "false",
  }) {
    this.conversationRepository = conversationRepository;
    this.createGroupUseCase = createGroupUseCase;
    this.addParticipantUseCase = addParticipantUseCase;
    this.userCacheService = userCacheService;
    this.visibilityServiceUrl = visibilityServiceUrl;
    this.includeHierarchy = includeHierarchy;
  }

  /**
   * Point d'entrée principal : synchroniser les groupes pour un utilisateur
   */
  async syncUserGroups(userId, senderSocketId = null) {
    console.log(
      `🔄 [AutoGroupSync] Synchronisation des groupes pour: ${userId}`,
    );

    try {
      // 1. Récupérer les structures depuis le service de visibilité
      const visibilityData = await this.fetchUserStructures(userId);

      if (!visibilityData || !visibilityData.groups) {
        console.log(
          `⚠️ [AutoGroupSync] Aucune structure trouvée pour: ${userId}`,
        );
        return {
          success: true,
          created: 0,
          joined: 0,
          skipped: 0,
          message: "Aucune structure à synchroniser",
        };
      }

      // 2. Récupérer les infos de l'utilisateur
      const userInfo = await this.getUserInfo(userId);

      // 3. Sélectionner les groupes à synchroniser.
      // Par défaut on synchronise les groupes où l'utilisateur est membre.
      // Si `includeHierarchy` est activé, on ajoute également la hiérarchie des parents fournie par le service de visibilité.
      let memberGroups = visibilityData.groups.filter((g) => g.is_member);

      if (this.includeHierarchy && visibilityData.grouped) {
        // Ajouter la 'ma_structure' (si présente) et tous les parents listés
        const grouped = visibilityData.grouped || {};
        const parents = Array.isArray(grouped.parents) ? grouped.parents : [];
        const ma = Array.isArray(grouped.ma_structure)
          ? grouped.ma_structure
          : [];

        // Construire une map pour éviter doublons
        const map = new Map();
        memberGroups.forEach((g) => map.set(g.id, g));
        ma.concat(parents).forEach((g) => {
          if (g && g.id && !map.has(g.id)) map.set(g.id, g);
        });

        memberGroups = Array.from(map.values());
      }

      console.log(
        `📋 [AutoGroupSync] ${memberGroups.length} groupes à synchroniser pour ${userId}`,
      );

      // 4. Synchroniser chaque groupe
      const results = {
        success: true,
        created: 0,
        joined: 0,
        skipped: 0,
        errors: [],
      };

      for (const groupData of memberGroups) {
        // delai de 0.5s pour chaque groupe afin d'éviter les problèmes de charge ou de rate limit
        await new Promise((resolve) => setTimeout(resolve, 500));
        try {
          const result = await this.syncSingleGroup(
            groupData,
            userId,
            userInfo,
            senderSocketId,
          );

          if (result.created) results.created++;
          else if (result.joined) results.joined++;
          else if (result.skipped) results.skipped++;
        } catch (error) {
          console.error(
            `❌ [AutoGroupSync] Erreur sync groupe ${groupData.id}:`,
            error.message,
          );
          results.errors.push({
            groupId: groupData.id,
            error: error.message,
          });
        }
      }

      console.log(
        `✅ [AutoGroupSync] Synchronisation terminée pour ${userId}:`,
        results,
      );

      return results;
    } catch (error) {
      console.error(
        `❌ [AutoGroupSync] Erreur globale sync pour ${userId}:`,
        error.message,
      );
      throw error;
    }
  }

  /**
   * Synchroniser un seul groupe de structure
   */
  async syncSingleGroup(groupData, userId, userInfo, senderSocketId) {
    // Le groupId ne doit pas être basé sur le code_structure ni avoir de préfixe auto_structure
    // On laisse Mongo générer l'_id automatiquement
    console.log(
      `🔍 [AutoGroupSync] Vérification du groupe (code_structure: ${groupData.code_structure}) (${groupData.name})`,
    );

    // 1. Vérifier si le groupe existe déjà par code_structure
    let conversation = null;
    if (groupData.code_structure) {
      conversation = await this.conversationRepository.findOne({
        code_structure: groupData.code_structure,
      });
    }

    if (!conversation) {
      // 2a. Le groupe n'existe pas → Le créer VIDE
      console.log(
        `➕ [AutoGroupSync] Création du groupe institutionnel (code_structure: ${groupData.code_structure})`,
      );

      conversation = await this.createGroupUseCase.execute({
        // groupId non fourni → Mongo génère un ObjectId
        name: groupData.name,
        type: this.mapGroupType(groupData.group_type),
        adminId: "SYSTEM", // ✅ Créateur système
        members: [], // ✅ Créé VIDE
        finalAdmins: ["SYSTEM"],
        senderSocketId,
        userInfo,
        autoCreated: true, // ✅ Flag pour identifier les groupes auto-créés
        code_structure: groupData.code_structure, // Ajout direct pour le modèle
        structureMetadata: {
          codeStructure: groupData.code_structure,
          groupType: groupData.group_type,
          niveauLocal: groupData.niveau_local,
          niveauGlobal: groupData.niveau_global,
          description: groupData.description,
        },
      });

      console.log(
        `✅ [AutoGroupSync] Groupe créé (code_structure: ${groupData.code_structure})`,
      );

      return { created: true, joined: true, skipped: false };
    } else {
      // 2b. Le groupe existe déjà
      const convId = conversation._id
        ? conversation._id.toString()
        : groupData.id || groupData.code_structure || "unknown";
      console.log(`✓ [AutoGroupSync] Groupe existant: ${convId}`);

      // 3. Vérifier si l'utilisateur est déjà membre
      if (conversation.participants.includes(userId)) {
        console.log(
          `⏭️ [AutoGroupSync] Utilisateur ${userId} déjà membre de ${convId}`,
        );

        // 4. Vérifier si les métadonnées utilisateur sont à jour
        const needsUpdate = await this.checkUserMetadataUpdate(
          conversation,
          userId,
          userInfo,
        );

        if (needsUpdate) {
          await this.updateUserMetadata(conversation, userId, userInfo);
          console.log(
            `🔄 [AutoGroupSync] Métadonnées mises à jour pour ${userId} dans ${convId}`,
          );
        }

        return { created: false, joined: false, skipped: true };
      } else {
        // 5. L'utilisateur n'est pas membre → L'ajouter
        console.log(
          `➕ [AutoGroupSync] Ajout de ${userId} au groupe ${convId}`,
        );

        await this.addUserToGroup(
          conversation,
          userId,
          userInfo,
          senderSocketId,
        );

        return { created: false, joined: true, skipped: false };
      }
    }
  }

  /**
   * Ajouter un utilisateur à un groupe existant
   */
  async addUserToGroup(conversation, userId, userInfo, senderSocketId) {
    try {
      await this.addParticipantUseCase.execute({
        conversationId: conversation._id.toString(),
        participantId: userId,
        addedBy: "SYSTEM", // ✅ Ajout automatique par le système
        createdBy: "SYSTEM",
        adminId: "SYSTEM",
        senderSocketId,
        autoAdded: true, // ✅ Flag pour identifier les ajouts automatiques
        userInfo, // ✅ Passer les infos utilisateur
      });

      console.log(
        `✅ [AutoGroupSync] Utilisateur ${userId} ajouté au groupe ${conversation._id}`,
      );
    } catch (error) {
      // Si l'erreur est "déjà membre", on l'ignore
      if (error.message.includes("déjà membre")) {
        console.log(
          `⏭️ [AutoGroupSync] ${userId} déjà membre de ${conversation._id}`,
        );
      } else {
        throw error;
      }
    }
  }

  /**
   * Vérifier si les métadonnées utilisateur doivent être mises à jour
   */
  async checkUserMetadataUpdate(conversation, userId, userInfo) {
    if (
      !conversation.userMetadata ||
      !Array.isArray(conversation.userMetadata)
    ) {
      return true;
    }

    const existingMeta = conversation.userMetadata.find(
      (m) => m.userId === userId,
    );

    if (!existingMeta) {
      return true;
    }

    // Vérifier si des champs ont changé
    const fieldsChanged =
      existingMeta.nom !== userInfo.nom ||
      existingMeta.prenom !== userInfo.prenom ||
      existingMeta.sexe !== userInfo.sexe ||
      existingMeta.avatar !== userInfo.avatar ||
      existingMeta.ministere !== userInfo.ministere;

    return fieldsChanged;
  }

  /**
   * Mettre à jour les métadonnées utilisateur dans une conversation
   */
  async updateUserMetadata(conversation, userId, userInfo) {
    const metaIndex = conversation.userMetadata.findIndex(
      (m) => m.userId === userId,
    );

    if (metaIndex !== -1) {
      // Mettre à jour les champs
      conversation.userMetadata[metaIndex].nom = userInfo.nom || null;
      conversation.userMetadata[metaIndex].prenom = userInfo.prenom || null;
      conversation.userMetadata[metaIndex].sexe = userInfo.sexe || null;
      conversation.userMetadata[metaIndex].avatar = userInfo.avatar || null;
      conversation.userMetadata[metaIndex].ministere =
        userInfo.ministere || null;

      // Sauvegarder
      // Assurer que l'objet passé à la repository est un plain object
      let payload = conversation;
      if (typeof conversation.toObject === "function") {
        payload = conversation.toObject();
      } else {
        payload = { ...conversation };
      }

      // Garantir que participants et userMetadata sont des arrays
      payload.participants = Array.isArray(payload.participants)
        ? payload.participants
        : [];
      payload.userMetadata = Array.isArray(payload.userMetadata)
        ? payload.userMetadata
        : [];

      await this.conversationRepository.save(payload);

      console.log(`✅ [AutoGroupSync] Métadonnées mises à jour pour ${userId}`);
    }
  }

  /**
   * Récupérer les structures depuis le service de visibilité
   */
  async fetchUserStructures(userId) {
    try {
      const url = `${this.visibilityServiceUrl}/api/visibility/groups-for-user/${userId}`;

      console.log(`🌐 [AutoGroupSync] Récupération des structures: ${url}`);

      const response = await axios.get(url, {
        timeout: 5000,
        headers: {
          Accept: "application/json",
        },
      });

      // Correction : vérifier la présence de data et data.groups
      if (
        !response.data ||
        !response.data.success ||
        !response.data.data ||
        !Array.isArray(response.data.data.groups)
      ) {
        console.error(
          "❌ [AutoGroupSync] Réponse inattendue:",
          JSON.stringify(response.data),
        );
        throw new Error("Réponse invalide du service de visibilité");
      }

      console.log(
        `✅ [AutoGroupSync] ${response.data.data.groups.length} structures récupérées`,
      );

      return response.data.data;
    } catch (error) {
      if (error.code === "ECONNREFUSED") {
        console.error(
          `❌ [AutoGroupSync] Service de visibilité non disponible: ${this.visibilityServiceUrl}`,
        );
        throw new Error("Service de visibilité non disponible");
      }

      console.error(
        `❌ [AutoGroupSync] Erreur récupération structures:`,
        error.message,
      );
      throw error;
    }
  }

  /**
   * Récupérer les informations d'un utilisateur
   */
  async getUserInfo(userId) {
    try {
      if (!this.userCacheService) {
        console.warn("⚠️ [AutoGroupSync] UserCacheService non disponible");
        return {
          userId,
          nom: null,
          prenom: null,
          sexe: null,
          avatar: null,
          ministere: null,
          matricule: userId,
        };
      }

      const users = await this.userCacheService.fetchUsersInfo([userId]);

      if (!users || users.length === 0) {
        throw new Error(`Utilisateur ${userId} introuvable`);
      }

      return users[0];
    } catch (error) {
      console.error(
        `❌ [AutoGroupSync] Erreur récupération info utilisateur:`,
        error.message,
      );
      throw error;
    }
  }

  /**
   * Mapper le type de groupe du service de visibilité vers le type MongoDB
   */
  mapGroupType(visibilityGroupType) {
    const mapping = {
      auto_structure: "GROUP",
      auto_parent_direct: "GROUP",
      auto_parent: "GROUP",
      auto_soeurs: "GROUP",
      auto_enfants: "GROUP",
      gouvernement: "CHANNEL",
      diccg: "CHANNEL",
      comite: "GROUP",
    };

    return mapping[visibilityGroupType] || "GROUP";
  }

  /**
   * Synchroniser tous les utilisateurs connectés (cron job)
   */
  async syncAllConnectedUsers(connectedUsers) {
    console.log(
      `🔄 [AutoGroupSync] Synchronisation de ${connectedUsers.length} utilisateurs connectés`,
    );

    const results = {
      total: connectedUsers.length,
      success: 0,
      errors: 0,
      details: [],
    };

    for (const userId of connectedUsers) {
      try {
        const result = await this.syncUserGroups(userId);
        results.success++;
        results.details.push({ userId, ...result });
      } catch (error) {
        results.errors++;
        console.error(
          `❌ [AutoGroupSync] Erreur sync ${userId}:`,
          error.message,
        );
        results.details.push({
          userId,
          success: false,
          error: error.message,
        });
      }
    }

    console.log(`✅ [AutoGroupSync] Synchronisation terminée:`, {
      total: results.total,
      success: results.success,
      errors: results.errors,
    });

    return results;
  }

  /**
   * Supprimer un utilisateur de tous ses groupes auto (déconnexion ou mutation)
   */
  async removeUserFromAutoGroups(userId) {
    console.log(`🗑️ [AutoGroupSync] Suppression de ${userId} des groupes auto`);

    try {
      // Trouver toutes les conversations auto dont l'utilisateur est membre
      const conversations = await this.conversationRepository.findByParticipant(
        userId,
        {
          type: "GROUP",
          includeArchived: false,
        },
      );

      const autoConversations = conversations.filter(
        (c) => c.metadata?.autoCreated === true,
      );

      let removed = 0;

      for (const conv of autoConversations) {
        try {
          // Retirer l'utilisateur
          const index = conv.participants.indexOf(userId);
          if (index !== -1) {
            conv.participants.splice(index, 1);

            // Retirer les métadonnées
            conv.userMetadata = conv.userMetadata.filter(
              (m) => m.userId !== userId,
            );

            // Audit log
            conv.metadata.auditLog.push({
              action: "AUTO_PARTICIPANT_REMOVED",
              userId: "SYSTEM",
              timestamp: new Date(),
              details: {
                participantId: userId,
                reason: "structure_change_or_disconnect",
              },
            });

            await this.conversationRepository.save(conv);
            removed++;
          }
        } catch (error) {
          console.error(`❌ Erreur suppression de ${conv._id}:`, error.message);
        }
      }

      console.log(
        `✅ [AutoGroupSync] ${userId} supprimé de ${removed} groupes auto`,
      );

      return { success: true, removed };
    } catch (error) {
      console.error(
        `❌ [AutoGroupSync] Erreur suppression globale:`,
        error.message,
      );
      throw error;
    }
  }
}

module.exports = AutoGroupSyncService;
