const UserCacheService = require("../../infrastructure/services/UserCacheService");

class CreateGroup {
  constructor(
    conversationRepository,
    resilientMessageService = null,
    userCacheService = null,
  ) {
    this.conversationRepository = conversationRepository;
    this.resilientMessageService = resilientMessageService;
    this.userCacheService = userCacheService || new UserCacheService();
  }

  async execute({
    groupId = null,
    name,
    type,
    adminId,
    members,
    finalAdmins = [],
    senderSocketId = null,
  }) {
    if (!name || !adminId || !Array.isArray(members) || members.length === 0) {
      throw new Error("name, adminId et members requis");
    }

    // ✅ Valider l'existence des utilisateurs via UserCacheService
    const participants = [adminId, ...members.filter((id) => id !== adminId)];
    let usersInfo = [];
    try {
      console.log(
        `🔍 Validation des ${participants.length} participants du groupe...`,
      );
      usersInfo = await this.userCacheService.fetchUsersInfo(participants);

      // ✅ VÉRIFIER LA PERMISSION DE CRÉER UN GROUPE (si pas autoCreated)
      if (!autoCreated) {
        try {
          console.log(
            `🔐 Vérification permission création groupe pour: ${adminId}`,
          );
          const visibilityResponse = await fetch(
            `${process.env.VISIBILITY_API_URL}/api/visibility/contacts/${adminId}`,
          );

          if (!visibilityResponse.ok) {
            throw new Error(
              `Erreur API visibility: ${visibilityResponse.status}`,
            );
          }

          const visibilityData = await visibilityResponse.json();

          if (!visibilityData.success || !visibilityData.data?.agent) {
            throw new Error("Impossible de récupérer les permissions");
          }

          const agent = visibilityData.data.agent;

          // Vérifier la permission peut_creer_groupe
          if (!agent.peut_creer_groupe) {
            throw new Error(
              `L'utilisateur ${adminId} n'a pas la permission de créer des groupes`,
            );
          }

          // Vérifier la taille maximale du groupe
          const totalParticipants = members.length + 1; // +1 pour l'admin
          if (
            agent.taille_max_groupe > 0 &&
            totalParticipants > agent.taille_max_groupe
          ) {
            throw new Error(
              `Taille maximale du groupe dépassée: ${totalParticipants}/${agent.taille_max_groupe}`,
            );
          }

          console.log(`✅ Permission validée pour ${adminId}:`, {
            peut_creer_groupe: agent.peut_creer_groupe,
            taille_max_groupe: agent.taille_max_groupe,
            totalParticipants,
          });
        } catch (permissionError) {
          console.error(
            `❌ Erreur vérification permissions:`,
            permissionError.message,
          );
          throw new Error(`Permission refusée: ${permissionError.message}`);
        }
      }

      // Vérifier que tous les utilisateurs existent
      const invalidUsers = usersInfo.filter(
        (u) => u.name === "Utilisateur inconnu",
      );
      if (invalidUsers.length > 0) {
        const invalidIds = invalidUsers.map((u) => u.matricule).join(", ");
        throw new Error(`Utilisateurs invalides: ${invalidIds}`);
      }
      console.log(`✅ Tous les participants du groupe sont valides`, {
        count: usersInfo.length,
        users: usersInfo.map((u) => ({ id: u.userId, name: u.name })),
      });
    } catch (validationError) {
      console.error(
        `❌ Erreur validation participants:`,
        validationError.message,
      );
      throw new Error(
        `Impossible de valider les participants: ${validationError.message}`,
      );
    }

    // ✅ CRÉER userMetadata AVEC LES INFOS UTILISATEURS
    const unreadCounts = {};
    const userMetadata = participants.map((participantId) => {
      const userInfo = usersInfo.find((u) => u.userId === participantId) || {
        userId: participantId,

        nom: null,
        prenom: null,
        avatar: null,
        matricule: participantId,
        departement: null,
        ministere: null,
        sexe: null,
      };

      unreadCounts[participantId] = 0;

      return {
        userId: participantId,
        unreadCount: 0,
        lastReadAt: null,
        isMuted: false,
        isPinned: false,
        customName: null,
        notificationSettings: {
          enabled: true,
          sound: true,
          vibration: true,
        },
        // ✅ POPULATED À PARTIR DE UserCacheService
        nom: userInfo.nom || null,
        prenom: userInfo.prenom || null,
        sexe: userInfo.sexe || null,
        avatar: userInfo.avatar || null,
        departement: userInfo.departement || null,
        ministere: userInfo.ministere || null,
      };
    });

    const totalRecipients = participants.filter((id) => id !== adminId).length;

    const conversationData = {
      name,
      type: type || "GROUP",
      participants,
      createdBy: adminId,
      createdAt: new Date(),
      updatedAt: new Date(),
      lastMessage: null,
      isActive: true,
      unreadCounts,
      userMetadata,
      totalRecipients,
      metadata: {
        autoCreated: true,
        createdFrom: "CreateGroup",
        version: 1,
        tags: [],
        auditLog: [
          {
            action: "CREATED",
            userId: adminId,
            timestamp: new Date(),
            details: { trigger: "group_create" },
            metadata: { source: "CreateGroup-UseCase" },
          },
        ],
        stats: {
          totalMessages: 0,
          totalFiles: 0,
          totalParticipants: participants.length,
          lastActivity: new Date(),
        },
      },
      settings: {
        allowInvites: true,
        isPublic: false,
        maxParticipants: 200,
        messageRetention: 0,
        autoDeleteAfter: 0,
        broadcastAdmins: finalAdmins.length > 0 ? finalAdmins : [adminId],
        broadcastRecipients: finalAdmins.length > 0 ? participants : [], // Si des admins spécifiques sont définis, ce sont eux les destinataires de diffusion
      },
    };

    // ✅ groupId optionnel: si fourni, il devient l'_id métier; sinon Mongo génère automatiquement l'_id
    if (groupId) {
      conversationData._id = groupId;
    }

    console.log("userMetadata", userMetadata);

    const savedConversation =
      await this.conversationRepository.save(conversationData);

    // ✅ PUBLIER DANS REDIS STREAMS chat:stream:events:conversations
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:conversations",
          {
            event: "conversation.created",
            conversationId: savedConversation._id.toString(),
            type: "GROUP",
            createdBy: adminId,
            participants: JSON.stringify(participants),
            name: name,
            participantCount: participants.length.toString(),
            senderSocketId: senderSocketId || "", // ✅ Propager pour exclusion MDS
            timestamp: Date.now().toString(),
          },
        );
        console.log(
          `📤 [conversation.created] publié dans chat:stream:events:conversations`,
        );
      } catch (streamErr) {
        console.error(
          "❌ Erreur publication stream conversation.created:",
          streamErr.message,
        );
      }
    }

    // ✅ PUBLIER DANS LE STREAM REDIS POUR CONVERSATION CRÉÉE
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:conversation:created", // Nouveau stream pour les événements de conversation
          {
            event: "conversation.created",
            conversationId: savedConversation._id.toString(),
            conversation: savedConversation, // Inclure les détails de la conversation pour les consommateurs qui veulent plus d'infos
            type: type || "GROUP",
            createdBy: adminId,
            participants: JSON.stringify(participants),
            name: name,
            participantCount: participants.length.toString(),
            senderSocketId: senderSocketId || "", // ✅ Propager pour exclusion MDS
            timestamp: Date.now().toString(),
          },
        );
        console.log(
          `📤 [conversation.created] publié dans chat:stream:events:conversation:created`,
        );
      } catch (streamErr) {
        console.error(
          "❌ Erreur publication stream conversation.created:",
          streamErr.message,
        );
      }
    }

    return savedConversation;
  }
}

module.exports = CreateGroup;
