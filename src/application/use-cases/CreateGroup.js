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
    userInfo = null, // Optionnel: peut être fourni pour éviter un appel supplémentaire à UserCacheService
    autoCreated = false, // Flag pour indiquer si le groupe est créé automatiquement (ex: par un workflow)
    code_structure = null,
  }) {
    // Autoriser la création de groupes vides si autoCreated est true
    if (
      !name ||
      !adminId ||
      !Array.isArray(members) ||
      (members.length === 0 && !autoCreated)
    ) {
      throw new Error("name, adminId et members requis");
    }

    // ✅ VÉRIFIER LA PERMISSION DE CRÉER UN GROUPE (si pas autoCreated)
    if (!autoCreated) {
      try {
        console.log(
          `🔐 Vérification permission création groupe pour: ${adminId}`,
        );

        // Vérifier que l'URL du service de visibilité est configurée
        if (!process.env.VISIBILITY_SERVICE_URL) {
          console.warn(
            `⚠️ VISIBILITY_SERVICE_URL non configurée - vérification des permissions désactivée`,
          );
          // Continuer sans vérification si le service n'est pas configuré
          console.log(
            `✅ Permission création groupe accordée par défaut (service non configuré)`,
          );
        } else {
          const visibilityResponse = await fetch(
            `${process.env.VISIBILITY_SERVICE_URL}/api/visibility/contacts/${adminId}`,
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
        }
      } catch (permissionError) {
        console.error(
          `❌ Erreur vérification permissions:`,
          permissionError.message,
        );
        throw new Error(`Permission refusée: ${permissionError.message}`);
      }
    }

    // ✅ Valider l'existence des utilisateurs via UserCacheService
    // Déclarer les variables à portée de fonction pour réutilisation
    let participants = [];
    let usersInfo = [];
    let userMetadata = [];
    let unreadCounts = {};
    let totalRecipients = 0;

    if (!autoCreated) {
      participants = [adminId, ...members.filter((id) => id !== adminId)];
      try {
        console.log(
          `🔍 Validation des ${participants.length} participants du groupe...`,
        );
        usersInfo = await this.userCacheService.fetchUsersInfo(participants);

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
      userMetadata = participants.map((participantId) => {
        const userInfo = usersInfo.find((u) => u.userId === participantId) || {
          userId: participantId,
          nom: null,
          prenom: null,
          avatar: null,
          matricule: participantId,
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
          ministere: userInfo.ministere || null,
        };
      });

      totalRecipients = participants.filter((id) => id !== adminId).length;
    } else {
      // Pour les groupes auto-créés, on initialise userMetadata et unreadCounts à vide
      userMetadata = [
        {
          userId: userInfo?.userId,
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
          nom: userInfo?.nom || null,
          prenom: userInfo?.prenom || null,
          sexe: userInfo?.sexe || null,
          avatar: userInfo?.avatar || null,
          ministere: userInfo?.ministere || null,
        },
      ];
      unreadCounts = {};
      totalRecipients = 0;
      participants = [userInfo?.userId];
    }

    const conversationData = {
      name,
      type: type || "GROUP",
      // Stocker le code_structure séparément (peut être null)
      code_structure: code_structure || null,
      participants,
      createdBy: adminId || "SYSTEM",
      createdAt: new Date(),
      updatedAt: new Date(),
      lastMessage: null,
      isActive: true,
      unreadCounts,
      userMetadata,
      totalRecipients,
      metadata: {
        autoCreated: autoCreated,
        createdFrom: "CreateGroup",
        version: 1,
        tags: [],
        auditLog: [
          {
            action: "CREATED",
            userId: adminId || "SYSTEM",
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
            conversation: savedConversation, // Inclure les détails de la conversation pour les consommateurs qui veulent plus d'infos
            type: "GROUP",
            createdBy: adminId || "SYSTEM",
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

    // // ✅ PUBLIER NOTIFICATION SYSTÈME VIA RESILIENT MESSAGE SERVICE
    // if (this.resilientMessageService) {
    //   try {
    //     console.log(
    //       `📢 Publication notification système GROUP_CREATED pour: ${savedConversation._id}`,
    //     );

    //     await this.resilientMessageService.publishSystemMessage(
    //       {
    //         conversationId: String(savedConversation._id),
    //         type: "SYSTEM",
    //         subType: "GROUP_CREATED",
    //         senderId: adminId || "SYSTEM",
    //         senderName: "Système",
    //         content: `Le groupe "${name}" a été créé`,
    //         participants: participants,
    //         metadata: {
    //           event: "group_created",
    //           groupName: name,
    //           groupId: String(savedConversation._id),
    //           creatorId: adminId || "SYSTEM" ,
    //           participantCount: participants.length,
    //           timestamp: new Date().toISOString(),
    //         },
    //       },
    //       {
    //         eventType: "GROUP_CREATED",
    //         stream: "chat:stream:messages:group",
    //       },
    //     );
    //     console.log(
    //       `✅ Notification système GROUP_CREATED publiée pour: ${savedConversation._id}`,
    //     );
    //   } catch (notifError) {
    //     console.warn(
    //       "⚠️ Erreur publication notification GROUP_CREATED:",
    //       notifError.message,
    //     );
    //     // Ne pas bloquer la création si la notification échoue
    //   }
    // }

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
            createdBy: adminId || "SYSTEM",
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
