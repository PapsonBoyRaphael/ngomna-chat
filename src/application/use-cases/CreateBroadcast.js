const UserCacheService = require("../../infrastructure/services/UserCacheService");

class CreateBroadcast {
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
    broadcastId,
    name,
    adminIds,
    recipientIds,
    senderSocketId = null,
  }) {
    if (
      !broadcastId ||
      !name ||
      !Array.isArray(adminIds) ||
      adminIds.length === 0 ||
      !Array.isArray(recipientIds) ||
      recipientIds.length === 0
    ) {
      throw new Error("broadcastId, name, adminIds et recipientIds requis");
    }

    // ✅ Valider l'existence des utilisateurs via UserCacheService
    const participants = [
      ...adminIds,
      ...recipientIds.filter((id) => !adminIds.includes(id)),
    ];
    let usersInfo = [];
    try {
      console.log(
        `🔍 Validation des ${participants.length} participants du broadcast...`,
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
      console.log(`✅ Tous les participants du broadcast sont valides`, {
        count: usersInfo.length,
        admins: adminIds.length,
        recipients: recipientIds.length,
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
        name: "Utilisateur inconnu",
        avatar: null,
        matricule: participantId,
        departement: null,
        ministere: null,
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
        name: userInfo.name,
        avatar: userInfo.avatar,
        departement: userInfo.departement || null,
        ministere: userInfo.ministere || null,
      };
    });

    const totalRecipients = participants.filter(
      (id) => id !== adminIds[0],
    ).length;

    const conversationData = {
      _id: broadcastId,
      name,
      type: "BROADCAST",
      participants,
      createdBy: adminIds[0],
      createdAt: new Date(),
      updatedAt: new Date(),
      lastMessage: null,
      isActive: true,
      unreadCounts,
      userMetadata,
      totalRecipients,
      metadata: {
        autoCreated: true,
        createdFrom: "CreateBroadcast",
        version: 1,
        tags: [],
        auditLog: [
          {
            action: "CREATED",
            userId: adminIds[0],
            timestamp: new Date(),
            details: { trigger: "broadcast_create" },
            metadata: { source: "CreateBroadcast-UseCase" },
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
        allowInvites: false,
        isPublic: false,
        maxParticipants: 200,
        messageRetention: 0,
        autoDeleteAfter: 0,
        broadcastAdmins: adminIds,
        broadcastRecipients: recipientIds,
      },
    };

    const savedConversation =
      await this.conversationRepository.save(conversationData);

    // ✅ CRÉER LES CONVERSATIONS PRIVÉES (admin↔chaque destinataire)
    // et stocker le mapping dans broadcastMetadata
    const senderId = adminIds[0];
    const senderInfo = usersInfo.find((u) => u.userId === senderId) || {};
    const privateConversationEntries = [];

    for (const recipientId of recipientIds) {
      try {
        // Vérifier si une conv privée existe déjà
        let privateConv =
          await this.conversationRepository.findPrivateConversation(
            senderId,
            recipientId,
          );

        if (!privateConv) {
          const recipientInfo =
            usersInfo.find((u) => u.userId === recipientId) || {};
          const privateConvData = {
            name: `Conversation ${senderId} - ${recipientId}`,
            type: "PRIVATE",
            participants: [senderId, recipientId],
            createdBy: senderId,
            settings: {
              allowInvites: false,
              isPublic: false,
              maxParticipants: 2,
              messageRetention: 0,
              autoDeleteAfter: 0,
            },
            userMetadata: [
              {
                userId: senderId,
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
                nom: senderInfo.nom || null,
                prenom: senderInfo.prenom || null,
                sexe: senderInfo.sexe || null,
                avatar: senderInfo.avatar || null,
                ministere: senderInfo.ministere || null,
              },
              {
                userId: recipientId,
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
                nom: recipientInfo.nom || null,
                prenom: recipientInfo.prenom || null,
                sexe: recipientInfo.sexe || null,
                avatar: recipientInfo.avatar || null,
                ministere: recipientInfo.ministere || null,
              },
            ],
          };

          privateConv = await this.conversationRepository.save(privateConvData);

          // Publier événement de création de la conv privée
          if (this.resilientMessageService) {
            this.resilientMessageService
              .addToStream("chat:stream:events:conversation:created", {
                event: "conversation.created",
                conversationId: privateConv._id.toString(),
                type: "PRIVATE",
                createdBy: senderId,
                participants: JSON.stringify([senderId, recipientId]),
                name: privateConvData.name,
                participantCount: "2",
                senderSocketId: senderSocketId || "",
                timestamp: Date.now().toString(),
              })
              .catch((err) =>
                console.warn(
                  `⚠️ Erreur publication conv privée broadcast:`,
                  err.message,
                ),
              );
          }

          console.log(
            `✅ Conv privée créée: ${senderId}↔${recipientId} (${privateConv._id})`,
          );
        } else {
          console.log(
            `ℹ️ Conv privée existante réutilisée: ${senderId}↔${recipientId} (${privateConv._id})`,
          );
        }

        privateConversationEntries.push({
          recipientId: String(recipientId),
          conversationId: (privateConv._id || privateConv.id).toString(),
        });
      } catch (err) {
        console.error(
          `❌ Erreur création conv privée broadcast pour ${recipientId}:`,
          err.message,
        );
      }
    }

    // Stocker le mapping dans broadcastMetadata
    if (privateConversationEntries.length > 0) {
      try {
        await this.conversationRepository.updateBroadcastMetadata(
          savedConversation._id,
          privateConversationEntries,
        );
      } catch (err) {
        console.warn(`⚠️ Erreur stockage broadcastMetadata:`, err.message);
      }
    }
    // ✅ Confirmation envoyée directement via socket.emit("broadcast:created") dans chatHandler
    // Pas de publication stream nécessaire : les destinataires ne sont pas notifiés de la création

    return savedConversation;
  }
}

module.exports = CreateBroadcast;
