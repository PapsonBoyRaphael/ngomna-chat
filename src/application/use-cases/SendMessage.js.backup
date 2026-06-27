const axios = require("axios");

async function fetchUsersInfo(userIds) {
  // Adapter l’URL selon la config réseau/déploiement
  const baseUrl = process.env.AUTH_USER_SERVICE_URL || "http://localhost:8001";
  // Si tu as une route batch, préfère /users?ids=1,2,3
  // Sinon, fais plusieurs requêtes en parallèle
  const requests = userIds.map((id) =>
    axios
      .get(`${baseUrl}/${id}`)
      .then((res) => ({
        userId: id,
        name: res.data.nom
          ? `${res.data.prenom || ""} ${res.data.nom}`.trim()
          : res.data.name || res.data.username || "",
        avatar: res.data.profile_pic || res.data.avatar || null,
      }))
      .catch(() => ({
        userId: id,
        name: null,
        avatar: null,
      }))
  );
  return Promise.all(requests);
}

class SendMessage {
  constructor(
    messageRepository,
    conversationRepository,
    cacheService = null,
    resilientService = null
  ) {
    this.messageRepository = messageRepository;
    this.conversationRepository = conversationRepository;
    this.cacheService = cacheService;
    this.resilientService = resilientService;
  }

  // ✅ MODIFIER LA MÉTHODE execute() - RETIRER KAFKA
  async execute(messageData) {
    const startTime = Date.now();

    try {
      const {
        content,
        senderId,
        conversationId = "",
        type = "TEXT",
        receiverId = null,
        conversationName = null,
        duration = null,
        fileId = null,
        fileName = null,
        fileUrl = null,
        fileSize = null,
        mimeType = null,
      } = messageData;

      if (!content || !senderId) {
        throw new Error("Données de message incomplètes");
      }

      console.log(`💬 Traitement message: ${senderId} → ${conversationId}`, {
        hasReceiverId: !!receiverId,
        contentLength: content.length,
        type,
        fileId,
        fileName,
        duration,
      });

      if (conversationId === null) {
        conversationId = "";
      }

      // ✅ CRÉER/VÉRIFIER LA CONVERSATION
      let conversation = null;

      try {
        console.log(`🔍 Recherche conversation: ${conversationId}`);
        conversation = await this.conversationRepository.findById(
          conversationId
        );

        if (conversation && conversation._id) {
          console.log(`✅ Conversation trouvée: ${conversationId}`);

          // Vérifier que l'expéditeur est participant
          if (!conversation.participants.includes(senderId)) {
            throw new Error(
              `L'utilisateur ${senderId} n'est pas participant de cette conversation`
            );
          }
        } else {
          console.log(`⚠️ Conversation ${conversationId} introuvable`);
          conversation = null;
        }
      } catch (findError) {
        console.log(
          `⚠️ Erreur lors de la recherche conversation ${conversationId}:`,
          findError.message
        );
        conversation = null;
      }

      // ✅ CRÉER LA CONVERSATION SI ELLE N'EXISTE PAS
      if (!conversation) {
        if (!receiverId) {
          throw new Error(
            "receiverId est requis pour créer une nouvelle conversation"
          );
        }

        if (receiverId === senderId) {
          throw new Error("receiverId doit être différent du senderId");
        }

        console.log(
          `🆕 Création automatique conversation privée: ${conversationId}`
        );

        try {
          conversation = await this.createConversationIfNotExists(
            conversationId,
            senderId,
            receiverId,
            conversationName
          );

          if (conversation && conversation._id) {
            console.log(`✅ Conversation privée créée: ${conversation._id}`, {
              participants: conversation.participants,
              participantsCount: conversation.participants?.length,
            });
          } else {
            throw new Error(
              "Échec de la création automatique de la conversation"
            );
          }
        } catch (createError) {
          console.error(
            `❌ Erreur création conversation ${conversationId}:`,
            createError.message
          );
          throw new Error(
            `Impossible de créer la conversation: ${createError.message}`
          );
        }
      }

      // ✅ VÉRIFICATION FINALE
      if (!conversation || !conversation._id) {
        throw new Error(
          "Conversation finale invalide après vérification/création"
        );
      }

      // ✅ VÉRIFICATION SUPPLÉMENTAIRE POUR CONVERSATIONS PRIVÉES
      if (
        conversation.type === "PRIVATE" &&
        conversation.participants.length !== 2
      ) {
        console.error("❌ Conversation privée invalide:", {
          id: conversation._id,
          participants: conversation.participants,
          count: conversation.participants.length,
        });
        throw new Error(
          `Conversation privée doit avoir exactement 2 participants (actuel: ${conversation.participants.length})`
        );
      }

      console.log(`✅ Conversation validée pour traitement:`, {
        id: conversation._id,
        type: conversation.type,
        participants: conversation.participants,
      });

      // ✅ CRÉER LE MESSAGE
      const message = {
        conversationId: conversation._id || conversation.id,
        senderId,
        // ✅ ASSURER QUE receiverId EST TOUJOURS UNE STRING
        receiverId: String(
          receiverId ||
            conversation.participants.find(
              (p) => String(p) !== String(senderId)
            ) ||
            null
        ),
        content,
        type,
        status: "SENT",
        ...(fileId && { fileId }),
        ...(fileName && { fileName }),
        ...(fileUrl && { fileUrl }),
        ...(fileSize && { fileSize }),
        ...(mimeType && { mimeType }),
        ...(duration && { duration }),
        timestamp: new Date(),
        metadata: {
          conversationName,
          technical: {
            source: "SendMessage-UseCase",
            clientTimestamp: messageData.timestamp || new Date().toISOString(),
          },
        },
      };

      console.log(`📝 Création message:`, {
        senderId: message.senderId,
        conversationId: message.conversationId,
        contentLength: message.content.length,
        type: message.type,
        hasMetadata: !!message.metadata,
      });

      // ✅ ÉTAPE 1 : LOG PRE-WRITE (Write-Ahead Logging)
      let walId = null;
      if (this.resilientService) {
        walId = await this.resilientService.logPreWrite(message);
      }

      // ✅ ÉTAPE 2 : SAUVEGARDER AVEC CIRCUIT BREAKER
      let savedMessage;
      try {
        if (this.resilientService) {
          savedMessage = await this.resilientService.circuitBreaker.execute(
            () => this.messageRepository.save(message)
          );

          // ✅ PUBLIER DANS LE STREAM REDIS AVEC DONNÉES COMPLÈTES
          if (savedMessage && conversation) {
            await this.resilientService.publishToMessageStream(savedMessage, {
              event: "NEW_MESSAGE",
              source: "SendMessage-UseCase",
              conversationParticipants: conversation.participants, // ✅ AJOUTER LES PARTICIPANTS
            });
          }
        } else {
          savedMessage = await this.messageRepository.save(message);
        }

        // ✅ MÉTRIQUES (PROTÉGÉ)
        if (this.resilientService && this.resilientService.metrics) {
          this.resilientService.metrics.totalMessages++;
          this.resilientService.metrics.successfulSaves++;
        }

        console.log(`✅ Message sauvegardé: ${savedMessage._id}`);
      } catch (saveError) {
        console.error(`❌ Erreur sauvegarde message:`, saveError.message);

        // ✅ RETRY AUTOMATIQUE
        if (this.resilientService && saveError.retryable !== false) {
          await this.resilientService.addRetry(message, 1, saveError);
        }

        // ✅ FALLBACK REDIS SI DISPONIBLE
        if (this.resilientService) {
          try {
            savedMessage = await this.resilientService.redisFallback(message);
            console.log(`✅ Message stocké en fallback Redis`);
          } catch (fallbackError) {
            // ✅ DEAD LETTER QUEUE EN DERNIER RECOURS
            await this.resilientService.addToDLQ(message, saveError, 1, {
              operation: "SendMessage.save",
              walId,
            });
            throw new Error(
              `Impossible de sauvegarder le message: ${saveError.message}`
            );
          }
        } else {
          throw new Error(
            `Impossible de sauvegarder le message: ${saveError.message}`
          );
        }
      }

      // ✅ ÉTAPE 3 : LOG POST-WRITE
      if (this.resilientService && walId) {
        await this.resilientService.logPostWrite(savedMessage._id, walId);
      }

      // ✅ ÉTAPE 4 : METTRE À JOUR LA CONVERSATION
      try {
        await this.conversationRepository.updateLastMessage(conversationId, {
          content: message.content,
          timestamp: message.timestamp,
          senderId: message.senderId,
          messageId: savedMessage._id || savedMessage.id,
        });
        console.log(`🔄 Conversation mise à jour: ${conversationId}`);
      } catch (updateError) {
        console.warn(
          "⚠️ Erreur mise à jour conversation:",
          updateError.message
        );
        // ✅ NE PAS FAIRE ÉCHOUER LE MESSAGE SI LA MISE À JOUR ÉCHOUE
      }

      // ✅ RETOURNER LE RÉSULTAT (SANS KAFKA)
      const result = {
        success: true,
        message: {
          id: savedMessage._id || savedMessage.id,
          content: savedMessage.content,
          senderId: savedMessage.senderId,
          conversationId: savedMessage.conversationId,
          type: savedMessage.type,
          status: savedMessage.status,
          timestamp: savedMessage.timestamp,
          createdAt: savedMessage.createdAt,
        },
        conversation: {
          id: conversation._id || conversation.id,
          name: conversation.name,
          type: conversation.type,
          participants: conversation.participants,
        },
      };

      console.log(`✅ Message traité avec succès: ${result.message.id}`);

      // Après la sauvegarde du message, incrémenter les compteurs non-lus
      const otherParticipants = conversation.participants.filter(
        (p) => p !== messageData.senderId
      );

      // Incrémenter le compteur pour chaque participant sauf l'expéditeur
      const updatePromises = otherParticipants.map((participantId) =>
        this.conversationRepository.incrementUnreadCountInUserMetadata(
          conversation._id || conversation.id,
          participantId,
          1
        )
      );

      await Promise.all(updatePromises);

      return result;
    } catch (error) {
      console.error("❌ Erreur SendMessage use case:", error);
      // ✅ KAFKA COMPLÈTEMENT SUPPRIMÉ
      throw error;
    }
  }

  // ✅ MÉTHODE CORRIGÉE POUR CRÉER LA CONVERSATION
  async createConversationIfNotExists(
    conversationId,
    senderId,
    receiverId = null,
    conversationName = null
  ) {
    try {
      const participants = [senderId, receiverId];
      const type = "PRIVATE";

      const conversationData = {
        _id: conversationId,
        name: conversationName || `Conversation ${senderId} - ${receiverId}`,
        type,
        participants,
        createdBy: senderId,
        isPrivate: true,
        settings: {
          allowInvites: true,
          isPublic: false,
          maxParticipants: type === "PRIVATE" ? 2 : 200,
          messageRetention: 0,
          autoDeleteAfter: 0,
        },
      };

      // Validation
      this.validateConversationData(conversationData);

      // Sauvegarde
      const savedConversation = await this.conversationRepository.save(
        conversationData
      );

      // ✅ KAFKA SUPPRIMÉ D'ICI AUSSI

      return savedConversation;
    } catch (error) {
      throw new Error(`Impossible de créer la conversation: ${error.message}`);
    }
  }

  // ✅ MÉTHODE DE VALIDATION EXISTANTE (INCHANGÉE)
  validateConversationData(conversationData) {
    const errors = [];

    if (!conversationData.name || conversationData.name.trim().length === 0) {
      errors.push("Le nom de la conversation est requis");
    }

    if (!conversationData.type) {
      errors.push("Le type de conversation est requis");
    }

    if (
      !Array.isArray(conversationData.participants) ||
      conversationData.participants.length === 0
    ) {
      errors.push("La conversation doit avoir au moins 1 participant");
    }

    if (!conversationData.createdBy) {
      errors.push("Le créateur de la conversation est requis");
    }

    if (conversationData.userMetadata) {
      if (!Array.isArray(conversationData.userMetadata)) {
        errors.push("userMetadata doit être un array");
      } else {
        for (const metadata of conversationData.userMetadata) {
          const participantId = metadata.userId || metadata.participantId;
          if (!conversationData.participants.includes(participantId)) {
            errors.push(
              `Métadonnées pour un participant non-existent: ${participantId}`
            );
          }
        }
      }
    }

    if (conversationData.metadata) {
      if (
        conversationData.metadata.auditLog &&
        !Array.isArray(conversationData.metadata.auditLog)
      ) {
        errors.push("metadata.auditLog doit être un array");
      }
    }

    if (errors.length > 0) {
      console.error("❌ Erreurs validation conversation:", errors);
      throw new Error(
        `Données de conversation invalides: ${errors.join(", ")}`
      );
    }

    console.log("✅ Validation conversation réussie");
    return true;
  }
}

module.exports = SendMessage;
