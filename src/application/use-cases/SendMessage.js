const UserCacheService = require("../../infrastructure/services/UserCacheService");

class SendMessage {
  constructor(
    messageRepository,
    conversationRepository,
    cacheService = null,
    resilientService = null,
    userCacheService = null,
    getFileUseCase = null, // ✅ AJOUT
  ) {
    this.messageRepository = messageRepository;
    this.conversationRepository = conversationRepository;
    this.cacheService = cacheService;
    this.resilientService = resilientService;
    // ✅ Service intelligent avec Redis cache + fallback HTTP
    this.userCacheService = userCacheService || new UserCacheService();
    this.getFileUseCase = getFileUseCase; // ✅ AJOUT
  }

  // ✅ MODIFIER LA MÉTHODE execute() - RETIRER KAFKA
  async execute(messageData) {
    const startTime = Date.now();

    try {
      const {
        content = "",
        senderId,
        senderSocketId = null,
        conversationId = null,
        type = "TEXT",
        receiverId = null,
        conversationName = null,
        fileId = null,
        callMetadata = null,
        // ✅ CHAMP DE RÉPONSE (optionnel, fourni par replyToMessage)
        replyTo = null,
        // ✅ CHAMPS DE TRANSFERT (optionnels, fournis par ForwardMessage)
        isForwarded = false,
        forwardedFrom = null,
        originalSenderId = null,
        temporaryId = null, // ID temporaire fourni par le client pour corréler l'ACK
      } = messageData;

      // ✅ Pour les appels, le contenu est auto-généré si absent
      const isCallType = type === "CALL" || type === "VIDEO_CALL";
      // const finalContent =
      //   isCallType && !content
      //     ? type === "CALL"
      //       ? "📞 Appel audio"
      //       : "📹 Appel vidéo"
      //     : content;

      // if (!finalContent || !senderId) {
      //   throw new Error("Données de message incomplètes");
      // }

      // ✅ RÉCUPÉRER LES INFOS DU FICHIER SI fileId EST FOURNI
      let fileMetadata = null;
      if (fileId && this.getFileUseCase) {
        try {
          console.log(`📎 Récupération métadonnées fichier: ${fileId}`);
          const file = await this.getFileUseCase.execute(fileId, senderId);

          if (file) {
            // ✅ La durée est dans file.metadata.content.duration (audio/vidéo)
            const fileDuration = file.metadata?.content?.duration || null;

            fileMetadata = {
              fileId: file._id,
              fileName: file.originalName,
              fileSize: file.size,
              duration: fileDuration,
              mimeType: file.mimeType,
              url: file.url,
              thumbnailUrl: file.metadata?.processing?.thumbnailUrl || null,
              uploadedAt: file.createdAt,
              status: file.status,
              isClientRecorded: file.isClientRecorded || false, // Ajout de la propriété
            };
            console.log(`✅ Métadonnées fichier récupérées:`, fileMetadata);
          }
        } catch (fileError) {
          // Bloquer l'envoi si le fichier est invalide/supprimé
          console.error(`❌ Fichier invalide (${fileId}):`, fileError.message);
          throw new Error(`Fichier invalide: ${fileError.message}`);
        }
      }

      // ✅ CONSTRUIRE LES MÉTADONNÉES D'APPEL SI TYPE CALL/VIDEO_CALL
      let callMeta = null;
      if (isCallType && callMetadata) {
        callMeta = {
          callId: callMetadata.callId || null,
          callType: type === "VIDEO_CALL" ? "VIDEO" : "AUDIO",
          status: callMetadata.status || "INITIATED",
          initiatorId: callMetadata.initiatorId || senderId,
          receiverIds:
            callMetadata.receiverIds ||
            (receiverId
              ? Array.isArray(receiverId)
                ? receiverId
                : [receiverId]
              : []),
          startedAt: callMetadata.startedAt || null,
          endedAt: callMetadata.endedAt || null,
          duration: callMetadata.duration || 0,
          endReason: callMetadata.endReason || null,
        };
        console.log(`📞 Métadonnées appel construites:`, callMeta);
      }

      console.log(`💬 Traitement message: ${senderId} → ${conversationId}`, {
        hasReceiverId: !!receiverId,
        contentLength: content.length,
        type,
        fileId,
        isCall: isCallType,
      });

      // ✅ CRÉER/VÉRIFIER LA CONVERSATION
      let conversation = null;

      if (conversationId) {
        try {
          console.log(`🔍 Recherche conversation: ${conversationId}`);
          conversation =
            await this.conversationRepository.findById(conversationId);

          if (conversation && conversation._id) {
            console.log(`✅ Conversation trouvée: ${conversationId}`);

            // Vérifier que l'expéditeur est participant
            if (!conversation.participants.includes(senderId)) {
              throw new Error(
                `L'utilisateur ${senderId} n'est pas participant de cette conversation`,
              );
            }

            if (conversation.type === "CHANNEL") {
              if (!conversation.settings.broadcastAdmins.includes(senderId)) {
                throw new Error(
                  `L'utilisateur ${senderId} n'est pas autorisé à envoyer des messages dans ce canal`,
                );
              }
            }

            if (conversation.type === "BROADCAST") {
              if (conversation.createdBy !== senderId) {
                throw new Error(
                  `Seul le créateur peut envoyer des messages dans un broadcast`,
                );
              }
            }
          } else {
            console.log(`⚠️ Conversation ${conversationId} introuvable`);
            conversation = null;
          }
        } catch (findError) {
          console.log(
            `⚠️ Erreur lors de la recherche conversation ${conversationId}:`,
            findError.message,
          );
          conversation = null;
        }
      }

      // ✅ CRÉER LA CONVERSATION SI ELLE N'EXISTE PAS
      if (!conversation) {
        if (!receiverId) {
          throw new Error(
            "receiverId est requis pour créer une nouvelle conversation",
          );
        }

        if (receiverId === senderId) {
          throw new Error("receiverId doit être différent du senderId");
        }

        console.log(
          `🆕 Création automatique conversation privée: ${conversationId}`,
        );

        try {
          conversation = await this.createConversationIfNotExists(
            conversationId,
            senderId,
            receiverId,
            conversationName,
          );

          if (conversation && conversation._id) {
            console.log(`✅ Conversation privée créée: ${conversation._id}`, {
              participants: conversation.participants,
              participantsCount: conversation.participants?.length,
            });

            // ✅ PUBLIER ÉVÉNEMENT CONVERSATION CRÉÉE
            if (this.resilientService) {
              try {
                await this.resilientService.addToStream(
                  "stream:conversation:created",
                  {
                    event: "conversation.created",
                    conversationId: conversation._id.toString(),
                    conversation: conversation,
                    type: "PRIVATE",
                    createdBy: senderId,
                    participants: JSON.stringify(conversation.participants),
                    name: conversation.name || "Conversation privée",
                    temporaryId: temporaryId || null,
                    participantCount:
                      conversation.participants.length.toString(),
                    timestamp: Date.now().toString(),
                  },
                );
                console.log(
                  `📤 Événement conversation créée publiée pour ${conversation._id}`,
                );

                // ✅ ATTENDRE 100ms pour laisser le temps au consumer de distribuer l'événement
                await new Promise((resolve) => setTimeout(resolve, 100));
                console.log(
                  `⏱️ Délai de 100ms appliqué pour synchronisation conversation/message`,
                );
              } catch (streamErr) {
                console.error(
                  "❌ Erreur publication conversation créée:",
                  streamErr.message,
                );
              }
            }
          } else {
            throw new Error(
              "Échec de la création automatique de la conversation",
            );
          }
        } catch (createError) {
          console.error(
            `❌ Erreur création conversation ${conversationId}:`,
            createError.message,
          );
          throw new Error(
            `Impossible de créer la conversation: ${createError.message}`,
          );
        }
      }

      // ✅ VÉRIFICATION FINALE
      if (!conversation || !conversation._id) {
        throw new Error(
          "Conversation finale invalide après vérification/création",
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
          `Conversation privée doit avoir exactement 2 participants (actuel: ${conversation.participants.length})`,
        );
      }

      console.log(`✅ Conversation validée pour traitement:`, {
        id: conversation._id,
        type: conversation.type,
        participants: conversation.participants,
      });

      // ✅ DÉLÉGUER À LA MÉTHODE DÉDIÉE SI BROADCAST
      if (conversation.type === "BROADCAST") {
        return await this._executeBroadcast(
          {
            senderId,
            senderSocketId,
            content,
            type,
            fileMetadata,
            callMeta,
            replyTo,
            isForwarded,
            forwardedFrom,
            originalSenderId,
            conversationName,
          },
          conversation,
        );
      }

      // ✅ CALCULER totalRecipients SELON LE TYPE DE CONVERSATION
      // Utiliser la valeur stockée si disponible, sinon recalculer
      let totalRecipients = conversation.totalRecipients || 1; // Par défaut pour PRIVATE
      if (!conversation.totalRecipients) {
        if (
          conversation.type === "GROUP" ||
          conversation.type === "BROADCAST" ||
          conversation.type === "CHANNEL"
        ) {
          // Pour GROUP/BROADCAST: tous les participants sauf l'expéditeur
          totalRecipients = conversation.participants.filter(
            (p) => String(p) !== String(senderId),
          ).length;
        }
      }

      // ✅ CHIFFREMENT E2EE (si activé et destinataire connu)
      let finalContent = content || "";
      let encryptionMeta = {
        mode: "none",
        iv: null,
        tag: null,
        encryptedKey: null,
        keyVersion: null,
      };

      if (
        this.encryptionService?.isE2EEEnabled() &&
        this.keyManagementService &&
        receiverId
      ) {
        try {
          const recipientPublicKey =
            await this.keyManagementService.getPublicKey(String(receiverId));
          const keyMeta = await this.keyManagementService.getKeyMetadata(
            String(receiverId),
          );
          const encrypted = await this.encryptionService.encryptText(
            finalContent,
            recipientPublicKey,
          );

          finalContent = encrypted.encryptedContent;
          encryptionMeta = {
            mode: "e2ee",
            iv: encrypted.encryptionIV,
            tag: encrypted.encryptionTag,
            encryptedKey: encrypted.encryptedKey,
            keyVersion: keyMeta?.keyVersion ?? null,
          };
          console.log(
            `🔐 Message chiffré E2EE pour receiverId=${receiverId} (keyVersion=${encryptionMeta.keyVersion})`,
          );
        } catch (encErr) {
          // Clé publique absente → envoi en clair avec avertissement
          console.warn(
            `⚠️ Chiffrement E2EE ignoré pour ${receiverId}: ${encErr.message}`,
          );
        }
      }

      // ✅ CRÉER LE MESSAGE
      const message = {
        conversationId: conversation._id || conversation.id,
        senderId,
        // ✅ ASSURER QUE receiverId EST TOUJOURS UNE STRING
        receiverId: String(
          receiverId ||
            conversation.participants.find(
              (p) => String(p) !== String(senderId),
            ) ||
            null,
        ),
        content: finalContent,
        type,
        status: "SENT",
        totalRecipients,
        deliveredCount: 0,
        readCount: 0,
        deliveredBy: [],
        readBy: [],
        timestamp: new Date(),
        // ✅ CHAMP DE RÉPONSE (optionnel)
        ...(replyTo ? { replyTo } : {}),
        // ✅ CHAMPS DE TRANSFERT (optionnels)
        ...(isForwarded
          ? {
              isForwarded: true,
              forwardedFrom: forwardedFrom,
              originalSenderId: originalSenderId,
            }
          : {}),
        metadata: {
          conversationName,
          technical: {
            source: isForwarded
              ? "ForwardMessage-UseCase"
              : "SendMessage-UseCase",
            clientTimestamp: messageData.timestamp || new Date().toISOString(),
            ...(isForwarded
              ? {
                  forwardedAt: new Date().toISOString(),
                  originalMessageId: forwardedFrom,
                }
              : {}),
          },
          // ✅ MÉTADONNÉES CONTENU (fichier et/ou appel)
          contentMetadata: {
            file: fileMetadata ? fileMetadata : null,
            call: callMeta ? callMeta : null,
            encryptionMetadata: encryptionMeta,
          },
        },
      };

      console.log(`📝 Création message:`, {
        senderId: message.senderId,
        conversationId: message.conversationId,
        contentLength: message.content.length,
        type: message.type,
        hasMetadata: !!message.metadata,
        hasCallMeta: !!callMeta,
      });

      // ✅ ÉTAPES 1-5 : SAUVEGARDER, LOG WAL ET PUBLIER (via helper partagé)
      const savedMessage = await this._saveAndPublishMessage(
        message,
        conversation,
        senderSocketId,
      );

      // ✅ CONSTRUIRE LE RÉSULTAT IMMÉDIATEMENT (ACK RAPIDE)
      const messageTimestamp =
        savedMessage.createdAt || savedMessage.timestamp || message.timestamp;
      const result = {
        success: true,
        message: {
          id: savedMessage._id || savedMessage.id,
          content: savedMessage.content,
          senderId: savedMessage.senderId,
          temporaryId: temporaryId || null, // Inclure le temporaryId pour corrélation côté client
          conversationId: savedMessage.conversationId,
          type: savedMessage.type,
          status: savedMessage.status,
          timestamp: messageTimestamp,
          createdAt: savedMessage.createdAt,
          // ✅ Inclure les métadonnées d'appel si présentes
          ...(callMeta ? { callMetadata: callMeta } : {}),
          // ✅ Inclure replyTo si présent
          ...(replyTo ? { replyTo } : {}),
          // ✅ Inclure les champs de transfert si présents
          ...(isForwarded
            ? { isForwarded: true, forwardedFrom, originalSenderId }
            : {}),
        },
        conversation: {
          id: conversation._id || conversation.id,
          name: conversation.name,
          type: conversation.type,
          participants: conversation.participants,
          temporaryId: temporaryId || null, // Inclure le temporaryId pour corrélation côté client
        },
      };

      console.log(`✅ Message traité avec succès: ${result.message.id}`);

      // ✅ ÉTAPE 6 : Incrémenter les compteurs non-lus (NON-BLOQUANT)
      // Fire-and-forget pour ne pas retarder l'ACK
      const otherParticipants = conversation.participants.filter(
        (p) => p !== messageData.senderId,
      );

      Promise.all(
        otherParticipants.map((participantId) =>
          this.conversationRepository.incrementUnreadCountInUserMetadata(
            conversation._id || conversation.id,
            participantId,
            1,
          ),
        ),
      ).catch((err) => {
        console.error(
          `❌ Erreur incrémentation compteurs non-lus (non-bloquant):`,
          err.message,
        );
      });

      return result;
    } catch (error) {
      console.error("❌ Erreur SendMessage use case:", error);
      // ✅ KAFKA COMPLÈTEMENT SUPPRIMÉ
      throw error;
    }
  }

  /**
   * ✅ HELPER PARTAGÉ : Sauvegarde un message avec WAL, circuit breaker, updateLastMessage et publication Redis.
   * Utilisé par execute() et _executeBroadcast() pour éviter la duplication.
   */
  async _saveAndPublishMessage(message, conversation, senderSocketId) {
    // ÉTAPE 1 : LOG PRE-WRITE (Write-Ahead Logging)
    let walId = null;
    if (this.resilientService) {
      walId = await this.resilientService.logPreWrite(message);
    }

    // ÉTAPE 2 : SAUVEGARDER AVEC CIRCUIT BREAKER
    let savedMessage;
    try {
      if (this.resilientService) {
        savedMessage = await this.resilientService.circuitBreaker.execute(() =>
          this.messageRepository.save(message),
        );
      } else {
        savedMessage = await this.messageRepository.save(message);
      }
      if (this.resilientService?.metrics) {
        this.resilientService.metrics.totalMessages++;
        this.resilientService.metrics.successfulSaves++;
      }
      console.log(`✅ Message sauvegardé: ${savedMessage._id}`);
    } catch (saveError) {
      console.error(`❌ Erreur sauvegarde message:`, saveError.message);
      if (this.resilientService && saveError.retryable !== false) {
        await this.resilientService.addRetry(message, 1, saveError);
      }
      if (this.resilientService) {
        try {
          savedMessage = await this.resilientService.redisFallback(message);
          console.log(`✅ Message stocké en fallback Redis`);
        } catch {
          await this.resilientService.addToDLQ(message, saveError, 1, {
            operation: "SendMessage.save",
            walId,
          });
          throw new Error(
            `Impossible de sauvegarder le message: ${saveError.message}`,
          );
        }
      } else {
        throw new Error(
          `Impossible de sauvegarder le message: ${saveError.message}`,
        );
      }
    }

    // ÉTAPE 3 : LOG POST-WRITE
    if (this.resilientService && walId) {
      await this.resilientService.logPostWrite(savedMessage._id, walId);
    }

    // ÉTAPE 4 : METTRE À JOUR lastMessage AVANT la publication Redis
    try {
      await this.conversationRepository.updateLastMessage(conversation._id, {
        _id: savedMessage._id || savedMessage.id,
        content: message.content,
        type: message.type,
        timestamp: message.timestamp,
        senderId: message.senderId,
        messageId: savedMessage._id || savedMessage.id,
        fileId: message.fileId || null,
      });
      console.log(`🔄 Conversation mise à jour: ${conversation._id}`);
    } catch (updateError) {
      console.warn("⚠️ Erreur mise à jour conversation:", updateError.message);
    }

    // ÉTAPE 5 : PUBLIER DANS LE STREAM REDIS (non-bloquant)
    if (this.resilientService && savedMessage) {
      this.resilientService
        .publishToMessageStream(savedMessage, {
          event: "NEW_MESSAGE",
          source: "SendMessage-UseCase",
          conversationParticipants: conversation.participants,
          senderSocketId,
        })
        .catch((err) =>
          console.error(`❌ Erreur publication stream:`, err.message),
        );
    }

    return savedMessage;
  }

  /**
   * ✅ MÉTHODE DÉDIÉE AUX BROADCASTS
   * Appelée automatiquement par execute() quand conversation.type === "BROADCAST".
   * Pour chaque destinataire :
   *   - Trouve ou crée la conversation privée sender↔destinataire
   *   - Sauvegarde le message dans cette conv privée (via _saveAndPublishMessage)
   *   - Stocke les IDs des convs privées dans contentMetadata.broadcast du message broadcast
   * Le message broadcast (côté expéditeur) contient la liste complète dans contentMetadata.broadcast.
   * Les messages privés (côté destinataires) référencent le broadcast via contentMetadata.broadcast.broadcastConversationId.
   */
  async _executeBroadcast(params, broadcastConversation) {
    const {
      senderId,
      senderSocketId,
      content,
      type,
      fileMetadata,
      callMeta,
      replyTo,
      isForwarded,
      forwardedFrom,
      originalSenderId,
      conversationName,
    } = params;

    const recipients = (
      broadcastConversation.settings?.broadcastRecipients?.length > 0
        ? broadcastConversation.settings.broadcastRecipients
        : broadcastConversation.participants
    ).filter((p) => String(p) !== String(senderId));

    console.log(
      `📡 Broadcast: dispatch vers ${recipients.length} conv(s) privée(s)`,
    );

    // ✅ Lire le mapping stocké par CreateBroadcast dans broadcastMetadata
    const existingMap = new Map(
      (broadcastConversation.broadcastMetadata?.privateConversations || []).map(
        (e) => [String(e.recipientId), String(e.conversationId)],
      ),
    );

    const privateConversationEntries = [];
    const newEntries = []; // convs créées à la volée (cas limite)
    const broadcastConvId = broadcastConversation._id.toString();

    for (const recipientId of recipients) {
      try {
        // 1. Trouver la conversation privée via le mapping
        let privateConv;
        const knownConvId = existingMap.get(String(recipientId));

        if (knownConvId) {
          privateConv = await this.conversationRepository.findById(knownConvId);
        }

        if (!privateConv) {
          // Cas limite : chercher ou créer la conv privée
          privateConv =
            await this.conversationRepository.findPrivateConversation(
              senderId,
              recipientId,
            );

          if (!privateConv) {
            privateConv = await this.createConversationIfNotExists(
              null,
              senderId,
              recipientId,
              null,
            );
            if (this.resilientService) {
              this.resilientService
                .addToStream("chat:stream:events:conversation:created", {
                  event: "conversation.created",
                  conversationId: privateConv._id.toString(),
                  type: "PRIVATE",
                  createdBy: senderId,
                  participants: JSON.stringify(privateConv.participants),
                  name: privateConv.name || "Conversation privée",
                  participantCount: privateConv.participants.length.toString(),
                  timestamp: Date.now().toString(),
                })
                .catch((err) =>
                  console.warn(
                    `⚠️ Erreur publication conv privée broadcast:`,
                    err.message,
                  ),
                );
            }
          }

          newEntries.push({
            recipientId: String(recipientId),
            conversationId: (privateConv._id || privateConv.id).toString(),
          });
        }

        // 2. Construire et sauvegarder le message dans la conv privée
        const privateMsg = {
          conversationId: privateConv._id || privateConv.id,
          senderId,
          receiverId: String(recipientId),
          content: content || "",
          type,
          status: "SENT",
          totalRecipients: 1,
          deliveredCount: 0,
          readCount: 0,
          deliveredBy: [],
          readBy: [],
          timestamp: new Date(),
          ...(replyTo ? { replyTo } : {}),
          ...(isForwarded
            ? { isForwarded: true, forwardedFrom, originalSenderId }
            : {}),
          metadata: {
            conversationName,
            technical: {
              source: "SendMessage-UseCase-BroadcastDispatch",
              clientTimestamp: new Date().toISOString(),
            },
            contentMetadata: {
              file: fileMetadata || null,
              call: callMeta || null,
              broadcast: { broadcastConversationId: broadcastConvId },
            },
          },
        };

        const savedPrivate = await this._saveAndPublishMessage(
          privateMsg,
          privateConv,
          senderSocketId,
        );

        // Incrémenter non-lu du destinataire (non-bloquant)
        this.conversationRepository
          .incrementUnreadCountInUserMetadata(
            privateConv._id || privateConv.id,
            recipientId,
            1,
          )
          .catch(() => {});

        privateConversationEntries.push({
          recipientId: String(recipientId),
          conversationId: (privateConv._id || privateConv.id).toString(),
          messageId: (savedPrivate._id || savedPrivate.id).toString(),
        });

        console.log(
          `✅ Broadcast dispatché → conv privée ${privateConv._id} (dest: ${recipientId})`,
        );
      } catch (err) {
        console.error(
          `❌ Erreur dispatch broadcast → ${recipientId}:`,
          err.message,
        );
      }
    }

    // Persister les nouvelles convs privées créées à la volée dans broadcastMetadata
    if (newEntries.length > 0) {
      this.conversationRepository
        .updateBroadcastMetadata(broadcastConvId, newEntries)
        .catch((err) =>
          console.warn(
            `⚠️ Erreur mise à jour broadcastMetadata (fallback):`,
            err.message,
          ),
        );
    }

    // 3. Sauvegarder le message dans la conv broadcast (historique expéditeur uniquement)
    //    ⚠️ PAS de publication stream : ce message est privé à l'expéditeur
    //       et ne doit jamais être livré aux destinataires
    const broadcastMsg = {
      conversationId: broadcastConversation._id || broadcastConversation.id,
      senderId,
      receiverId: null,
      content: content || "",
      type,
      status: "SENT",
      totalRecipients: recipients.length,
      deliveredCount: 0,
      readCount: 0,
      deliveredBy: [],
      readBy: [],
      timestamp: new Date(),
      ...(replyTo ? { replyTo } : {}),
      ...(isForwarded
        ? { isForwarded: true, forwardedFrom, originalSenderId }
        : {}),
      metadata: {
        conversationName,
        technical: {
          source: "SendMessage-UseCase-Broadcast",
          clientTimestamp: new Date().toISOString(),
        },
        contentMetadata: {
          file: fileMetadata || null,
          call: callMeta || null,
          broadcast: { privateConversations: privateConversationEntries },
        },
      },
    };

    let savedBroadcast;
    try {
      if (this.resilientService) {
        savedBroadcast = await this.resilientService.circuitBreaker.execute(
          () => this.messageRepository.save(broadcastMsg),
        );
      } else {
        savedBroadcast = await this.messageRepository.save(broadcastMsg);
      }
      console.log(`✅ Message sauvegardé: ${savedBroadcast._id}`);

      // Mettre à jour lastMessage de la conv broadcast
      await this.conversationRepository.updateLastMessage(
        broadcastConversation._id,
        {
          _id: savedBroadcast._id,
          content: broadcastMsg.content,
          type: broadcastMsg.type,
          timestamp: broadcastMsg.timestamp,
          senderId: broadcastMsg.senderId,
          messageId: savedBroadcast._id,
          fileId: null,
        },
      );
    } catch (err) {
      console.error(`❌ Erreur sauvegarde historique broadcast:`, err.message);
      savedBroadcast = {
        _id: null,
        content: broadcastMsg.content,
        senderId: broadcastMsg.senderId,
        conversationId: broadcastMsg.conversationId,
        type: broadcastMsg.type,
        status: "SENT",
        createdAt: broadcastMsg.timestamp,
      };
    }

    // ✅ Incrémenter totalMessagesSent UNE FOIS par message broadcast envoyé
    this.conversationRepository
      .incrementBroadcastMessageCount(
        broadcastConversation._id || broadcastConversation.id,
      )
      .catch((err) =>
        console.warn(`⚠️ Erreur incrementBroadcastMessageCount:`, err.message),
      );

    console.log(
      `✅ Message broadcast sauvegardé: ${savedBroadcast._id} (${privateConversationEntries.length} conv(s) privée(s) sync)`,
    );

    return {
      success: true,
      message: {
        id: savedBroadcast._id || savedBroadcast.id,
        content: savedBroadcast.content,
        senderId: savedBroadcast.senderId,
        conversationId: savedBroadcast.conversationId,
        type: savedBroadcast.type,
        status: savedBroadcast.status,
        timestamp: savedBroadcast.createdAt || savedBroadcast.timestamp,
        createdAt: savedBroadcast.createdAt,
        broadcastDispatched: privateConversationEntries.length,
      },
      conversation: {
        id: broadcastConversation._id || broadcastConversation.id,
        name: broadcastConversation.name,
        type: broadcastConversation.type,
        participants: broadcastConversation.participants,
      },
    };
  }

  // ✅ MÉTHODE CORRIGÉE POUR CRÉER LA CONVERSATION
  async createConversationIfNotExists(
    conversationId,
    senderId,
    receiverId = null,
    conversationName = null,
  ) {
    try {
      const participants = [senderId, receiverId];

      // ✅ Récupérer les infos utilisateurs via UserCacheService
      let usersInfo = [];
      try {
        console.log(
          `🔍 Récupération infos participants de la conversation privée...`,
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
        console.log(`✅ Infos participants récupérées:`, {
          count: usersInfo.length,
          users: usersInfo.map((u) => ({ id: u.userId, name: u.name })),
        });
      } catch (fetchError) {
        console.error(
          `❌ Erreur récupération infos participants:`,
          fetchError.message,
        );
        throw new Error(
          `Impossible de récupérer les infos participants: ${fetchError.message}`,
        );
      }

      const type = "PRIVATE";

      // ✅ CRÉER userMetadata AVEC LES INFOS UTILISATEURS
      const userMetadata = participants.map((participantId) => {
        const userInfo = usersInfo.find((u) => u.userId === participantId) || {
          userId: participantId,
          nom: null,
          prenom: null,
          sexe: null,
          avatar: null,
          matricule: participantId,
          departement: null,
          ministere: null,
        };

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

      const conversationData = {
        name: conversationName || `Conversation ${senderId} - ${receiverId}`,
        type,
        participants,
        createdBy: senderId,
        isPrivate: true,
        // ✅ REMPLIR userMetadata AVEC LES INFOS DES PARTICIPANTS
        userMetadata,
        settings: {
          allowInvites: true,
          isPublic: false,
          maxParticipants: type === "PRIVATE" ? 2 : 200,
          messageRetention: 0,
          autoDeleteAfter: 0,
        },
      };

      // ✅ conversationId optionnel: si présent, on le conserve pour l'idempotence
      if (conversationId) {
        conversationData._id = conversationId;
      }

      // Validation
      this.validateConversationData(conversationData);

      // Sauvegarde
      const savedConversation =
        await this.conversationRepository.save(conversationData);

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
              `Métadonnées pour un participant non-existent: ${participantId}`,
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
        `Données de conversation invalides: ${errors.join(", ")}`,
      );
    }

    console.log("✅ Validation conversation réussie");
    return true;
  }
}

module.exports = SendMessage;
