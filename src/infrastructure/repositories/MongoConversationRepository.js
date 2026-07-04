const Conversation = require("../mongodb/models/ConversationModel");

class MongoConversationRepository {
  /**
   * Recherche une conversation par un champ arbitraire (ex: code_structure)
   * @param {Object} query - Objet de requête MongoDB (ex: { code_structure: '...' })
   * @returns {Promise<Object|null>} Conversation trouvée ou null
   */

  /**
   * Trouve une conversation par query MongoDB (ex: { code_structure: "..." })
   */
  async findOne(query = {}) {
    try {
      if (!query || typeof query !== "object") {
        throw new Error("Query object requis pour findOne");
      }
      return await Conversation.findOne(query).lean();
    } catch (error) {
      console.error("❌ Erreur findOne conversation:", error);
      throw error;
    }
  }

  constructor(kafkaProducer = null, resilientMessageService = null) {
    this.kafkaProducer = kafkaProducer;
    this.resilientMessageService = resilientMessageService;
    this.metrics = {
      cacheHits: 0,
      cacheMisses: 0,
      errors: 0,
      kafkaEvents: 0,
      kafkaErrors: 0,
    };
  }

  async save(conversationData) {
    const startTime = Date.now();

    try {
      console.log(`💾 Début sauvegarde conversation:`, {
        id: conversationData._id,
        name: conversationData.name,
        type: conversationData.type,
        participants: conversationData.participants,
        hasRequiredFields: !!(
          conversationData.name && conversationData.participants
        ),
        hasMetadata: !!conversationData.metadata,
        hasAuditLog: !!conversationData.metadata?.auditLog,
      });

      // ✅ NETTOYAGE ET VALIDATION DES DONNÉES AVANT CRÉATION DU MODÈLE
      const cleanedData = this._sanitizeConversationData(conversationData);

      // ✅ VÉRIFIER SI LA CONVERSATION EXISTE DÉJÀ (uniquement si _id est fourni)
      if (cleanedData._id) {
        let existingConversation;
        try {
          existingConversation = await Conversation.findById(cleanedData._id);
          if (existingConversation) {
            console.log(
              `✅ Conversation existante trouvée: ${cleanedData._id}, mise à jour...`,
            );

            // ✅ METTRE À JOUR LA CONVERSATION AU LIEU DE RETOURNER L'ANCIENNE VERSION
            const { _id, __v, ...updateFields } = cleanedData;
            const updatedConversation = await Conversation.findByIdAndUpdate(
              cleanedData._id,
              { $set: updateFields },
              { new: true, runValidators: true },
            );

            if (!updatedConversation) {
              throw new Error(
                `Échec mise à jour conversation ${cleanedData._id}`,
              );
            }

            console.log(`✅ Conversation mise à jour en base:`, {
              id: updatedConversation._id,
              participantsCount: updatedConversation.participants?.length,
              unreadCountsKeys: Object.keys(
                updatedConversation.unreadCounts || {},
              ),
            });

            return updatedConversation;
          }
        } catch (findError) {
          console.log(
            `🔍 Conversation ${cleanedData._id} non trouvée, création nécessaire`,
          );
        }
      }

      // ✅ CRÉER UNE NOUVELLE CONVERSATION AVEC DONNÉES NETTOYÉES
      let conversationModel;
      try {
        console.log(
          `🏗️ Création du modèle conversation avec données nettoyées`,
        );
        conversationModel = new Conversation(cleanedData);

        // ✅ VALIDATION EXPLICITE AVEC GESTION D'ERREUR DÉTAILLÉE
        const validationError = conversationModel.validateSync();
        if (validationError) {
          console.error(`❌ Erreur validation conversation:`, {
            message: validationError.message,
            errors: validationError.errors,
            conversationId: cleanedData._id,
          });

          // ✅ GESTION SPÉCIFIQUE DES ERREURS D'ENUM
          if (validationError.message.includes("is not a valid enum value")) {
            console.error(
              `🔧 Erreur enum détectée - tentative de correction...`,
            );

            // ✅ CORRIGER LES VALEURS D'ENUM INVALIDES
            const correctedData = this._fixEnumValues(cleanedData);
            conversationModel = new Conversation(correctedData);

            const retryValidation = conversationModel.validateSync();
            if (retryValidation) {
              throw new Error(
                `Données encore invalides après correction: ${retryValidation.message}`,
              );
            }

            console.log(`✅ Données d'enum corrigées avec succès`);
          } else {
            throw new Error(
              `Données de conversation invalides: ${validationError.message}`,
            );
          }
        }

        console.log(`✅ Modèle conversation créé et validé`);
      } catch (modelError) {
        console.error(`❌ Erreur création modèle conversation:`, {
          error: modelError.message,
          stack: modelError.stack,
          conversationId: cleanedData._id,
          hasMetadata: !!cleanedData.metadata,
          auditLogLength: cleanedData.metadata?.auditLog?.length || 0,
        });
        throw new Error(`Impossible de créer le modèle: ${modelError.message}`);
      }

      // ✅ SAUVEGARDER AVEC GESTION D'ERREUR ROBUSTE ET DEBUG
      let savedConversation;
      try {
        console.log(`💾 Tentative de sauvegarde en base de données...`);
        this.metrics.dbQueries++;

        // ✅ INITIALISATION MANUELLE DES COMPTEURS AVANT SAUVEGARDE
        try {
          if (typeof conversationModel.initializeUnreadCounts === "function") {
            conversationModel.initializeUnreadCounts();
          } else {
            console.warn(
              `⚠️ Méthode initializeUnreadCounts non disponible, initialisation manuelle`,
            );
            if (!conversationModel.unreadCounts) {
              conversationModel.unreadCounts = {};
            }
            if (
              conversationModel.participants &&
              Array.isArray(conversationModel.participants)
            ) {
              conversationModel.participants.forEach((participantId) => {
                if (!(participantId in conversationModel.unreadCounts)) {
                  conversationModel.unreadCounts[participantId] = 0;
                }
              });
              conversationModel.markModified("unreadCounts");
            }
          }

          // ✅ VALIDATION FINALE AVANT SAUVEGARDE
          if (
            typeof conversationModel.validateAndCleanUnreadCounts === "function"
          ) {
            conversationModel.validateAndCleanUnreadCounts();
          }
        } catch (initError) {
          console.warn(
            `⚠️ Erreur initialisation compteurs:`,
            initError.message,
          );
        }

        // ✅ SAUVEGARDE AVEC GESTION D'ERREUR SPÉCIFIQUE POUR LES HOOKS
        savedConversation = await conversationModel.save();

        if (!savedConversation || !savedConversation._id) {
          throw new Error(
            "Sauvegarde a échoué - conversation invalide retournée",
          );
        }

        console.log(`✅ Conversation sauvegardée en base:`, {
          id: savedConversation._id,
          name: savedConversation.name,
          participants: savedConversation.participants,
          auditLogCount: savedConversation.metadata?.auditLog?.length || 0,
          unreadCountsKeys: Object.keys(savedConversation.unreadCounts || {}),
        });
      } catch (saveError) {
        console.error(`❌ Erreur sauvegarde MongoDB:`, {
          error: saveError.message,
          code: saveError.code,
          keyPattern: saveError.keyPattern,
          conversationId: cleanedData._id,
          stack: saveError.stack,
        });

        // ✅ GESTION SPÉCIFIQUE DES ERREURS DE HOOKS
        if (
          saveError.message.includes("doc is not defined") ||
          saveError.message.includes("is not defined")
        ) {
          console.error(
            `❌ Erreur de référence dans les hooks détectée:`,
            saveError.message,
          );
          throw new Error(
            `Erreur hook MongoDB: ${saveError.message} - Vérifiez les hooks pre/post du modèle`,
          );
        }

        // ✅ GESTION SPÉCIFIQUE DES ERREURS MONGODB
        if (saveError.code === 11000 && cleanedData._id) {
          console.log(`🔄 Conversation en doublon détectée, récupération...`);
          try {
            const existing = await Conversation.findById(cleanedData._id);
            if (existing) {
              console.log(
                `✅ Conversation récupérée après doublon: ${existing._id}`,
              );
              return existing;
            }
          } catch (recoveryError) {
            console.error(
              `❌ Erreur récupération après doublon:`,
              recoveryError.message,
            );
          }
        }

        // ✅ GESTION DES ERREURS DE MÉTHODES MANQUANTES
        if (saveError.message.includes("is not a function")) {
          console.error(
            `❌ Erreur de méthode manquante détectée:`,
            saveError.message,
          );
          throw new Error(
            `Erreur méthode: ${saveError.message} - Vérifiez que toutes les méthodes du modèle sont définies`,
          );
        }

        throw new Error(`Erreur MongoDB: ${saveError.message}`);
      }

      const processingTime = Date.now() - startTime;

      // ✅ KAFKA AVEC GESTION D'ERREUR
      if (this.kafkaProducer) {
        try {
          await this._publishConversationEvent(
            "CONVERSATION_CREATED",
            savedConversation,
            { processingTime },
          );
          console.log(`📤 Événement Kafka publié: CONVERSATION_CREATED`);
        } catch (kafkaError) {
          console.warn(
            "⚠️ Erreur publication conversation:",
            kafkaError.message,
          );
          // ✅ NE PAS FAIRE ÉCHOUER LA SAUVEGARDE SI KAFKA ÉCHOUE
        }
      }

      console.log(
        `✅ Conversation complètement sauvegardée: ${savedConversation._id} (${processingTime}ms)`,
      );
      return savedConversation;
    } catch (error) {
      this.metrics.errors++;
      const processingTime = Date.now() - startTime;

      console.error(`❌ Erreur complète sauvegarde conversation:`, {
        error: error.message,
        stack: error.stack,
        conversationId: conversationData._id,
        processingTime,
      });

      // ✅ PUBLIER L'ERREUR SUR KAFKA SI DISPONIBLE
      if (this.kafkaProducer) {
        try {
          await this.kafkaProducer.publishMessage({
            eventType: "CONVERSATION_SAVE_FAILED",
            conversationId: conversationData._id,
            error: error.message,
            processingTime,
            timestamp: new Date().toISOString(),
            source: "MongoConversationRepository",
          });
        } catch (kafkaError) {
          console.warn(
            "⚠️ Erreur publication échec Kafka:",
            kafkaError.message,
          );
        }
      }

      throw error;
    }
  }

  async findById(conversationId, useCache = false) {
    const startTime = Date.now();

    try {
      this.metrics.dbQueries++;
      const conversation = await Conversation.findById(conversationId).lean();

      if (!conversation) {
        throw new Error(`Conversation ${conversationId} non trouvée`);
      }

      const processingTime = Date.now() - startTime;

      // Mettre en cache
      if (this.cacheService && useCache) {
        try {
          await this._cacheConversation(conversation);
        } catch (cacheError) {
          console.warn(
            "⚠️ Erreur mise en cache conversation:",
            cacheError.message,
          );
        }
      }

      console.log(
        `🔍 Conversation trouvée: ${conversationId} (${processingTime}ms)`,
      );
      return conversation;
    } catch (error) {
      this.metrics.errors++;
      console.error(
        `❌ Erreur recherche conversation ${conversationId}:`,
        error,
      );
      throw error;
    }
  }

  async findByParticipant(userId, options = {}) {
    const { page = 1, limit = 20, type = null, useCache = true } = options;
    const startTime = Date.now();

    try {
      // Correction ici : matcher string et number
      const filter = {
        participants: {
          $in: [
            userId,
            typeof userId === "string" ? Number(userId) : String(userId),
          ],
        },
      };

      if (type) filter.type = type;

      const skip = (page - 1) * limit;

      this.metrics.dbQueries += 2;

      const [conversations, totalCount] = await Promise.all([
        Conversation.find(filter)
          .sort({ lastMessageAt: -1 })
          .skip(skip)
          .limit(limit)
          .lean(),
        Conversation.countDocuments(filter),
      ]);

      const result = {
        conversations: conversations.map((conv) =>
          this._sanitizeConversationData(conv),
        ),
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalCount / limit),
          totalCount,
          hasNext: page * limit < totalCount,
          hasPrevious: page > 1,
        },
        fromCache: false,
      };

      const processingTime = Date.now() - startTime;

      if (this.cacheService && useCache) {
        try {
          await this.cacheService.set(cacheKey, result, this.defaultTTL);
        } catch (cacheError) {
          console.warn(
            "⚠️ Erreur mise en cache conversations:",
            cacheError.message,
          );
        }
      }

      console.log(
        `🔍 Conversations participant: ${userId} (${conversations.length} conversations, ${processingTime}ms)`,
      );
      return result;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur conversations participant ${userId}:`, error);
      throw error;
    }
  }

  /**
   * ✅ TROUVER UNE CONVERSATION PRIVÉE ENTRE DEUX UTILISATEURS
   */
  async findPrivateConversation(participant1, participant2) {
    try {
      return await Conversation.findOne({
        type: "PRIVATE",
        participants: { $all: [String(participant1), String(participant2)] },
        isActive: true,
      }).lean();
    } catch (error) {
      console.error("\u274c Erreur findPrivateConversation:", error.message);
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR broadcastMetadata sur une conversation BROADCAST
   * Ajoute les nouvelles entrées (recipientId → conversationId) dans privateConversations.
   * Ne touche PAS à totalMessagesSent (voir incrementBroadcastMessageCount).
   * @param {string} broadcastConversationId
   * @param {Array<{recipientId, conversationId}>} privateConversationEntries
   */
  async updateBroadcastMetadata(
    broadcastConversationId,
    privateConversationEntries,
  ) {
    try {
      const existing = await Conversation.findById(broadcastConversationId)
        .select("broadcastMetadata")
        .lean();

      const existingIds = new Set(
        (existing?.broadcastMetadata?.privateConversations || []).map(
          (e) => e.recipientId,
        ),
      );

      // ✅ Vérifier que chaque conversationId pointe bien vers une conv PRIVATE
      const convIds = privateConversationEntries
        .filter((e) => !existingIds.has(e.recipientId))
        .map((e) => e.conversationId);

      const validPrivateConvs =
        convIds.length > 0
          ? await Conversation.find({
              _id: { $in: convIds },
              type: "PRIVATE",
            })
              .select("_id")
              .lean()
          : [];

      const validConvIdSet = new Set(
        validPrivateConvs.map((c) => c._id.toString()),
      );

      const rejectedCount = convIds.length - validPrivateConvs.length;
      if (rejectedCount > 0) {
        console.warn(
          `⚠️ updateBroadcastMetadata: ${rejectedCount} entrée(s) rejetée(s) car non PRIVATE`,
        );
      }

      const newEntries = privateConversationEntries
        .filter(
          (e) =>
            !existingIds.has(e.recipientId) &&
            validConvIdSet.has(String(e.conversationId)),
        )
        .map((e) => ({
          recipientId: e.recipientId,
          conversationId: e.conversationId,
        }));

      if (newEntries.length > 0) {
        const totalKnown =
          (existing?.broadcastMetadata?.privateConversations?.length || 0) +
          newEntries.length;

        await Conversation.findByIdAndUpdate(broadcastConversationId, {
          $push: {
            "broadcastMetadata.privateConversations": { $each: newEntries },
          },
          $set: { "broadcastMetadata.totalRecipients": totalKnown },
        });
      }

      console.log(
        `✅ broadcastMetadata mis à jour: ${broadcastConversationId} (+${newEntries.length} conv(s) privée(s))`,
      );
    } catch (error) {
      console.error("❌ Erreur updateBroadcastMetadata:", error.message);
      throw error;
    }
  }

  /**
   * ✅ INCRÉMENTER le compteur de messages broadcast envoyés.
   * À appeler UNE FOIS par envoi de message broadcast (depuis SendMessage).
   * @param {string} broadcastConversationId
   */
  async incrementBroadcastMessageCount(broadcastConversationId) {
    try {
      await Conversation.findByIdAndUpdate(broadcastConversationId, {
        $inc: { "broadcastMetadata.totalMessagesSent": 1 },
        $set: { "broadcastMetadata.lastBroadcastAt": new Date() },
      });
      console.log(
        `📊 totalMessagesSent incrémenté pour ${broadcastConversationId}`,
      );
    } catch (error) {
      console.error("❌ Erreur incrementBroadcastMessageCount:", error.message);
    }
  }

  async updateLastMessage(conversationId, messageData) {
    const startTime = Date.now();

    try {
      const updateData = {
        lastMessage: {
          _id: messageData._id,
          content: messageData.content.substring(0, 100),
          type: messageData.type,
          senderId: messageData.senderId,
          status: messageData.status || "SENT",
          timestamp: new Date(),
        },
        lastMessageAt: new Date(),
        updatedAt: new Date(),
        "metadata.stats.lastActivity": new Date(),
      };

      // ✅ CONSTRUIRE LES INCRÉMENTS EN FONCTION DU TYPE DE MESSAGE
      const incrementFields = {
        "metadata.stats.totalMessages": 1,
      };

      // ✅ INCRÉMENTER LES STATS SELON LE TYPE
      const messageType = messageData.type?.toUpperCase();
      if (
        messageType === "FILE" ||
        messageType === "DOCUMENT" ||
        messageData.fileId
      ) {
        incrementFields["metadata.stats.totalFiles"] = 1;
      } else if (messageType === "IMAGE" || messageType === "PHOTO") {
        incrementFields["metadata.stats.totalImages"] = 1;
        incrementFields["metadata.stats.totalFiles"] = 1;
      } else if (messageType === "VIDEO") {
        incrementFields["metadata.stats.totalVideos"] = 1;
        incrementFields["metadata.stats.totalFiles"] = 1;
      }

      this.metrics.dbQueries++;
      const conversation = await Conversation.findByIdAndUpdate(
        conversationId,
        {
          $set: updateData,
          $inc: incrementFields,
        },
        { new: true },
      );

      if (!conversation) {
        throw new Error(`Conversation ${conversationId} non trouvée`);
      }

      const processingTime = Date.now() - startTime;

      // Kafka
      if (this.kafkaProducer) {
        try {
          await this._publishConversationEvent(
            "CONVERSATION_UPDATED",
            conversation,
            {
              lastMessage: messageData,
              processingTime,
            },
          );
        } catch (kafkaError) {
          console.warn("⚠️ Erreur publication update:", kafkaError.message);
        }
      }

      console.log(
        `🔄 Last message mis à jour: ${conversationId} (${processingTime}ms)`,
      );
      return conversation;
    } catch (error) {
      this.metrics.errors++;
      console.error(`❌ Erreur update last message ${conversationId}:`, error);
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR UNIQUEMENT LE STATUT DU DERNIER MESSAGE
   * Utilisé quand un message est marqué DELIVERED ou READ
   * @param {string} conversationId - ID de la conversation
   * @param {string} messageId - ID du message (doit correspondre au lastMessage._id)
   * @param {string} newStatus - Nouveau statut (DELIVERED, READ)
   */
  async updateLastMessageStatus(conversationId, messageId, newStatus) {
    const startTime = Date.now();

    try {
      // ✅ VÉRIFIER QUE LE MESSAGE EST BIEN LE DERNIER MESSAGE
      const conversation = await Conversation.findById(conversationId);

      if (!conversation) {
        console.warn(
          `⚠️ Conversation ${conversationId} non trouvée pour updateLastMessageStatus`,
        );
        return null;
      }

      // ✅ VÉRIFIER SI LE MESSAGE EST LE DERNIER MESSAGE
      const lastMsgId = conversation.lastMessage?._id?.toString();
      const targetMsgId =
        typeof messageId === "string" ? messageId : messageId?.toString();

      if (lastMsgId !== targetMsgId) {
        console.log(
          `ℹ️ Message ${targetMsgId} n'est pas le dernier message (${lastMsgId}), pas de mise à jour lastMessage.status`,
        );
        return null;
      }

      // ✅ METTRE À JOUR LE STATUT DU DERNIER MESSAGE
      this.metrics.dbQueries++;
      const updatedConversation = await Conversation.findByIdAndUpdate(
        conversationId,
        {
          $set: {
            "lastMessage.status": newStatus,
            updatedAt: new Date(),
          },
        },
        { new: true },
      );

      const processingTime = Date.now() - startTime;

      console.log(
        `✅ lastMessage.status mis à jour: ${conversationId} → ${newStatus} (${processingTime}ms)`,
      );

      return updatedConversation;
    } catch (error) {
      this.metrics.errors++;
      console.error(
        `❌ Erreur updateLastMessageStatus ${conversationId}:`,
        error,
      );
      throw error;
    }
  }

  async _publishConversationEvent(
    eventType,
    conversation,
    additionalData = {},
  ) {
    try {
      // ✅ PUBLIER DANS LES STREAMS REDIS
      if (this.resilientMessageService) {
        try {
          let conversationData = {
            conversationId:
              conversation?._id?.toString() || conversation?.id?.toString(),
          };

          // Ajouter les données spécifiques selon le type d'événement
          if (eventType === "CONVERSATION_CREATED") {
            conversationData = {
              ...conversationData,
              name: conversation.name,
              type: conversation.type,
              createdBy: conversation.createdBy,
              participants: conversation.participants,
            };
          } else if (eventType === "CONVERSATION_UPDATED") {
            conversationData = {
              ...conversationData,
              name: conversation.name,
              updatedBy: additionalData.updatedBy,
              changes: additionalData.changes,
            };
          } else if (eventType === "CONVERSATION_DELETED") {
            conversationData = {
              ...conversationData,
              deletedBy: additionalData.deletedBy,
            };
          }

          // ✅ PROPAGER senderSocketId si disponible dans additionalData
          if (additionalData.senderSocketId) {
            conversationData.senderSocketId = additionalData.senderSocketId;
          }

          await this.resilientMessageService.publishConversationEvent(
            eventType,
            conversationData,
          );
          console.log(`📤 Événement Redis stream publié: ${eventType}`);
        } catch (streamError) {
          console.warn(
            "⚠️ Erreur publication stream conversation:",
            streamError.message,
          );
        }
      }

      if (!this.kafkaProducer) {
        console.warn("⚠️ Pas de producer Kafka disponible");
        return false;
      }

      const eventData = {
        eventType,
        conversationId:
          conversation?._id?.toString() || conversation?.id?.toString(),
        type: conversation?.type,
        participantCount: conversation?.participants?.length || 0,
        lastMessageAt: conversation?.lastMessageAt,
        timestamp: new Date().toISOString(),
        serverId: process.env.SERVER_ID || "default",
        source: "MongoConversationRepository",
        ...additionalData,
      };

      // ✅ VÉRIFIER LE TYPE DE PRODUCER ET UTILISER LA BONNE API
      if (typeof this.kafkaProducer.publishMessage === "function") {
        // ✅ UTILISER L'API WRAPPER MessageProducer
        const result = await this.kafkaProducer.publishMessage(eventData);

        if (result) {
          this.metrics.kafkaEvents++;
          console.log(`📤 Événement Kafka publié: ${eventType}`);
          return true;
        } else {
          console.warn(`⚠️ Échec publication Kafka: ${eventType}`);
          return false;
        }
      } else if (typeof this.kafkaProducer.send === "function") {
        // ✅ UTILISER L'API KAFKAJS NATIVE
        const result = await this.kafkaProducer.send({
          topic: "chat.conversations",
          messages: [
            {
              key: eventData.conversationId,
              value: JSON.stringify(eventData),
              timestamp: Date.now(),
              headers: {
                "content-type": "application/json",
                "event-type": eventType,
                source: "MongoConversationRepository",
              },
            },
          ],
          acks: 1,
          timeout: 30000,
        });

        this.metrics.kafkaEvents++;
        console.log(`📤 Événement Kafka publié: ${eventType}`, {
          partition: result[0]?.partition,
          offset: result[0]?.offset,
        });
        return true;
      } else {
        // ✅ TYPE DE PRODUCER NON RECONNU
        console.error("❌ Type de producer Kafka non reconnu:", {
          hasPublishMessage:
            typeof this.kafkaProducer.publishMessage === "function",
          hasSend: typeof this.kafkaProducer.send === "function",
          availableMethods: Object.getOwnPropertyNames(
            this.kafkaProducer,
          ).filter((prop) => typeof this.kafkaProducer[prop] === "function"),
          producerType: this.kafkaProducer.constructor?.name || "unknown",
        });

        throw new Error(
          "Producer Kafka incompatible - aucune méthode de publication trouvée",
        );
      }
    } catch (error) {
      this.metrics.kafkaErrors++;
      console.error(`❌ Erreur publication Kafka ${eventType}:`, {
        error: error.message,
        stack: error.stack,
        conversationId: conversation?._id || conversation?.id,
        producerAvailable: !!this.kafkaProducer,
        producerType: this.kafkaProducer?.constructor?.name,
      });

      // ✅ NE PAS FAIRE ÉCHOUER L'OPÉRATION PRINCIPALE
      return false;
    }
  }

  async findById(conversationId) {
    try {
      const conversation = await Conversation.findById(conversationId).lean();
      return conversation;
    } catch (error) {
      console.error("❌ Erreur findById conversation:", error);
      throw error;
    }
  }

  async findAll(options = {}) {
    try {
      const { page = 1, limit = 50 } = options;
      const skip = (page - 1) * limit;

      const conversations = await Conversation.find()
        .sort({ updatedAt: -1 })
        .skip(skip)
        .limit(limit)
        .lean();

      return conversations;
    } catch (error) {
      console.error("❌ Erreur findAll conversations:", error);
      throw error;
    }
  }

  async create(conversationData) {
    try {
      const conversation = new Conversation(conversationData);
      const saved = await conversation.save();
      return saved.toObject();
    } catch (error) {
      console.error("❌ Erreur create conversation:", error);
      throw error;
    }
  }

  async update(id, updateData) {
    try {
      const updated = await Conversation.findByIdAndUpdate(
        id,
        { ...updateData, updatedAt: new Date() },
        { new: true, runValidators: true },
      ).lean();

      return updated;
    } catch (error) {
      console.error("❌ Erreur update conversation:", error);
      throw error;
    }
  }

  async delete(id) {
    try {
      const deleted = await Conversation.findByIdAndDelete(id);
      return !!deleted;
    } catch (error) {
      console.error("❌ Erreur delete conversation:", error);
      throw error;
    }
  }

  // ✅ MÉTHODES DE STATISTIQUES
  getMetrics() {
    return {
      ...this.metrics,
      cacheHitRate:
        this.metrics.cacheHits + this.metrics.cacheMisses > 0
          ? (
              (this.metrics.cacheHits /
                (this.metrics.cacheHits + this.metrics.cacheMisses)) *
              100
            ).toFixed(2) + "%"
          : "0%",
      timestamp: new Date().toISOString(),
    };
  }

  // ✅ MÉTHODE POUR NETTOYER LES DONNÉES DE CONVERSATION
  _sanitizeConversationData(data) {
    const sanitized = { ...data };

    // ✅ VALIDATION ET NETTOYAGE DE BASE
    // _id optionnel: si absent, il sera généré automatiquement par MongoDB/Mongoose

    if (!sanitized._id) {
      throw new Error("ID de conversation requis pour la sanitisation");
    }

    if (!sanitized.name || typeof sanitized.name !== "string") {
      sanitized.name = `Conversation ${Date.now()}`;
    }

    if (!sanitized.participants || !Array.isArray(sanitized.participants)) {
      throw new Error("Participants requis et doivent être un array");
    }

    // ✅ NETTOYER LES MÉTADONNÉES
    if (sanitized.metadata) {
      // ✅ NETTOYER L'AUDIT LOG
      if (
        sanitized.metadata.auditLog &&
        Array.isArray(sanitized.metadata.auditLog)
      ) {
        sanitized.metadata.auditLog = sanitized.metadata.auditLog.map(
          (entry) => ({
            action: this._validateAction(entry.action),
            userId: String(entry.userId),
            timestamp:
              entry.timestamp instanceof Date
                ? entry.timestamp
                : new Date(entry.timestamp || Date.now()),
            details: entry.details || {},
            metadata: entry.metadata || {},
          }),
        );
      }

      // ✅ ASSURER LES STATISTIQUES
      if (!sanitized.metadata.stats) {
        sanitized.metadata.stats = {
          totalMessages: 0,
          totalFiles: 0,
          totalParticipants: sanitized.participants?.length || 0,
          lastActivity: new Date(),
        };
      }
    } else {
      // ✅ CRÉER DES MÉTADONNÉES DE BASE SI MANQUANTES
      sanitized.metadata = {
        autoCreated: true,
        createdFrom: "Repository",
        version: 1,
        stats: {
          totalMessages: 0,
          totalFiles: 0,
          totalParticipants: sanitized.participants?.length || 0,
          lastActivity: new Date(),
        },
      };
    }

    // ✅ NETTOYER USER METADATA
    if (sanitized.userMetadata && Array.isArray(sanitized.userMetadata)) {
      sanitized.userMetadata = sanitized.userMetadata.map((meta) => ({
        userId: String(meta.userId),
        unreadCount: Math.max(0, parseInt(meta.unreadCount) || 0),
        lastReadAt: meta.lastReadAt ? new Date(meta.lastReadAt) : null,
        isMuted: Boolean(meta.isMuted),
        isPinned: Boolean(meta.isPinned),
        customName: meta.customName ? String(meta.customName) : null,
        notificationSettings: {
          enabled: Boolean(meta.notificationSettings?.enabled ?? true),
          sound: Boolean(meta.notificationSettings?.sound ?? true),
          vibration: Boolean(meta.notificationSettings?.vibration ?? true),
        },
        // ✅ TOUS LES CHAMPS UTILISATEUR DU SCHÉMA
        nom: meta.nom ? String(meta.nom) : null,
        prenom: meta.prenom ? String(meta.prenom) : null,
        sexe: meta.sexe ? String(meta.sexe) : null,
        avatar: meta.avatar ? String(meta.avatar) : null,
        departement: meta.departement ? String(meta.departement) : null,
        ministere: meta.ministere ? String(meta.ministere) : null,
      }));
    } else {
      // ✅ CRÉER USER METADATA POUR TOUS LES PARTICIPANTS
      sanitized.userMetadata = sanitized.participants.map((participantId) => ({
        userId: String(participantId),
        unreadCount: 0,
        lastReadAt: null,
        isMuted: false,
        isPinned: false,
        notificationSettings: {
          enabled: true,
          sound: true,
          vibration: true,
        },
        nom: null,
        prenom: null,
        sexe: null,
        avatar: null,
        departement: null,
        ministere: null,
      }));
    }

    // ✅ NETTOYER ET VALIDER UNREADCOUNTS AVEC VALIDATION RENFORCÉE
    if (sanitized.unreadCounts) {
      const cleanedUnreadCounts = {};

      // ✅ SI C'EST UNE MAP, CONVERTIR EN OBJET
      if (sanitized.unreadCounts instanceof Map) {
        for (const [key, value] of sanitized.unreadCounts.entries()) {
          cleanedUnreadCounts[String(key)] = Math.max(0, parseInt(value) || 0);
        }
      }
      // ✅ SI C'EST UN OBJET, NETTOYER LES VALEURS
      else if (
        typeof sanitized.unreadCounts === "object" &&
        sanitized.unreadCounts !== null
      ) {
        for (const [key, value] of Object.entries(sanitized.unreadCounts)) {
          if (key && key !== "undefined" && key !== "null") {
            cleanedUnreadCounts[String(key)] = Math.max(
              0,
              parseInt(value) || 0,
            );
          }
        }
      }

      sanitized.unreadCounts = cleanedUnreadCounts;
    } else {
      // ✅ INITIALISER SI ABSENT
      sanitized.unreadCounts = {};
    }

    // ✅ VALIDER ET NETTOYER LES PARTICIPANTS
    if (sanitized.participants && Array.isArray(sanitized.participants)) {
      sanitized.participants = sanitized.participants
        .map((p) => String(p))
        .filter((p) => p && p !== "undefined" && p !== "null");

      // ✅ ASSURER QUE TOUS LES PARTICIPANTS ONT UN COMPTEUR NON-LU
      sanitized.participants.forEach((participantId) => {
        if (!(participantId in sanitized.unreadCounts)) {
          sanitized.unreadCounts[participantId] = 0;
        }
      });
    }

    // ✅ NETTOYER LES PARAMÈTRES
    if (!sanitized.settings) {
      sanitized.settings = {
        allowInvites: true,
        isPublic: false,
        maxParticipants: sanitized.type === "PRIVATE" ? 2 : 200,
        messageRetention: 0,
        autoDeleteAfter: 0,
      };
    }

    // ✅ VALIDATION FINALE
    console.log(`🧹 Données sanitisées:`, {
      id: sanitized._id,
      participantsCount: sanitized.participants.length,
      unreadCountsKeys: Object.keys(sanitized.unreadCounts),
      hasMetadata: !!sanitized.metadata,
      hasUserMetadata: !!sanitized.userMetadata,
    });

    return sanitized;
  }

  // ✅ MÉTHODE POUR VALIDER ET CORRIGER LES ACTIONS D'AUDIT
  _validateAction(action) {
    const validActions = [
      "CREATED",
      "UPDATED",
      "DELETED",
      "PARTICIPANT_ADDED",
      "PARTICIPANT_REMOVED",
      "PARTICIPANT_INVITED",
      "PARTICIPANT_LEFT",
      "ARCHIVED",
      "UNARCHIVED",
      "MUTED",
      "UNMUTED",
      "PINNED",
      "UNPINNED",
      "AUTO_CREATED",
      "AUTO_ARCHIVED",
      "AUTO_DELETED",
      "AUTO_PARTICIPANT_REMOVED",
      "MESSAGE_SENT",
      "MESSAGE_DELETED",
      "MESSAGE_EDITED",
      "STATUS_CHANGED",
      "SETTINGS_UPDATED",
      "PERMISSIONS_CHANGED",
    ];

    if (!action || typeof action !== "string") {
      return "CREATED"; // Valeur par défaut
    }

    const upperAction = action.toUpperCase();

    // ✅ MAPPINGS POUR CORRIGER LES ACTIONS COURANTES
    const actionMappings = {
      AUTO_CREATED: "CREATED", // ✅ MAPPER AUTO_CREATED → CREATED SI PAS DANS L'ENUM
      CREATION: "CREATED",
      CREATE: "CREATED",
      UPDATE: "UPDATED",
      DELETE: "DELETED",
      REMOVE: "DELETED",
    };

    // Vérifier si l'action est valide
    if (validActions.includes(upperAction)) {
      return upperAction;
    }

    // Appliquer les mappings
    if (actionMappings[upperAction]) {
      console.log(
        `🔧 Action mappée: ${upperAction} → ${actionMappings[upperAction]}`,
      );
      return actionMappings[upperAction];
    }

    // Valeur par défaut
    console.warn(`⚠️ Action inconnue "${action}", utilisation de "CREATED"`);
    return "CREATED";
  }

  // ✅ MÉTHODE POUR CORRIGER LES VALEURS D'ENUM
  _fixEnumValues(data) {
    const fixed = { ...data };

    if (fixed.metadata?.auditLog) {
      fixed.metadata.auditLog = fixed.metadata.auditLog.map((entry) => ({
        ...entry,
        action: this._validateAction(entry.action),
      }));
    }

    return fixed;
  }

  async searchConversations(query, options = {}) {
    const {
      userId,
      type,
      includeArchived = false,
      limit = 20,
      useCache = true,
      useLike = true, // Ajout d'une option pour activer %like%
    } = options;

    const startTime = Date.now();

    try {
      // Filtre principal
      let filter = {};
      if (userId) filter.participants = userId;
      if (type) filter.type = type;
      if (!includeArchived) filter.isArchived = false;

      if (query && typeof query === "string" && query.length >= 2) {
        filter.$text = { $search: query };
      }

      let conversations = await Conversation.find(filter)
        .sort({ score: { $meta: "textScore" }, lastMessageAt: -1 })
        .limit(limit)
        .lean();

      // Si aucun résultat et option %like% activée, faire une recherche regex
      if (useLike && conversations.length === 0 && query && query.length >= 2) {
        filter = {};
        if (userId) filter.participants = userId;
        if (type) filter.type = type;
        if (!includeArchived) filter.isArchived = false;
        filter.$or = [
          { name: { $regex: query, $options: "i" } },
          { description: { $regex: query, $options: "i" } },
          { "metadata.tags": { $regex: query, $options: "i" } },
        ];

        conversations = await Conversation.find(filter)
          .sort({ lastMessageAt: -1 })
          .limit(limit)
          .lean();
      }

      const result = {
        conversations,
        totalFound: conversations.length,
        query,
        searchTime: Date.now() - startTime,
      };

      return result;
    } catch (error) {
      console.error("❌ Erreur recherche conversations:", error);
      throw error;
    }
  }

  async incrementUnreadCountInUserMetadata(conversationId, userId, amount = 1) {
    try {
      console.log(`📝 Incrément compteur non-lus userMetadata:`, {
        conversationId,
        userId,
        amount,
      });

      // 1. Vérifier d'abord si l'entrée existe
      const conversation = await Conversation.findOne({
        _id: conversationId,
        "userMetadata.userId": userId,
      }).select("userMetadata.$");

      if (conversation) {
        // 2. Si existe, incrémenter le compteur
        const updateResult = await Conversation.findOneAndUpdate(
          {
            _id: conversationId,
            "userMetadata.userId": userId,
          },
          {
            $inc: { "userMetadata.$.unreadCount": amount },
            $set: {
              updatedAt: new Date(),
              "userMetadata.$.lastActivity": new Date(),
            },
          },
          {
            new: true,
            runValidators: true,
          },
        );

        console.log(`✅ Compteur incrémenté pour utilisateur existant:`, {
          userId,
          newCount: updateResult?.userMetadata?.find((m) => m.userId === userId)
            ?.unreadCount,
        });

        return updateResult;
      } else {
        // 3. Si n'existe pas, ajouter une nouvelle entrée
        const updateResult = await Conversation.findByIdAndUpdate(
          conversationId,
          {
            $push: {
              userMetadata: {
                userId,
                unreadCount: amount,
                lastReadAt: null,
                lastActivity: new Date(),
                lastSeen: null,
                isMuted: false,
                isPinned: false,
                notificationSettings: {
                  enabled: true,
                  sound: true,
                  vibration: true,
                },
              },
            },
            $set: { updatedAt: new Date() },
          },
          {
            new: true,
            runValidators: true,
          },
        );

        console.log(`✅ Nouvelle entrée userMetadata créée:`, {
          userId,
          initialCount: amount,
        });

        return updateResult;
      }
    } catch (error) {
      console.error(`❌ Erreur incrément userMetadata:`, {
        error: error.message,
        conversationId,
        userId,
      });
      throw error;
    }
  }

  async resetUnreadCountInUserMetadata(conversationId, userId) {
    try {
      console.log(`🔄 Réinitialisation compteur non-lus userMetadata:`, {
        conversationId,
        userId,
      });

      const updateResult = await Conversation.findOneAndUpdate(
        {
          _id: conversationId,
          "userMetadata.userId": userId,
        },
        {
          $set: {
            updatedAt: new Date(),
            "userMetadata.$.unreadCount": 0,
            "userMetadata.$.lastActivity": new Date(),
            "userMetadata.$.lastReadAt": new Date(),
          },
        },
        {
          new: true,
          runValidators: true,
        },
      );

      if (updateResult) {
        console.log(`✅ Compteur réinitialisé pour l'utilisateur:`, {
          userId,
          conversationId,
        });
      }

      return updateResult;
    } catch (error) {
      console.error(`❌ Erreur réinitialisation userMetadata:`, {
        error: error.message,
        conversationId,
        userId,
      });
      throw error;
    }
  }

  /**
   * ✅ DÉCRÉMENTER LE COMPTEUR unreadCount (au lieu de réinitialiser à 0)
   * @param {string} conversationId
   * @param {string} userId
   * @param {number} count - nombre de messages lus à soustraire
   */
  async decrementUnreadCountInUserMetadata(conversationId, userId, count = 1) {
    try {
      const safeCount = Math.max(0, Math.floor(count));
      if (safeCount === 0) return null;

      console.log(`📉 Décrémentation compteur non-lus userMetadata:`, {
        conversationId,
        userId,
        decrement: safeCount,
      });

      // Utiliser $inc avec valeur négative + pipeline pour garantir min 0
      const updateResult = await Conversation.findOneAndUpdate(
        {
          _id: conversationId,
          "userMetadata.userId": userId,
        },
        [
          {
            $set: {
              updatedAt: new Date(),
              userMetadata: {
                $map: {
                  input: "$userMetadata",
                  as: "meta",
                  in: {
                    $cond: {
                      if: { $eq: ["$$meta.userId", userId] },
                      then: {
                        $mergeObjects: [
                          "$$meta",
                          {
                            unreadCount: {
                              $max: [
                                0,
                                {
                                  $subtract: [
                                    { $ifNull: ["$$meta.unreadCount", 0] },
                                    safeCount,
                                  ],
                                },
                              ],
                            },
                            lastReadAt: new Date(),
                            lastActivity: new Date(),
                          },
                        ],
                      },
                      else: "$$meta",
                    },
                  },
                },
              },
            },
          },
        ],
        {
          new: true,
          runValidators: false,
        },
      );

      if (updateResult) {
        const newCount = updateResult.userMetadata?.find(
          (m) => m.userId === userId,
        )?.unreadCount;
        console.log(`✅ Compteur décrémenté pour l'utilisateur:`, {
          userId,
          conversationId,
          decremented: safeCount,
          newCount,
        });
      }

      return updateResult;
    } catch (error) {
      console.error(`❌ Erreur décrémentation userMetadata:`, {
        error: error.message,
        conversationId,
        userId,
        count,
      });
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR LE lastSeen POUR UN UTILISATEUR DANS TOUTES SES CONVERSATIONS
   * Appelé lors de la déconnexion
   */
  async updateLastSeenForUser(userId) {
    try {
      const timestamp = new Date();
      console.log(`📝 Mise à jour lastSeen pour utilisateur ${userId}`);

      const result = await Conversation.updateMany(
        {
          "userMetadata.userId": String(userId),
          isActive: true,
        },
        {
          $set: {
            "userMetadata.$[elem].lastSeen": timestamp,
          },
        },
        {
          arrayFilters: [{ "elem.userId": String(userId) }],
        },
      );

      console.log(`✅ lastSeen mis à jour:`, {
        userId,
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
        timestamp: timestamp.toISOString(),
      });

      return {
        success: true,
        modifiedCount: result.modifiedCount,
        timestamp,
      };
    } catch (error) {
      console.error(`❌ Erreur updateLastSeenForUser:`, {
        error: error.message,
        userId,
      });
      return { success: false, error: error.message };
    }
  }

  /**
   * ✅ OBTENIR LE lastSeen D'UN UTILISATEUR DANS UNE CONVERSATION
   */
  async getLastSeenForUser(conversationId, userId) {
    try {
      const conversation = await Conversation.findOne(
        {
          _id: conversationId,
          "userMetadata.userId": String(userId),
        },
        { "userMetadata.$": 1 },
      );

      if (conversation?.userMetadata?.[0]) {
        return conversation.userMetadata[0].lastSeen;
      }
      return null;
    } catch (error) {
      console.error(`❌ Erreur getLastSeenForUser:`, error.message);
      return null;
    }
  }

  /**
   * ✅ OBTENIR LE lastSeen LE PLUS RÉCENT D'UN UTILISATEUR (toutes conversations confondues)
   * Utilisé comme fallback quand Redis ne contient pas l'info
   */
  async findLastSeenForUser(userId) {
    try {
      const userIdString = String(userId);

      // Chercher dans toutes les conversations actives de l'utilisateur,
      // le lastSeen le plus récent
      const conversations = await Conversation.find(
        {
          "userMetadata.userId": userIdString,
          isActive: true,
        },
        { "userMetadata.$": 1 },
      )
        .sort({ updatedAt: -1 })
        .limit(1)
        .lean();

      if (
        conversations.length > 0 &&
        conversations[0]?.userMetadata?.[0]?.lastSeen
      ) {
        const lastSeen = conversations[0].userMetadata[0].lastSeen;
        console.log(`📦 findLastSeenForUser: ${userIdString} → ${lastSeen}`);
        return lastSeen;
      }

      return null;
    } catch (error) {
      console.error(`❌ Erreur findLastSeenForUser:`, error.message);
      return null;
    }
  }

  // ===== MÉTHODES BROADCAST =====

  /**
   * ✅ TROUVER UNE CONVERSATION PRIVÉE ENTRE DEUX PARTICIPANTS
   * Délègue à la méthode statique du modèle Conversation
   * @param {string} participant1 - ID du premier participant
   * @param {string} participant2 - ID du second participant
   * @returns {Promise<object|null>} La conversation privée ou null
   */
  async findPrivateConversation(participant1, participant2) {
    try {
      console.log(`🔍 Recherche conversation privée:`, {
        participant1,
        participant2,
      });

      // Déléguer à la méthode statique du modèle
      const conversation = await Conversation.findPrivateConversation(
        participant1,
        participant2,
      );

      if (conversation) {
        console.log(
          `✅ Conversation privée trouvée: ${conversation._id || conversation.id}`,
        );
      } else {
        console.log(
          `ℹ️ Aucune conversation privée existante entre ${participant1} et ${participant2}`,
        );
      }

      return conversation;
    } catch (error) {
      console.error(`❌ Erreur findPrivateConversation:`, {
        error: error.message,
        participant1,
        participant2,
      });
      throw error;
    }
  }

  /**
   * ✅ INCRÉMENTER LE COMPTEUR DE MESSAGES D'UNE DIFFUSION BROADCAST
   * Incrémente broadcastMetadata.totalMessagesSent de 1
   * @param {string} broadcastConversationId - ID de la conversation broadcast
   * @returns {Promise<object|null>} La conversation mise à jour
   */
  async incrementBroadcastMessageCount(broadcastConversationId) {
    try {
      console.log(
        `📊 Incrémentation compteur messages broadcast: ${broadcastConversationId}`,
      );

      const updatedConversation = await Conversation.findByIdAndUpdate(
        broadcastConversationId,
        {
          $inc: { "broadcastMetadata.totalMessagesSent": 1 },
          $set: {
            "broadcastMetadata.lastBroadcastAt": new Date(),
            updatedAt: new Date(),
          },
        },
        { new: true, runValidators: false },
      );

      if (updatedConversation) {
        console.log(
          `✅ Compteur broadcast incrémenté: ${broadcastConversationId} → ${updatedConversation.broadcastMetadata?.totalMessagesSent || 0}`,
        );
      } else {
        console.warn(
          `⚠️ Conversation broadcast ${broadcastConversationId} non trouvée pour incrément`,
        );
      }

      return updatedConversation;
    } catch (error) {
      console.error(`❌ Erreur incrementBroadcastMessageCount:`, {
        error: error.message,
        broadcastConversationId,
      });
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR LES MÉTADONNÉES D'UNE DIFFUSION BROADCAST
   * Ajoute/met à jour le mapping destinataire → conversation privée dans broadcastMetadata
   * @param {string} broadcastConversationId - ID de la conversation broadcast
   * @param {Array<{recipientId: string, conversationId: string}>} privateConversationEntries - Mapping à ajouter
   * @returns {Promise<object|null>} La conversation mise à jour
   */
  async updateBroadcastMetadata(
    broadcastConversationId,
    privateConversationEntries,
  ) {
    try {
      console.log(`📝 Mise à jour broadcastMetadata:`, {
        broadcastConversationId,
        entriesCount: privateConversationEntries?.length || 0,
      });

      if (
        !privateConversationEntries ||
        privateConversationEntries.length === 0
      ) {
        console.warn(`⚠️ Aucune entrée à ajouter dans broadcastMetadata`);
        return null;
      }

      // Récupérer la conversation pour vérifier le type
      const conversation = await Conversation.findById(broadcastConversationId);

      if (!conversation) {
        throw new Error(
          `Conversation broadcast ${broadcastConversationId} introuvable`,
        );
      }

      if (conversation.type !== "BROADCAST") {
        throw new Error(
          `La conversation ${broadcastConversationId} n'est pas de type BROADCAST`,
        );
      }

      // Initialiser broadcastMetadata si nécessaire
      if (!conversation.broadcastMetadata) {
        conversation.broadcastMetadata = {
          privateConversations: [],
          totalRecipients: 0,
          totalMessagesSent: 0,
          lastBroadcastAt: null,
        };
      }

      // Fusionner les nouvelles entrées avec les existantes (éviter les doublons)
      const existingMap = new Map(
        (conversation.broadcastMetadata.privateConversations || []).map(
          (entry) => [entry.recipientId, entry.conversationId],
        ),
      );

      // Ajouter les nouvelles entrées
      privateConversationEntries.forEach((entry) => {
        existingMap.set(entry.recipientId, entry.conversationId);
      });

      // Reconstruire le tableau
      conversation.broadcastMetadata.privateConversations = Array.from(
        existingMap.entries(),
      ).map(([recipientId, conversationId]) => ({
        recipientId,
        conversationId,
      }));

      // Mettre à jour totalRecipients
      conversation.broadcastMetadata.totalRecipients =
        conversation.broadcastMetadata.privateConversations.length;

      // Sauvegarder
      await conversation.save();

      console.log(`✅ broadcastMetadata mis à jour:`, {
        broadcastConversationId,
        totalPrivateConversations:
          conversation.broadcastMetadata.privateConversations.length,
        totalRecipients: conversation.broadcastMetadata.totalRecipients,
      });

      return conversation.toObject();
    } catch (error) {
      console.error(`❌ Erreur updateBroadcastMetadata:`, {
        error: error.message,
        broadcastConversationId,
        entriesCount: privateConversationEntries?.length || 0,
      });
      throw error;
    }
  }
}

module.exports = MongoConversationRepository;
