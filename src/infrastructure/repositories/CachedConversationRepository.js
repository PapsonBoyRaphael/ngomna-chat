/**
 * CachedConversationRepository - Repository pattern avec cache Redis
 * ✅ SEUL RESPONSABLE du cache des conversations
 * ✅ OPTIMISÉ : Cache stratégique intelligent
 */
class CachedConversationRepository {
  constructor(conversationRepository, cacheService) {
    this.primaryStore = conversationRepository;
    this.cache = cacheService;
    this.redis = cacheService?.redis || null;

    // ✅ VÉRIFICATION DU CACHE
    if (!this.cache) {
      console.warn(
        "⚠️ CachedConversationRepository: cacheService est null - CACHE DÉSACTIVÉ",
      );
      console.warn(
        "   Cela signifie que chaque conversation sera récupérée depuis MongoDB",
      );
    } else {
      console.log("✅ CachedConversationRepository: Cache activé");
    }

    // ✅ Configuration cache optimisée
    this.defaultTTL = 3600; // 1 heure
    this.shortTTL = 300; // 5 minutes
    this.quickTTL = 60; // 1 minute pour quick load
    this.listTTL = 600; // 10 minutes pour listes

    this.cacheKeyPrefix = "chat:cache:convs";
  }

  // ===== TROUVER UNE CONVERSATION PAR UN CHAMP (ex: code_structure) =====
  async findOne(query = {}) {
    // Pas de cache ici, accès direct au primaryStore (MongoDB)
    if (!query || typeof query !== "object") {
      throw new Error("Query object requis pour findOne");
    }
    return this.primaryStore.findOne(query);
  }

  // ===== LIRE LES CONVERSATIONS D'UN UTILISATEUR (CACHE INTELLIGENT) =====
  async findByParticipant(userId, options = {}) {
    const {
      page = 1,
      limit = 20,
      cursor = null,
      includeArchived = false,
      useCache = true,
    } = options;

    try {
      // ✅ STRATÉGIE CACHE DIFFÉRENCIÉE
      let cacheKey = null;
      let ttl = this.shortTTL;

      if (useCache && this.cache) {
        if (cursor) {
          // Pagination avec cursor - cache court
          cacheKey = `${this.cacheKeyPrefix}:user:${userId}:cursor:${cursor}:${limit}`;
          ttl = this.shortTTL;
        } else if (page === 1) {
          // Première page - cache long
          cacheKey = `${this.cacheKeyPrefix}:user:${userId}:first:${limit}`;
          ttl = this.listTTL;
        } else {
          // Autres pages - cache moyen
          cacheKey = `${this.cacheKeyPrefix}:user:${userId}:p${page}:${limit}`;
          ttl = this.shortTTL;
        }

        // ✅ VÉRIFIER LE CACHE
        const cached = await this.cache.get(cacheKey);
        if (cached) {
          console.log(`📦 Conversations depuis cache: ${userId} (${cacheKey})`);
          console.log(`📦 ✅ HIT CACHE: ${cached}`);

          // ✅ RENOUVELER TTL SUR HIT
          await this.cache.renewTTL(cacheKey, ttl);

          return {
            ...cached,
            fromCache: true,
          };
        }
      }

      // ✅ CACHE MISS → MongoDB
      console.log(
        `🔍 Conversations depuis MongoDB: ${userId} ${
          cursor ? `(cursor: ${cursor})` : `(page: ${page})`
        }`,
      );

      const result = await this.primaryStore.findByParticipant(userId, {
        page,
        limit,
        cursor,
        includeArchived,
        useCache: false,
      });

      // ✅ METTRE EN CACHE SELON LA STRATÉGIE
      console.log(
        `🔍 DEBUG CACHE: useCache=${useCache}, cache=${!!this.cache}, cacheKey=${cacheKey}, conversations=${result.conversations?.length || 0}`,
      );
      if (
        useCache &&
        this.cache &&
        cacheKey &&
        result.conversations?.length > 0
      ) {
        try {
          await this.cache.set(cacheKey, result, ttl);
          console.log(
            `💾 Conversations mises en cache: ${result.conversations.length} (TTL: ${ttl}s)`,
          );
        } catch (error) {
          console.error(`❌ Erreur mise en cache: ${error.message}`);
        }
      }

      return {
        ...result,
        fromCache: false,
      };
    } catch (error) {
      console.error("❌ Erreur findByParticipant:", error.message);

      // ✅ FALLBACK SANS CACHE
      const result = await this.primaryStore.findByParticipant(userId, {
        page: 1,
        limit: 20,
        includeArchived: false,
      });

      return {
        ...result,
        fromCache: false,
      };
    }
  }

  // ===== RÉCUPÉRER LES CONVERSATIONS RAPIDES (POUR QUICK LOAD) =====
  async getQuickConversations(userId, limit = 10) {
    try {
      const quickCacheKey = `${this.cacheKeyPrefix}:quick:${userId}:${limit}`;

      // ✅ CACHE QUICK (très court TTL)
      if (this.cache) {
        const cached = await this.cache.get(quickCacheKey);
        if (cached) {
          console.log(`⚡ Quick conversations depuis cache: ${userId}`);
          return {
            ...cached,
            fromCache: true,
            isQuick: true,
          };
        }
      }

      // ✅ CACHE MISS → MongoDB
      console.log(`⚡ Quick conversations depuis MongoDB: ${userId}`);

      const result = await this.primaryStore.findByParticipant(userId, {
        page: 1,
        limit,
        useCache: false,
      });

      const quickData = {
        conversations: result.conversations || [],
        totalUnreadMessages: result.totalUnreadMessages || 0,
        unreadConversations: result.unreadConversations || 0,
      };

      // ✅ METTRE EN CACHE QUICK (TTL court)
      if (this.cache && quickData.conversations.length > 0) {
        await this.cache.set(quickCacheKey, quickData, this.quickTTL);
        console.log(
          `⚡ Quick conversations mises en cache: ${quickData.conversations.length} (${this.quickTTL}s)`,
        );
      }

      return {
        ...quickData,
        fromCache: false,
        isQuick: true,
      };
    } catch (error) {
      console.error("❌ Erreur getQuickConversations:", error.message);

      // Fallback
      const result = await this.primaryStore.findByParticipant(userId, {
        page: 1,
        limit: 10,
      });

      return {
        conversations: result.conversations || [],
        totalUnreadMessages: 0,
        unreadConversations: 0,
        fromCache: false,
        isQuick: true,
      };
    }
  }

  // ===== RÉCUPÉRER UNE CONVERSATION SPÉCIFIQUE =====
  async findById(conversationId, options = {}) {
    const { useCache = true } = options;

    try {
      let cacheKey = null;

      // ✅ ÉTAPE 1 : VÉRIFIER LE CACHE
      if (useCache && this.cache) {
        cacheKey = `${this.cacheKeyPrefix}:id:${conversationId}`;
        console.log(`🔍 Vérification cache pour: ${conversationId}`);

        const cached = await this.cache.get(cacheKey);
        if (cached) {
          console.log(`📦 ✅ HIT CACHE: Conversation trouvée en cache`);

          // ✅ RENOUVELER TTL
          await this.cache.renewTTL(cacheKey, this.defaultTTL);

          // ✅ RETOURNER DIRECTEMENT LA CONVERSATION (pas de wrapper)
          return cached;
        } else {
          console.log(`📦 ❌ MISS CACHE: Conversation non en cache`);
        }
      } else {
        if (!useCache) {
          console.log(`⏭️ Cache désactivé pour cette requête (useCache=false)`);
        } else {
          console.log(`⚠️ Cache non disponible (this.cache est null)`);
        }
      }

      // ✅ ÉTAPE 2 : CACHE MISS → MONGODB
      console.log(`🔍 Récupération depuis MongoDB: ${conversationId}`);

      const conversation = await this.primaryStore.findById(conversationId);

      if (!conversation) {
        throw new Error(`Conversation ${conversationId} non trouvée`);
      }

      // ✅ ÉTAPE 3 : METTRE EN CACHE
      if (useCache && this.cache && cacheKey) {
        try {
          await this.cache.set(cacheKey, conversation, this.defaultTTL);
          console.log(
            `💾 ✅ Conversation mise en cache (TTL: ${this.defaultTTL}s)`,
          );
        } catch (cacheError) {
          console.warn(`⚠️ Erreur mise en cache:`, cacheError.message);
          // Continue même si le cache échoue - on retourne quand même la conversation
        }
      }

      // ✅ RETOURNER DIRECTEMENT LA CONVERSATION (pas de wrapper)
      return conversation;
    } catch (error) {
      console.error("❌ Erreur findById conversation:", error.message);
      throw error;
    }
  }

  // ===== SAUVEGARDER UNE CONVERSATION (avec invalidation intelligente) =====
  async save(conversationData) {
    try {
      const startTime = Date.now();

      // 1. ✅ SAUVEGARDE MongoDB
      const savedConversation = await this.primaryStore.save(conversationData);

      console.log(
        `📦 [CachedConversationRepository] Conversation reçue de MongoDB:`,
        {
          _id: savedConversation?._id,
          name: savedConversation?.name,
          participantsCount: savedConversation?.participants?.length,
        },
      );

      if (!savedConversation) {
        throw new Error("Conversation not saved");
      }

      // 2. ✅ INVALIDATION CACHE INTELLIGENTE
      console.log(`🔄 Début invalidation cache pour: ${savedConversation._id}`);
      await this.invalidateConversationCaches(savedConversation._id, {
        isNewConversation: true,
        participants: savedConversation.participants,
        invalidateConversation: true, // Invalider car c'est une nouvelle conversation
      });
      console.log(
        `✅ Invalidation cache terminée pour: ${savedConversation._id}`,
      );

      const processingTime = Date.now() - startTime;
      console.log(
        `✅ Conversation sauvegardée avec cache: ${savedConversation._id} (${processingTime}ms)`,
      );

      return savedConversation;
    } catch (error) {
      console.error("❌ Erreur save conversation (cached):", error.message);
      throw error;
    }
  }

  // ===== INVALIDATION CACHE INTELLIGENTE =====
  async invalidateConversationCaches(conversationId, options = {}) {
    console.log(
      `🔍 invalidateConversationCaches début pour: ${conversationId}`,
    );
    if (!this.cache) {
      console.log(`⚠️ Cache non disponible, skip invalidation`);
      return;
    }

    const {
      isNewConversation = false,
      participants = [],
      invalidateConversation = false,
    } = options;

    try {
      // ✅ PATTERNS D'INVALIDATION CIBLÉS
      const patterns = [];

      // ✅ INVALIDER LA CONVERSATION SEULEMENT SI EXPLICITEMENT DEMANDÉ
      if (invalidateConversation) {
        patterns.push(
          // Conversation spécifique
          `${this.cacheKeyPrefix}:id:${conversationId}`,
        );
      }

      // ✅ INVALIDER POUR TOUS LES PARTICIPANTS
      for (const participantId of participants) {
        patterns.push(
          // Listes utilisateur
          `${this.cacheKeyPrefix}:user:${participantId}:*`,
          // Quick loads
          `${this.cacheKeyPrefix}:quick:${participantId}:*`,
        );
      }

      console.log(`🔍 ${patterns.length} patterns à invalider:`, patterns);
      let invalidated = 0;
      for (const pattern of patterns) {
        console.log(`🗑️ Tentative suppression cache: ${pattern}`);
        try {
          const deleted = await this.cache.delete(pattern);
          console.log(
            `✅ Suppression terminée pour ${pattern}: ${deleted} clés`,
          );
          if (deleted > 0) {
            invalidated += deleted;
            console.log(
              `🗑️ Cache conversation invalidé: ${pattern} (${deleted} clés)`,
            );
          }
        } catch (error) {
          console.warn(`⚠️ Erreur invalidation ${pattern}:`, error.message);
        }
      }

      if (invalidated > 0) {
        console.log(
          `🗑️ Total cache conversation invalidé: ${invalidated} clé(s) pour ${conversationId}`,
        );
      }

      console.log(
        `✅ Boucle d'invalidation terminée. Total: ${invalidated} clés`,
      );
      // ✅ INVALIDATION PROACTIVE (pré-charger)
      if (isNewConversation && this.cache) {
        console.log(`🔄 Lancement setImmediate pour pré-chargement`);
        setImmediate(async () => {
          try {
            console.log(
              `🔄 Pré-chargement cache conversations pour participants: ${participants.join(
                ", ",
              )}`,
            );

            // Pré-charger pour chaque participant
            for (const participantId of participants) {
              try {
                await this.getQuickConversations(participantId, 10);
              } catch (preloadError) {
                console.warn(
                  `⚠️ Erreur pré-chargement ${participantId}:`,
                  preloadError.message,
                );
              }
            }
          } catch (preloadError) {
            console.warn(
              "⚠️ Erreur pré-chargement conversations:",
              preloadError.message,
            );
          }
        });
      }
    } catch (error) {
      console.error(
        "❌ Erreur invalidation cache conversations:",
        error.message,
      );
    }
  }

  // ===== AUTRES MÉTHODES (déléguées au primaryStore) =====

  async updateLastMessage(conversationId, messageData) {
    try {
      const result = await this.primaryStore.updateLastMessage(
        conversationId,
        messageData,
      );

      if (result) {
        // ✅ RÉCUPÉRER LES PARTICIPANTS POUR INVALIDER LE CACHE
        const participants = result.participants || [];

        // ✅ INVALIDER LE CACHE DES CONVERSATIONS POUR TOUS LES PARTICIPANTS
        await this.invalidateConversationCaches(conversationId, {
          participants,
          invalidateConversation: true, // ✅ INVALIDER AUSSI LA CONVERSATION SPÉCIFIQUE
        });

        console.log(
          `🔄 Derniers messages mis à jour et cache invalidé: ${conversationId}`,
        );
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur updateLastMessage:", error.message);
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR UNIQUEMENT LE STATUT DU DERNIER MESSAGE
   * @param {string} conversationId - ID de la conversation
   * @param {string} messageId - ID du message (doit correspondre au lastMessage._id)
   * @param {string} newStatus - Nouveau statut (DELIVERED, READ)
   */
  async updateLastMessageStatus(conversationId, messageId, newStatus) {
    try {
      const result = await this.primaryStore.updateLastMessageStatus(
        conversationId,
        messageId,
        newStatus,
      );

      if (result) {
        // ✅ RÉCUPÉRER LES PARTICIPANTS POUR INVALIDER LE CACHE
        const participants = result.participants || [];

        // ✅ INVALIDER LE CACHE DES CONVERSATIONS POUR TOUS LES PARTICIPANTS
        await this.invalidateConversationCaches(conversationId, {
          participants,
          invalidateConversation: true,
        });

        console.log(
          `🔄 lastMessage.status mis à jour (${newStatus}) et cache invalidé: ${conversationId}`,
        );
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur updateLastMessageStatus:", error.message);
      throw error;
    }
  }

  async findPrivateConversation(participant1, participant2) {
    return this.primaryStore.findPrivateConversation(
      participant1,
      participant2,
    );
  }

  async incrementBroadcastMessageCount(broadcastConversationId) {
    return this.primaryStore.incrementBroadcastMessageCount(
      broadcastConversationId,
    );
  }

  async updateBroadcastMetadata(
    broadcastConversationId,
    privateConversationEntries,
  ) {
    return this.primaryStore.updateBroadcastMetadata(
      broadcastConversationId,
      privateConversationEntries,
    );
  }

  async incrementUnreadCountInUserMetadata(conversationId, userId, amount = 1) {
    try {
      const result = await this.primaryStore.incrementUnreadCountInUserMetadata(
        conversationId,
        userId,
        amount,
      );

      // Invalider le cache pour cet utilisateur
      await this.invalidateConversationCaches(conversationId, {
        participants: [userId],
      });

      return result;
    } catch (error) {
      console.error(
        "❌ Erreur incrementUnreadCountInUserMetadata:",
        error.message,
      );
      throw error;
    }
  }

  async resetUnreadCountInUserMetadata(conversationId, userId) {
    try {
      const result = await this.primaryStore.resetUnreadCountInUserMetadata(
        conversationId,
        userId,
      );

      // Invalider le cache pour cet utilisateur
      await this.invalidateConversationCaches(conversationId, {
        participants: [userId],
      });

      return result;
    } catch (error) {
      console.error("❌ Erreur resetUnreadCountInUserMetadata:", error.message);
      throw error;
    }
  }

  /**
   * ✅ DÉCRÉMENTER LE COMPTEUR unreadCount (au lieu de réinitialiser à 0)
   */
  async decrementUnreadCountInUserMetadata(conversationId, userId, count = 1) {
    try {
      const result = await this.primaryStore.decrementUnreadCountInUserMetadata(
        conversationId,
        userId,
        count,
      );

      // Invalider le cache pour cet utilisateur
      await this.invalidateConversationCaches(conversationId, {
        participants: [userId],
      });

      return result;
    } catch (error) {
      console.error(
        "❌ Erreur decrementUnreadCountInUserMetadata:",
        error.message,
      );
      throw error;
    }
  }

  /**
   * ✅ METTRE À JOUR LE lastSeen POUR UN UTILISATEUR (DÉCONNEXION)
   */
  async updateLastSeenForUser(userId) {
    try {
      const result = await this.primaryStore.updateLastSeenForUser(userId);

      // Pas d'invalidation massive du cache, juste log
      console.log(`📝 [Cache] lastSeen mis à jour pour ${userId}`);

      return result;
    } catch (error) {
      console.error("❌ Erreur updateLastSeenForUser:", error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * ✅ OBTENIR LE lastSeen D'UN UTILISATEUR DANS UNE CONVERSATION
   */
  async getLastSeenForUser(conversationId, userId) {
    return await this.primaryStore.getLastSeenForUser(conversationId, userId);
  }

  /**
   * ✅ TROUVER LE lastSeen LE PLUS RÉCENT D'UN UTILISATEUR (TOUTES CONVERSATIONS)
   * Utilisé comme fallback MongoDB quand Redis est vide (ex: après FLUSHALL)
   */
  async findLastSeenForUser(userId) {
    return await this.primaryStore.findLastSeenForUser(userId);
  }

  // ===== ARCHIVAGE =====

  /**
   * Archive ou désarchive une conversation pour un utilisateur.
   * Invalide ensuite le cache de la liste de conversations de l'utilisateur.
   */
  async archiveForUser(conversationId, userId, action = "archive") {
    const result = await this.primaryStore.archiveForUser(
      conversationId,
      userId,
      action,
    );

    // Invalider le cache de liste de conversations de l'utilisateur
    if (this.cache) {
      try {
        const patterns = [`${this.cacheKeyPrefix}:user:${userId}:*`];
        for (const pattern of patterns) {
          await this.cache.delete(pattern);
        }
        console.log(
          `🗑️ Cache conversations invalidé pour userId=${userId} après ${action}`,
        );
      } catch (err) {
        console.warn("⚠️ Erreur invalidation cache archivage:", err.message);
      }
    }

    return result;
  }

  /**
   * Récupère les conversations archivées d'un utilisateur (pas de cache — liste dynamique).
   */
  async findArchivedByUser(userId, options = {}) {
    return await this.primaryStore.findArchivedByUser(userId, options);
  }

  // ===== RECHERCHE =====
  async searchConversations(query, options = {}) {
    const { userId, useCache = false } = options;

    // Pour la recherche, on évite généralement le cache car les résultats peuvent changer
    return await this.primaryStore.searchConversations(query, {
      ...options,
      useCache: false,
    });
  }

  // ===== NETTOYAGE =====
  async clearCache() {
    if (!this.cache) return;

    try {
      const patterns = [`${this.cacheKeyPrefix}:*`];

      for (const pattern of patterns) {
        await this.cache.delete(pattern);
      }

      console.log("✅ Cache conversations complètement nettoyé");
    } catch (error) {
      console.error("❌ Erreur clearCache conversations:", error.message);
    }
  }
}

module.exports = CachedConversationRepository;
