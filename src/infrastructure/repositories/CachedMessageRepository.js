/**
 * CachedMessageRepository - Repository pattern avec cache Redis
 * ✅ SEUL RESPONSABLE du cache des messages
 * ✅ OPTIMISÉ : Cache stratégique intelligent
 */
class CachedMessageRepository {
  constructor(messageRepository, cacheService) {
    this.primaryStore = messageRepository;
    this.cache = cacheService;
    this.redis = cacheService?.redis || null;

    // ✅ Configuration cache optimisée
    this.defaultTTL = 3600; // 1 heure
    this.shortTTL = 300; // 5 minutes
    this.quickTTL = 60; // 1 minute pour quick load
    this.unreadTTL = 86400; // 24 heures

    this.userUnreadPrefix = "chat:cache:unread:user";
    this.conversationUnreadPrefix = "chat:cache:unread:conv";
    this.cacheKeyPrefix = "chat:cache:msgs";
  }

  // ===== LIRE LES MESSAGES D'UNE CONVERSATION (CACHE INTELLIGENT) =====
  async findByConversation(conversationId, options = {}) {
    const {
      cursor = null,
      page = 1,
      limit = 50,
      direction = "older",
      userId,
      useCache = true,
    } = options;

    try {
      // ✅ STRATÉGIE CACHE DIFFÉRENCIÉE
      let cacheKey = null;
      let ttl = this.shortTTL;

      if (useCache && this.cache && !cursor) {
        // ✅ Inclure userId dans la clé de cache pour isoler les suppressions FOR_ME
        const userSegment = userId ? `:u${userId}` : "";

        if (cursor) {
          // Pagination avec cursor - cache court
          cacheKey = `${this.cacheKeyPrefix}:${conversationId}${userSegment}:cursor:${cursor}:${limit}`;
          ttl = this.shortTTL;
        } else if (page === 1) {
          // Première page - cache long
          cacheKey = `${this.cacheKeyPrefix}:${conversationId}${userSegment}:first:${limit}`;
          ttl = this.defaultTTL;
        } else {
          // Autres pages - cache moyen
          cacheKey = `${this.cacheKeyPrefix}:${conversationId}${userSegment}:p${page}:${limit}`;
          ttl = this.shortTTL;
        }

        // ✅ VÉRIFIER LE CACHE
        const cached = await this.cache.get(cacheKey);
        if (cached) {
          console.log(
            `📦 Messages depuis cache: ${conversationId} (${cacheKey})`,
          );

          // ✅ RENOUVELER TTL SUR HIT
          await this.cache.renewTTL(cacheKey, ttl);

          return {
            messages: cached.messages || cached,
            fromCache: true,
            nextCursor: cached.nextCursor || null,
            hasMore: cached.hasMore || false,
          };
        }
      }

      // ✅ CACHE MISS → MongoDB
      console.log(
        `🔍 Messages depuis MongoDB: ${conversationId} ${
          cursor ? `(cursor: ${cursor})` : `(page: ${page})`
        }`,
      );

      let result;
      if (cursor) {
        // ✅ PAGINATION CURSOR-BASED
        result = await this.primaryStore.findByConversationWithCursor(
          conversationId,
          { cursor, limit, direction, userId },
        );
      } else {
        // ✅ PAGINATION PAGE-BASED (fallback)
        const messages = await this.primaryStore.findByConversation(
          conversationId,
          { page, limit, userId },
        );

        result = {
          messages: messages.messages || messages,
          nextCursor: messages.nextCursor || null,
          hasMore: messages.hasMore || false,
        };
      }

      // ✅ METTRE EN CACHE SELON LA STRATÉGIE
      if (useCache && this.cache && cacheKey && result.messages?.length > 0) {
        await this.cache.set(cacheKey, result, ttl);
        console.log(
          `💾 Messages mis en cache: ${result.messages.length} (TTL: ${ttl}s)`,
        );
      }

      return {
        ...result,
        fromCache: false,
      };
    } catch (error) {
      console.error("❌ Erreur findByConversation:", error.message);

      // ✅ FALLBACK SANS CACHE (userId propagé pour filtrer les messages supprimés FOR_ME)
      const messages = await this.primaryStore.findByConversation(
        conversationId,
        { page: 1, limit: 20, userId },
      );

      return {
        messages,
        fromCache: false,
        hasMore: false,
      };
    }
  }

  // ===== RÉCUPÉRER LES DERNIERS MESSAGES (POUR QUICK LOAD) =====
  async getLastMessagesWithPreload(conversationId, limit = 20) {
    try {
      const quickCacheKey = `${this.cacheKeyPrefix}:quick:${conversationId}:${limit}`;

      // ✅ CACHE QUICK (très court TTL)
      if (this.cache) {
        const cached = await this.cache.get(quickCacheKey);
        if (cached) {
          console.log(`⚡ Quick messages depuis cache: ${conversationId}`);
          return {
            messages: cached.messages || cached,
            fromCache: true,
            isQuick: true,
          };
        }
      }

      // ✅ CACHE MISS → MongoDB
      console.log(`⚡ Quick messages depuis MongoDB: ${conversationId}`);

      const result = await this.primaryStore.findByConversation(
        conversationId,
        { page: 1, limit, useCache: false },
      );

      const messages = result.messages || result;

      // ✅ METTRE EN CACHE QUICK (TTL court)
      if (this.cache && messages.length > 0) {
        await this.cache.set(quickCacheKey, { messages }, this.quickTTL);
        console.log(
          `⚡ Quick messages mis en cache: ${messages.length} (${this.quickTTL}s)`,
        );
      }

      return {
        messages,
        fromCache: false,
        isQuick: true,
      };
    } catch (error) {
      console.error("❌ Erreur getLastMessagesWithPreload:", error.message);

      // Fallback
      const result = await this.primaryStore.findByConversation(
        conversationId,
        { page: 1, limit: 20 },
      );

      return {
        messages: result.messages || result,
        fromCache: false,
        isQuick: true,
      };
    }
  }

  // ===== SAUVEGARDER UN MESSAGE (avec invalidation intelligente) =====
  async save(messageOrData) {
    try {
      const startTime = Date.now();

      // 1. ✅ SAUVEGARDE MongoDB
      const savedMessage = await this.primaryStore.save(messageOrData);

      if (!savedMessage) {
        throw new Error("Message not saved");
      }

      // 2. ✅ INCRÉMENTER LES COMPTEURS UNREAD
      if (savedMessage.receiverId) {
        await this.incrementUnreadCount(
          savedMessage.conversationId,
          savedMessage.receiverId,
        );
      }

      // 3. ✅ INVALIDATION CACHE INTELLIGENTE
      await this.invalidateConversationCaches(savedMessage.conversationId, {
        isNewMessage: true,
        messageType: savedMessage.type,
      });

      const processingTime = Date.now() - startTime;
      console.log(
        `✅ Message sauvegardé avec cache: ${savedMessage._id} (${processingTime}ms)`,
      );

      return savedMessage;
    } catch (error) {
      console.error("❌ Erreur save (cached):", error.message);
      throw error;
    }
  }

  // ===== INVALIDATION CACHE INTELLIGENTE =====
  async invalidateConversationCaches(conversationId, options = {}) {
    if (!this.cache) return;

    const { isNewMessage = false, messageType = "TEXT" } = options;

    try {
      // ✅ PATTERNS D'INVALIDATION CIBLÉS
      // NOTE: On N'invalide PAS la clé `chat:cache:convs:id:${conversationId}`
      // car elle est gérée par CachedConversationRepository
      const patterns = [
        // Messages paginés UNIQUEMENT
        `${this.cacheKeyPrefix}:${conversationId}:*`,
        // Quick load messages UNIQUEMENT
        `${this.cacheKeyPrefix}:quick:${conversationId}:*`,
        // Derniers messages classiques UNIQUEMENT
        `chat:last_messages:${conversationId}`,
        // ❌ SUPPRIMÉ: `chat:conversation:${conversationId}*`
        // Raison: Cela invalide aussi `chat:cache:convs:id:${conversationId}` de CachedConversationRepository
        // Les conversations doivent rester en cache après la sauvegarde d'un message
      ];

      let invalidated = 0;
      for (const pattern of patterns) {
        try {
          const deleted = await this.cache.delete(pattern);
          if (deleted > 0) {
            invalidated += deleted;
            console.log(
              `🗑️ Cache invalidé (messages): ${pattern} (${deleted} clés)`,
            );
          }
        } catch (error) {
          console.warn(`⚠️ Erreur invalidation ${pattern}:`, error.message);
        }
      }

      if (invalidated > 0) {
        console.log(
          `🗑️ Total cache invalidé: ${invalidated} clé(s) pour ${conversationId}`,
        );
      }

      // ✅ INVALIDATION PROACTIVE (pré-charger)
      if (isNewMessage && this.cache) {
        // Pré-charger les derniers messages en arrière-plan
        setImmediate(async () => {
          try {
            console.log(`🔄 Pré-chargement cache pour: ${conversationId}`);
            await this.getLastMessagesWithPreload(conversationId, 20);
          } catch (preloadError) {
            console.warn("⚠️ Erreur pré-chargement:", preloadError.message);
          }
        });
      }
    } catch (error) {
      console.error("❌ Erreur invalidation cache:", error.message);
    }
  }

  // ===== MÉTHODES UNREAD (inchangées) =====

  async incrementUnreadCount(conversationId, userId) {
    if (!this.redis) return 0;

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const convKey = `${this.conversationUnreadPrefix}:${conversationId}:${userId}`;

      const [userCount, convCount] = await Promise.all([
        this.redis.incr(userKey),
        this.redis.incr(convKey),
      ]);

      await Promise.all([
        this.redis.expire(userKey, this.unreadTTL),
        this.redis.expire(convKey, this.unreadTTL),
      ]);

      console.log(
        `📈 Unread incrémenté: ${userId} dans ${conversationId} = ${userCount}`,
      );
      return userCount;
    } catch (error) {
      console.error("❌ Erreur incrementUnreadCount:", error.message);
      return 0;
    }
  }

  async resetUnreadCount(conversationId, userId) {
    if (!this.redis) return true;

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const convKey = `${this.conversationUnreadPrefix}:${conversationId}:${userId}`;

      await Promise.all([this.redis.del(userKey), this.redis.del(convKey)]);
      console.log(`🔄 Unread réinitialisé: ${userId} dans ${conversationId}`);
      return true;
    } catch (error) {
      console.error("❌ Erreur resetUnreadCount:", error.message);
      return false;
    }
  }

  /**
   * ✅ DÉCRÉMENTER le compteur Redis (au lieu de supprimer la clé)
   */
  async decrementUnreadCount(conversationId, userId, count = 1) {
    if (!this.redis) return true;

    try {
      const safeCount = Math.max(0, Math.floor(count));
      if (safeCount === 0) return true;

      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const convKey = `${this.conversationUnreadPrefix}:${conversationId}:${userId}`;

      // Décrémenter les deux clés
      const [userResult, convResult] = await Promise.all([
        this.redis.decrBy(userKey, safeCount),
        this.redis.decrBy(convKey, safeCount),
      ]);

      // Garantir min 0 (DECRBY peut descendre en négatif)
      if (userResult < 0) await this.redis.set(userKey, "0");
      if (convResult < 0) await this.redis.set(convKey, "0");

      // Maintenir le TTL
      await Promise.all([
        this.redis.expire(userKey, this.unreadTTL),
        this.redis.expire(convKey, this.unreadTTL),
      ]);

      const finalCount = Math.max(0, userResult);
      console.log(
        `📉 Unread décrémenté: ${userId} dans ${conversationId} (-${safeCount}) → ${finalCount}`,
      );
      return finalCount;
    } catch (error) {
      console.error("❌ Erreur decrementUnreadCount:", error.message);
      return 0;
    }
  }

  async getUnreadCount(userId, conversationId) {
    if (!this.redis) {
      return await this.primaryStore.countUnreadMessages(
        conversationId,
        userId,
      );
    }

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      let count = await this.redis.get(userKey);

      if (count !== null) {
        const result = parseInt(count) || 0;
        console.log(`📦 Unread depuis Redis: ${result}`);
        return result;
      }

      // Fallback MongoDB
      console.log(`🔍 Unread miss → recalcul MongoDB`);
      const realCount = await this.primaryStore.countUnreadMessages(
        conversationId,
        userId,
      );

      if (realCount > 0) {
        await this.redis.setEx(userKey, this.unreadTTL, realCount.toString());
      }

      return realCount;
    } catch (error) {
      console.warn("⚠️ Erreur getUnreadCount:", error.message);
      return await this.primaryStore.countUnreadMessages(
        conversationId,
        userId,
      );
    }
  }

  // ===== AUTRES MÉTHODES (déléguées au primaryStore) =====

  async findById(messageId) {
    return await this.primaryStore.findById(messageId);
  }

  async getLastMessage(conversationId) {
    return await this.primaryStore.getLastMessage(conversationId);
  }

  async getMessageCount(conversationId) {
    return await this.primaryStore.getMessageCount(conversationId);
  }

  async markMessagesAsRead(conversationId, userId, messageIds = null) {
    try {
      const result = await this.primaryStore.updateMessageStatus(
        conversationId,
        userId,
        "READ",
        messageIds,
      );

      if (result.modifiedCount > 0) {
        await this.decrementUnreadCount(
          conversationId,
          userId,
          result.modifiedCount,
        );
        await this.invalidateConversationCaches(conversationId);
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur markMessagesAsRead:", error.message);
      throw error;
    }
  }

  async updateMessageStatus(conversationId, userId, status, messageIds = null) {
    try {
      const result = await this.primaryStore.updateMessageStatus(
        conversationId,
        userId,
        status,
        messageIds,
      );

      await this.invalidateConversationCaches(conversationId);

      if (status === "READ" && result.modifiedCount > 0) {
        await this.decrementUnreadCount(
          conversationId,
          userId,
          result.modifiedCount,
        );
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur updateMessageStatus:", error.message);
      throw error;
    }
  }

  /**
   * ✅ Mettre à jour le statut d'un message unique
   */
  async updateSingleMessageStatus(messageId, receiverId, status) {
    try {
      const result = await this.primaryStore.updateSingleMessageStatus(
        messageId,
        receiverId,
        status,
      );

      // Invalider le cache de la conversation si le message existe
      if (result && result.message && result.message.conversationId) {
        await this.invalidateConversationCaches(result.message.conversationId);

        if (status === "READ" && result.modifiedCount > 0) {
          await this.decrementUnreadCount(
            result.message.conversationId,
            receiverId,
            result.modifiedCount || 1,
          );
        }
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur updateSingleMessageStatus:", error.message);
      throw error;
    }
  }

  async deleteMessage(messageId) {
    try {
      const result = await this.primaryStore.deleteById(messageId);

      if (result && result.conversationId) {
        await this.invalidateConversationCaches(result.conversationId);
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur deleteMessage:", error.message);
      throw error;
    }
  }

  // ===== NETTOYAGE =====
  async clearCache() {
    if (!this.cache) return;

    try {
      const patterns = [
        `${this.cacheKeyPrefix}:*`,
        "chat:last_messages:*",
        "chat:conversation:*",
      ];

      for (const pattern of patterns) {
        await this.cache.delete(pattern);
      }

      console.log("✅ Cache messages complètement nettoyé");
    } catch (error) {
      console.error("❌ Erreur clearCache:", error.message);
    }
  }

  // ✅ DÉLÉGATION updateCallStatus au repository sous-jacent
  async updateCallStatus(messageId, updates) {
    return await this.primaryStore.updateCallStatus(messageId, updates);
  }

  // ===== RÉACTIONS (avec invalidation cache) =====

  /**
   * ✅ Ajouter/mettre à jour une réaction (délègue au primaryStore + invalide cache)
   */
  async addReaction(messageId, userId, emoji) {
    try {
      const result = await this.primaryStore.addReaction(
        messageId,
        userId,
        emoji,
      );

      // Invalider le cache de la conversation
      if (result && result.conversationId) {
        await this.invalidateConversationCaches(result.conversationId);
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur addReaction (cached):", error.message);
      throw error;
    }
  }

  /**
   * ✅ Supprimer une réaction (délègue au primaryStore + invalide cache)
   */
  async removeReaction(messageId, userId) {
    try {
      const result = await this.primaryStore.removeReaction(messageId, userId);

      // Invalider le cache de la conversation si suppression effective
      if (result && result.removed && result.conversationId) {
        await this.invalidateConversationCaches(result.conversationId);
      }

      return result;
    } catch (error) {
      console.error("❌ Erreur removeReaction (cached):", error.message);
      throw error;
    }
  }
}

module.exports = CachedMessageRepository;
