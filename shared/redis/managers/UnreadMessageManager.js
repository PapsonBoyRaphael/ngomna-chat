/**
 * UnreadMessageManager - Gestionnaire de messages non lus avec Redis
 * ✅ Migré vers le module partagé
 * ✅ Utilise RedisManager singleton
 * ✅ DÉCOUPLÉ DE MongoDB - utilise des callbacks pour le recalcul
 */

class UnreadMessageManager {
  constructor(options = {}) {
    this.redis = null;
    this.redisManager = null;

    // ✅ PLUS DE primaryStore/messageRepository ICI
    // On utilise des callbacks à la place
    this.recalculateFn = null; // Callback pour recalcul par conversation
    this.recalculateTotalFn = null; // Callback pour recalcul total

    this.keyPrefix = options.keyPrefix || "chat:cache:unread";
    this.userUnreadPrefix =
      options.userUnreadPrefix || "chat:cache:unread:user";
    this.conversationUnreadPrefix =
      options.conversationUnreadPrefix || "chat:cache:unread:conv";
    this.defaultTTL = options.defaultTTL || 3 * 24 * 3600; // 3 jours
    this.isInitialized = false;
  }

  /**
   * Initialiser avec RedisManager
   */
  async initialize(RedisManager) {
    if (this.isInitialized) return;

    this.redisManager = RedisManager;
    await this.redisManager.connect();
    this.redis = this.redisManager.getCacheClient();
    this.isInitialized = true;

    console.log("✅ UnreadMessageManager initialisé via RedisManager");
  }

  /**
   * Initialiser avec un client Redis direct (compatibilité)
   */
  initializeWithClient(redisClient) {
    this.redis = redisClient;
    this.isInitialized = true;
    console.log("✅ UnreadMessageManager initialisé avec client direct");
  }

  /**
   * ✅ INJECTION DU CALLBACK DE RECALCUL PAR CONVERSATION
   * Le service appelant fournit la fonction de recalcul
   * @param {Function} fn - async (conversationId, userId) => number
   */
  setRecalculateFunction(fn) {
    if (typeof fn !== "function") {
      throw new Error("Recalculate function must be a function");
    }
    this.recalculateFn = fn;
    console.log("✅ Fonction de recalcul par conversation injectée");
  }

  /**
   * ✅ INJECTION DU CALLBACK DE RECALCUL TOTAL
   * @param {Function} fn - async (userId) => number
   */
  setRecalculateTotalFunction(fn) {
    if (typeof fn !== "function") {
      throw new Error("Recalculate total function must be a function");
    }
    this.recalculateTotalFn = fn;
    console.log("✅ Fonction de recalcul total injectée");
  }

  /**
   * ✅ MÉTHODE COMBINÉE POUR INJECTER LES DEUX CALLBACKS
   * @param {Object} callbacks - { recalculate, recalculateTotal }
   */
  setCallbacks(callbacks = {}) {
    if (callbacks.recalculate) {
      this.setRecalculateFunction(callbacks.recalculate);
    }
    if (callbacks.recalculateTotal) {
      this.setRecalculateTotalFunction(callbacks.recalculateTotal);
    }
  }

  async incrementUnreadCount(conversationId, userId) {
    if (!this.redis) return 0;

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const conversationKey = `${this.conversationUnreadPrefix}:${conversationId}:${userId}`;

      const [userResult] = await Promise.all([
        this.redis.incr(userKey),
        this.redis.incr(conversationKey),
      ]);

      await Promise.all([
        this.redis.expire(userKey, this.defaultTTL),
        this.redis.expire(conversationKey, this.defaultTTL),
      ]);

      console.log(
        `📈 Compteur incrémenté pour ${userId} dans ${conversationId}: ${userResult}`,
      );
      return userResult;
    } catch (error) {
      console.error("❌ Erreur incrementUnreadCount:", error);
      return 0;
    }
  }

  async resetUnreadCount(conversationId, userId) {
    if (!this.redis) return false;

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const conversationKey = `${this.conversationUnreadPrefix}:${conversationId}:${userId}`;

      await Promise.all([
        this.redis.del(userKey),
        this.redis.del(conversationKey),
      ]);

      console.log(
        `🔄 Compteur réinitialisé pour ${userId} dans ${conversationId}`,
      );
      return true;
    } catch (error) {
      console.error("❌ Erreur resetUnreadCount:", error);
      return false;
    }
  }

  async getUnreadCount(conversationId, userId) {
    // ✅ SI PAS DE REDIS → UTILISER LE CALLBACK SI DISPONIBLE
    if (!this.redis) {
      if (this.recalculateFn) {
        return await this.recalculateFn(conversationId, userId);
      }
      return 0;
    }

    try {
      const userKey = `${this.userUnreadPrefix}:${userId}:${conversationId}`;
      const cached = await this.redis.get(userKey);

      if (cached !== null) {
        const count = parseInt(cached) || 0;
        console.log(`Hit Redis unread: ${count}`);
        return count;
      }

      // ✅ CACHE MISS → UTILISER LE CALLBACK SI DISPONIBLE
      if (this.recalculateFn) {
        console.log(
          `Miss Redis → recalcul via callback pour ${userId} dans ${conversationId}`,
        );
        const realCount = await this.recalculateFn(conversationId, userId);

        if (realCount > 0) {
          await this.redis.set(userKey, realCount, { EX: this.defaultTTL });
        }

        return realCount;
      }

      // Pas de callback → retourner 0
      console.warn(
        `⚠️ Cache miss et pas de callback de recalcul pour ${conversationId}/${userId}`,
      );
      return 0;
    } catch (error) {
      console.error("Erreur getUnreadCount:", error);

      // Fallback sur callback si disponible
      if (this.recalculateFn) {
        return await this.recalculateFn(conversationId, userId);
      }
      return 0;
    }
  }

  async getTotalUnreadCount(userId) {
    // ✅ SI PAS DE REDIS → UTILISER LE CALLBACK SI DISPONIBLE
    if (!this.redis) {
      if (this.recalculateTotalFn) {
        return await this.recalculateTotalFn(userId);
      }
      return 0;
    }

    try {
      const pattern = `${this.userUnreadPrefix}:${userId}:*`;
      let total = 0;
      let cursor = "0";

      do {
        const result = await this.redis.scan(cursor, {
          MATCH: pattern,
          COUNT: 100,
        });

        cursor = String(result.cursor);

        if (result.keys.length > 0) {
          const counts = await Promise.all(
            result.keys.map((key) => this.redis.get(key)),
          );
          total += counts.reduce(
            (sum, count) => sum + (parseInt(count) || 0),
            0,
          );
        }
      } while (cursor !== "0");

      // ✅ SI REDIS VIDE → UTILISER LE CALLBACK SI DISPONIBLE
      if (total === 0 && this.recalculateTotalFn) {
        console.log(`Total Redis = 0 → recalcul global via callback`);
        total = await this.recalculateTotalFn(userId);
      }

      return total;
    } catch (error) {
      console.error("❌ Erreur getTotalUnreadCount:", error);

      // Fallback sur callback si disponible
      if (this.recalculateTotalFn) {
        return await this.recalculateTotalFn(userId);
      }
      return 0;
    }
  }

  async cleanup() {
    if (!this.redis) return 0;

    try {
      let deleted = 0;
      const patterns = [
        `${this.userUnreadPrefix}:*`,
        `${this.conversationUnreadPrefix}:*`,
      ];

      for (const pattern of patterns) {
        let cursor = "0";
        do {
          const result = await this.redis.scan(cursor, {
            MATCH: pattern,
            COUNT: 100,
          });

          cursor = String(result.cursor);

          if (result.keys.length > 0) {
            const expired = await Promise.all(
              result.keys.map(async (key) => {
                const ttl = await this.redis.ttl(key);
                return ttl <= 0 ? key : null;
              }),
            );

            const keysToDelete = expired.filter(Boolean);
            if (keysToDelete.length > 0) {
              await this.redis.del(keysToDelete);
              deleted += keysToDelete.length;
            }
          }
        } while (cursor !== "0");
      }

      console.log(`🧹 Nettoyage terminé: ${deleted} compteurs supprimés`);
      return deleted;
    } catch (error) {
      console.error("❌ Erreur cleanup:", error);
      return 0;
    }
  }

  /**
   * ✅ MÉTHODE UTILITAIRE POUR VÉRIFIER L'ÉTAT
   */
  getStatus() {
    return {
      isInitialized: this.isInitialized,
      hasRedis: !!this.redis,
      hasRecalculateCallback: !!this.recalculateFn,
      hasRecalculateTotalCallback: !!this.recalculateTotalFn,
      config: {
        keyPrefix: this.keyPrefix,
        userUnreadPrefix: this.userUnreadPrefix,
        conversationUnreadPrefix: this.conversationUnreadPrefix,
        defaultTTL: this.defaultTTL,
      },
    };
  }
}

module.exports = UnreadMessageManager;
