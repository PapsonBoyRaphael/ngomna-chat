const RedisManager = require("../redis/RedisManager");

/**
 * UserCache - Cache centralisé des profils utilisateurs dans Redis
 *
 * Stratégie :
 * - Stockage dans des Redis Hashes (HSET/HGETALL)
 * - Clé: identity:cache:users:{userId} (ex: identity:cache:users:570479H)
 * - TTL par défaut: 7 jours
 * - Champs: matricule (clé primaire), nom, ministere, sexe
 *
 * Avantages :
 * - Latence < 1ms pour les lectures
 * - Réduction des appels HTTP entre services
 * - Cache partagé entre tous les services
 * - Stockage des nom/prenom séparés pour éviter les incohérences
 */
class UserCache {
  constructor(options = {}) {
    this.prefix = options.prefix || "identity:cache:users:";
    this.defaultTTL = options.defaultTTL || 7 * 24 * 3600; // 7 jours
    this.redis = null;
  }

  /**
   * Initialise le cache avec le client Redis
   */
  async initialize() {
    this.redis = RedisManager?.clients?.cache;

    if (!this.redis) {
      console.warn("⚠️ [UserCache] Redis non disponible");
      return false;
    }

    console.log("✅ [UserCache] Initialisé avec succès");
    return true;
  }

  /**
   * Sauvegarde un profil utilisateur dans le cache
   * @param {Object} user - Objet utilisateur avec id, nom, prenom, fullName, avatar, matricule, ministere, sexe
   * @returns {Promise<void>}
   */
  async set(user) {
    if (!this.redis || !user || !user.matricule) {
      console.warn("⚠️ [UserCache] Impossible de sauvegarder:", {
        hasRedis: !!this.redis,
        hasUser: !!user,
        userId: user?.matricule,
      });
      return;
    }

    try {
      const userId = String(user.matricule); // ✅ Force string pour cohérence (570479H, pas 1)
      const key = `${this.prefix}${userId}`;

      const data = {
        ...user,
        updatedAt: Date.now().toString(),
      };

      // ✅ Sauvegarde dans Redis Hash
      await this.redis.hSet(key, data);

      // ✅ Définir le TTL
      await this.redis.expire(key, this.defaultTTL);
    } catch (error) {
      console.error(
        `❌ [UserCache] Erreur sauvegarde ${user.matricule}:`,
        error.message,
      );
    }
  }

  /**
   * Récupère un profil utilisateur depuis le cache
   * @param {string} userId - ID de l'utilisateur (ex: 570479H)
   * @returns {Promise<Object|null>} Profil ou null si absent
   */
  async get(userId) {
    if (!this.redis || !userId) {
      return null;
    }

    try {
      const userIdStr = String(userId); // ✅ Force string
      const key = `${this.prefix}${userIdStr}`;
      const data = await this.redis.hGetAll(key);

      if (!data || Object.keys(data).length === 0) {
        return null; // Cache miss
      }

      return data; // Cache hit (données brutes du hash)
    } catch (error) {
      console.error(`❌ [UserCache] Erreur lecture ${userId}:`, error.message);
      return null;
    }
  }

  /**
   * Récupère plusieurs profils utilisateurs en batch (optimisé avec pipeline)
   * @param {Array<string>} userIds - Liste des IDs (ex: [570479H, 534589D, 792665T])
   * @returns {Promise<Array<Object>>} Liste des profils
   */
  async batchGet(userIds) {
    if (!this.redis || !userIds || userIds.length === 0) {
      return [];
    }

    try {
      const pipeline = this.redis.multi();
      const userIdStrs = userIds.map((id) => String(id)); // ✅ Force string
      const keys = userIdStrs.map((id) => `${this.prefix}${id}`);

      keys.forEach((key) => {
        pipeline.hGetAll(key);
      });

      const results = await pipeline.exec();

      return results.map((result, i) => {
        // ✅ redis v4: exec() retourne directement la réponse
        // ✅ redis v3 / compat: exec() retourne [err, data]
        let err = null;
        let data = null;

        if (Array.isArray(result)) {
          [err, data] = result; // format legacy [err, data]
        } else {
          data = result; // format redis v4 direct
        }

        const userId = userIdStrs[i];

        if (err || !data || Object.keys(data).length === 0) {
          // return null; // Cache miss ou erreur
          return {
            nom: "Utilisateur inconnu",
            matricule: userId,
          };
        }

        return data;
      });
    } catch (error) {
      console.error(`❌ [UserCache] Erreur batchGet:`, error.message);
      return userIds.map((userId) => ({
        nom: "Utilisateur inconnu",
        matricule: String(userId),
      }));
    }
  }

  /**
   * Invalide le cache d'un utilisateur
   * @param {string} userId - ID de l'utilisateur
   * @returns {Promise<void>}
   */
  async invalidate(userId) {
    if (!this.redis || !userId) {
      return;
    }

    try {
      const userIdStr = String(userId); // ✅ Force string
      const key = `${this.prefix}${userIdStr}`;
      await this.redis.del(key);
      console.log(`🗑️ [UserCache] Cache invalidé: ${userIdStr}`);
    } catch (error) {
      console.error(
        `❌ [UserCache] Erreur invalidation ${userId}:`,
        error.message,
      );
    }
  }

  /**
   * Invalide plusieurs utilisateurs en batch
   * @param {Array<string>} userIds - Liste des IDs
   * @returns {Promise<void>}
   */
  async batchInvalidate(userIds) {
    if (!this.redis || !userIds || userIds.length === 0) {
      return;
    }

    try {
      const userIdStrs = userIds.map((id) => String(id)); // ✅ Force string
      const keys = userIdStrs.map((id) => `${this.prefix}${id}`);
      await this.redis.del(keys);
      console.log(`🗑️ [UserCache] ${userIds.length} caches invalidés`);
    } catch (error) {
      console.error(`❌ [UserCache] Erreur batchInvalidate:`, error.message);
    }
  }

  /**
   * Compte le nombre d'utilisateurs en cache
   * @returns {Promise<number>}
   */
  async count() {
    if (!this.redis) {
      return 0;
    }

    try {
      let cursor = "0";
      let count = 0;

      do {
        const result = await this.redis.scan(cursor, {
          MATCH: `${this.prefix}*`,
          COUNT: 1000,
        });

        cursor = result.cursor.toString();
        count += result.keys.length;
      } while (cursor !== "0");

      return count;
    } catch (error) {
      console.error(`❌ [UserCache] Erreur count:`, error.message);
      return 0;
    }
  }

  /**
   * Vérifie si un utilisateur est en cache
   * @param {string} userId - ID de l'utilisateur
   * @returns {Promise<boolean>}
   */
  async exists(userId) {
    if (!this.redis || !userId) {
      return false;
    }

    try {
      const userIdStr = String(userId); // ✅ Force string
      const key = `${this.prefix}${userIdStr}`;
      const exists = await this.redis.exists(key);
      return exists === 1;
    } catch (error) {
      console.error(`❌ [UserCache] Erreur exists ${userId}:`, error.message);
      return false;
    }
  }

  /**
   * Récupère les statistiques du cache
   * @returns {Promise<Object>}
   */
  async getStats() {
    try {
      const count = await this.count();

      return {
        totalCached: count,
        prefix: this.prefix,
        ttl: this.defaultTTL,
        ttlHours: Math.floor(this.defaultTTL / 3600),
      };
    } catch (error) {
      console.error(`❌ [UserCache] Erreur getStats:`, error.message);
      return {
        totalCached: 0,
        prefix: this.prefix,
        ttl: this.defaultTTL,
        ttlHours: Math.floor(this.defaultTTL / 3600),
      };
    }
  }
}

// Export singleton
module.exports = new UserCache();
