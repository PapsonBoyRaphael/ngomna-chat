/**
 * CacheService - Service de cache Redis optimisé pour 500k+ users
 * ✅ Migré vers le module partagé
 * ✅ Utilise RedisManager singleton
 */

class CacheService {
  constructor(options = {}) {
    // ✅ INJECTION VIA CONSTRUCTEUR OU LAZY LOADING
    this.redisManager = null;
    this.redis = null;

    this.options = {
      defaultTTL: options.defaultTTL || 3600,
      keyPrefix: options.keyPrefix || "chat",
      maxScanCount: options.maxScanCount || 100,
      ...options,
    };

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

    console.log("✅ CacheService initialisé via RedisManager");
  }

  /**
   * Initialiser avec un client Redis direct (compatibilité)
   */
  initializeWithClient(redisClient) {
    this.redis = redisClient;
    this.isInitialized = true;
    console.log("✅ CacheService initialisé avec client direct");
  }

  /**
   * Renouveler le TTL d'une clé existante
   * Utile pour les cache hits - étendre la vie des entrées utilisées
   */
  async renewTTL(key, ttl = this.options.defaultTTL) {
    if (!this.redis) return false;

    try {
      const cacheKey = `${this.options.keyPrefix}:${this.sanitizeKey(key)}`;

      // Vérifier existence
      const exists = await this.redis.exists(cacheKey);
      if (!exists) {
        console.warn(`⚠️ renewTTL: Clé inexistante: ${key}`);
        return false;
      }

      // Appliquer TTL
      await this.redis.expire(cacheKey, ttl);
      console.log(`🔄 TTL renouvelé: ${key} (${ttl}s)`);
      return true;
    } catch (err) {
      console.error(`❌ Erreur renewTTL ${key}:`, err.message);
      return false;
    }
  }

  // ✅ UTILITAIRE (ajouter si manquant)
  sanitizeKey(key) {
    if (!key || key === "null" || key === "undefined") return "unknown";
    return String(key).trim();
  }

  // ✅ CACHE BASIQUE
  async get(key) {
    if (!this.redis) {
      console.warn("⚠️ Cache get: Redis n'est pas disponible");
      return null;
    }

    try {
      const cacheKey = `${this.options.keyPrefix}:${this.sanitizeKey(key)}`;
      const value = await this.redis.get(cacheKey);

      if (!value) {
        return null;
      }

      try {
        return JSON.parse(value);
      } catch (parseErr) {
        console.error(
          `❌ Erreur parsing JSON en cache pour clé '${cacheKey}':`,
          parseErr.message,
        );
        // Supprimer la clé corrompue
        await this.redis.del(cacheKey);
        return null;
      }
    } catch (err) {
      console.warn("⚠️ Cache get error:", err.message);
      return null;
    }
  }

  async set(key, value, ttl = this.options.defaultTTL) {
    if (!this.redis) {
      console.warn("⚠️ Cache set: Redis n'est pas disponible");
      return false;
    }

    try {
      const cacheKey = `${this.options.keyPrefix}:${this.sanitizeKey(key)}`;

      let jsonValue;
      try {
        jsonValue = JSON.stringify(value);
      } catch (stringifyErr) {
        console.error(
          `❌ Erreur stringify JSON pour clé '${cacheKey}':`,
          stringifyErr.message,
        );
        return false;
      }

      await this.redis.setEx(cacheKey, ttl, jsonValue);
      return true;
    } catch (err) {
      console.warn("⚠️ Cache set error:", err.message);
      return false;
    }
  }

  async delete(keyOrPattern) {
    if (!this.redis) return 0;

    try {
      if (keyOrPattern.includes("*")) {
        return await this._deleteByPattern(keyOrPattern);
      } else {
        const cacheKey = `${this.options.keyPrefix}:${this.sanitizeKey(
          keyOrPattern,
        )}`;
        return await this.redis.del(cacheKey);
      }
    } catch (err) {
      console.warn("⚠️ Cache delete error:", err.message);
      return 0;
    }
  }

  async _deleteByPattern(pattern) {
    let deletedCount = 0;
    let cursor = "0"; // ✅ CHAÎNE au lieu de nombre

    try {
      do {
        const result = await this.redis.scan(cursor, {
          MATCH: `${this.options.keyPrefix}:${pattern}`,
          COUNT: this.options.maxScanCount,
        });

        cursor = String(result.cursor); // ✅ CONVERTIR en chaîne

        if (result.keys.length > 0) {
          const count = await this.redis.del(result.keys);
          deletedCount += count;
        }
      } while (cursor !== "0"); // ✅ COMPARER avec chaîne

      return deletedCount;
    } catch (err) {
      console.warn("⚠️ Cache deleteByPattern error:", err.message);
      return deletedCount;
    }
  }

  // Cache des derniers messages d'une room
  async cacheLastMessages(roomId, messages, ttl = 3600) {
    if (!this.redis) return false;

    try {
      const cacheKey = `${
        this.options.keyPrefix
      }:last_messages:${this.sanitizeKey(roomId)}`;
      const data = {
        messages,
        count: messages.length,
        cachedAt: new Date().toISOString(),
      };
      await this.redis.setEx(cacheKey, ttl, JSON.stringify(data));
      console.log(`📦 Cached ${messages.length} messages for room ${roomId}`);
      return true;
    } catch (err) {
      console.warn("⚠️ Erreur cacheLastMessages:", err.message);
      return false;
    }
  }

  async getCachedLastMessages(roomId) {
    if (!this.redis) return null;

    try {
      const cacheKey = `${
        this.options.keyPrefix
      }:last_messages:${this.sanitizeKey(roomId)}`;
      const cached = await this.redis.get(cacheKey);

      if (!cached) {
        console.log(`📦 Last messages miss: ${roomId}`);
        return null;
      }

      const data = JSON.parse(cached);
      console.log(`📦 Last messages hit: ${data.count} messages (${roomId})`);
      return data.messages || [];
    } catch (err) {
      console.warn("⚠️ Erreur getCachedLastMessages:", err.message);
      return null;
    }
  }

  async invalidateRoomMessages(roomId) {
    return await this.delete(`last_messages:${roomId}`);
  }

  /**
   * Statistiques du cache
   */
  async getStats() {
    if (!this.redis) {
      return { status: "disconnected" };
    }

    try {
      const info = await this.redis.info("memory");
      const keyCount = await this.redis.dbSize();

      return {
        status: "connected",
        keyCount,
        memoryInfo: info.substring(0, 500),
        prefix: this.options.keyPrefix,
        defaultTTL: this.options.defaultTTL,
      };
    } catch (err) {
      return { status: "error", error: err.message };
    }
  }
}

module.exports = CacheService;
