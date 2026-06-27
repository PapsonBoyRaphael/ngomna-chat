/**
 * RedisManager - Gestionnaire centralisé Redis (Singleton)
 * ✅ Gère les clients Redis (main, stream, cache, pub, sub)
 * ✅ Intègre StreamManager et CircuitBreaker
 * ✅ Gère les workers de résilience
 * ✅ Compatible avec ResilientMessageService
 */

let redisModule = null;

const getRedisModule = async () => {
  if (!redisModule) {
    redisModule = await import("redis");
  }
  return redisModule;
};

const getCreateClient = async () => {
  const redis = await getRedisModule();
  return redis.createClient || (redis.default && redis.default.createClient);
};
const StreamManager = require("../resilience/StreamManager");
const CircuitBreaker = require("../resilience/CircuitBreaker");

class RedisManager {
 
  constructor() {
    if (RedisManager.instance) {
      return RedisManager.instance;
    }

    // ========== CLIENTS REDIS ==========
    this.clients = {
      main: null,
      stream: null,
      cache: null,
      pub: null,
      sub: null,
    };

    // ========== COMPOSANTS DE RÉSILIENCE ==========
    this.streamManager = null;
    this.circuitBreaker = null;

    // ========== ÉTAT ==========
    this.isConnected = false;
    this._errorLogged = false;
    this._reconnectAttempts = 0;

    // ========== CONFIGURATION ==========
    this.config = {
      host: process.env.REDIS_HOST || "localhost",
      port: parseInt(process.env.REDIS_PORT) || 6379,
      password: process.env.REDIS_PASSWORD || undefined,
      db: parseInt(process.env.REDIS_DB) || 0,
      connectTimeout: parseInt(process.env.REDIS_CONNECTION_TIMEOUT) || 5000,
      maxRetries: parseInt(process.env.REDIS_MAX_RETRY_ATTEMPTS) || 5,
    };

    // ========== MÉTRIQUES ==========
    this.metrics = {
      connectionsCreated: 0,
      reconnections: 0,
      errors: 0,
      lastConnectedAt: null,
      lastErrorAt: null,
    };

    RedisManager.instance = this;
    console.log("✅ RedisManager initialisé (Singleton)");
  }

  // ========================================
  // CONFIGURATION CLIENT
  // ========================================

  /**
   * Créer les options de connexion Redis
   */
  _createClientOptions() {
    return {
      socket: {
        host: this.config.host,
        port: this.config.port,
        connectTimeout: this.config.connectTimeout,
        reconnectStrategy: (retries) => {
          this._reconnectAttempts = retries;

          if (retries > this.config.maxRetries) {
            console.warn(
              `⚠️ Redis: abandon après ${this.config.maxRetries} tentatives`,
            );
            return false;
          }

          const delay = Math.min(retries * 500, 3000);
          console.log(
            `🔄 Redis: reconnexion dans ${delay}ms (tentative ${retries})`,
          );
          return delay;
        },
      },
      password: this.config.password,
      database: this.config.db,
    };
  }

  /**
   * Créer un client Redis avec handlers d'événements
   */
  async _createClient(name) {
    const createClient = await getCreateClient();
    if (!createClient) {
      throw new Error("Redis createClient introuvable");
    }
    const client = createClient(this._createClientOptions());

    client.on("error", (err) => {
      if (!this._errorLogged) {
        console.error(`❌ Erreur Redis (${name}):`, err.message);
        this._errorLogged = true;
        this.metrics.errors++;
        this.metrics.lastErrorAt = new Date();
      }
      this.isConnected = false;
    });

    client.on("ready", () => {
      console.log(`✅ Redis client "${name}" prêt`);
      this.isConnected = true;
      this._errorLogged = false;
      this._reconnectAttempts = 0;
    });

    client.on("reconnecting", () => {
      console.log(`🔄 Redis client "${name}" en reconnexion...`);
      this.metrics.reconnections++;
    });

    client.on("end", () => {
      console.log(`🔌 Redis client "${name}" déconnecté`);
      this.isConnected = false;
    });

    this.metrics.connectionsCreated++;
    return client;
  }

  // ========================================
  // CONNEXION / DÉCONNEXION
  // ========================================

  /**
   * Connecter tous les clients Redis
   */
  async connect(options = {}) {
    if (this.isConnected) {
      console.log("ℹ️ RedisManager déjà connecté");
      return true;
    }

    const config = {
      host: options.host || process.env.REDIS_HOST || "localhost",
      port: options.port || parseInt(process.env.REDIS_PORT) || 6379,
      password: options.password || process.env.REDIS_PASSWORD,
      db: options.db || parseInt(process.env.REDIS_DB) || 0,
      connectTimeout: parseInt(process.env.REDIS_CONNECTION_TIMEOUT) || 5000,
      maxRetries: parseInt(process.env.REDIS_MAX_RETRY_ATTEMPTS) || 5,
    };

    try {
      console.log("🔌 Connexion Redis...");

      // Créer les clients
      this.clients.main = await this._createClient("main");
      this.clients.stream = await this._createClient("stream");
      this.clients.cache = await this._createClient("cache");
      this.clients.pub = await this._createClient("pub");
      this.clients.sub = await this._createClient("sub");
      
      // Connecter tous les clients en parallèle
      await Promise.all([
        this.clients.main.connect(config),
        this.clients.stream.connect(config),
        this.clients.cache.connect(config),
        this.clients.pub.connect(config),
        this.clients.sub.connect(config),
      ]);
      
      // Initialiser les composants de résilience
      this._initializeResilienceComponents();

      this.isConnected = true;
      this.metrics.lastConnectedAt = new Date();

      console.log("✅ RedisManager: Tous les clients connectés");
      return true;
    } catch (error) {
      console.error("❌ RedisManager: Connexion échouée:", error.message);
      this.isConnected = false;
      throw error;
    }
  }

  /**
   * Initialiser les composants de résilience
   */
  _initializeResilienceComponents() {
    // StreamManager avec le client stream
    this.streamManager = new StreamManager(this.clients.stream);

    // CircuitBreaker par défaut
    this.circuitBreaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 30000,
    });

    console.log("✅ Composants de résilience initialisés");
  }

  /**
   * Déconnecter tous les clients
   */
  async disconnect() {
    try {
      console.log("🔌 Déconnexion Redis...");

      // Déconnecter tous les clients
      const disconnectPromises = Object.entries(this.clients)
        .filter(([_, client]) => client !== null)
        .map(async ([name, client]) => {
          try {
            await client.quit();
            console.log(`   ✅ Client "${name}" déconnecté`);
          } catch (err) {
            console.warn(`   ⚠️ Erreur déconnexion "${name}":`, err.message);
          }
        });

      await Promise.all(disconnectPromises);

      // Réinitialiser l'état
      this.clients = {
        main: null,
        stream: null,
        cache: null,
        pub: null,
        sub: null,
      };

      this.streamManager = null;
      this.circuitBreaker = null;
      this.isConnected = false;

      console.log("✅ RedisManager: Déconnecté");
    } catch (error) {
      console.error("❌ RedisManager: Erreur déconnexion:", error.message);
    }
  }

  // ========================================
  // GETTERS CLIENTS
  // ========================================

  /**
   * Obtenir un client par type
   */
  getClient(type = "main") {
    if (!this.clients[type]) {
      throw new Error(`Client Redis "${type}" non disponible`);
    }
    return this.clients[type];
  }

  getMainClient() {
    return this.clients.main;
  }
  

  getStreamClient() {
    return this.clients.stream;
  }

  getCacheClient() {
    return this.clients.cache;
  }

  getPubClient() {
    return this.clients.pub;
  }

  getSubClient() {
    return this.clients.sub;
  }

  // ========================================
  // GETTERS COMPOSANTS RÉSILIENCE
  // ========================================

  /**
   * Obtenir le StreamManager
   */
  getStreamManager() {
    if (!this.streamManager) {
      throw new Error(
        "StreamManager non initialisé. Appelez connect() d'abord.",
      );
    }
    return this.streamManager;
  }

  /**
   * Obtenir le CircuitBreaker
   */
  getCircuitBreaker() {
    if (!this.circuitBreaker) {
      throw new Error(
        "CircuitBreaker non initialisé. Appelez connect() d'abord.",
      );
    }
    return this.circuitBreaker;
  }

  /**
   * Créer un nouveau CircuitBreaker avec options personnalisées
   */
  createCircuitBreaker(options = {}) {
    return new CircuitBreaker(options);
  }

  // ========================================
  // MÉTHODES PROXY VERS STREAMMANAGER
  // ========================================

  /**
   * Ajouter à un stream (proxy vers StreamManager)
   */
  async addToStream(streamName, fields) {
    if (!this.streamManager) {
      console.warn("⚠️ StreamManager non disponible");
      return null;
    }
    return this.streamManager.addToStream(streamName, fields);
  }
  
  /**
   * Lire depuis un stream (proxy vers StreamManager)
   */
  async readFromStream(streamName, options = {}) {
    if (!this.streamManager) {
      return [];
    }
    return this.streamManager.readFromStream(streamName, options);
  }

  /**
   * Supprimer du stream (proxy vers StreamManager)
   */
  async deleteFromStream(streamName, messageId) {
    if (!this.streamManager) {
      return false;
    }
    return this.streamManager.deleteFromStream(streamName, messageId);
  }

  /**
   * Obtenir les stats des streams (proxy vers StreamManager)
   */
  async getStreamStats() {
    if (!this.streamManager) {
      return null;
    }
    return this.streamManager.getStreamStats();
  }

  /**
   * Initialiser les consumer groups (proxy vers StreamManager)
   */
  async initConsumerGroups() {
    if (!this.streamManager) {
      console.warn("⚠️ StreamManager non disponible");
      return;
    }
    return this.streamManager.initConsumerGroups();
  }



  // ========================================
  // HEALTH & STATS
  // ========================================
  
  /**
   * Vérifier l'état de santé
   */
  async getHealthStatus() {
    if (!this.isConnected || !this.clients.main) {
      return {
        status: "disconnected",
        latency: null,
        clients: this._getClientsStatus(),
      };
    }

    try {
      const start = Date.now();
      await this.clients.main.ping();
      const latency = Date.now() - start;

      return {
        status: "connected",
        latency: `${latency}ms`,
        clients: this._getClientsStatus(),
        streamManager: this.streamManager ? "ready" : "not_initialized",
        circuitBreaker: this.circuitBreaker?.getState() || "not_initialized",
      };
    } catch (error) {
      return {
        status: "error",
        error: error.message,
        clients: this._getClientsStatus(),
      };
    }
  }

  /**
   * Obtenir le statut des clients
   */
  _getClientsStatus() {
    const status = {};
    for (const [name, client] of Object.entries(this.clients)) {
      status[name] = client ? "connected" : "disconnected";
    }
    return status;
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return {
      ...this.metrics,
      isConnected: this.isConnected,
      reconnectAttempts: this._reconnectAttempts,
      streamManager: this.streamManager ? "active" : "inactive",
      circuitBreaker: this.circuitBreaker?.getMetrics() || null,
    };
  }

  /**
   * Obtenir les statistiques Redis
   */
  async getStats() {
    if (!this.clients.main) {
      return null;
    }

    try {
      const info = await this.clients.main.info("memory");
      const dbSize = await this.clients.main.dbSize();

      // Parser la mémoire utilisée
      const usedMemoryMatch = info.match(/used_memory:(\d+)/);
      const usedMemoryMB = usedMemoryMatch
        ? parseInt(usedMemoryMatch[1]) / 1024 / 1024
        : 0;

      return {
        dbSize,
        usedMemoryMB: usedMemoryMB.toFixed(2),
        connectedClients: Object.keys(this.clients).filter(
          (k) => this.clients[k],
        ).length,
        streamStats: await this.getStreamStats(),
      };
    } catch (error) {
      return { error: error.message };
    }
  }

  // ========================================
  // UTILITAIRES
  // ========================================

  /**
   * Exécuter une opération avec le circuit breaker
   */
  async executeWithCircuitBreaker(operation, fallback = null) {
    if (!this.circuitBreaker) {
      return operation();
    }

    const cb = fallback
      ? new CircuitBreaker({
          failureThreshold: 5,
          resetTimeout: 30000,
          fallback,
        })
      : this.circuitBreaker;

    return cb.execute(operation);
  }

  /**
   * Vérifier si Redis est disponible
   */
  isAvailable() {
    return this.isConnected && this.clients.main !== null;
  }

  /**
   * Réinitialiser le singleton (pour les tests)
   */
  static resetInstance() {
    if (RedisManager.instance) {
      RedisManager.instance.disconnect().catch(() => {});
      RedisManager.instance = null;
    }
  }
}

// Export Singleton
module.exports = new RedisManager();
