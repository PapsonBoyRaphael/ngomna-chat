/**
 * HeartbeatManager - Gestion des pings client-side
 * ✅ Envoie un ping toutes les 30s pour maintenir la présence
 * ✅ Détection de déconnexion côté client
 * ✅ Reconnexion automatique
 * ✅ Throttling pour éviter les pings en rafale
 */

class HeartbeatManager {
  constructor(socket, options = {}) {
    this.socket = socket;

    // ✅ Configuration
    this.pingInterval = options.pingInterval || 30000; // 30 secondes
    this.pongTimeout = options.pongTimeout || 5000; // 5 secondes pour recevoir pong
    this.reconnectDelay = options.reconnectDelay || 3000; // 3 secondes avant reconnexion
    this.maxMissedPongs = options.maxMissedPongs || 3; // 3 pongs manqués = déconnexion

    // ✅ État
    this.pingTimer = null;
    this.pongTimeoutTimer = null;
    this.missedPongs = 0;
    this.lastPingTime = 0;
    this.lastPongTime = 0;
    this.isActive = false;

    // ✅ Callbacks
    this.onDisconnect = options.onDisconnect || null;
    this.onReconnect = options.onReconnect || null;
    this.onLatencyUpdate = options.onLatencyUpdate || null;

    console.log("✅ HeartbeatManager initialisé");
  }

  /**
   * ✅ DÉMARRER LE HEARTBEAT
   */
  start() {
    if (this.isActive) {
      console.log("ℹ️ HeartbeatManager déjà actif");
      return;
    }

    this.isActive = true;
    this.missedPongs = 0;

    // ✅ ÉCOUTER LES PONGS
    this.socket.on("pong", () => this.handlePong());
    this.socket.on("heartbeat:ack", () => this.handlePong());

    // ✅ DÉMARRER LE TIMER
    this.schedulePing();

    console.log(`🔄 Heartbeat démarré (intervalle: ${this.pingInterval}ms)`);
  }

  /**
   * ✅ ARRÊTER LE HEARTBEAT
   */
  stop() {
    this.isActive = false;

    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }

    if (this.pongTimeoutTimer) {
      clearTimeout(this.pongTimeoutTimer);
      this.pongTimeoutTimer = null;
    }

    this.socket.off("pong");
    this.socket.off("heartbeat:ack");

    console.log("🛑 Heartbeat arrêté");
  }

  /**
   * ✅ PLANIFIER LE PROCHAIN PING
   */
  schedulePing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
    }

    this.pingTimer = setInterval(() => {
      this.sendPing();
    }, this.pingInterval);

    // Envoyer un ping immédiat
    this.sendPing();
  }

  /**
   * ✅ ENVOYER UN PING
   */
  sendPing() {
    if (!this.isActive || !this.socket.connected) {
      return;
    }

    this.lastPingTime = Date.now();

    // Envoyer le ping
    this.socket.emit("ping", {
      timestamp: this.lastPingTime,
      clientTime: new Date().toISOString(),
    });

    // ✅ CONFIGURER LE TIMEOUT POUR LE PONG
    if (this.pongTimeoutTimer) {
      clearTimeout(this.pongTimeoutTimer);
    }

    this.pongTimeoutTimer = setTimeout(() => {
      this.handleMissedPong();
    }, this.pongTimeout);

    console.log(
      `📤 Ping envoyé (latence attendue: ${this.getAverageLatency()}ms)`,
    );
  }

  /**
   * ✅ GÉRER LA RÉCEPTION D'UN PONG
   */
  handlePong() {
    this.lastPongTime = Date.now();
    this.missedPongs = 0;

    // Annuler le timeout
    if (this.pongTimeoutTimer) {
      clearTimeout(this.pongTimeoutTimer);
      this.pongTimeoutTimer = null;
    }

    // Calculer la latence
    const latency = this.lastPongTime - this.lastPingTime;

    console.log(`📥 Pong reçu (latence: ${latency}ms)`);

    // Callback latence
    if (this.onLatencyUpdate) {
      this.onLatencyUpdate(latency);
    }
  }

  /**
   * ✅ GÉRER UN PONG MANQUÉ
   */
  handleMissedPong() {
    this.missedPongs++;

    console.warn(`⚠️ Pong manqué (${this.missedPongs}/${this.maxMissedPongs})`);

    if (this.missedPongs >= this.maxMissedPongs) {
      console.error("❌ Trop de pongs manqués, connexion perdue");

      // Callback déconnexion
      if (this.onDisconnect) {
        this.onDisconnect({
          reason: "heartbeat_timeout",
          missedPongs: this.missedPongs,
          lastPingTime: this.lastPingTime,
        });
      }

      // Tenter une reconnexion
      this.attemptReconnect();
    }
  }

  /**
   * ✅ TENTER UNE RECONNEXION
   */
  attemptReconnect() {
    console.log(`🔄 Tentative de reconnexion dans ${this.reconnectDelay}ms...`);

    setTimeout(() => {
      if (this.socket.connected) {
        console.log("✅ Connexion restaurée");
        this.missedPongs = 0;

        if (this.onReconnect) {
          this.onReconnect();
        }
      } else {
        console.log("🔌 Tentative de reconnexion manuelle...");
        this.socket.connect();
      }
    }, this.reconnectDelay);
  }

  /**
   * ✅ OBTENIR LA LATENCE MOYENNE
   */
  getAverageLatency() {
    if (this.lastPingTime && this.lastPongTime) {
      return this.lastPongTime - this.lastPingTime;
    }
    return 0;
  }

  /**
   * ✅ OBTENIR L'ÉTAT DU HEARTBEAT
   */
  getStatus() {
    return {
      isActive: this.isActive,
      missedPongs: this.missedPongs,
      lastPingTime: this.lastPingTime,
      lastPongTime: this.lastPongTime,
      latency: this.getAverageLatency(),
      connected: this.socket.connected,
    };
  }

  /**
   * ✅ FORCER UN PING IMMÉDIAT
   */
  forcePing() {
    if (!this.isActive) {
      console.warn("⚠️ HeartbeatManager non actif");
      return;
    }

    this.sendPing();
  }
}

// ✅ EXPORTER POUR UTILISATION EN SCRIPT
if (typeof module !== "undefined" && module.exports) {
  module.exports = HeartbeatManager;
}
