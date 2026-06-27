/**
 * StreamMonitorWorker - Surveillance des streams Redis
 * ✅ Monitore la taille des streams
 * ✅ Alerte en cas de dépassement des limites
 * ✅ Log périodique des statistiques
 */

class StreamMonitorWorker {
  constructor(streamManager, options = {}) {
    this.streamManager = streamManager;
    this.redis = streamManager.redis;

    this.options = {
      checkIntervalMs: options.checkIntervalMs || 60000,
      reportIntervalMs: options.reportIntervalMs || 300000, // 5 min
      overflowThreshold: options.overflowThreshold || 1.5,
      ...options,
    };

    this.interval = null;
    this.isRunning = false;
    this.lastReportTime = Date.now();

    this.metrics = {
      checks: 0,
      overflowAlerts: 0,
      totalEntries: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ StreamMonitorWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ StreamMonitorWorker:", err.message)
        ),
      this.options.checkIntervalMs
    );

    console.log("✅ StreamMonitorWorker démarré");
  }

  /**
   * Arrêter le worker
   */
  stop() {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.isRunning = false;
    console.log("✅ StreamMonitorWorker arrêté");
  }

  /**
   * Surveiller les streams
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const streamSizes = {};
      let totalSize = 0;

      for (const [streamName, maxLen] of Object.entries(
        this.streamManager.STREAM_MAXLEN
      )) {
        try {
          const length = await this.streamManager.getStreamLength(streamName);
          streamSizes[streamName] = {
            current: length,
            max: maxLen,
            usage: ((length / maxLen) * 100).toFixed(2) + "%",
          };
          totalSize += length;

          // Alerte si dépassement
          if (length > maxLen * this.options.overflowThreshold) {
            this.metrics.overflowAlerts++;
            console.warn(
              `⚠️ ${streamName} dépasse limites: ${length}/${maxLen}`
            );
          }
        } catch (err) {
          // Stream n'existe pas encore
        }
      }

      this.metrics.checks++;
      this.metrics.totalEntries = totalSize;

      // Report périodique
      const now = Date.now();
      if (now - this.lastReportTime > this.options.reportIntervalMs) {
        console.log("📊 === ÉTAT DES STREAMS ===");
        console.table(streamSizes);
        console.log(`📊 Total: ${totalSize} entrées`);
        this.lastReportTime = now;
      }
    } catch (error) {
      console.error("❌ Erreur stream monitoring:", error.message);
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = StreamMonitorWorker;
