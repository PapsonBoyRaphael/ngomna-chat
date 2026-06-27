/**
 * DLQMonitorWorker - Dead Letter Queue Monitor
 * ✅ Surveille la DLQ
 * ✅ Alerte en cas de messages bloqués
 * ✅ Émet des notifications
 */

class DLQMonitorWorker {
  constructor(streamManager, options = {}) {
    this.streamManager = streamManager;
    this.redis = streamManager.redis;

    this.options = {
      checkIntervalMs: options.checkIntervalMs || 5000,
      alertThreshold: options.alertThreshold || 10,
      criticalThreshold: options.criticalThreshold || 100,
      ...options,
    };

    // Callbacks injectés
    this.notifyCallback = options.notifyCallback || null;

    this.interval = null;
    this.isRunning = false;

    this.metrics = {
      checks: 0,
      alerts: 0,
      criticalAlerts: 0,
      currentDLQSize: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ DLQMonitorWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ DLQMonitorWorker:", err.message)
        ),
      this.options.checkIntervalMs
    );

    console.log("✅ DLQMonitorWorker démarré");
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
    console.log("✅ DLQMonitorWorker arrêté");
  }

  /**
   * Surveiller la DLQ
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const dlqLength = await this.streamManager.getStreamLength(
        this.streamManager.STREAMS.DLQ
      );

      this.metrics.checks++;
      this.metrics.currentDLQSize = dlqLength;

      if (dlqLength > 0) {
        const severity =
          dlqLength > this.options.criticalThreshold
            ? "critical"
            : dlqLength > this.options.alertThreshold
            ? "warning"
            : "info";

        if (severity === "critical") {
          this.metrics.criticalAlerts++;
        } else if (severity === "warning") {
          this.metrics.alerts++;
        }

        console.error(`🚨 DLQ NON VIDE: ${dlqLength} messages (${severity})`);

        // Afficher les derniers messages DLQ
        const dlqMessages = await this.streamManager.getStreamReverseRange(
          this.streamManager.STREAMS.DLQ,
          5
        );

        dlqMessages.forEach((entry) => {
          const { fields } = this.streamManager.parseStreamMessage(entry);
          console.error(`  ❌ ${fields.messageId}: ${fields.error}`);
        });

        // Notification
        if (this.notifyCallback) {
          this.notifyCallback("dlqAlert", {
            count: dlqLength,
            severity,
            timestamp: new Date().toISOString(),
          });
        }
      }
    } catch (error) {
      console.error("❌ Erreur monitorDLQ:", error.message);
    }
  }

  /**
   * Ajouter un message à la DLQ
   */
  async addToDLQ(messageData, error, attempts, context = {}) {
    if (!this.redis) return null;

    try {
      const dlqId = await this.streamManager.addToStream(
        this.streamManager.STREAMS.DLQ,
        {
          messageId: messageData._id?.toString() || "unknown",
          conversationId: messageData.conversationId?.toString() || "unknown",
          error: (error.message || "Unknown error").substring(0, 500),
          attempts: attempts.toString(),
          timestamp: Date.now().toString(),
          operation: context.operation || "save",
          poison: (context.poison || false).toString(),
          walId: context.walId || "",
        }
      );

      console.error(`❌ Message en DLQ: ${dlqId}`);
      return dlqId;
    } catch (err) {
      console.error("❌ Erreur addToDLQ:", err.message);
      return null;
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = DLQMonitorWorker;
