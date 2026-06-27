/**
 * MemoryMonitorWorker - Surveillance mémoire Redis
 * ✅ Monitore l'utilisation mémoire
 * ✅ Alerte en cas de dépassement
 */

class MemoryMonitorWorker {
  constructor(redisClient, options = {}) {
    this.redis = redisClient;

    this.options = {
      checkIntervalMs: options.checkIntervalMs || 60000,
      memoryLimitMB: options.memoryLimitMB || 512,
      warningThreshold: options.warningThreshold || 0.8,
      criticalThreshold: options.criticalThreshold || 0.9,
      ...options,
    };

    // Callbacks
    this.alertCallback = options.alertCallback || null;

    this.interval = null;
    this.isRunning = false;

    this.metrics = {
      checks: 0,
      warnings: 0,
      criticals: 0,
      peakMemoryMB: 0,
      currentMemoryMB: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ MemoryMonitorWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ MemoryMonitorWorker:", err.message)
        ),
      this.options.checkIntervalMs
    );

    console.log("✅ MemoryMonitorWorker démarré");
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
    console.log("✅ MemoryMonitorWorker arrêté");
  }

  /**
   * Vérifier l'utilisation mémoire
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const info = await this.redis.info("memory");
      const usedMemoryMatch = info.match(/used_memory:(\d+)/);

      if (!usedMemoryMatch) return;

      const usedMemoryMB = parseInt(usedMemoryMatch[1]) / 1024 / 1024;

      this.metrics.checks++;
      this.metrics.currentMemoryMB = usedMemoryMB;
      this.metrics.peakMemoryMB = Math.max(
        this.metrics.peakMemoryMB,
        usedMemoryMB
      );

      const warningLevel =
        this.options.memoryLimitMB * this.options.warningThreshold;
      const criticalLevel =
        this.options.memoryLimitMB * this.options.criticalThreshold;

      if (usedMemoryMB > criticalLevel) {
        this.metrics.criticals++;
        console.error(
          `🚨 Mémoire CRITIQUE: ${usedMemoryMB.toFixed(2)}MB / ${
            this.options.memoryLimitMB
          }MB`
        );

        if (this.alertCallback) {
          this.alertCallback("memory_critical", {
            usedMB: usedMemoryMB,
            limitMB: this.options.memoryLimitMB,
            percentage: (
              (usedMemoryMB / this.options.memoryLimitMB) *
              100
            ).toFixed(2),
          });
        }
      } else if (usedMemoryMB > warningLevel) {
        this.metrics.warnings++;
        console.warn(
          `⚠️ Mémoire Redis: ${usedMemoryMB.toFixed(2)}MB / ${
            this.options.memoryLimitMB
          }MB`
        );
      }
    } catch (err) {
      console.warn("⚠️ Erreur monitoring mémoire:", err.message);
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = MemoryMonitorWorker;
