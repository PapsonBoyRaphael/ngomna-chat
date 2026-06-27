/**
 * WALRecoveryWorker - Write-Ahead Log Recovery Worker
 * ✅ Détecte les écritures incomplètes
 * ✅ Vérifie si les messages ont été sauvegardés
 * ✅ Déplace vers DLQ les messages perdus
 */

class WALRecoveryWorker {
  constructor(streamManager, options = {}) {
    this.streamManager = streamManager;
    this.redis = streamManager.redis;

    this.options = {
      walTimeout: options.walTimeout || 60000, // 1 minute
      processingDelayMs: options.processingDelayMs || 3000,
      ...options,
    };

    // Callbacks injectés
    this.findMessageCallback = options.findMessageCallback || null;
    this.dlqCallback = options.dlqCallback || null;

    this.interval = null;
    this.isRunning = false;

    this.metrics = {
      checked: 0,
      recovered: 0,
      lost: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ WALRecoveryWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ WALRecoveryWorker:", err.message)
        ),
      this.options.processingDelayMs
    );

    console.log("✅ WALRecoveryWorker démarré");
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
    console.log("✅ WALRecoveryWorker arrêté");
  }

  /**
   * Traiter les WAL entries en attente
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const walEntries = await this.streamManager.getStreamRange(
        this.streamManager.STREAMS.WAL,
        "-",
        "+"
      );

      if (!walEntries || walEntries.length === 0) return;

      const incompleteWALs = new Map();

      // Identifier les WAL incomplets
      for (const entry of walEntries) {
        const { id, fields } = this.streamManager.parseStreamMessage(entry);

        if (fields.type === "pre_write") {
          const walId = fields.walId || id;
          incompleteWALs.set(walId, {
            id,
            walId,
            messageId: fields.messageId,
            conversationId: fields.conversationId,
            senderId: fields.senderId,
            timestamp: parseInt(fields.timestamp),
          });
        } else if (fields.type === "post_write") {
          incompleteWALs.delete(fields.walId);
        }
      }

      const now = Date.now();

      // Traiter les WAL expirés
      for (const [walId, walData] of incompleteWALs.entries()) {
        try {
          const age = now - walData.timestamp;

          if (age > this.options.walTimeout) {
            console.warn(
              `⚠️ WAL incomplet: ${walId} (${(age / 1000).toFixed(2)}s)`
            );
            this.metrics.checked++;

            // Vérifier si le message existe
            const existingMessage = await this.findMessageCallback(
              walData.messageId
            ).catch(() => null);
            const messageExists = !!existingMessage;

            if (messageExists) {
              console.log(`✅ Message retrouvé: ${walData.messageId}`);
              this.metrics.recovered++;
            } else {
              console.warn(`❌ Message PERDU: ${walData.messageId}`);
              this.metrics.lost++;

              // Déplacer vers DLQ
              if (this.dlqCallback) {
                await this.dlqCallback(
                  {
                    _id: walData.messageId,
                    conversationId: walData.conversationId,
                    senderId: walData.senderId,
                  },
                  new Error("Message lost - incomplete WAL"),
                  0,
                  { operation: "processWALRecovery", walId, poison: true }
                );
              }
            }

            // Nettoyer le WAL entry
            await this.streamManager.deleteFromStream(
              this.streamManager.STREAMS.WAL,
              walData.id
            );
          }
        } catch (error) {
          console.error("❌ Erreur WAL recovery:", error.message);
        }
      }
    } catch (error) {
      console.error("❌ Erreur processWALRecovery:", error.message);
    }
  }

  /**
   * Logger un pre-write
   */
  async logPreWrite(messageData) {
    if (!this.redis) return null;

    try {
      const walEntry = await this.streamManager.addToStream(
        this.streamManager.STREAMS.WAL,
        {
          type: "pre_write",
          messageId: messageData._id?.toString() || "unknown",
          conversationId: messageData.conversationId?.toString() || "unknown",
          senderId: messageData.senderId?.toString() || "unknown",
          timestamp: Date.now().toString(),
          status: "pending",
        }
      );

      console.log(`📝 WAL entry: ${walEntry}`);
      return walEntry;
    } catch (err) {
      console.warn("⚠️ Erreur WAL pre-write:", err.message);
      return null;
    }
  }

  /**
   * Logger un post-write
   */
  async logPostWrite(messageId, walId) {
    if (!this.redis || !walId) return;

    try {
      await this.streamManager.addToStream(this.streamManager.STREAMS.WAL, {
        type: "post_write",
        messageId: messageId?.toString() || "unknown",
        walId: walId,
        timestamp: Date.now().toString(),
        status: "completed",
      });

      await this.streamManager.deleteFromStream(
        this.streamManager.STREAMS.WAL,
        walId
      );
      console.log(`✅ WAL cleanup: ${walId}`);
    } catch (err) {
      console.warn("⚠️ Erreur WAL post-write:", err.message);
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = WALRecoveryWorker;
