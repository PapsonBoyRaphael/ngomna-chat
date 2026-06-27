/**
 * RetryWorker - Worker de traitement des retries
 * ✅ Lit le stream RETRY
 * ✅ Réexécute les opérations échouées
 * ✅ Déplace vers DLQ après max retries
 */

class RetryWorker {
  constructor(streamManager, options = {}) {
    this.streamManager = streamManager;
    this.redis = streamManager.redis;

    this.options = {
      maxRetries: options.maxRetries || 5,
      batchSize: options.batchSize || 10,
      processingDelayMs: options.processingDelayMs || 20,
      ...options,
    };

    // Callbacks injectés
    this.saveCallback = options.saveCallback || null;
    this.publishCallback = options.publishCallback || null;
    this.dlqCallback = options.dlqCallback || null;
    this.notifyCallback = options.notifyCallback || null;

    this.interval = null;
    this.isRunning = false;

    this.metrics = {
      processed: 0,
      successful: 0,
      failed: 0,
      movedToDLQ: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ RetryWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ RetryWorker:", err.message)
        ),
      this.options.processingDelayMs
    );

    console.log("✅ RetryWorker démarré");
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
    console.log("✅ RetryWorker arrêté");
  }

  /**
   * Traiter les retries en attente
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const retries = await this.streamManager.readFromStream(
        this.streamManager.STREAMS.RETRY,
        { count: this.options.batchSize }
      );

      if (!retries || retries.length === 0) return;

      for (const entry of retries) {
        const { id, message } = entry;

        try {
          const attempt = parseInt(message.attempt) || 1;
          const nextRetryAt = parseInt(message.nextRetryAt);
          const now = Date.now();

          // Pas encore le moment du retry
          if (nextRetryAt > now) continue;

          // Parser les données du message
          let messageData;
          try {
            if (
              !message.data ||
              message.data.trim() === "" ||
              message.data === "undefined"
            ) {
              console.error("❌ RetryWorker: data vide ou undefined");
              await this.streamManager.deleteFromStream(
                this.streamManager.STREAMS.RETRY,
                id
              );
              continue;
            }
            messageData = JSON.parse(message.data);
          } catch (e) {
            console.error("❌ Erreur parsing:", e.message);
            await this.streamManager.deleteFromStream(
              this.streamManager.STREAMS.RETRY,
              id
            );
            continue;
          }

          console.log(`🔄 Retry #${attempt} pour ${message.messageId}...`);
          this.metrics.processed++;

          try {
            // Callback de sauvegarde
            if (this.saveCallback) {
              const savedMessage = await this.saveCallback(messageData);
              console.log(`✅ Retry réussi: ${message.messageId}`);
              this.metrics.successful++;

              // Callback de publication
              if (this.publishCallback) {
                await this.publishCallback(savedMessage, {
                  event: "NEW_MESSAGE",
                  source: "retry",
                });
              }

              // Supprimer du stream retry
              await this.streamManager.deleteFromStream(
                this.streamManager.STREAMS.RETRY,
                id
              );

              // Notification
              if (this.notifyCallback) {
                this.notifyCallback("messageRetried", {
                  messageId: savedMessage._id,
                  conversationId: messageData.conversationId,
                  status: "DELIVERED",
                  attempt,
                });
              }
            }
          } catch (saveError) {
            this.metrics.failed++;

            if (attempt >= this.options.maxRetries) {
              console.error(`❌ Max retries atteint pour ${message.messageId}`);
              this.metrics.movedToDLQ++;

              // Déplacer vers DLQ
              if (this.dlqCallback) {
                await this.dlqCallback(messageData, saveError, attempt, {
                  operation: "processRetries",
                  poison: true,
                });
              }

              await this.streamManager.deleteFromStream(
                this.streamManager.STREAMS.RETRY,
                id
              );
            } else {
              // Planifier un nouveau retry
              const nextAttempt = attempt + 1;
              console.warn(`⚠️ Retry échoué. Tentative ${nextAttempt}...`);

              await this.addRetry(messageData, nextAttempt, saveError);
              await this.streamManager.deleteFromStream(
                this.streamManager.STREAMS.RETRY,
                id
              );
            }
          }
        } catch (error) {
          console.error("❌ Erreur traitement retry:", error.message);
        }
      }
    } catch (error) {
      console.error("❌ Erreur processRetries:", error.message);
    }
  }

  /**
   * Ajouter un message au stream retry
   */
  async addRetry(messageData, attempt, error) {
    if (!this.redis || !messageData) {
      console.warn("⚠️ addRetry: messageData est undefined ou Redis absent");
      return;
    }

    try {
      this.metrics.retryCount++;

      const dataStr = JSON.stringify(messageData);
      if (!dataStr || dataStr === "undefined" || dataStr.trim() === "") {
        console.error("❌ addRetry: impossible de stringifier messageData", {
          messageData,
        });
        return;
      }

      const retryEntry = await this.streamManager.addToStream(
        this.streamManager.STREAMS.RETRY,
        {
          messageId: messageData._id?.toString() || "unknown",
          conversationId: messageData.conversationId?.toString() || "unknown",
          attempt: attempt.toString(),
          error: (error.message || "unknown").substring(0, 300),
          timestamp: Date.now().toString(),
          nextRetryAt: (Date.now() + 100 * Math.pow(2, attempt - 1)).toString(),
          data: dataStr,
        }
      );

      console.log(`🔄 Retry #${attempt}: ${retryEntry}`);
      return retryEntry;
    } catch (err) {
      console.warn("⚠️ Erreur addRetry:", err.message);
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = RetryWorker;
