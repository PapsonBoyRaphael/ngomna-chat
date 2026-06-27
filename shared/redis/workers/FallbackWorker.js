/**
 * FallbackWorker - Worker de traitement des fallbacks Redis
 * ✅ Lit le stream FALLBACK
 * ✅ Rejoue les messages stockés en fallback
 * ✅ Synchronise vers MongoDB
 */

class FallbackWorker {
  constructor(streamManager, options = {}) {
    this.streamManager = streamManager;
    this.redis = streamManager.redis;

    this.options = {
      batchSize: options.batchSize || 10,
      processingDelayMs: options.processingDelayMs || 2000,
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
      replayed: 0,
      failed: 0,
    };
  }

  /**
   * Démarrer le worker
   */
  start() {
    if (this.isRunning) {
      console.warn("⚠️ FallbackWorker déjà en cours");
      return;
    }

    this.isRunning = true;
    this.interval = setInterval(
      () =>
        this.process().catch((err) =>
          console.error("❌ FallbackWorker:", err.message)
        ),
      this.options.processingDelayMs
    );

    console.log("✅ FallbackWorker démarré");
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
    console.log("✅ FallbackWorker arrêté");
  }

  /**
   * Traiter les fallbacks en attente
   */
  async process() {
    if (!this.redis || !this.isRunning) return;

    try {
      const fallbacks = await this.streamManager.readFromStream(
        this.streamManager.STREAMS.FALLBACK,
        { count: this.options.batchSize }
      );

      if (!fallbacks || fallbacks.length === 0) return;

      for (const entry of fallbacks) {
        const { id, message } = entry;

        try {
          const fallbackId = message.fallbackId;
          const conversationId = message.conversationId;

          // Récupérer les données du fallback depuis le hash
          const hashKey = `fallback:${fallbackId}`;
          const fallbackData = await this.redis.hGetAll(hashKey);

          if (!fallbackData || Object.keys(fallbackData).length === 0) {
            console.warn(`⚠️ Fallback data non trouvée: ${fallbackId}`);
            await this.streamManager.deleteFromStream(
              this.streamManager.STREAMS.FALLBACK,
              id
            );
            continue;
          }

          console.log(`🔄 Replay fallback: ${fallbackId}...`);
          this.metrics.processed++;

          try {
            if (this.saveCallback) {
              const mongoMessage = await this.saveCallback({
                _id:
                  fallbackData.originalId === "pending"
                    ? undefined
                    : fallbackData.originalId,
                conversationId: fallbackData.conversationId,
                senderId: fallbackData.senderId,
                content: fallbackData.content,
                type: fallbackData.type || "TEXT",
                status: "DELIVERED",
                createdAt: new Date(fallbackData.createdAt),
                metadata: {
                  fromFallback: true,
                  fallbackId,
                },
              });

              console.log(
                `✅ Fallback rejoué: ${fallbackId} → ${mongoMessage._id}`
              );
              this.metrics.replayed++;

              // Publier le message
              if (this.publishCallback) {
                await this.publishCallback(mongoMessage, {
                  event: "NEW_MESSAGE",
                  source: "fallback_replay",
                });
              }

              // Nettoyer
              await this.redis.del(hashKey);
              await this.redis.zRem("fallback:active", fallbackId);
              await this.streamManager.deleteFromStream(
                this.streamManager.STREAMS.FALLBACK,
                id
              );
              await this.redis.hIncrBy("fallback:stats", "active", -1);
              await this.redis.hIncrBy("fallback:stats", "replayed", 1);

              // Notification
              if (this.notifyCallback) {
                this.notifyCallback("messageFallbackReplayed", {
                  fallbackId,
                  messageId: mongoMessage._id,
                  conversationId,
                  status: "DELIVERED",
                });
              }
            }
          } catch (saveError) {
            console.error(`❌ Erreur replay fallback:`, saveError.message);
            this.metrics.failed++;

            // Déplacer vers DLQ
            if (this.dlqCallback) {
              await this.dlqCallback(
                {
                  _id: fallbackData.originalId,
                  conversationId: fallbackData.conversationId,
                  senderId: fallbackData.senderId,
                  content: fallbackData.content,
                },
                saveError,
                1,
                { operation: "processFallback", fallbackId, poison: true }
              );
            }

            await this.redis.del(hashKey);
            await this.streamManager.deleteFromStream(
              this.streamManager.STREAMS.FALLBACK,
              id
            );
          }
        } catch (error) {
          console.error("❌ Erreur traitement fallback:", error.message);
        }
      }
    } catch (error) {
      console.error("❌ Erreur processFallback:", error.message);
    }
  }

  /**
   * Créer un fallback pour un message
   */
  async createFallback(messageData) {
    if (!this.redis) {
      throw new Error("Redis non disponible");
    }

    const fallbackId = `fb_${Date.now()}_${Math.random()
      .toString(36)
      .substr(2, 9)}`;

    try {
      const hashKey = `fallback:${fallbackId}`;

      await this.redis.hSet(hashKey, {
        id: fallbackId,
        originalId: messageData._id?.toString() || "pending",
        conversationId: messageData.conversationId?.toString(),
        senderId: messageData.senderId?.toString(),
        content: messageData.content || "",
        type: messageData.type || "TEXT",
        status: "pending_fallback",
        createdAt: new Date().toISOString(),
        ts: Date.now().toString(),
      });

      await this.redis.expire(hashKey, 86400); // 24h TTL

      const streamId = await this.streamManager.addToStream(
        this.streamManager.STREAMS.FALLBACK,
        {
          fallbackId,
          conversationId: messageData.conversationId?.toString(),
          action: "needs_replay",
          priority: "high",
          ts: Date.now().toString(),
        }
      );

      await this.redis.zAdd("fallback:active", {
        score: Date.now(),
        value: fallbackId,
      });

      await this.redis.hIncrBy("fallback:stats", "total", 1);
      await this.redis.hIncrBy("fallback:stats", "active", 1);

      console.log(`✅ Fallback créé: ${fallbackId}`);

      return {
        _id: fallbackId,
        ...messageData,
        status: "pending_fallback",
        fromFallback: true,
        fallbackStreamId: streamId,
      };
    } catch (error) {
      console.error("❌ Erreur création fallback:", error.message);
      throw new Error(`Fallback échoué: ${error.message}`);
    }
  }

  /**
   * Obtenir les métriques
   */
  getMetrics() {
    return { ...this.metrics, isRunning: this.isRunning };
  }
}

module.exports = FallbackWorker;
