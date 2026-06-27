/**
 * MessageDeliveryService - CONSOMMATEUR MULTI-STREAMS avec xReadGroup
 * ✅ Consomme PLUSIEURS streams par type (privé, groupe, typing, etc.)
 * ✅ Priorisation automatique (typing > privé > groupe)
 * ✅ Acknowledge après livraison
 * ✅ Messages en attente pour utilisateurs déconnectés
 * ✅ Scalable jusqu'à des millions d'utilisateurs
 * ✅ CONSUMER PARTITIONING: Séparation des consumers par priorité
 * ✅ SMART CONSUMPTION: Consommation intelligente selon connectivité
 * ✅ LAZY SUBSCRIPTION: Abonnement progressif aux streams
 */

class MessageDeliveryService {
  constructor(redis, io) {
    if (!redis || !io) {
      throw new Error(
        "Redis et Socket.io sont requis pour MessageDeliveryService",
      );
    }

    this.redis = redis;
    this.io = io;

    // ✅ CONFIGURATION DES CONSUMERS PARTITIONNÉS
    this.WORKER_PARTITIONS = {
      // CONSUMER 1 : Temps réel critique (3 consumers)
      HIGH_PRIORITY_WORKER: {
        name: "high-priority",
        streams: [
          "conversationCreated",
          "private",
          "call",
          "statusRead",
          "statusDelivered",
        ],
        workers: 3,
        priority: 0,
      },
      // CONSUMER 2 : Messages groupe (2 consumers)
      GROUP_WORKER: {
        name: "group-messages",
        streams: ["group", "channel", "reactions", "replies"],
        workers: 2,
        priority: 1,
      },
      // CONSUMER 3 : Événements système (1 consumer)
      SYSTEM_WORKER: {
        name: "system-events",
        streams: [
          "notifications",
          "conversations",
          "conversationUpdated",
          "participantAdded",
          "participantRemoved",
          "conversationDeleted",
          "files",
          "analytics",
          "statusEdited",
          "statusDeleted",
        ],
        workers: 1,
        priority: 2,
      },
    };

    // ✅ CONFIGURATION DES STREAMS PAR PRIORITÉ
    this.STREAM_CONFIGS = {
      // typing retiré — géré exclusivement par TypingIndicatorService
      // Priorité 0 : Ultra-temps réel (typing, présence)
      // Priorité 1 : Temps réel (messages privés)
      private: {
        streamKey: "chat:stream:messages:private",
        groupId: "delivery-private",
        priority: 1,
        interval: 100,
        workerPartition: "HIGH_PRIORITY_WORKER",
      },
      // Priorité 2 : Normal (messages groupe)
      group: {
        streamKey: "chat:stream:messages:group",
        groupId: "delivery-group",
        priority: 2,
        interval: 200,
        workerPartition: "GROUP_WORKER",
      },
      // Priorité 2.5 : Messages canal
      channel: {
        streamKey: "chat:stream:messages:channel",
        groupId: "delivery-channel",
        priority: 2,
        interval: 200,
        workerPartition: "GROUP_WORKER",
      },
      // Priorité 3 : Notifications
      notifications: {
        streamKey: "chat:stream:events:notifications",
        groupId: "events-notifications",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      // Priorité 3.5 : Événements conversations
      conversations: {
        streamKey: "chat:stream:events:conversations",
        groupId: "events-conversations",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      // Priorité 0.5 : Création conversation (CRITIQUE - doit arriver avant messages)
      conversationCreated: {
        streamKey: "chat:stream:events:conversation:created",
        groupId: "events-conversation-created",
        priority: 0.5,
        interval: 50,
        workerPartition: "HIGH_PRIORITY_WORKER",
      },
      conversationUpdated: {
        streamKey: "chat:stream:events:conversation:updated",
        groupId: "events-conversation-updated",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      participantAdded: {
        streamKey: "chat:stream:events:conversation:participants:added",
        groupId: "events-participant-added",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      participantRemoved: {
        streamKey: "chat:stream:events:conversation:participants:removed",
        groupId: "events-participant-removed",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      conversationDeleted: {
        streamKey: "chat:stream:events:conversation:deleted",
        groupId: "events-conversation-deleted",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      // Priorité 0.5 : Événements appels (temps réel critique)
      call: {
        streamKey: "chat:stream:events:call",
        groupId: "events-call",
        priority: 0.5,
        interval: 50,
        workerPartition: "HIGH_PRIORITY_WORKER",
      },
      // Priorité 3.5 : Événements fichiers
      files: {
        streamKey: "chat:stream:events:files",
        groupId: "events-files",
        priority: 3,
        interval: 500,
        workerPartition: "SYSTEM_WORKER",
      },
      // Priorité 4 : Message status (faible priorité)
      statusDelivered: {
        streamKey: "chat:stream:status:delivered",
        groupId: "delivery-delivered",
        priority: 1,
        interval: 50,
        workerPartition: "HIGH_PRIORITY_WORKER",
      },
      statusRead: {
        streamKey: "chat:stream:status:read",
        groupId: "delivery-read",
        priority: 1,
        interval: 50,
        workerPartition: "HIGH_PRIORITY_WORKER",
      },
      statusEdited: {
        streamKey: "chat:stream:status:edited",
        groupId: "delivery-edited",
        priority: 5,
        interval: 1500,
        workerPartition: "SYSTEM_WORKER",
      },
      statusDeleted: {
        streamKey: "chat:stream:status:deleted",
        groupId: "delivery-deleted",
        priority: 5,
        interval: 1500,
        workerPartition: "SYSTEM_WORKER",
      },
      // Priorité 6 : Interactions (réactions, réponses)
      reactions: {
        streamKey: "chat:stream:events:reactions",
        groupId: "delivery-reactions",
        priority: 6,
        interval: 2000,
        workerPartition: "GROUP_WORKER",
      },
      replies: {
        streamKey: "chat:stream:events:replies",
        groupId: "delivery-replies",
        priority: 6,
        interval: 2000,
        workerPartition: "GROUP_WORKER",
      },
      // Priorité 7 : Analytics (faible priorité)
      analytics: {
        streamKey: "chat:stream:events:analytics",
        groupId: "events-analytics",
        priority: 7,
        interval: 3000,
        workerPartition: "SYSTEM_WORKER",
      },
    };

    // ✅ PHASES D'ABONNEMENT PROGRESSIF (LAZY SUBSCRIPTION)
    this.SUBSCRIPTION_PHASES = {
      PHASE_1: [
        // typing retiré — géré par TypingIndicatorService
        "private",
        "statusRead",
        "statusDelivered",
        "statusEdited",
        "statusDeleted",
        "conversationCreated",
        "call",
      ], // Immédiat (statusEdited/statusDeleted en Phase 1 pour éviter le flash de messages supprimés/modifiés)
      PHASE_2: ["group", "channel"], // Après 100ms
      PHASE_3: [
        "notifications",
        "conversations",
        "conversationUpdated",
        "participantAdded",
        "participantRemoved",
        "conversationDeleted",
      ], // Après 300ms
      PHASE_4: ["files", "reactions", "replies"], // Après 800ms
      PHASE_5: ["analytics"], // Background
    };

    this.streamConsumers = new Map(); // streamKey → { redis, config, isRunning, interval }
    this.userSockets = new Map(); // userId → [socketIds]
    this.userConversations = new Map(); // userId → [conversationIds]
    this.activeUserStreams = new Map(); // userId → Set of active stream types

    // ✅ CONFIGURATION GÉNÉRALE
    this.pendingMessagesPrefix = "chat:stream:pending:messages:"; // chat:stream:pending:messages:2
    this.blockTimeout = 50; // 50ms max per stream (réduit pour éviter les délais en cascade)
    this.maxMessagesPerRead = 50; // ✅ Augmenté de 20→50 pour absorber les bursts de status

    this.isRunning = false;
    this.workers = new Map(); // workerPartition → worker instances

    // ✅ QUEUE DE LIVRAISON SÉRIALISÉE pour éviter la saturation Socket.IO en burst
    this._statusDeliveryQueues = new Map(); // userId → { queue: [], processing: boolean }
    this._STATUS_INTER_MESSAGE_DELAY_MS = 20; // 20ms entre chaque emit pour éviter la saturation
    
    // ✅ CACHE DE DÉDUPLICATION pour éviter double livraison (direct + stream consumer)
    // Clé: "userId:messageId:status" → timestamp de livraison
    this._deliveredStatusCache = new Map();
    this._DEDUP_TTL_MS = 30000; // 30s de TTL pour la déduplication
    // Nettoyage périodique du cache de dédup
    this._dedupCleanupInterval = setInterval(() => {
      const now = Date.now();
      for (const [key, ts] of this._deliveredStatusCache) {
        if (now - ts > this._DEDUP_TTL_MS) {
          this._deliveredStatusCache.delete(key);
        }
      }
    }, 60000); // Nettoyer chaque minute
  }

  /**
   * ✅ LIVRAISON DIRECTE SÉRIALISÉE D'UN STATUT VIA SOCKET.IO
   * Évite la perte de messages quand 26+ status arrivent en burst
   */
  async enqueueDirectStatusDelivery(recipientId, eventData) {
    const recipientIdStr = String(recipientId);

    if (!this._statusDeliveryQueues.has(recipientIdStr)) {
      this._statusDeliveryQueues.set(recipientIdStr, {
        queue: [],
        processing: false,
      });
    }

    const entry = this._statusDeliveryQueues.get(recipientIdStr);
    entry.queue.push(eventData);

    if (!entry.processing) {
      entry.processing = true;
      this._processStatusDeliveryQueue(recipientIdStr).catch((err) => {
        console.error(
          `❌ Erreur queue status delivery pour ${recipientIdStr}:`,
          err.message,
        );
        entry.processing = false;
      });
    }
  }

  /**
   * ✅ TRAITER LA QUEUE DE LIVRAISON SÉRIALISÉE POUR UN UTILISATEUR
   */
  async _processStatusDeliveryQueue(userId) {
    const entry = this._statusDeliveryQueues.get(userId);
    if (!entry) return;

    while (entry.queue.length > 0) {
      const eventData = entry.queue.shift();

      try {
        const socketIds = this.userSockets.get(userId);
        if (socketIds && socketIds.length > 0) {
          for (const socketId of socketIds) {
            const socket = this.io.sockets.sockets.get(socketId);
            if (socket) {
              socket.emit("message:status", eventData);
              console.log(`✅ [DIRECT-QUEUE] Statut livré via Socket.IO:`, {
                socketId,
                recipientId: userId,
                messageId: eventData.messageId || "N/A",
                status: eventData.status,
              });
            }
          }
          // ✅ Marquer comme déjà livré pour éviter double livraison par le stream consumer
          const dedupKey = `${userId}:${eventData.messageId}:${eventData.status}`;
          this._deliveredStatusCache.set(dedupKey, Date.now());
        }
      } catch (err) {
        console.error(
          `❌ [DIRECT-QUEUE] Erreur livraison status ${eventData.messageId} → ${userId}:`,
          err.message,
        );
      }

      // ✅ Délai inter-messages pour éviter la saturation du transport WebSocket
      if (entry.queue.length > 0) {
        await new Promise((r) =>
          setTimeout(r, this._STATUS_INTER_MESSAGE_DELAY_MS),
        );
      }
    }

    entry.processing = false;
    // Nettoyer si la queue est vide
    if (entry.queue.length === 0) {
      this._statusDeliveryQueues.delete(userId);
    }
  }

  /**
   * ✅ INITIALISER TOUS LES WORKERS PARTITIONNÉS
   */
  async initialize() {
    try {
      console.log(
        "🚀 Initialisation MessageDeliveryService avec Workers Partitionnés...",
      );

      // Créer les workers pour chaque partition
      for (const [partitionKey, partitionConfig] of Object.entries(
        this.WORKER_PARTITIONS,
      )) {
        await this.createWorkerPartition(partitionKey, partitionConfig);
      }

      // Démarrer tous les consumers
      this.startAllConsumers();

      console.log(
        `✅ MessageDeliveryService initialisé avec ${this.workers.size} partitions de workers`,
      );
      return true;
    } catch (error) {
      console.error("❌ Erreur initialisation MessageDeliveryService:", error);
      throw error;
    }
  }

  /**
   * ✅ CRÉER UNE PARTITION DE WORKERS
   */
  async createWorkerPartition(partitionKey, partitionConfig) {
    try {
      console.log(
        `🔧 Création partition ${partitionKey} avec ${partitionConfig.workers} worker(s)...`,
      );

      const workers = [];

      // Créer le nombre spécifié de workers pour cette partition
      for (let i = 0; i < partitionConfig.workers; i++) {
        const worker = await this.createWorkerInstance(
          partitionKey,
          partitionConfig,
          i,
        );
        workers.push(worker);
      }

      this.workers.set(partitionKey, {
        config: partitionConfig,
        workers: workers,
        isRunning: false,
      });

      console.log(
        `✅ Partition ${partitionKey} créée avec ${workers.length} worker(s)`,
      );
    } catch (error) {
      console.error(`❌ Erreur création partition ${partitionKey}:`, error);
      throw error;
    }
  }

  /**
   * ✅ CRÉER UNE INSTANCE DE WORKER
   */
  async createWorkerInstance(partitionKey, partitionConfig, workerIndex) {
    try {
      const workerId = `${partitionKey}-worker-${workerIndex}`;
      const redisConsumer = this.redis.duplicate();
      await redisConsumer.connect();

      // Créer les consumers pour les streams de cette partition
      const streamConsumers = new Map();

      for (const streamType of partitionConfig.streams) {
        const config = this.STREAM_CONFIGS[streamType];
        if (!config) {
          console.warn(`⚠️ Configuration manquante pour stream: ${streamType}`);
          continue;
        }

        // Créer le consumer group pour ce stream
        try {
          await redisConsumer.xGroupCreate(
            config.streamKey,
            config.groupId,
            "$",
            { MKSTREAM: true },
          );
          console.log(
            `✅ Consumer group créé: ${config.groupId} pour ${streamType}`,
          );
        } catch (groupErr) {
          if (!groupErr.message.includes("BUSYGROUP")) {
            throw groupErr;
          }
        }

        streamConsumers.set(config.streamKey, {
          redis: redisConsumer,
          config,
          streamType,
          isRunning: false,
          interval: null,
        });
      }

      return {
        id: workerId,
        redis: redisConsumer,
        streamConsumers,
        partitionKey,
        isRunning: false,
      };
    } catch (error) {
      console.error(`❌ Erreur création worker ${workerId}:`, error);
      throw error;
    }
  }

  /**
   * ✅ DÉMARRER TOUS LES CONSUMERS AVEC PRIORITÉ
   */
  startAllConsumers() {
    this.isRunning = true;

    for (const [partitionKey, partition] of this.workers.entries()) {
      this.startConsumerPartition(partitionKey, partition);
    }

    console.log("▶️ Tous les consumers démarrés avec partitionnement");
  }

  /**
   * ✅ DÉMARRER UNE PARTITION DE CONSUMERS
   */
  startConsumerPartition(partitionKey, partition) {
    if (partition.isRunning) return;

    partition.isRunning = true;

    // Démarrer chaque consumer de la partition
    for (const worker of partition.workers) {
      this.startConsumerInstance(worker);
    }

    console.log(
      `▶️ Partition ${partitionKey} démarrée avec ${partition.workers.length} consumer(s)`,
    );
  }

  /**
   * ✅ DÉMARRER UNE INSTANCE DE CONSUMER
   */
  startConsumerInstance(worker) {
    if (worker.isRunning) return;

    worker.isRunning = true;

    // Trier les streams par priorité pour ce consumer
    const sortedConsumers = Array.from(worker.streamConsumers.values()).sort(
      (a, b) => a.config.priority - b.config.priority,
    );

    // Démarrer un consumer pour chaque stream
    for (const consumer of sortedConsumers) {
      this.startStreamConsumerForWorker(worker, consumer);
    }

    console.log(
      `⏱️ Consumer ${worker.id} démarré avec ${sortedConsumers.length} stream(s)`,
    );
  }

  /**
   * ✅ DÉMARRER UN CONSUMER DE STREAM POUR UN WORKER
   */
  startStreamConsumerForWorker(worker, consumer) {
    if (consumer.isRunning) return;

    consumer.isRunning = true;
    const interval = consumer.config.interval;

    consumer.interval = setInterval(async () => {
      if (!this.isRunning || !worker.isRunning) return;

      try {
        await this.consumeStreamSmart(consumer);
      } catch (error) {
        console.error(
          `❌ Erreur boucle ${consumer.streamType} (${worker.id}):`,
          error.message,
        );
      }
    }, interval);

    console.log(
      `⏱️ Consumer ${consumer.streamType} démarré (${worker.id}, interval: ${interval}ms, priorité: ${consumer.config.priority})`,
    );
  }

  /**
   * ✅ CONSOMMER UN STREAM AVEC LOGIQUE INTELLIGENTE
   */
  async consumeStreamSmart(consumer) {
    try {
      // ✅ STRATÉGIE DE CONSOMMATION INTELLIGENTE
      // Si aucun utilisateur n'est connecté et que le stream n'est pas critique, ralentir
      if (this.userSockets.size === 0 && consumer.config.priority > 2) {
        // Pas d'utilisateurs connectés, skip les streams non-critiques
        await this.sleep(consumer.config.interval * 2);
        return;
      }

      // ✅ UTILISER UN CONSUMER ID GÉNÉRIQUE (pas par utilisateur)
      const consumerId = `${consumer.config.groupId}:delivery-worker`;

      try {
        // ✅ LIRE TOUS LES MESSAGES DU STREAM
        const messages = await consumer.redis.xReadGroup(
          consumer.config.groupId,
          consumerId,
          { key: consumer.config.streamKey, id: ">" },
          { COUNT: this.maxMessagesPerRead, BLOCK: this.blockTimeout },
        );

        if (messages && messages.length > 0) {
          const entries = messages[0]?.messages || [];

          for (const entry of entries) {
            try {
              const message = entry.message;

              // ✅ DISTRIBUER LE MESSAGE AU BON DESTINATAIRE
              await this.distributeMessageToRecipient(
                consumer.streamType,
                message,
                entry.id,
              );

              // ✅ ACK APRÈS LIVRAISON RÉUSSIE
              await consumer.redis.xAck(
                consumer.config.streamKey,
                consumer.config.groupId,
                entry.id,
              );
            } catch (messageError) {
              console.warn(
                `⚠️ Erreur traitement message ${consumer.streamType}:`,
                messageError.message,
              );
            }
          }
        }
      } catch (streamError) {
        if (!streamError.message.includes("timeout")) {
          console.warn(
            `⚠️ Erreur consommation stream ${consumer.streamType}:`,
            streamError.message,
          );
        }
      }
    } catch (error) {
      console.error(
        `❌ Erreur consumeStreamSmart ${consumer.streamType}:`,
        error.message,
      );
    }
  }

  /**
   * ✅ UTILITAIRE SLEEP POUR RALENTIR LA CONSOMMATION
   */
  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * ✅ NOUVELLE MÉTHODE : DISTRIBUER LE MESSAGE AU BON DESTINATAIRE
   */
  async distributeMessageToRecipient(streamType, message, entryId) {
    try {
      console.log(`📬 Distribution message ${streamType}:`, {
        ...(streamType === "conversationCreated" && {
          event: message.event,
          conversationId: message.conversationId,
          createdBy: message.createdBy,
          participants: message.participants,
          type: message.type,
        }),
        ...((streamType === "private" ||
          streamType === "group" ||
          streamType === "channel") && {
          messageId: message.messageId,
          senderId: message.senderId,
          conversationId: message.conversationId,
        }),
        ...(streamType === "files" && {
          fileId: message.fileId,
          userId: message.userId,
          fileName: message.fileName,
          fileSize: message.fileSize,
        }),
        ...([
          "statusDelivered",
          "statusRead",
          "statusEdited",
          "statusDeleted",
        ].includes(streamType) && {
          messageId: message.messageId,
          userId: message.userId,
          status: message.status,
          isBulk: message.isBulk,
          messageCount: message.messageCount,
          conversationId: message.conversationId,
        }),
        ...(![
          "conversationCreated",
          "private",
          "group",
          "channel",
          "files",
          "statusDelivered",
          "statusRead",
          "statusEdited",
          "statusDeleted",
          "reactions",
          "replies",
        ].includes(streamType) && {
          messageId: message.messageId,
          senderId: message.senderId,
          receiverId: message.receiverId,
          conversationId: message.conversationId,
        }),
        ...(streamType === "reactions" && {
          messageId: message.messageId,
          userId: message.userId,
          reaction: message.reaction,
          action: message.action,
          conversationId: message.conversationId,
        }),
        ...(streamType === "replies" && {
          messageId: message.messageId,
          userId: message.userId,
          replyId: message.replyId,
          conversationId: message.conversationId,
        }),
      });

      switch (streamType) {
        // ✅ CAS 1 : MESSAGES PRIVÉS
        case "private":
          if (message.receiverId) {
            const receiverId = String(message.receiverId);
            const senderId = String(message.senderId);
            const senderSocketId = message.senderSocketId || null;

            console.log(
              `➡️ Livraison message privé: ${message.senderId} → ${receiverId}`,
            );

            // ✅ LIVRER AU DESTINATAIRE
            if (this.isStreamActiveForUser(receiverId, streamType)) {
              if (this.userSockets.has(receiverId)) {
                await this.deliverPrivateMessage(message, receiverId);
              } else {
                console.log(
                  `⏳ Destinataire ${receiverId} déconnecté, message en attente`,
                );
                await this.addToPendingQueue(
                  receiverId,
                  message,
                  "message",
                  "private",
                );
              }
            } else {
              console.log(
                `⏸️ Stream ${streamType} pas encore actif pour ${receiverId}, message mis en attente`,
              );
              await this.addToPendingQueue(
                receiverId,
                message,
                "message",
                "private",
              );
            }

            // ✅ LIVRER AUX AUTRES APPAREILS DU SENDER (multi-device)
            // Le socket émetteur a déjà reçu l'ACK, mais ses autres appareils doivent voir le message
            if (senderId && this.userSockets.has(senderId)) {
              const senderSockets = this.userSockets.get(senderId) || [];
              if (senderSockets.length > 1 || !senderSocketId) {
                await this.deliverPrivateMessage(
                  message,
                  senderId,
                  senderSocketId,
                );
              }
            }
          } else {
            console.warn("⚠️ Message privé sans receiverId:", message);
          }
          break;

        // ✅ CAS 2 : MESSAGES DE GROUPE
        case "group":
          if (message.conversationId) {
            console.log(
              `➡️ Livraison message groupe: ${message.conversationId}`,
            );

            // ✅ LIVRER À TOUS LES PARTICIPANTS CONNECTÉS DONT LE STREAM EST ACTIF
            await this.deliverGroupMessageToAllParticipants(message);
          } else {
            console.warn("⚠️ Message groupe sans conversationId:", message);
          }
          break;

        // ✅ CAS 2.5 : MESSAGES CANAL
        case "channel":
          if (message.conversationId) {
            console.log(
              `➡️ Livraison message canal: ${message.conversationId}`,
            );

            // ✅ LIVRER À TOUS LES PARTICIPANTS CONNECTÉS DONT LE STREAM EST ACTIF
            await this.deliverChannelMessageToAllParticipants(message);
          } else {
            console.warn("⚠️ Message canal sans conversationId:", message);
          }
          break;

        // typing retiré — géré par TypingIndicatorService

        // ✅ CAS 4-7 : MESSAGE STATUS
        case "statusDelivered":
        case "statusRead":
        case "statusEdited":
        case "statusDeleted":
          // ✅ CAS INDIVIDUEL: messageId présent
          if (message.messageId && message.userId) {
            const targetUser = String(message.userId);

            // ✅ DÉDUPLICATION: vérifier si déjà livré directement par la queue sérialisée
            const dedupKey = `${targetUser}:${message.messageId}:${message.status}`;
            if (
              this._deliveredStatusCache &&
              this._deliveredStatusCache.has(dedupKey)
            ) {
              console.log(
                `⏭️ [DEDUP] Status ${message.status} déjà livré directement pour ${message.messageId} → ${targetUser}, skip consumer`,
              );
              break;
            }

            // Livrer le statut du message à l'expéditeur original
            if (this.isStreamActiveForUser(targetUser, streamType)) {
              await this.deliverMessageStatus(message, targetUser);
            } else if (this.userSockets.has(targetUser)) {
              // ✅ FIX: L'utilisateur est connecté mais le stream n'est pas encore activé
              // (peut arriver pendant l'abonnement progressif) → livrer quand même
              console.log(
                `⚠️ [STATUS] Stream ${streamType} pas encore actif pour ${targetUser}, mais socket connecté → livraison forcée`,
              );
              await this.deliverMessageStatus(message, targetUser);
            } else {
              // ✅ FIX: L'utilisateur est offline → mettre en attente au lieu de drop silencieux
              console.log(
                `⏳ [STATUS] Utilisateur ${targetUser} offline → mise en attente ${streamType}`,
              );
              await this.addToPendingQueue(targetUser, message, streamType);
            }
          }
          // ✅ CAS BULK: isBulk présent avec participants
          else if (
            (message.isBulk === "true" || message.isBulk === true) &&
            message.conversationId
          ) {
            console.log(
              `📡 [ROUTE] Événement statut bulk détecté pour conversation ${message.conversationId}`,
            );
            // Livrer à TOUS les participants
            await this.deliverMessageStatus(message, message.userId || null);
          }
          break;

        // ✅ CAS 8 : NOTIFICATIONS SYSTÈME
        case "notifications":
          if (message.userId) {
            const targetUser = String(message.userId);
            if (this.isStreamActiveForUser(targetUser, streamType)) {
              if (this.userSockets.has(targetUser)) {
                await this.deliverNotification(message, targetUser);
              } else {
                await this.addToPendingQueue(
                  targetUser,
                  message,
                  "notifications",
                );
              }
            } else {
              await this.addToPendingQueue(
                targetUser,
                message,
                "notifications",
              );
            }
          }
          break;

        // ✅ CAS 9 : ÉVÉNEMENTS CONVERSATIONS
        case "conversations":
          if (message.conversationId) {
            await this.deliverConversationEventToParticipants(message);
          }
          break;

        // ✅ CAS 9.1 : CONVERSATION CRÉÉE
        case "conversationCreated":
          if (message.conversationId) {
            await this.deliverConversationCreatedEvent(message);
          }
          break;

        // ✅ CAS 9.2 : CONVERSATION MISE À JOUR
        case "conversationUpdated":
          if (message.conversationId) {
            await this.deliverConversationUpdatedEvent(message);
          }
          break;

        // ✅ CAS 9.3 : PARTICIPANT AJOUTÉ
        case "participantAdded":
          if (message.conversationId) {
            await this.deliverParticipantAddedEvent(message);
          }
          break;

        // ✅ CAS 9.4 : PARTICIPANT RETIRÉ
        case "participantRemoved":
          if (message.conversationId) {
            await this.deliverParticipantRemovedEvent(message);
          }
          break;

        // ✅ CAS 9.5 : CONVERSATION SUPPRIMÉE
        case "conversationDeleted":
          if (message.conversationId) {
            await this.deliverConversationDeletedEvent(message);
          }
          break;

        // ✅ CAS 10 : ÉVÉNEMENTS FICHIERS
        case "files":
          if (message.userId) {
            const targetUser = String(message.userId);
            if (this.isStreamActiveForUser(targetUser, streamType)) {
              await this.deliverFileEvent(message);
            } else {
              // ✅ UTILISATEUR DÉCONNECTÉ - AJOUTER EN FILE D'ATTENTE
              console.log(
                `⏳ Utilisateur ${targetUser} déconnecté - événement fichier en attente`,
              );
              await this.addToPendingQueue(targetUser, message, "files");
            }
          }
          break;

        // ✅ CAS 11 : RÉACTIONS
        case "reactions":
          if (message.messageId) {
            await this.deliverReactionEvent(message);
          }
          break;

        // ✅ CAS 12 : RÉPONSES
        case "replies":
          if (message.messageId) {
            await this.deliverReplyEvent(message);
          }
          break;

        // ✅ CAS 13 : ANALYTICS
        case "analytics":
          // Analytics events peuvent être ignorés côté client
          console.log(`📊 Analytics event reçu: ${message.event}`);
          break;

        // ✅ CAS 14 : ÉVÉNEMENTS APPELS
        case "call":
          if (message.conversationId) {
            try {
              const participants = message.participants
                ? typeof message.participants === "string"
                  ? JSON.parse(message.participants)
                  : message.participants
                : [];

              const callEvent = {
                messageId: message.messageId,
                callId: message.callId,
                conversationId: message.conversationId,
                status: message.status,
                userId: message.userId,
                startedAt: message.startedAt || null,
                endedAt: message.endedAt || null,
                duration: message.duration ? Number(message.duration) : 0,
                endReason: message.endReason || null,
                timestamp: message.timestamp,
              };

              console.log(
                `📞 Distribution call.status.updated (${message.status}) à ${participants.length} participant(s)`,
              );

              const excludeSocketId = message.senderSocketId || "";

              for (const participantId of participants) {
                const userIdStr = String(participantId);

                // ✅ VÉRIFIER SI LE STREAM CALL EST ACTIF POUR CET UTILISATEUR
                if (this.isStreamActiveForUser(userIdStr, "call")) {
                  if (this.userSockets.has(userIdStr)) {
                    const socketIds = this.userSockets.get(userIdStr);
                    for (const socketId of socketIds) {
                      // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
                      if (excludeSocketId && socketId === excludeSocketId) {
                        console.log(
                          `⏭️ call:statusUpdated - socket émetteur exclu: ${socketId}`,
                        );
                        continue;
                      }
                      const targetSocket =
                        this.io?.sockets?.sockets?.get(socketId);
                      if (targetSocket) {
                        targetSocket.emit("call:statusUpdated", callEvent);
                        console.log(
                          `📞 call:statusUpdated (${message.status}) livré à ${userIdStr}`,
                        );
                      }
                    }
                  } else {
                    console.log(
                      `⏳ Participant ${userIdStr} déconnecté — événement appel en attente`,
                    );
                    await this.addToPendingQueue(
                      userIdStr,
                      { ...callEvent, event: "call:statusUpdated" },
                      "call",
                    );
                  }
                } else {
                  console.log(
                    `⏸️ Stream call pas encore actif pour ${userIdStr}, événement mis en attente`,
                  );
                  await this.addToPendingQueue(
                    userIdStr,
                    { ...callEvent, event: "call:statusUpdated" },
                    "call",
                  );
                }
              }
            } catch (callErr) {
              console.error(
                "❌ Erreur distribution événement appel:",
                callErr.message,
              );
            }
          }
          break;

        default:
          console.warn(`⚠️ Stream type inconnu: ${streamType}`);
      }
    } catch (error) {
      console.error(`❌ Erreur distribution message ${streamType}:`, error);
      throw error;
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE GROUPE À TOUS LES PARTICIPANTS
   */
  async deliverGroupMessageToAllParticipants(message) {
    try {
      const conversationId = String(message.conversationId);
      const senderId = String(message.senderId);
      const isSystemMessage = message.type === "SYSTEM";

      // ✅ DÉTERMINER LES DESTINATAIRES
      let targetParticipants = [];

      if (isSystemMessage && message.participants) {
        // ✅ CAS 1 : MESSAGE SYSTÈME AVEC LISTE DE PARTICIPANTS (création groupe, etc.)
        try {
          const participants =
            typeof message.participants === "string"
              ? JSON.parse(message.participants)
              : message.participants;

          console.log(
            `📢 Message système trouvé avec ${
              participants.length
            } participant(s): ${participants.join(", ")}`,
          );

          // Livrer à chaque participant connecté dont le stream est actif
          for (const participantId of participants) {
            const userIdStr = String(participantId);
            if (
              this.isStreamActiveForUser(userIdStr, "group") &&
              this.userSockets.has(userIdStr)
            ) {
              targetParticipants.push(userIdStr);
              console.log(
                `✅ Participant ${userIdStr} connecté et stream actif - sera notifié`,
              );
            } else if (!this.userSockets.has(userIdStr)) {
              console.log(
                `⏳ Participant ${userIdStr} non connecté - message en attente`,
              );
              // Ajouter en queue pour délivrance ultérieure
              await this.addToPendingQueue(
                userIdStr,
                message,
                "message",
                "group",
              );
            } else {
              console.log(
                `⏸️ Participant ${userIdStr} connecté mais stream 'group' pas actif`,
              );
              await this.addToPendingQueue(
                userIdStr,
                message,
                "message",
                "group",
              );
            }
          }
        } catch (parseErr) {
          console.warn(
            "⚠️ Erreur parsing participants du message système:",
            parseErr.message,
          );
        }
      } else {
        // ✅ CAS 2 : MESSAGE NORMAL - CHERCHER DANS userConversations
        const senderSocketId = message.senderSocketId || null;

        for (const [userId, socketIds] of this.userSockets.entries()) {
          // ✅ VÉRIFIER SI L'UTILISATEUR EST DANS LA CONVERSATION ET QUE LE STREAM EST ACTIF
          const userConversations = this.userConversations.get(userId) || [];
          if (
            userConversations.includes(conversationId) &&
            this.isStreamActiveForUser(userId, "group")
          ) {
            targetParticipants.push(userId);
          } else if (userConversations.includes(conversationId)) {
            // ✅ NE PAS METTRE EN QUEUE le sender (il a déjà l'ACK)
            if (userId !== senderId) {
              await this.addToPendingQueue(userId, message, "message", "group");
            }
          }
        }
      }

      console.log(
        `👥 Livraison message ${isSystemMessage ? "SYSTÈME" : "groupe"} à ${
          targetParticipants.length
        } utilisateur(s) connecté(s) avec stream actif`,
      );

      // ✅ LIVRER À CHAQUE UTILISATEUR CONNECTÉ
      for (const userId of targetParticipants) {
        await this.deliverGroupMessage(message, userId);
      }

      console.log(
        `✅ Message ${isSystemMessage ? "SYSTÈME" : "groupe"} livré: ${
          isSystemMessage ? message.subType : senderId
        } → conv:${conversationId} (${targetParticipants.length} destinataires)`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison message groupe:", error);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE CANAL À TOUS LES PARTICIPANTS
   */
  async deliverChannelMessageToAllParticipants(message) {
    try {
      const conversationId = String(message.conversationId);
      const senderId = String(message.senderId);

      // ✅ TROUVER LES DESTINATAIRES CONNECTÉS AVEC STREAM ACTIF
      let targetParticipants = [];

      for (const [userId, socketIds] of this.userSockets.entries()) {
        // ✅ VÉRIFIER SI L'UTILISATEUR EST DANS LA CONVERSATION ET QUE LE STREAM EST ACTIF
        const userConversations = this.userConversations.get(userId) || [];
        if (
          userConversations.includes(conversationId) &&
          this.isStreamActiveForUser(userId, "channel")
        ) {
          targetParticipants.push(userId);
        } else if (userConversations.includes(conversationId)) {
          // ✅ NE PAS METTRE EN QUEUE le sender (il a déjà l'ACK)
          if (userId !== senderId) {
            await this.addToPendingQueue(userId, message, "message", "channel");
          }
        }
      }

      console.log(
        `📺 Livraison message canal à ${targetParticipants.length} utilisateur(s) connecté(s) avec stream actif`,
      );

      // ✅ LIVRER À CHAQUE UTILISATEUR CONNECTÉ
      for (const userId of targetParticipants) {
        await this.deliverChannelMessage(message, userId);
      }

      console.log(
        `✅ Message canal livré: ${senderId} → conv:${conversationId} (${targetParticipants.length} destinataires)`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison message canal:", error);
    }
  }

  // deliverTypingEventToConversationParticipants retiré — géré par TypingIndicatorService

  /**
   * ✅ ROUTER LES MESSAGES SELON LE TYPE DE STREAM
   */
  async routeMessageByStreamType(streamType, message, userId) {
    const userIdStr = String(userId);

    console.log(
      `➡️ Routing message ${streamType} pour utilisateur ${userIdStr}`,
    );

    console.log(
      "Receiver check:",
      message.receiverId && String(message.receiverId) === userIdStr,
    );

    switch (streamType) {
      // ✅ CAS 1 : MESSAGES PRIVÉS
      case "private":
        if (message.receiverId && String(message.receiverId) === userIdStr) {
          console.log("Livraison message privé à", userIdStr);
          await this.deliverPrivateMessage(message, userIdStr);
        }
        break;

      // ✅ CAS 2 : MESSAGES DE GROUPE
      case "group":
        if (
          message.conversationId &&
          (await this.isUserInConversation(userIdStr, message.conversationId))
        ) {
          await this.deliverGroupMessage(message, userIdStr);
        }
        break;

      // ✅ CAS 2.5 : MESSAGES CANAL
      case "channel":
        if (
          message.conversationId &&
          (await this.isUserInConversation(userIdStr, message.conversationId))
        ) {
          await this.deliverChannelMessage(message, userIdStr);
        }
        break;

      // typing retiré — géré par TypingIndicatorService

      // ✅ CAS 4-7 : MESSAGE STATUS
      case "statusDelivered":
      case "statusRead":
      case "statusEdited":
      case "statusDeleted":
        if (message.userId && String(message.userId) === userIdStr) {
          await this.deliverMessageStatus(message, userIdStr);
        }
        break;

      // ✅ CAS 8 : NOTIFICATIONS SYSTÈME
      case "notifications":
        await this.deliverNotification(message, userIdStr);
        break;

      // ✅ CAS 9 : ÉVÉNEMENTS CONVERSATIONS
      case "conversations":
        if (
          message.conversationId &&
          (await this.isUserInConversation(userIdStr, message.conversationId))
        ) {
          await this.deliverConversationEvent(message, userIdStr);
        }
        break;

      // ✅ CAS 10 : ÉVÉNEMENTS FICHIERS
      case "files":
        if (message.userId && String(message.userId) === userIdStr) {
          await this.deliverFileEvent(message);
        }
        break;

      // ✅ CAS 11 : RÉACTIONS
      case "reactions":
        // Les réactions sont broadcastées à tous
        await this.deliverReactionEvent(message);
        break;

      // ✅ CAS 12 : RÉPONSES
      case "replies":
        // Les réponses sont broadcastées à tous
        await this.deliverReplyEvent(message);
        break;

      // ✅ CAS 13 : ANALYTICS
      case "analytics":
        // Analytics ignorés côté client
        break;

      default:
        console.warn(`⚠️ Stream type inconnu: ${streamType}`);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE PRIVÉ
   */
  async deliverPrivateMessage(message, userId, excludeSocketId = null) {
    try {
      const socketIds = this.userSockets.get(userId);

      console.log("userSockets", socketIds);

      if (!socketIds || socketIds.length === 0) {
        // Utilisateur pas connecté - ajouter en queue d'attente
        await this.addToPendingQueue(userId, message);
        return;
      }

      // Envoyer à toutes les connexions de l'utilisateur
      // (sauf le socket émetteur spécifique qui a déjà reçu l'ACK)
      for (const socketId of socketIds) {
        if (excludeSocketId && socketId === excludeSocketId) {
          continue;
        }

        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("newMessage", {
            messageId: message.messageId,
            conversationId: message.conversationId,
            senderId: message.senderId,
            receiverId: message.receiverId,
            content: message.content,
            type: message.type,
            status: message.status || "SENT",
            timestamp: message.timestamp,
            metadata: message.metadata,
            ...(message.replyTo ? { replyTo: message.replyTo } : {}),
            ...(message.isForwarded
              ? {
                  isForwarded: true,
                  forwardedFrom: message.forwardedFrom,
                  originalSenderId: message.originalSenderId,
                }
              : {}),
          });
        }
      }

      console.log(`✅ Message privé livré: ${message.senderId} → ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverPrivateMessage:", error);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE GROUPE
   */
  async deliverGroupMessage(message, userId) {
    try {
      const room = `conversation_${message.conversationId}`;
      const socketIds = this.userSockets.get(userId);
      const isSystemMessage = message.type === "SYSTEM";
      const senderId = String(message.senderId || "");
      const senderSocketId = message.senderSocketId || null;

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      // ✅ CONSTRUIRE LES DONNÉES DU MESSAGE
      const messageData = {
        messageId: message.messageId,
        conversationId: message.conversationId,
        senderId: message.senderId,
        senderName: message.senderName || "Système",
        content: message.content,
        type: message.type,
        subType: message.subType,
        status: message.status || "DELIVERED",
        timestamp: message.timestamp || message.createdAt,
        metadata: message.metadata,
        ...(message.replyTo ? { replyTo: message.replyTo } : {}),
        ...(message.isForwarded
          ? {
              isForwarded: true,
              forwardedFrom: message.forwardedFrom,
              originalSenderId: message.originalSenderId,
            }
          : {}),
      };

      // ✅ ENVOYER À TOUTES LES CONNEXIONS DE L'UTILISATEUR
      // Pour le sender : exclure uniquement le socket émetteur (ses autres appareils reçoivent)
      for (const socketId of socketIds) {
        // ✅ Exclure le socket émetteur spécifique (pas tous les sockets du sender)
        if (userId === senderId && socketId === senderSocketId) {
          continue;
        }

        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          // ✅ CAS 1 : MESSAGE SYSTÈME - UTILISER EVENT 'newMessage' POUR UNIFORMITÉ
          if (isSystemMessage) {
            socket.emit("newMessage", messageData);
            console.log(
              `📢 Message SYSTÈME livré: ${message.subType} → userId:${userId}`,
            );
          } else {
            // ✅ CAS 2 : MESSAGE NORMAL - EVENT 'message:group'
            socket.emit("message:group", messageData);
            console.log(
              `📬 Message groupe livré: ${message.senderId} → userId:${userId}`,
            );
          }
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverGroupMessage:", error);
    }
  }

  // deliverTypingEvent retiré — géré par TypingIndicatorService

  /**
   * ✅ LIVRER UN STATUT DE MESSAGE
   */
  async deliverMessageStatus(message, userId) {
    try {
      // ✅ PARSER LES PARTICIPANTS S'ILS SONT EN JSON
      let participants = [];
      if (message.participants) {
        try {
          participants =
            typeof message.participants === "string"
              ? JSON.parse(message.participants)
              : message.participants;
        } catch (parseErr) {
          console.warn("⚠️ Erreur parsing participants:", parseErr.message);
          participants = [];
        }
      }

      // ✅ SI PARTICIPANTS EXISTE, ENVOYER À TOUS LES PARTICIPANTS
      // SINON, ENVOYER À L'UTILISATEUR SPÉCIFIÉ
      const recipientIds =
        participants && participants.length > 0
          ? participants.map((p) => p.userId || p).filter(Boolean)
          : [userId];

      // ✅ GÉRER DEUX TYPES D'ÉVÉNEMENTS: individuels (messageId) ou en masse (isBulk)
      const eventData =
        message.isBulk === "true" || message.isBulk === true
          ? {
              // Événement en masse
              isBulk: true,
              conversationId: message.conversationId,
              userId: message.userId,
              status: message.status,
              messageCount: parseInt(message.messageCount) || 0,
              participants: participants,
              timestamp: message.timestamp,
            }
          : {
              // Événement individuel
              messageId: message.messageId,
              conversationId: message.conversationId,
              userId: message.userId,
              status: message.status,
              participants: participants,
              timestamp: message.timestamp,
              // ✅ Inclure le contenu pour EDITED (pour que les clients puissent mettre à jour l'UI)
              ...(message.messageContent
                ? { newContent: message.messageContent }
                : {}),
              // ✅ Inclure le type de suppression pour DELETED
              ...(message.deleteType ? { deleteType: message.deleteType } : {}),
            };

      console.log(
        `📢 [deliverMessageStatus] Envoi à ${recipientIds.length} participant(s):`,
        {
          recipientIds,
          participantsCount: participants.length,
          isBulk: eventData.isBulk || false,
        },
      );

      // ✅ ENVOYER À CHAQUE PARTICIPANT (ONLINE OU EN ATTENTE)
      const excludeSocketId = message.senderSocketId || "";

      for (const recipientId of recipientIds) {
        const recipientIdStr = String(recipientId);
        const socketIds = this.userSockets.get(recipientIdStr);

        if (socketIds && socketIds.length > 0) {
          // ✅ PARTICIPANT ONLINE - LIVRER IMMÉDIATEMENT
          console.log(
            `✅ Participant ${recipientIdStr} ONLINE - livraison immédiate`,
          );
          for (const socketId of socketIds) {
            // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
            if (excludeSocketId && socketId === excludeSocketId) {
              console.log(
                `⏭️ message:status - socket émetteur exclu: ${socketId}`,
              );
              continue;
            }
            const socket = this.io.sockets.sockets.get(socketId);
            if (socket) {
              socket.emit("message:status", eventData);
              console.log(`✅ Statut livré via Socket.IO:`, {
                socketId,
                recipientId: recipientIdStr,
                isBulk: eventData.isBulk || false,
                messageId: eventData.messageId || "N/A",
                conversationId: eventData.conversationId || "N/A",
              });
            }
          }
        } else {
          // ✅ PARTICIPANT OFFLINE - METTRE EN ATTENTE POUR LIVRAISON ULTÉRIEURE
          console.log(
            `⏳ Participant ${recipientIdStr} OFFLINE - mise en attente`,
          );
          try {
            const statusStreamType =
              eventData.status === "DELIVERED"
                ? "statusDelivered"
                : eventData.status === "READ"
                  ? "statusRead"
                  : eventData.status === "EDITED"
                    ? "statusEdited"
                    : eventData.status === "DELETED"
                      ? "statusDeleted"
                      : "statusDelivered";

            const pendingKey = `chat:stream:pending:messages:${recipientIdStr}:${statusStreamType}`;

            // ✅ AJOUTER EN FILE D'ATTENTE (Redis STREAM)
            await this.redis.xAdd(pendingKey, "*", {
              event: JSON.stringify(eventData),
              streamType: statusStreamType,
              addedAt: new Date().toISOString(),
            });

            // ✅ DÉFINIR TTL DE 24H
            await this.redis.expire(pendingKey, 86400);

            console.log(
              `✅ Statut mis en attente pour ${recipientIdStr}: ${pendingKey}`,
            );
          } catch (queueErr) {
            console.error(
              `❌ Erreur mise en attente pour ${recipientIdStr}:`,
              queueErr.message,
            );
          }
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverMessageStatus:", error);
    }
  }

  /**
   * ✅ LIVRER UN MESSAGE DE CANAL
   */
  async deliverChannelMessage(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);
      const senderId = String(message.senderId || "");
      const senderSocketId = message.senderSocketId || null;

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      // ✅ CONSTRUIRE LES DONNÉES DU MESSAGE
      const messageData = {
        messageId: message.messageId,
        conversationId: message.conversationId,
        senderId: message.senderId,
        senderName: message.senderName || "Système",
        content: message.content,
        type: message.type,
        status: message.status || "DELIVERED",
        timestamp: message.timestamp || message.createdAt,
        metadata: message.metadata,
      };

      // ✅ ENVOYER À TOUTES LES CONNEXIONS DE L'UTILISATEUR
      // Pour le sender : exclure uniquement le socket émetteur
      for (const socketId of socketIds) {
        if (userId === senderId && socketId === senderSocketId) {
          continue;
        }

        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("message:channel", messageData);
          console.log(
            `📺 Message canal livré: ${message.senderId} → userId:${userId}`,
          );
        }
      }
    } catch (error) {
      console.error("❌ Erreur deliverChannelMessage:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT CONVERSATION
   */
  async deliverConversationEvent(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:event", {
            conversationId: message.conversationId,
            event: message.event,
            userId: message.userId,
            data: message.data,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`🏢 Événement conversation livré: ${message.event}`);
    } catch (error) {
      console.error("❌ Erreur deliverConversationEvent:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT CONVERSATION
   */
  async deliverConversationEventToParticipants(message) {
    try {
      const conversationId = String(message.conversationId);

      // ✅ LIVRER À TOUS LES PARTICIPANTS CONNECTÉS DE LA CONVERSATION AVEC STREAM ACTIF
      for (const [userId, socketIds] of this.userSockets.entries()) {
        const userConversations = this.userConversations.get(userId) || [];
        if (
          userConversations.includes(conversationId) &&
          this.isStreamActiveForUser(userId, "conversations")
        ) {
          await this.deliverConversationEvent(message, userId);
        }
      }

      console.log(`🏢 Événement conversation livré: ${conversationId}`);
    } catch (error) {
      console.error("❌ Erreur livraison événement conversation:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT CONVERSATION CRÉÉE
   */
  async deliverConversationCreatedEvent(message) {
    try {
      const conversationId = String(message.conversationId);
      const excludeSocketId = message.senderSocketId || "";

      // ✅ RÉCUPÉRER TOUS LES PARTICIPANTS DE LA CONVERSATION
      const allParticipants = JSON.parse(message.participants) || [];

      console.log(
        `🆕 Livraison événement conversation créée à ${allParticipants.length} participant(s)`,
      );

      // ✅ DEBUG: Afficher les utilisateurs connectés
      const connectedUsers = Array.from(this.userSockets.keys());
      console.log(
        `🔍 DEBUG: Utilisateurs connectés en ce moment: [${connectedUsers.join(", ")}]`,
      );

      for (const participantId of allParticipants) {
        const userId = String(participantId);
        const isConnected = this.userSockets.has(userId);
        const isStreamActive = this.isStreamActiveForUser(
          userId,
          "conversationCreated",
        );

        console.log(
          `🔍 DEBUG: ${userId} - connecté: ${isConnected}, stream actif: ${isStreamActive}`,
        );

        if (isConnected && isStreamActive) {
          // ✅ UTILISATEUR CONNECTÉ - LIVRAISON IMMÉDIATE
          await this.deliverConversationCreated(
            message,
            userId,
            excludeSocketId,
          );
        } else {
          // ✅ UTILISATEUR DÉCONNECTÉ OU STREAM PAS ACTIF - STOCKAGE EN ATTENTE
          console.log(
            `⏳ Participant ${userId} ${!isConnected ? "déconnecté" : "stream pas actif"}, événement conversation créée en attente`,
          );
          await this.addToPendingQueue(userId, message, "conversationCreated");
        }
      }

      console.log(
        `🆕 Événement conversation créée distribué: ${conversationId}`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison événement conversation créée:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT CONVERSATION MISE À JOUR
   */
  async deliverConversationUpdatedEvent(message) {
    try {
      const conversationId = String(message.conversationId);
      const excludeSocketId = message.senderSocketId || "";

      // ✅ RÉCUPÉRER TOUS LES PARTICIPANTS DE LA CONVERSATION
      const allParticipants =
        await this.getAllConversationParticipants(conversationId);

      console.log(
        `📝 Livraison événement conversation mise à jour à ${allParticipants.length} participant(s)`,
      );

      for (const participantId of allParticipants) {
        const userId = String(participantId);

        if (
          this.userSockets.has(userId) &&
          this.isStreamActiveForUser(userId, "conversationUpdated")
        ) {
          // ✅ UTILISATEUR CONNECTÉ - LIVRAISON IMMÉDIATE
          await this.deliverConversationUpdated(
            message,
            userId,
            excludeSocketId,
          );
        } else {
          // ✅ UTILISATEUR DÉCONNECTÉ - STOCKAGE EN ATTENTE
          console.log(
            `⏳ Participant ${userId} déconnecté, événement conversation mise à jour en attente`,
          );
          await this.addToPendingQueue(userId, message, "conversationUpdated");
        }
      }

      console.log(
        `📝 Événement conversation mise à jour distribué: ${conversationId}`,
      );
    } catch (error) {
      console.error(
        "❌ Erreur livraison événement conversation mise à jour:",
        error,
      );
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT PARTICIPANT AJOUTÉ
   */
  async deliverParticipantAddedEvent(message) {
    try {
      const conversationId = String(message.conversationId);
      const newParticipantId = String(message.participantId);
      const excludeSocketId = message.senderSocketId || "";

      // ✅ RÉCUPÉRER LES PARTICIPANTS DEPUIS LE MESSAGE OU DEPUIS LE CACHE
      let allParticipants = [];

      // ✅ PRIORITÉ 1 : Utiliser la liste de participants du message si disponible
      if (message.participants) {
        try {
          const parsedParticipants =
            typeof message.participants === "string"
              ? JSON.parse(message.participants)
              : message.participants;
          allParticipants = parsedParticipants.map(String);
          console.log(
            `➕ Participants depuis le message: ${allParticipants.join(", ")}`,
          );
        } catch (parseErr) {
          console.warn("⚠️ Erreur parsing participants:", parseErr.message);
        }
      }

      // ✅ PRIORITÉ 2 : Fallback vers le cache mémoire
      if (allParticipants.length === 0) {
        allParticipants =
          await this.getAllConversationParticipants(conversationId);
      }

      // ✅ S'ASSURER QUE LE NOUVEAU PARTICIPANT EST INCLUS
      if (newParticipantId && !allParticipants.includes(newParticipantId)) {
        allParticipants.push(newParticipantId);
      }

      // ✅ METTRE À JOUR LE CACHE userConversations POUR LE NOUVEAU PARTICIPANT
      if (newParticipantId && this.userSockets.has(newParticipantId)) {
        const userConvs = this.userConversations.get(newParticipantId) || [];
        if (!userConvs.includes(conversationId)) {
          userConvs.push(conversationId);
          this.userConversations.set(newParticipantId, userConvs);
          console.log(
            `✅ userConversations mis à jour pour ${newParticipantId}: +${conversationId}`,
          );
        }
      }

      console.log(
        `➕ Livraison événement participant ajouté à ${allParticipants.length} participant(s)`,
      );

      for (const participantId of allParticipants) {
        const userId = String(participantId);

        if (
          this.userSockets.has(userId) &&
          this.isStreamActiveForUser(userId, "participantAdded")
        ) {
          // ✅ UTILISATEUR CONNECTÉ - LIVRAISON IMMÉDIATE
          await this.deliverParticipantAdded(message, userId, excludeSocketId);
        } else {
          // ✅ UTILISATEUR DÉCONNECTÉ - STOCKAGE EN ATTENTE
          console.log(
            `⏳ Participant ${userId} déconnecté, événement participant ajouté en attente`,
          );
          await this.addToPendingQueue(userId, message, "participantAdded");
        }
      }

      console.log(
        `➕ Événement participant ajouté distribué: ${conversationId}`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison événement participant ajouté:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT PARTICIPANT RETIRÉ
   */
  async deliverParticipantRemovedEvent(message) {
    try {
      const conversationId = String(message.conversationId);
      const removedParticipantId = String(message.participantId);
      const excludeSocketId = message.senderSocketId || "";

      // ✅ RÉCUPÉRER LES PARTICIPANTS DEPUIS LE MESSAGE OU DEPUIS LE CACHE
      let allParticipants = [];

      // ✅ PRIORITÉ 1 : Utiliser la liste de participants du message si disponible
      if (message.participants) {
        try {
          const parsedParticipants =
            typeof message.participants === "string"
              ? JSON.parse(message.participants)
              : message.participants;
          allParticipants = parsedParticipants.map(String);
          console.log(
            `➖ Participants depuis le message: ${allParticipants.join(", ")}`,
          );
        } catch (parseErr) {
          console.warn("⚠️ Erreur parsing participants:", parseErr.message);
        }
      }

      // ✅ PRIORITÉ 2 : Fallback vers le cache mémoire
      if (allParticipants.length === 0) {
        allParticipants =
          await this.getAllConversationParticipants(conversationId);
      }

      // ✅ S'ASSURER QUE LE PARTICIPANT RETIRÉ EST AUSSI NOTIFIÉ
      if (
        removedParticipantId &&
        !allParticipants.includes(removedParticipantId)
      ) {
        allParticipants.push(removedParticipantId);
      }

      // ✅ NETTOYER LE CACHE userConversations POUR LE PARTICIPANT RETIRÉ
      if (
        removedParticipantId &&
        this.userConversations.has(removedParticipantId)
      ) {
        const userConvs =
          this.userConversations.get(removedParticipantId) || [];
        const idx = userConvs.indexOf(conversationId);
        if (idx !== -1) {
          userConvs.splice(idx, 1);
          this.userConversations.set(removedParticipantId, userConvs);
          console.log(
            `✅ userConversations nettoyé pour ${removedParticipantId}: -${conversationId}`,
          );
        }
      }
      console.log(
        `➖ Livraison événement participant retiré à ${allParticipants.length} participant(s)`,
      );

      for (const participantId of allParticipants) {
        const userId = String(participantId);

        if (
          this.userSockets.has(userId) &&
          this.isStreamActiveForUser(userId, "participantRemoved")
        ) {
          // ✅ UTILISATEUR CONNECTÉ - LIVRAISON IMMÉDIATE
          await this.deliverParticipantRemoved(
            message,
            userId,
            excludeSocketId,
          );
        } else {
          // ✅ UTILISATEUR DÉCONNECTÉ - STOCKAGE EN ATTENTE
          console.log(
            `⏳ Participant ${userId} déconnecté, événement participant retiré en attente`,
          );
          await this.addToPendingQueue(userId, message, "participantRemoved");
        }
      }

      console.log(
        `➖ Événement participant retiré distribué: ${conversationId}`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison événement participant retiré:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT CONVERSATION SUPPRIMÉE
   */
  async deliverConversationDeletedEvent(message) {
    try {
      const conversationId = String(message.conversationId);

      // ✅ RÉCUPÉRER TOUS LES PARTICIPANTS DE LA CONVERSATION
      const allParticipants =
        await this.getAllConversationParticipants(conversationId);

      console.log(
        `🗑️ Livraison événement conversation supprimée à ${allParticipants.length} participant(s)`,
      );

      for (const participantId of allParticipants) {
        const userId = String(participantId);

        if (
          this.userSockets.has(userId) &&
          this.isStreamActiveForUser(userId, "conversationDeleted")
        ) {
          // ✅ UTILISATEUR CONNECTÉ - LIVRAISON IMMÉDIATE
          await this.deliverConversationDeleted(message, userId);
        } else {
          // ✅ UTILISATEUR DÉCONNECTÉ - STOCKAGE EN ATTENTE
          console.log(
            `⏳ Participant ${userId} déconnecté, événement conversation supprimée en attente`,
          );
          await this.addToPendingQueue(userId, message, "conversationDeleted");
        }
      }

      console.log(
        `🗑️ Événement conversation supprimée distribué: ${conversationId}`,
      );
    } catch (error) {
      console.error(
        "❌ Erreur livraison événement conversation supprimée:",
        error,
      );
    }
  }

  /**
   * ✅ LIVRER ÉVÉNEMENT CONVERSATION CRÉÉE À UN UTILISATEUR
   */
  async deliverConversationCreated(message, userId, excludeSocketId = "") {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
        if (excludeSocketId && socketId === excludeSocketId) {
          console.log(
            `⏭️ conversation:created - socket émetteur exclu: ${socketId}`,
          );
          continue;
        }
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:created", {
            conversationId: message.conversationId,
            conversation: message.conversation,
            name: message.name,
            type: message.type,
            createdBy: message.createdBy,
            participants: message.participants,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`🆕 Conversation créée livrée à ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverConversationCreated:", error);
    }
  }

  /**
   * ✅ LIVRER ÉVÉNEMENT CONVERSATION MISE À JOUR À UN UTILISATEUR
   */
  async deliverConversationUpdated(message, userId, excludeSocketId = "") {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
        if (excludeSocketId && socketId === excludeSocketId) {
          console.log(
            `⏭️ conversation:updated - socket émetteur exclu: ${socketId}`,
          );
          continue;
        }
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:updated", {
            conversationId: message.conversationId,
            name: message.name,
            updatedBy: message.updatedBy,
            changes: message.changes,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`📝 Conversation mise à jour livrée à ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverConversationUpdated:", error);
    }
  }

  /**
   * ✅ LIVRER ÉVÉNEMENT PARTICIPANT AJOUTÉ À UN UTILISATEUR
   */
  async deliverParticipantAdded(message, userId, excludeSocketId = "") {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      // Préparer la conversation complète si fournie dans le message
      let conversationPayload = null;
      if (message.conversation) {
        try {
          conversationPayload =
            typeof message.conversation === "string"
              ? JSON.parse(message.conversation)
              : message.conversation;
        } catch (err) {
          console.warn(
            "⚠️ Erreur parsing conversation dans participantAdded:",
            err.message,
          );
          conversationPayload = null;
        }
      }

      for (const socketId of socketIds) {
        // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
        if (excludeSocketId && socketId === excludeSocketId) {
          console.log(
            `⏭️ conversation:participant:added - socket émetteur exclu: ${socketId}`,
          );
          continue;
        }
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:participant:added", {
            conversationId: message.conversationId,
            participantId: message.participantId,
            participantName: message.participantName,
            addedBy: message.addedBy,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`➕ Participant ajouté livré à ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverParticipantAdded:", error);
    }
  }

  /**
   * ✅ LIVRER ÉVÉNEMENT PARTICIPANT RETIRÉ À UN UTILISATEUR
   */
  async deliverParticipantRemoved(message, userId, excludeSocketId = "") {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        // ✅ EXCLURE LE SOCKET SPÉCIFIQUE DE L'ÉMETTEUR (multi-device)
        if (excludeSocketId && socketId === excludeSocketId) {
          console.log(
            `⏭️ conversation:participant:removed - socket émetteur exclu: ${socketId}`,
          );
          continue;
        }
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:participant:removed", {
            conversationId: message.conversationId,
            participantId: message.participantId,
            participantName: message.participantName,
            removedBy: message.removedBy,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`➖ Participant retiré livré à ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverParticipantRemoved:", error);
    }
  }

  /**
   * ✅ LIVRER ÉVÉNEMENT CONVERSATION SUPPRIMÉE À UN UTILISATEUR
   */
  async deliverConversationDeleted(message, userId) {
    try {
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("conversation:deleted", {
            conversationId: message.conversationId,
            deletedBy: message.deletedBy,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`🗑️ Conversation supprimée livrée à ${userId}`);
    } catch (error) {
      console.error("❌ Erreur deliverConversationDeleted:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT FICHIER
   */
  async deliverFileEvent(message) {
    try {
      const userId = String(message.userId);
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("file:event", {
            fileId: message.fileId,
            event: message.event,
            fileName: message.fileName,
            fileSize: message.fileSize,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`📁 Événement fichier livré: ${message.event}`);
    } catch (error) {
      console.error("❌ Erreur livraison événement fichier:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT RÉACTION
   * Filtré par participants de la conversation + exclusion senderSocketId
   */

  async deliverReactionEvent(message) {
    try {
      const messageId = String(message.messageId);
      const conversationId = String(message.conversationId || "");
      const senderSocketId = message.senderSocketId || null;
      const senderId = String(message.userId || "");
      let deliveredCount = 0;

      // ✅ LIVRER UNIQUEMENT AUX PARTICIPANTS DE LA CONVERSATION
      for (const [userId, socketIds] of this.userSockets.entries()) {
        if (!socketIds || socketIds.length === 0) continue;

        // ✅ FILTRER : ne livrer qu'aux utilisateurs de cette conversation
        if (conversationId) {
          const userConversations = this.userConversations.get(userId) || [];
          if (!userConversations.includes(conversationId)) continue;
        }
        for (const socketId of socketIds) {
          // ✅ EXCLURE le socket émetteur (ses autres appareils reçoivent)
          if (userId === senderId && socketId === senderSocketId) {
            continue;
          }

          const socket = this.io.sockets.sockets.get(socketId);
          if (socket) {
            socket.emit("message:reaction", {
              messageId: message.messageId,
              conversationId,
              userId: message.userId,
              reaction: message.reaction,
              action: message.action, // "add" ou "remove"
              timestamp: message.timestamp,
            });
            deliveredCount++;
          }
        }
      }

      console.log(
        `😀 Réaction livrée pour message: ${messageId} → ${deliveredCount} socket(s) (conv: ${conversationId})`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison réaction:", error);
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT RÉPONSE
   * Filtré par participants de la conversation + exclusion senderSocketId
   */
  async deliverReplyEvent(message) {
    try {
      const messageId = String(message.messageId);
      const conversationId = String(message.conversationId || "");
      const senderSocketId = message.senderSocketId || null;
      const senderId = String(message.userId || "");
      let deliveredCount = 0;

      // ✅ LIVRER UNIQUEMENT AUX PARTICIPANTS DE LA CONVERSATION
      for (const [userId, socketIds] of this.userSockets.entries()) {
        if (!socketIds || socketIds.length === 0) continue;

        // ✅ FILTRER : ne livrer qu'aux utilisateurs de cette conversation
        if (conversationId) {
          const userConversations = this.userConversations.get(userId) || [];
          if (!userConversations.includes(conversationId)) continue;
        }

        for (const socketId of socketIds) {
          // ✅ EXCLURE le socket émetteur (ses autres appareils reçoivent)
          if (userId === senderId && socketId === senderSocketId) {
            continue;
          }

          const socket = this.io.sockets.sockets.get(socketId);
          if (socket) {
            socket.emit("message:reply", {
              messageId: message.messageId,
              replyId: message.replyId,
              conversationId,
              userId: message.userId,
              content: message.content,
              timestamp: message.timestamp,
            });
            deliveredCount++;
          }
        }
      }

      console.log(
        `💬 Réponse livrée pour message: ${messageId} → ${deliveredCount} socket(s) (conv: ${conversationId})`,
      );
    } catch (error) {
      console.error("❌ Erreur livraison réponse:", error);
    }
  }

  /**
   * ✅ ENREGISTRER UN SOCKET UTILISATEUR AVEC ABONNEMENT PROGRESSIF
   */
  registerUserSocket(userId, socket, conversationIds = []) {
    try {
      const userIdStr = String(userId);

      if (!this.userSockets.has(userIdStr)) {
        this.userSockets.set(userIdStr, []);
      }

      this.userSockets.get(userIdStr).push(socket.id);
      this.userConversations.set(userIdStr, conversationIds);
      this.activeUserStreams.set(userIdStr, new Set()); // Initialiser les streams actifs

      console.log(
        `✅ Socket enregistré: ${userIdStr} (${
          this.userSockets.get(userIdStr).length
        } socket(s))`,
      );

      // ✅ DÉMARRER L'ABONNEMENT PROGRESSIF
      this.subscribeUserToStreams(userIdStr, socket.id);

      return true;
    } catch (error) {
      console.error("❌ Erreur registerUserSocket:", error);
      return false;
    }
  }

  /**
   * ✅ ABONNEMENT PROGRESSIF AUX STREAMS (LAZY SUBSCRIPTION)
   */
  subscribeUserToStreams(userId, socketId) {
    try {
      const userIdStr = String(userId);
      const phases = this.SUBSCRIPTION_PHASES;

      console.log(`🔄 Démarrage abonnement progressif pour ${userIdStr}`);

      // Phase 1 : Immédiat (streams critiques)
      this.activateUserStreams(userIdStr, phases.PHASE_1);
      console.log(
        `📡 Phase 1 activée pour ${userIdStr}: ${phases.PHASE_1.join(", ")}`,
      );

      // Phase 2 : Délai de 100ms (group, channel)
      setTimeout(() => {
        if (this.userSockets.has(userIdStr)) {
          this.activateUserStreams(userIdStr, phases.PHASE_2);
          console.log(
            `📡 Phase 2 activée pour ${userIdStr}: ${phases.PHASE_2.join(", ")}`,
          );
          this.deliverPendingEventsForStreamTypes(userIdStr, phases.PHASE_2);
        }
      }, 100);

      // Phase 3 : Délai de 300ms (conversations, notifications)
      setTimeout(() => {
        if (this.userSockets.has(userIdStr)) {
          this.activateUserStreams(userIdStr, phases.PHASE_3);
          console.log(
            `📡 Phase 3 activée pour ${userIdStr}: ${phases.PHASE_3.join(", ")}`,
          );
          this.deliverPendingEventsForStreamTypes(userIdStr, phases.PHASE_3);
        }
      }, 300);

      // Phase 4 : Délai de 800ms (files, reactions, replies)
      setTimeout(() => {
        if (this.userSockets.has(userIdStr)) {
          this.activateUserStreams(userIdStr, phases.PHASE_4);
          console.log(
            `📡 Phase 4 activée pour ${userIdStr}: ${phases.PHASE_4.join(", ")}`,
          );
          this.deliverPendingEventsForStreamTypes(userIdStr, phases.PHASE_4);
        }
      }, 800);

      // Phase 5 : Background (1.5 secondes - analytics)
      setTimeout(() => {
        if (this.userSockets.has(userIdStr)) {
          this.activateUserStreams(userIdStr, phases.PHASE_5);
          console.log(
            `📡 Phase 5 activée pour ${userIdStr}: ${phases.PHASE_5.join(", ")}`,
          );
          this.deliverPendingEventsForStreamTypes(userIdStr, phases.PHASE_5);
        }
      }, 1500);
    } catch (error) {
      console.error(`❌ Erreur abonnement progressif pour ${userId}:`, error);
    }
  }

  /**
   * ✅ ACTIVER LES STREAMS POUR UN UTILISATEUR
   */
  activateUserStreams(userId, streamTypes) {
    try {
      const userIdStr = String(userId);
      const activeStreams = this.activeUserStreams.get(userIdStr) || new Set();

      for (const streamType of streamTypes) {
        activeStreams.add(streamType);
      }

      this.activeUserStreams.set(userIdStr, activeStreams);

      console.log(
        `✅ Streams activés pour ${userIdStr}: ${streamTypes.join(", ")}`,
      );
    } catch (error) {
      console.error(`❌ Erreur activation streams pour ${userId}:`, error);
    }
  }

  /**
   * ✅ VÉRIFIER SI UN STREAM EST ACTIF POUR UN UTILISATEUR
   */
  isStreamActiveForUser(userId, streamType) {
    try {
      const userIdStr = String(userId);
      const activeStreams = this.activeUserStreams.get(userIdStr);
      return activeStreams ? activeStreams.has(streamType) : false;
    } catch (error) {
      console.error(`❌ Erreur vérification stream actif:`, error);
      return false;
    }
  }

  /**
   * ✅ DÉSENREGISTRER UN SOCKET
   */
  unregisterUserSocket(userId, socketId) {
    try {
      const userIdStr = String(userId);

      if (!this.userSockets.has(userIdStr)) {
        return true;
      }

      const sockets = this.userSockets.get(userIdStr);
      const index = sockets.indexOf(socketId);

      if (index > -1) {
        sockets.splice(index, 1);
      }

      if (sockets.length === 0) {
        this.userSockets.delete(userIdStr);
        this.userConversations.delete(userIdStr);
        this.activeUserStreams.delete(userIdStr); // Nettoyer les streams actifs
      }

      return true;
    } catch (error) {
      console.error("❌ Erreur unregisterUserSocket:", error);
      return false;
    }
  }

  /**
   * ✅ AJOUTER UNE CONVERSATION À LA LISTE D'UN UTILISATEUR CONNECTÉ
   * Appelé quand l'utilisateur rejoint une conversation après la connexion initiale
   */
  addUserConversation(userId, conversationId) {
    const userIdStr = String(userId);
    const convIdStr = String(conversationId);
    const conversations = this.userConversations.get(userIdStr) || [];
    if (!conversations.includes(convIdStr)) {
      conversations.push(convIdStr);
      this.userConversations.set(userIdStr, conversations);
      console.log(
        `✅ [MDS] Conversation ${convIdStr} ajoutée pour ${userIdStr} (total: ${conversations.length})`,
      );
    }
  }

  /**
   * ✅ RETIRER UNE CONVERSATION DE LA LISTE D'UN UTILISATEUR CONNECTÉ
   * Appelé quand l'utilisateur quitte une conversation
   */
  removeUserConversation(userId, conversationId) {
    const userIdStr = String(userId);
    const convIdStr = String(conversationId);
    const conversations = this.userConversations.get(userIdStr) || [];
    const index = conversations.indexOf(convIdStr);
    if (index > -1) {
      conversations.splice(index, 1);
      this.userConversations.set(userIdStr, conversations);
      console.log(
        `✅ [MDS] Conversation ${convIdStr} retirée pour ${userIdStr} (total: ${conversations.length})`,
      );
    }
  }

  /**
   * ✅ RÉCUPÉRER TOUS LES PARTICIPANTS D'UNE CONVERSATION
   */
  async getAllConversationParticipants(conversationId) {
    try {
      // ✅ ESSAYER DEPUIS LE CACHE MÉMOIRE D'ABORD
      const connectedUsers = Array.from(this.userSockets.keys());
      const conversationParticipants = [];

      for (const userId of connectedUsers) {
        const userConversations = this.userConversations.get(userId) || [];
        if (userConversations.includes(conversationId)) {
          conversationParticipants.push(userId);
        }
      }

      // ✅ SI ON A DES PARTICIPANTS EN CACHE, LES RETOURNER
      if (conversationParticipants.length > 0) {
        return conversationParticipants;
      }

      // ✅ SINON, RETOURNER UNE LISTE VIDE (le caller peut avoir un fallback)
      console.log(
        `ℹ️ Aucun participant connecté trouvé en cache pour la conversation ${conversationId}`,
      );
      return [];
    } catch (error) {
      console.error(
        `❌ Erreur récupération participants conversation ${conversationId}:`,
        error,
      );
      return [];
    }
  }

  /**
   * ✅ LIVRER LES ÉVÉNEMENTS EN ATTENTE À LA CONNEXION
   */
  async deliverPendingMessagesOnConnect(userId, socket) {
    try {
      const userIdStr = String(userId);

      console.log(`📥 Livraison événements en attente pour ${userIdStr}...`);

      const activeStreams = Array.from(
        this.activeUserStreams.get(userIdStr) || [],
      );

      const deliveredCount = await this.deliverPendingEventsForStreamTypes(
        userIdStr,
        activeStreams,
      );

      console.log(
        `✅ ${deliveredCount} événement(s) livré(s) à ${userIdStr} à la connexion`,
      );

      return deliveredCount;
    } catch (error) {
      console.error("❌ Erreur livraison événements en attente:", error);
      return 0;
    }
  }

  /**
   * ✅ LIVRER LES ÉVÉNEMENTS EN ATTENTE POUR DES STREAMS DONNÉS
   */
  async deliverPendingEventsForStreamTypes(userId, streamTypes = []) {
    try {
      const userIdStr = String(userId);
      const streamTypeSet = new Set(streamTypes.map((s) => String(s)));

      // ✅ MAPPING STREAM → EVENT TYPE EN ATTENTE
      const streamToEventType = {
        private: "message",
        group: "message",
        channel: "message",
        notifications: "notifications",
        files: "files",
        conversationCreated: "conversationCreated",
        conversationUpdated: "conversationUpdated",
        participantAdded: "participantAdded",
        participantRemoved: "participantRemoved",
        conversationDeleted: "conversationDeleted",
        // ✅ AJOUTER LES STATUTS (NOUVELLES CLÉS EN ATTENTE DEPUIS ResilientMessageService)
        statusDelivered: "statusDelivered",
        statusRead: "statusRead",
        statusEdited: "statusEdited",
        statusDeleted: "statusDeleted",
      };

      const eventTypes = new Set();
      for (const streamType of streamTypeSet) {
        const eventType = streamToEventType[streamType];
        if (eventType) {
          eventTypes.add(eventType);
        }
      }

      let deliveredCount = 0;

      for (const eventType of eventTypes) {
        // ✅ GÉRER LES DEUX FORMATS D'ATTENTE
        // Format ancien: chat:stream:pending:message:userId (Redis LIST)
        // Format nouveau: chat:stream:pending:messages:userId:streamType (Redis STREAM)
        const oldPendingKey = `chat:stream:pending:${eventType}:${userIdStr}`;
        const newPendingKey = `chat:stream:pending:messages:${userIdStr}:${eventType}`;

        // ✅ TRAITER LES ANCIENNES CLÉS (Redis LIST)
        try {
          const pendingEvents = await this.redis.lRange(oldPendingKey, 0, -1);

          console.log(
            `📨 ${pendingEvents.length} événement(s) ${eventType} en attente trouvé(s) pour ${userIdStr}`,
          );

          for (const eventJson of pendingEvents) {
            try {
              const event = JSON.parse(eventJson);

              if (eventType === "message") {
                const eventStreamType =
                  event.streamType || (event.receiverId ? "private" : null);

                if (!eventStreamType || !streamTypeSet.has(eventStreamType)) {
                  continue;
                }
              }

              // ✅ TRAITER SELON LE TYPE D'ÉVÉNEMENT
              await this.deliverPendingEvent(event, userIdStr);

              // ✅ SUPPRIMER DE LA LISTE D'ATTENTE
              await this.redis.lRem(oldPendingKey, 1, eventJson);

              deliveredCount++;
              console.log(
                `✅ Événement ${eventType} en attente livré et supprimé`,
              );
            } catch (error) {
              console.error(
                `❌ Erreur traitement événement ${eventType} en attente:`,
                error.message,
              );
            }
          }
        } catch (pendingError) {
          console.warn(
            `⚠️ Erreur récupération événements ${eventType} en attente (old format):`,
            pendingError.message,
          );
        }

        // ✅ TRAITER LES NOUVELLES CLÉS (Redis STREAM - depuis ResilientMessageService)
        try {
          const pendingStreamId = await this.redis.xRange(
            newPendingKey,
            "-",
            "+",
            "COUNT",
            100,
          );

          console.log(
            `📨 ${pendingStreamId.length} événement(s) ${eventType} en attente trouvé(s) pour ${userIdStr} (new format)`,
          );

          for (const entry of pendingStreamId) {
            try {
              // ✅ GÉRER LE FORMAT node-redis v4: entry est un objet {id, message: {...}}
              const id = entry.id;
              const fields = entry.message || entry;

              // ✅ RÉCUPÉRER LES DONNÉES DE L'ÉVÉNEMENT
              let event;
              if (fields.event) {
                // Si "event" est une clé stockée dans le stream
                event =
                  typeof fields.event === "string"
                    ? JSON.parse(fields.event)
                    : fields.event;
              } else {
                // Sinon, utiliser tous les champs comme événement
                event = fields;
              }

              console.log(`📤 Livraison événement ${eventType} en attente:`, {
                isBulk: event.isBulk,
                messageCount: event.messageCount,
                conversationId: event.conversationId,
              });

              // ✅ PUBLIER DIRECTEMENT DANS LE STREAM APPROPRIÉ
              if (
                eventType === "statusDelivered" ||
                eventType === "statusRead" ||
                eventType === "statusEdited" ||
                eventType === "statusDeleted"
              ) {
                // ✅ LIVRER LE STATUT DE MESSAGE
                await this.deliverMessageStatus(event, userIdStr);
              } else {
                // ✅ AUTRES TYPES D'ÉVÉNEMENTS
                await this.deliverPendingEvent(event, userIdStr);
              }

              // ✅ SUPPRIMER DE LA STREAM D'ATTENTE
              await this.redis.xDel(newPendingKey, id);

              deliveredCount++;
              console.log(
                `✅ Événement ${eventType} (new format) en attente livré et supprimé`,
              );
            } catch (error) {
              console.error(
                `❌ Erreur traitement événement ${eventType} (new format) en attente:`,
                error.message,
              );
            }
          }
        } catch (pendingError) {
          console.warn(
            `⚠️ Erreur récupération événements ${eventType} en attente (new format):`,
            pendingError.message,
          );
        }
      }

      return deliveredCount;
    } catch (error) {
      console.error(
        "❌ Erreur livraison événements en attente par stream:",
        error,
      );
      return 0;
    }
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT EN ATTENTE À LA CONNEXION
   */
  async deliverPendingEvent(event, userId) {
    try {
      switch (event.eventType) {
        case "message":
          // ✅ LIVRER MESSAGE EN ATTENTE SELON LE STREAM
          if (event.streamType) {
            await this.routeMessageByStreamType(
              event.streamType,
              event,
              userId,
            );
          } else {
            await this.deliverPrivateMessage(event, userId);
          }
          break;

        case "statusDelivered":
        case "statusRead":
        case "statusEdited":
        case "statusDeleted":
          // ✅ LIVRER STATUT DE MESSAGE EN ATTENTE
          await this.deliverMessageStatus(event, userId);
          break;

        case "conversationCreated":
          // ✅ LIVRER ÉVÉNEMENT CONVERSATION CRÉÉE EN ATTENTE
          await this.deliverConversationCreated(event, userId);
          break;

        case "conversationUpdated":
          // ✅ LIVRER ÉVÉNEMENT CONVERSATION MISE À JOUR EN ATTENTE
          await this.deliverConversationUpdated(event, userId);
          break;

        case "participantAdded":
          // ✅ LIVRER ÉVÉNEMENT PARTICIPANT AJOUTÉ EN ATTENTE
          await this.deliverParticipantAdded(event, userId);
          break;

        case "participantRemoved":
          // ✅ LIVRER ÉVÉNEMENT PARTICIPANT RETIRÉ EN ATTENTE
          await this.deliverParticipantRemoved(event, userId);
          break;

        case "conversationDeleted":
          // ✅ LIVRER ÉVÉNEMENT CONVERSATION SUPPRIMÉE EN ATTENTE
          await this.deliverConversationDeleted(event, userId);
          break;

        case "files":
          // ✅ LIVRER ÉVÉNEMENT FICHIER EN ATTENTE
          await this.deliverFileEvent(event);
          break;

        case "notifications":
          // ✅ LIVRER NOTIFICATION EN ATTENTE
          await this.deliverNotification(event, userId);
          break;

        default:
          console.warn(
            `⚠️ Type d'événement en attente inconnu: ${event.eventType}`,
          );
      }
    } catch (error) {
      console.error(
        `❌ Erreur livraison événement ${event.eventType} en attente:`,
        error,
      );
    }
  }

  /**
   * ✅ AJOUTER UN ÉVÉNEMENT EN ATTENTE (MESSAGES OU ÉVÉNEMENTS CONVERSATION)
   */
  async addToPendingQueue(
    userId,
    eventData,
    eventType = "message",
    streamType = null,
  ) {
    try {
      const userIdStr = String(userId);
      const pendingKey = `chat:stream:pending:${eventType}:${userIdStr}`;

      // ✅ ADAPTER LA STRUCTURE SELON LE TYPE D'ÉVÉNEMENT
      let eventJson;
      switch (eventType) {
        case "message":
          // ✅ STRUCTURE POUR LES MESSAGES (PRIVÉ/GROUPE/CANAL)
          eventJson = JSON.stringify({
            eventType: "message",
            streamType:
              streamType ||
              eventData.streamType ||
              (eventData.receiverId ? "private" : null),
            messageId: eventData.messageId,
            conversationId: eventData.conversationId,
            senderId: eventData.senderId,
            receiverId: eventData.receiverId,
            content: eventData.content,
            type: eventData.type,
            status: eventData.status || "SENT",
            timestamp: eventData.timestamp,
            metadata: eventData.metadata,
            participants: eventData.participants,
            senderName: eventData.senderName,
            subType: eventData.subType,
          });
          break;

        case "statusDelivered":
        case "statusRead":
        case "statusEdited":
        case "statusDeleted":
          // ✅ STRUCTURE POUR LES STATUTS DE MESSAGE
          eventJson = JSON.stringify({
            eventType,
            messageId: eventData.messageId,
            conversationId: eventData.conversationId,
            userId: eventData.userId,
            status: eventData.status,
            timestamp: eventData.timestamp,
            isBulk: eventData.isBulk,
            participants: eventData.participants,
            messageCount: eventData.messageCount,
          });
          break;

        case "conversationCreated":
          // ✅ STRUCTURE POUR ÉVÉNEMENT CONVERSATION CRÉÉE
          eventJson = JSON.stringify({
            eventType: "conversationCreated",
            conversationId: eventData.conversationId,
            name: eventData.name,
            type: eventData.type,
            createdBy: eventData.createdBy,
            participants: eventData.participants,
            timestamp: eventData.timestamp,
          });
          break;

        case "conversationUpdated":
          // ✅ STRUCTURE POUR ÉVÉNEMENT CONVERSATION MISE À JOUR
          eventJson = JSON.stringify({
            eventType: "conversationUpdated",
            conversationId: eventData.conversationId,
            name: eventData.name,
            updatedBy: eventData.updatedBy,
            changes: eventData.changes,
            timestamp: eventData.timestamp,
          });
          break;

        case "participantAdded":
          // ✅ STRUCTURE POUR ÉVÉNEMENT PARTICIPANT AJOUTÉ
          eventJson = JSON.stringify({
            eventType: "participantAdded",
            conversationId: eventData.conversationId,
            participantId: eventData.participantId,
            participantName: eventData.participantName,
            addedBy: eventData.addedBy,
            timestamp: eventData.timestamp,
          });
          break;

        case "participantRemoved":
          // ✅ STRUCTURE POUR ÉVÉNEMENT PARTICIPANT RETIRÉ
          eventJson = JSON.stringify({
            eventType: "participantRemoved",
            conversationId: eventData.conversationId,
            participantId: eventData.participantId,
            participantName: eventData.participantName,
            removedBy: eventData.removedBy,
            timestamp: eventData.timestamp,
          });
          break;

        case "conversationDeleted":
          // ✅ STRUCTURE POUR ÉVÉNEMENT CONVERSATION SUPPRIMÉE
          eventJson = JSON.stringify({
            eventType: "conversationDeleted",
            conversationId: eventData.conversationId,
            deletedBy: eventData.deletedBy,
            timestamp: eventData.timestamp,
          });
          break;

        case "files":
          // ✅ STRUCTURE POUR ÉVÉNEMENT FICHIER
          eventJson = JSON.stringify({
            eventType: "files",
            fileId: eventData.fileId,
            event: eventData.event,
            fileName: eventData.fileName,
            fileSize: eventData.fileSize,
            userId: eventData.userId,
            timestamp: eventData.timestamp,
          });
          break;

        case "notifications":
          // ✅ STRUCTURE POUR ÉVÉNEMENT NOTIFICATION
          eventJson = JSON.stringify({
            eventType: "notifications",
            userId: eventData.userId,
            title: eventData.title,
            message: eventData.message,
            level: eventData.level,
            payload: eventData.payload,
            timestamp: eventData.timestamp,
          });
          break;

        default:
          throw new Error(`Type d'événement non supporté: ${eventType}`);
      }

      await this.redis.lPush(pendingKey, eventJson);
      await this.redis.expire(pendingKey, 86400); // 24h TTL

      console.log(
        `📝 Événement ${eventType} ajouté en attente pour ${userIdStr}`,
      );
    } catch (error) {
      console.error("❌ Erreur addToPendingQueue:", error);
    }
  }

  /**
   * ✅ ARRÊTER TOUS LES CONSUMERS
   */
  async stopAllConsumers() {
    this.isRunning = false;

    for (const [streamKey, consumer] of this.streamConsumers.entries()) {
      if (consumer.interval) {
        clearInterval(consumer.interval);
      }

      if (consumer.redis) {
        try {
          await consumer.redis.quit();
        } catch (err) {
          console.warn(
            `⚠️ Erreur fermeture consumer ${streamKey}:`,
            err.message,
          );
        }
      }
    }

    console.log("✅ Tous les consumers arrêtés");
  }

  /**
   * ✅ STATISTIQUES
   */
  getStats() {
    return {
      isRunning: this.isRunning,
      streams: Array.from(this.streamConsumers.keys()),
      streamConsumers: Array.from(this.streamConsumers.values()).map((c) => ({
        streamType: c.streamType,
        streamKey: c.config.streamKey,
        priority: c.config.priority,
        interval: c.config.interval,
        isRunning: c.isRunning,
      })),
      connectedUsers: this.userSockets.size,
      totalSockets: Array.from(this.userSockets.values()).reduce(
        (sum, sockets) => sum + sockets.length,
        0,
      ),
      users: Array.from(this.userSockets.entries()).map(
        ([userId, sockets]) => ({
          userId,
          socketsCount: sockets.length,
          conversationsCount: (this.userConversations.get(userId) || []).length,
        }),
      ),
    };
  }

  /**
   * ✅ NETTOYER ET ARRÊTER
   */
  async cleanup() {
    try {
      await this.stopAllConsumers();
      this.userSockets.clear();
      this.userConversations.clear();
      console.log("✅ MessageDeliveryService nettoyé");
    } catch (error) {
      console.error("❌ Erreur nettoyage MessageDeliveryService:", error);
    }
  }

  /**
   * ✅ DIAGNOSTIC COMPLET DE LA LIVRAISON
   */
  async diagnoseDelivery(userId) {
    const userIdStr = String(userId);

    console.log(
      `🔍 ========== DIAGNOSTIC LIVRAISON POUR ${userIdStr} ==========`,
    );

    try {
      const diagnostics = {
        userId: userIdStr,
        timestamp: new Date().toISOString(),
        checks: {},
      };

      // ✅ CHECK 1 : Utilisateur enregistré dans userSockets?
      const isRegistered = this.userSockets.has(userIdStr);
      const socketIds = this.userSockets.get(userIdStr) || [];

      diagnostics.checks.userRegistration = {
        registered: isRegistered,
        socketCount: socketIds.length,
        socketIds: socketIds,
        status: isRegistered ? "✅ OK" : "❌ PAS ENREGISTRÉ",
      };

      console.log(`   ${diagnostics.checks.userRegistration.status}`);
      if (socketIds.length > 0) {
        console.log(`   Sockets: ${socketIds.join(", ")}`);
      }

      // ✅ CHECK 2 : Vérifier chaque stream Redis
      console.log("\n📊 État des streams Redis:");

      const streamChecks = {};

      for (const [streamType, config] of Object.entries(this.STREAM_CONFIGS)) {
        try {
          const streamKey = config.streamKey;
          const length = await this.redis.xLen(streamKey);

          // Récupérer les derniers messages du stream
          const recentMessages = await this.redis.xRevRange(
            streamKey,
            "+",
            "-",
            {
              COUNT: 5,
            },
          );

          const relevantMessages = recentMessages.filter((msg) => {
            const data = msg.message || msg;
            // Messages pour cet utilisateur ou dans ses conversations
            return (
              data.receiverId === userIdStr ||
              (this.userConversations.get(userIdStr) || []).includes(
                data.conversationId,
              )
            );
          });

          streamChecks[streamType] = {
            streamKey,
            totalMessages: length,
            relevantMessages: relevantMessages.length,
            priority: config.priority,
            status: relevantMessages.length > 0 ? "⚠️ EN ATTENTE" : "✅ VIDE",
          };

          console.log(
            `   ${streamChecks[streamType].status} ${streamType}: ${length} total, ${relevantMessages.length} pour ${userIdStr}`,
          );

          if (relevantMessages.length > 0) {
            relevantMessages.forEach((msg, i) => {
              const data = msg.message || msg;
              console.log(
                `      ${i + 1}. ID: ${msg.id} | receiver: ${
                  data.receiverId || "N/A"
                } | conv: ${data.conversationId}`,
              );
            });
          }
        } catch (streamErr) {
          console.log(`   ❌ ERREUR ${streamType}: ${streamErr.message}`);
          streamChecks[streamType] = { error: streamErr.message };
        }
      }

      diagnostics.checks.streams = streamChecks;

      // ✅ CHECK 3 : Messages en attente (Redis List)
      console.log("\n📨 Messages en attente (Redis List):");

      const pendingKey = `${this.pendingMessagesPrefix}${userIdStr}`;
      try {
        const pendingMessages = await this.redis.lRange(pendingKey, 0, -1);

        diagnostics.checks.pendingQueue = {
          count: pendingMessages.length,
          status: pendingMessages.length > 0 ? "⚠️ EN ATTENTE" : "✅ VIDE",
        };

        console.log(
          `   ${diagnostics.checks.pendingQueue.status}: ${pendingMessages.length} message(s)`,
        );

        if (pendingMessages.length > 0) {
          pendingMessages.slice(0, 3).forEach((msgJson, i) => {
            try {
              const msg = JSON.parse(msgJson);
              console.log(
                `      ${i + 1}. De: ${msg.senderId} | Conv: ${
                  msg.conversationId
                }`,
              );
            } catch (e) {
              console.log(`      ${i + 1}. [JSON invalide]`);
            }
          });
        }
      } catch (pendingErr) {
        console.log(`   ❌ ERREUR: ${pendingErr.message}`);
        diagnostics.checks.pendingQueue = { error: pendingErr.message };
      }

      // ✅ CHECK 4 : Conversations de l'utilisateur
      console.log("\n🏢 Conversations associées:");

      const conversations = this.userConversations.get(userIdStr) || [];
      diagnostics.checks.conversations = {
        count: conversations.length,
        ids: conversations,
        status: conversations.length > 0 ? "✅ OK" : "⚠️ AUCUNE",
      };

      console.log(
        `   ${diagnostics.checks.conversations.status}: ${conversations.length} conversation(s)`,
      );

      // ✅ CHECK 5 : Consumer groups
      console.log("\n👥 Consumer Groups:");

      const consumerChecks = {};

      for (const [streamType, consumer] of this.streamConsumers.entries()) {
        try {
          const consumerGroupInfo = await this.redis.xInfoConsumers(
            consumer.config.streamKey,
            consumer.config.groupId,
          );

          consumerChecks[streamType] = {
            groupId: consumer.config.groupId,
            consumerCount: consumerGroupInfo.length,
            active: consumer.isRunning,
            interval: consumer.config.interval,
          };

          console.log(
            `   ${streamType}: ${consumerGroupInfo.length} consumer(s) [${
              consumer.isRunning ? "▶️ ACTIF" : "⏸️ INACTIF"
            }]`,
          );
        } catch (groupErr) {
          console.log(`   ❌ ${streamType}: ${groupErr.message}`);
          consumerChecks[streamType] = { error: groupErr.message };
        }
      }

      diagnostics.checks.consumerGroups = consumerChecks;

      // ✅ RÉSUMÉ
      console.log("\n📋 RÉSUMÉ:");
      console.log(`   Utilisateur: ${userIdStr}`);
      console.log(
        `   Connecté: ${isRegistered ? "✅ OUI" : "❌ NON"} (${
          socketIds.length
        } socket(s))`,
      );
      console.log(
        `   Messages en attente: ${diagnostics.checks.pendingQueue.count}`,
      );
      const totalRelevant = Object.values(streamChecks).reduce(
        (sum, s) => sum + (s.relevantMessages || 0),
        0,
      );
      console.log(`   Messages dans les streams: ${totalRelevant}`);
      console.log(`🔍 ========== FIN DIAGNOSTIC ==========\n`);

      return diagnostics;
    } catch (error) {
      console.error("❌ Erreur diagnostic:", error);
      return { error: error.message };
    }
  }

  /**
   * ✅ ARRÊTER TOUS LES WORKERS (SHUTDOWN)
   */
  async shutdown() {
    try {
      console.log("🛑 Arrêt MessageDeliveryService...");

      this.isRunning = false;

      // ✅ Nettoyer le timer de déduplication
      if (this._dedupCleanupInterval) {
        clearInterval(this._dedupCleanupInterval);
        this._dedupCleanupInterval = null;
      }
      if (this._deliveredStatusCache) {
        this._deliveredStatusCache.clear();
      }
      if (this._statusDeliveryQueues) {
        this._statusDeliveryQueues.clear();
      }

      // Arrêter tous les workers
      for (const [partitionKey, partition] of this.workers.entries()) {
        await this.stopWorkerPartition(partitionKey, partition);
      }

      // Fermer toutes les connexions Redis
      for (const [partitionKey, partition] of this.workers.entries()) {
        for (const worker of partition.workers) {
          if (worker.redis) {
            await worker.redis.disconnect();
          }
        }
      }

      this.workers.clear();
      console.log("✅ MessageDeliveryService arrêté proprement");
    } catch (error) {
      console.error("❌ Erreur arrêt MessageDeliveryService:", error);
      throw error;
    }
  }

  /**
   * ✅ ARRÊTER UNE PARTITION DE WORKERS
   */
  async stopWorkerPartition(partitionKey, partition) {
    if (!partition.isRunning) return;

    partition.isRunning = false;

    // Arrêter chaque worker de la partition
    for (const worker of partition.workers) {
      await this.stopWorkerInstance(worker);
    }

    console.log(`🛑 Partition ${partitionKey} arrêtée`);
  }

  /**
   * ✅ ARRÊTER UNE INSTANCE DE WORKER
   */
  async stopWorkerInstance(worker) {
    if (!worker.isRunning) return;

    worker.isRunning = false;

    // Arrêter tous les consumers de ce worker
    for (const [streamKey, consumer] of worker.streamConsumers.entries()) {
      if (consumer.isRunning && consumer.interval) {
        clearInterval(consumer.interval);
        consumer.isRunning = false;
      }
    }

    console.log(`🛑 Worker ${worker.id} arrêté`);
  }

  /**
   * ✅ LIVRER UN ÉVÉNEMENT FICHIER
   */
  async deliverFileEvent(message) {
    try {
      const userId = String(message.userId);
      const socketIds = this.userSockets.get(userId);

      if (!socketIds || socketIds.length === 0) {
        return;
      }

      for (const socketId of socketIds) {
        const socket = this.io.sockets.sockets.get(socketId);
        if (socket) {
          socket.emit("file:event", {
            fileId: message.fileId,
            event: message.event,
            fileName: message.fileName,
            fileSize: message.fileSize,
            timestamp: message.timestamp,
          });
        }
      }

      console.log(`📁 Événement fichier livré: ${message.event}`);
    } catch (error) {
      console.error("❌ Erreur livraison événement fichier:", error);
    }
  }

  /**
   * ✅ VÉRIFIER SI UN UTILISATEUR EST DANS UNE CONVERSATION
   */
  async isUserInConversation(userId, conversationId) {
    try {
      const userIdStr = String(userId);
      const conversationIdStr = String(conversationId);

      // Vérifier dans le cache userConversations
      const userConversations = this.userConversations.get(userIdStr) || [];

      if (userConversations.includes(conversationIdStr)) {
        return true;
      }

      // Si le cache est vide, on retourne false (l'utilisateur n'est pas connecté ou pas dans la conversation)
      return false;
    } catch (error) {
      console.error(
        `❌ Erreur vérification user in conversation: ${userId} / ${conversationId}:`,
        error,
      );
      return false;
    }
  }

  /**
   * ✅ RÉSOUDRE UN PROBLÈME DE LIVRAISON
   */
  async troubleshootDelivery(userId) {
    const diagnostics = await this.diagnoseDelivery(userId);
    const userIdStr = String(userId);

    console.log("🔧 RÉSOLUTION AUTOMATIQUE:");

    // ✅ PROBLÈME 1 : Utilisateur pas connecté mais messages en attente
    if (
      !diagnostics.checks.userRegistration.registered &&
      diagnostics.checks.pendingQueue.count > 0
    ) {
      console.log("   ⚠️ Messages en attente mais utilisateur déconnecté");
      console.log("   → Les messages seront livrés à la reconnexion");
    }

    // ✅ PROBLÈME 2 : Messages dans le stream mais pas livrés
    const totalInStreams =
      Object.values(diagnostics.checks.streams || {}).reduce(
        (sum, s) => sum + (s.relevantMessages || 0),
        0,
      ) || 0;

    if (totalInStreams > 0 && !diagnostics.checks.userRegistration.registered) {
      console.log(
        "   ⚠️ Messages bloqués dans le stream (utilisateur déconnecté)",
      );
      console.log(
        "   → Les consumers continuent à tourner, messages seront livrés",
      );
    }

    // ✅ PROBLÈME 3 : Aucun consumer actif
    const inactiveConsumers = Object.entries(
      diagnostics.checks.consumerGroups || {},
    ).filter((entry) => !entry[1].active);

    if (inactiveConsumers.length > 0) {
      console.log(`   ⚠️ ${inactiveConsumers.length} consumer(s) inactif(s)`);
      console.log("   → Redémarrage des consumers...");
      this.startAllConsumers();
    }
  }
}

module.exports = MessageDeliveryService;
