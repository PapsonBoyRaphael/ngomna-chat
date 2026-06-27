const RedisManager = require("../redis/RedisManager");
const UserCache = require("./UserCache");

/**
 * UserStreamConsumer - Écoute les événements utilisateur via Redis Streams
 *
 * Stream: chat:stream:events:users
 * Événements écoutés:
 * - user.profile.updated : Mise à jour d'un profil
 * - user.profile.created : Création d'un profil
 * - user.profile.deleted : Suppression d'un profil
 *
 * Actions:
 * - Synchronise automatiquement le cache UserCache
 * - Propage les changements à tous les services
 */
class UserStreamConsumer {
  constructor(options = {}) {
    // this.streamName = options.streamName || "chat:stream:events:users";
    this.streamName =
      process.env.STREAM_DOMAIN_IDENTITY || "stream:domain:identity";
    this.consumerGroup = options.consumerGroup || "consumer-chat-service-group";
    this.consumerName = options.consumerName || `consumer-${process.pid}`;
    this.pollInterval = options.pollInterval || 1000; // 1 seconde
    this.batchSize = options.batchSize || 10;
    this.redis = null;
    this.isRunning = false;
    this.pollTimer = null;
  }

  /**
   * Initialise le consumer
   */
  async initialize() {
    this.redis = RedisManager?.clients?.main;

    if (!this.redis) {
      console.warn("⚠️ [UserStreamConsumer] Redis non disponible");
      return false;
    }

    try {
      // Créer le consumer group si inexistant
      try {
        await this.redis.xGroupCreate(
          this.streamName,
          this.consumerGroup,
          "0",
          {
            MKSTREAM: true,
          },
        );
        console.log(
          `✅ [UserStreamConsumer] Consumer group créé: ${this.consumerGroup}`,
        );
      } catch (error) {
        if (error.message.includes("BUSYGROUP")) {
          console.log(
            `✅ [UserStreamConsumer] Consumer group existe déjà: ${this.consumerGroup}`,
          );
        } else {
          throw error;
        }
      }

      console.log("✅ [UserStreamConsumer] Initialisé avec succès");
      return true;
    } catch (error) {
      console.error(
        "❌ [UserStreamConsumer] Erreur initialisation:",
        error.message,
      );
      return false;
    }
  }

  /**
   * Démarre l'écoute des événements
   */
  async start() {
    if (this.isRunning) {
      console.warn("⚠️ [UserStreamConsumer] Déjà en cours d'exécution");
      return;
    }

    if (!this.redis) {
      console.error("❌ [UserStreamConsumer] Redis non initialisé");
      return;
    }

    this.isRunning = true;
    console.log(
      `🚀 [UserStreamConsumer] Démarrage de l'écoute sur ${this.streamName}`,
    );

    // Boucle de polling
    this.pollTimer = setInterval(() => {
      this._poll().catch((error) => {
        console.error("❌ [UserStreamConsumer] Erreur polling:", error.message);
      });
    }, this.pollInterval);

    // Premier poll immédiat
    await this._poll();
  }

  /**
   * Arrête l'écoute
   */
  async stop() {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;

    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }

    console.log("⏹️ [UserStreamConsumer] Arrêté");
  }

  /**
   * Poll les nouveaux messages
   * @private
   */
  async _poll() {
    if (!this.isRunning || !this.redis) {
      return;
    }

    try {
      // Lire les messages en attente
      const messages = await this.redis.xReadGroup(
        this.consumerGroup,
        this.consumerName,
        [
          {
            key: this.streamName,
            id: ">", // Seulement les nouveaux messages
          },
        ],
        {
          COUNT: this.batchSize,
          BLOCK: 100, // ✅ 100ms max (évite le blocage indéfini)
        },
      );

      if (!messages || messages.length === 0) {
        return;
      }

      // Traiter chaque message
      for (const stream of messages) {
        for (const message of stream.messages) {
          await this._handleMessage(message.id, message.message);
        }
      }
    } catch (error) {
      if (!error.message.includes("NOGROUP")) {
        console.error("❌ [UserStreamConsumer] Erreur poll:", error.message);
      }
    }
  }

  /**
   * Traite un message du stream
   * @private
   */
  async _handleMessage(messageId, data) {
    try {
      // Récupérer le payload
      const payloadStr = data.payload || data.event;

      if (!payloadStr) {
        console.warn(
          "⚠️ [UserStreamConsumer] Message sans payload:",
          messageId,
        );
        await this._ack(messageId);
        return;
      }

      const event = JSON.parse(payloadStr);

      console.log(
        `📨 [UserStreamConsumer] Event reçu: ${event.event} pour user ${event.userId}`,
      );

      // Dispatcher selon le type d'événement
      switch (event.event) {
        case process.env.EVENT_IDENTITY_USERCREATED:
        case "user.profile.updated":
          await this._handleProfileUpdate(event);
          break;

        case "user.profile.deleted":
          await this._handleProfileDelete(event);
          break;

        default:
          console.warn(
            `⚠️ [UserStreamConsumer] Événement non géré: ${event.event}`,
          );
      }

      // Acquitter le message
      await this._ack(messageId);
    } catch (error) {
      console.error(
        `❌ [UserStreamConsumer] Erreur traitement ${messageId}:`,
        error.message,
      );
      // Le message ne sera pas acquitté et pourra être retraité
    }
  }

  /**
   * Gère la mise à jour d'un profil
   * @private
   */
  async _handleProfileUpdate(event) {
    const userProfile = {
      matricule: event.matricule,
      ministere: event.ministere,
      nom: event.nom,
      fcmToken: event.fcmToken,
    };

    // Mettre à jour le cache
    // ✅ Utilise matricule comme clé primaire pour cohérence avec fetchUsersInfo
    await UserCache.set({
      matricule: event.matricule,
      ministere: event.ministere,
      nom: event.nom,
      fcmToken: event.fcmToken,
    });

    console.log(
      `✅ [UserStreamConsumer] Profil mis à jour en cache: ${event.userId}`,
    );
  }

  /**
   * Gère la suppression d'un profil
   * @private
   */
  async _handleProfileDelete(event) {
    await UserCache.invalidate(event.userId);
    console.log(
      `🗑️ [UserStreamConsumer] Profil supprimé du cache: ${event.userId}`,
    );
  }

  /**
   * Acquitte un message
   * @private
   */
  async _ack(messageId) {
    try {
      await this.redis.xAck(this.streamName, this.consumerGroup, messageId);
    } catch (error) {
      console.error(
        `❌ [UserStreamConsumer] Erreur ACK ${messageId}:`,
        error.message,
      );
    }
  }

  /**
   * Récupère les statistiques du consumer
   */
  async getStats() {
    if (!this.redis) {
      return null;
    }

    try {
      const info = await this.redis.xInfoGroups(this.streamName);
      const groupInfo = info.find((g) => g.name === this.consumerGroup);

      return {
        streamName: this.streamName,
        consumerGroup: this.consumerGroup,
        consumerName: this.consumerName,
        isRunning: this.isRunning,
        pending: groupInfo?.pending || 0,
        lastDeliveredId: groupInfo?.["last-delivered-id"] || "0-0",
      };
    } catch (error) {
      console.error("❌ [UserStreamConsumer] Erreur getStats:", error.message);
      return null;
    }
  }
}

module.exports = UserStreamConsumer;
