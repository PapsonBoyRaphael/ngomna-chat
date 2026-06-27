// shared/auth/permission-stream-consumer.js

/**
 * Consumer Redis Stream pour synchroniser le LocalPermissionStore
 * 
 * Ce consumer écoute les événements de changement de permissions
 * publiés par le service Identity et met à jour le store local.
 */

class PermissionStreamConsumer {
  /**
   * @param {object} redisClient - Client Redis (ton redis-client.js)
   * @param {LocalPermissionStore} localStore - Le store local à synchroniser
   * @param {string} serviceName - Nom du service (ex: 'bulletin-service')
   */
  constructor(redisClient, localStore, serviceName) {
    this.redisClient = redisClient;
    this.store = localStore;
    this.serviceName = serviceName;
    
    // Configuration
    this.streamName = process.env.STREAM_DOMAIN_IDENTITY || 'stream:domain:identity';
    this.consumerGroup = `${serviceName}-rbac-consumers`;
    this.consumerId = `${serviceName}-${process.pid}`; // Unique par instance
    
    // État
    this.isRunning = false;
    this.client = null;
  }

  /**
   * Initialise le consumer group
   */
  async initialize() {
    try {
      this.client = await this.redisClient;
      
      // Créer le consumer group s'il n'existe pas
      try {
        await this.client.xGroupCreate(
          this.streamName, 
          this.consumerGroup, 
          '0',  // Lire depuis le début (ou '$' pour nouveaux messages seulement)
          { MKSTREAM: true }
        );
        console.log(`✅ [${this.serviceName}] Consumer group "${this.consumerGroup}" créé`);
      } catch (error) {
        if (!error.message.includes('BUSYGROUP')) {
          throw error;
        }
        console.log(`ℹ️ [${this.serviceName}] Consumer group "${this.consumerGroup}" existe déjà`);
      }

      return true;
    } catch (error) {
      console.error(`❌ [${this.serviceName}] Erreur initialisation consumer:`, error);
      throw error;
    }
  }

  /**
   * Démarre la consommation des messages
   */
  async start() {
    if (this.isRunning) {
      console.warn(`⚠️ [${this.serviceName}] Consumer déjà en cours d'exécution`);
      return;
    }

    this.isRunning = true;
    console.log(`🚀 [${this.serviceName}] Démarrage du consumer de permissions...`);

    // Traiter d'abord les messages en pending (non-ACK)
    await this.processPendingMessages();

    // Puis écouter les nouveaux messages
    await this.consumeLoop();
  }

  /**
   * Traite les messages en pending (non-ACK du précédent run)
   */
  async processPendingMessages() {
    try {
      console.log(`🔄 [${this.serviceName}] Traitement des messages pending...`);
      
      // Lire les messages pending pour ce consumer
      const pending = await this.client.xReadGroup(
        this.consumerGroup,
        this.consumerId,
        [{ key: this.streamName, id: '0' }], // '0' = messages pending
        { COUNT: 100 }
      );

      if (pending && pending.length > 0) {
        for (const stream of pending) {
          for (const message of stream.messages) {
            await this.processMessage(message);
          }
        }
        console.log(`✅ [${this.serviceName}] Messages pending traités`);
      } else {
        console.log(`ℹ️ [${this.serviceName}] Aucun message pending`);
      }
    } catch (error) {
      console.error(`❌ [${this.serviceName}] Erreur traitement pending:`, error);
    }
  }

  /**
   * Boucle principale de consommation
   */
  async consumeLoop() {
    while (this.isRunning) {
      try {
        // Lire les nouveaux messages (bloquant pendant 5s max)
        const messages = await this.client.xReadGroup(
          this.consumerGroup,
          this.consumerId,
          [{ key: this.streamName, id: '>' }], // '>' = nouveaux messages seulement
          {
            COUNT: 10,
            BLOCK: 5000  // Bloque 5s si pas de message
          }
        );
        
        if (messages && messages.length > 0) {
          for (const stream of messages) {
            for (const message of stream.messages) {
              await this.processMessage(message);
            }
          }
        }
      } catch (error) {
        console.error(`❌ [${this.serviceName}] Erreur consume loop:`, error);
        
        // Attendre avant de réessayer
        await this.sleep(1000);
      }
    }
  }

  /**
   * Traite un message individuel
   */
  async processMessage(message) {
    const { id, message: data } = message;
    
    try {
      // Parser les données
      const eventData = this.parseMessage(data);
      
      console.log(`📩 [${this.serviceName}] Message reçu: ${id}`, {
        type: eventData.type,
        matricule: eventData.matricule
      });

      // Traiter selon le type d'événement
      await this.handleEvent(eventData);
      
      // ACK le message (confirmer le traitement)
      await this.client.xAck(this.streamName, this.consumerGroup, id);
      
      console.log(`✅ [${this.serviceName}] Message ${id} traité et ACK`);
    } catch (error) {
      console.error(`❌ [${this.serviceName}] Erreur traitement message ${id}:`, error);
      // Le message reste en pending et sera re-traité
    }
  }

  /**
   * Parse un message du stream
   */
  parseMessage(data) {
    const result = {};
    
    for (const [key, value] of Object.entries(data)) {
      try {
        const parsed = JSON.parse(value);
        result[key] = parsed;
      } catch (e) {
        // Si ce n'est pas du JSON valide, garder la valeur telle quelle
        result[key] = value;
      }
    }
    
    // S'assurer que permissions et roles sont des arrays
    if (result.permissions && typeof result.permissions === 'string') {
      try {
        result.permissions = JSON.parse(result.permissions);
      } catch (e) {
        result.permissions = [];
      }
    }
    
    if (result.roles && typeof result.roles === 'string') {
      try {
        result.roles = JSON.parse(result.roles);
      } catch (e) {
        result.roles = [];
      }
    }
    
    // Garantir que ce sont des arrays
    result.permissions = Array.isArray(result.permissions) ? result.permissions : [];
    result.roles = Array.isArray(result.roles) ? result.roles : [];
    
    return result;
  }
  
  /**
   * Gère un événement selon son type
   */
  async handleEvent(eventData) {
    const { type, matricule, permissions, roles } = eventData;

    // DEBUG: Log complet des données reçues
    console.log(`🔍 [${this.serviceName}] DEBUG - Données brutes reçues:`, {
      eventData: JSON.stringify(eventData, null, 2)
    });

    // Validation des données de base
    if (!type) {
      console.warn(`⚠️ [${this.serviceName}] Événement sans type, ignoré:`, eventData);
      return;
    }

    if (!matricule) {
      console.warn(`⚠️ [${this.serviceName}] Événement sans matricule, ignoré:`, { type });
      return;
    }

    // Assurer que permissions et roles sont définis
    const safePermissions = permissions || [];
    const safeRoles = roles || [];

    console.log(`📩 [${this.serviceName}] Traitement événement:`, {
      type,
      matricule,
      permissions: safePermissions.length,
      roles: safeRoles.length,
      permissionsType: typeof permissions,
      rolesType: typeof roles
    });
    
    switch (type) {
      // ═══════════════════════════════════════════════════════════════════
      // ÉVÉNEMENTS DE RÔLES
      // ═══════════════════════════════════════════════════════════════════
      
      case process.env.EVENT_IDENTITY_USERROLEADDED:
      case 'identity.userroleadded':
        // Un rôle a été assigné à un utilisateur → mettre à jour les permissions
        if (safePermissions.length > 0 || safeRoles.length > 0) {
          this.store.setUser(matricule, {
            permissions: safePermissions,
            roles: safeRoles,
            syncSource: 'event'
          });
          console.log(`👤 [${this.serviceName}] Permissions mises à jour pour ${matricule} (rôle ajouté)`, {
            permissions: safePermissions.length,
            roles: safeRoles.length
          });
        } else {
          this.store.removeUser(matricule);
          console.log(`👤 [${this.serviceName}] Utilisateur supprimé ${matricule} (aucune permission/rôle)`);
        }
        break;
      
      case process.env.EVENT_IDENTITY_USERROLEREMOVED:
      case 'identity.userroleremoved':
        // Un rôle a été retiré → mettre à jour les permissions
        if (safePermissions.length > 0 || safeRoles.length > 0) {
          this.store.setUser(matricule, {
            permissions: safePermissions,
            roles: safeRoles,
            syncSource: 'event'
          });
          console.log(`👤 [${this.serviceName}] Permissions mises à jour pour ${matricule} (rôle retiré)`, {
            permissions: safePermissions.length,
            roles: safeRoles.length
          });
        } else {
          this.store.removeUser(matricule);
          console.log(`👤 [${this.serviceName}] Utilisateur supprimé ${matricule} (aucune permission/rôle)`);
        }
        break;

    //   case process.env.EVENT_IDENTITY_USERROLEUPDATED:
    //   case 'EVENT_IDENTITY_USERROLEUPDATED':
    //     // Les permissions d'un rôle ont changé
    //     if (matricule && permissions) {
    //       this.store.setUserPermissions(matricule, permissions);
    //       console.log(`👤 [${this.serviceName}] Permissions mises à jour pour ${matricule} (rôle modifié)`);
    //     }
    //     break;
      
      // ═══════════════════════════════════════════════════════════════════
      // ÉVÉNEMENTS DE PERMISSIONS DIRECTES
      // ═══════════════════════════════════════════════════════════════════
      
      case process.env.EVENT_IDENTITY_PERMISSIONADDED:
      case 'identity.permissionadded':
        if (safePermissions.length > 0 || safeRoles.length > 0) {
          this.store.setUser(matricule, {
            permissions: safePermissions,
            roles: safeRoles,
            syncSource: 'event'
          });
          console.log(`🔑 [${this.serviceName}] Permissions mises à jour pour ${matricule} (permission ajoutée)`, {
            permissions: safePermissions.length,
            roles: safeRoles.length
          });
        } else {
          this.store.removeUser(matricule);
          console.log(`🔑 [${this.serviceName}] Utilisateur supprimé ${matricule} (aucune permission/rôle)`);
        }
        break;
      
      case process.env.EVENT_IDENTITY_PERMISSIONREMOVED:
      case 'identity.permissionremoved':
        if (safePermissions.length > 0 || safeRoles.length > 0) {
          this.store.setUser(matricule, {
            permissions: safePermissions,
            roles: safeRoles,
            syncSource: 'event'
          });
          console.log(`🔑 [${this.serviceName}] Permissions mises à jour pour ${matricule} (permission retirée)`, {
            permissions: safePermissions.length,
            roles: safeRoles.length
          });
        } else {
          this.store.removeUser(matricule);
          console.log(`🔑 [${this.serviceName}] Utilisateur supprimé ${matricule} (aucune permission/rôle)`);
        }
        break;
      
      case process.env.EVENT_IDENTITY_PERMISSIONUPDATED:
      case 'identity.permissionupdated':
        if (safePermissions.length > 0 || safeRoles.length > 0) {
          this.store.setUser(matricule, {
            permissions: safePermissions,
            roles: safeRoles,
            syncSource: 'event'
          });
          console.log(`🔑 [${this.serviceName}] Permissions mises à jour pour ${matricule} (permission modifiée)`, {
            permissions: safePermissions.length,
            roles: safeRoles.length
          });
        } else {
          this.store.removeUser(matricule);
          console.log(`🔑 [${this.serviceName}] Utilisateur supprimé ${matricule} (aucune permission/rôle)`);
        }
        break;

      default:
        // Événement non lié aux permissions, ignorer silencieusement
        break;
    }
  }
  
  /**
   * Arrête le consumer proprement
   */
  async stop() {
    console.log(`🛑 [${this.serviceName}] Arrêt du consumer...`);
    this.isRunning = false;
  }

  /**
   * Helper sleep
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  
  /**
   * Statistiques du consumer
   */
  getStats() {
    return {
      serviceName: this.serviceName,
      consumerGroup: this.consumerGroup,
      consumerId: this.consumerId,
      isRunning: this.isRunning,
      storeStats: this.store.getStats()
    };
  }
}

module.exports = PermissionStreamConsumer;