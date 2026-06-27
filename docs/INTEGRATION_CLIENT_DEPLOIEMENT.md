# INTEGRATION CLIENT DEPLOIEMENT

Ce document est une fusion automatisée des fichiers de documentation suivants :



---
## Fichier d'origine : `FLUTTER_CLIENT_INTEGRATION.md`

# Documentation d'intégration - Client Flutter

## Vue d'ensemble

Cette documentation explique comment intégrer le système de chat et de présence dans une application Flutter mobile. Le système repose sur Socket.IO pour la communication temps réel et respecte la confidentialité en limitant la visibilité de la présence aux contacts uniquement.

---

## 🔌 Architecture de connexion

### 1. Configuration Socket.IO

**Package requis** : `socket_io_client`

**Paramètres de connexion** :

- URL du serveur : `http://localhost:8003` (ou votre serveur de production)
- Transports : WebSocket en priorité, polling en fallback
- Auto-reconnexion : Activée avec délai progressif
- Timeout : 5000ms

### 2. Cycle de vie de l'application

**États de connexion à gérer** :

- `disconnected` → État initial, socket non créé
- `connecting` → Tentative de connexion en cours
- `connected` → Socket connecté mais pas authentifié
- `authenticated` → Utilisateur authentifié et prêt

**Intégration avec le cycle de vie Flutter** :

- `AppLifecycleState.resumed` → Reconnecter si nécessaire
- `AppLifecycleState.paused` → Le serveur gère automatiquement l'idle après 2 minutes
- `AppLifecycleState.inactive` → Maintenir la connexion
- `AppLifecycleState.detached` → Déconnecter proprement

---

## 🔐 Flux d'authentification

### Étape 1 : Connexion du socket

Établir la connexion WebSocket avec le serveur sans authentification.

### Étape 2 : Émission du token

Envoyer l'événement `authenticate` avec le JWT stocké localement (SharedPreferences ou SecureStorage).

**Payload à envoyer** :

```
Événement: "authenticate"
Données: { token: "votre_jwt_token" }
```

### Étape 3 : Réception de la confirmation

**Succès** → Événement `authenticated` :

- Contient : userId, matricule, autres infos utilisateur
- Le serveur joint automatiquement :
  - Room personnelle : `user_${userId}`
  - Rooms des conversations : `conversation_${convId}` pour chaque conversation
- Déclenche l'envoi de `user_online` aux contacts

**Échec** → Événement `auth_error` :

- Contient : message d'erreur, code d'erreur
- Actions : Rediriger vers login, rafraîchir le token, afficher erreur

---

## 👥 Système de présence (Privacy-First)

### Principe de confidentialité

**Règle fondamentale** : Un utilisateur ne reçoit les événements de présence QUE pour ses contacts (personnes avec qui il partage au moins une conversation).

**Ce que cela signifie** :

- Si vous n'avez jamais discuté avec quelqu'un → Vous ne voyez pas son statut
- Si vous partagez une conversation privée → Vous voyez mutuellement vos statuts
- Si vous êtes dans un groupe → Tous les membres voient les statuts des autres membres

### Événements de présence à écouter

#### `user_online`

**Quand** : Un contact se connecte ou devient actif après idle
**Données reçues** :

- `userId` : Identifiant du contact
- `matricule` : Matricule ou nom d'affichage
- `lastActivity` : Timestamp de la dernière activité
- `status` : Toujours "online"

**Action UI** :

- Afficher un point vert à côté du contact
- Mettre à jour la liste des utilisateurs en ligne
- Éventuellement déclencher une notification légère

#### `user_offline`

**Quand** : Un contact se déconnecte complètement (ferme l'app, perd la connexion)
**Données reçues** :

- `userId` : Identifiant du contact
- `matricule` : Matricule ou nom d'affichage

**Action UI** :

- Retirer le point vert
- Afficher "Hors ligne" ou masquer le statut
- Mettre à jour le cache local

#### `user_idle`

**Quand** : Un contact est inactif depuis plus de 2 minutes (app en arrière-plan, écran verrouillé)
**Données reçues** :

- `userId` : Identifiant du contact
- `matricule` : Matricule ou nom d'affichage
- `lastActivity` : Timestamp de la dernière activité

**Action UI** :

- Afficher un point orange ou gris
- Montrer "Absent" ou "Inactif"
- Afficher "Actif il y a X minutes"

### Demande manuelle de statuts

#### Événement à émettre : `getOnlineUsers`

**Quand l'utiliser** :

- Au démarrage de l'application après authentification
- Lors du rafraîchissement de la liste de contacts
- Après une reconnexion

**Réponse attendue** → Événement `onlineUsers` :

- `users` : Array d'objets contenant :
  - `userId` : Identifiant
  - `matricule` : Nom d'affichage
  - `status` : "online" ou "idle"
  - `lastActivity` : Timestamp

---

## 💬 Événements de messagerie

### Réception de messages

#### `newMessage`

**Données** :

- `messageId` : ID unique du message
- `conversationId` : ID de la conversation
- `senderId` : ID de l'expéditeur
- `senderName` : Nom de l'expéditeur
- `content` : Contenu du message
- `type` : "text", "image", "file", etc.
- `timestamp` : Date d'envoi
- `status` : "sent", "delivered", "read"

**Actions** :

- Ajouter le message dans le StreamController ou StateNotifier
- Afficher une notification si conversation non ouverte
- Jouer un son (si paramètres permettent)
- Envoyer automatiquement `messageDelivered`

### Envoi de messages

**Événement à émettre** : `sendMessage`
**Payload** :

- `conversationId` : ID de la conversation
- `content` : Contenu du message
- `type` : Type de message
- Optionnel : `replyTo`, `metadata`

**Réponses possibles** :

- `message_sent` → Succès, contient `messageId`, `timestamp`
- `message_error` → Échec, contient `error`, `code`

### Accusés de réception

**Événements à émettre** :

- `messageDelivered` → Quand le message est reçu et affiché
- `messageRead` → Quand l'utilisateur ouvre la conversation

**Événements à écouter** :

- `messageDelivered` → Un contact a reçu votre message
- `messageRead` → Un contact a lu votre message

---

## ⌨️ Indicateurs de frappe

### Émettre son propre statut

**Événement** : `typing`
**Payload** :

- `conversationId` : ID de la conversation
- `isTyping` : `true` pour commencer, `false` pour arrêter

**Bonne pratique** :

- Envoyer `isTyping: true` dès le premier caractère tapé
- Utiliser un debounce de 1 seconde
- Envoyer `isTyping: false` après 3 secondes d'inactivité
- Toujours envoyer `false` avant d'envoyer le message

### Recevoir les indicateurs

**Événement** : `typing`
**Données** :

- `userId` : ID de l'utilisateur qui tape
- `userName` : Nom de l'utilisateur
- `conversationId` : ID de la conversation
- `isTyping` : Boolean

**Action UI** :

- Afficher "X est en train d'écrire..." dans la conversation
- Masquer après 3 secondes si aucune mise à jour

---

## 🏗️ Architecture Flutter recommandée

### State Management

**Trois couches principales** :

1. **Service Layer** (Socket Manager)
   - Gère la connexion Socket.IO
   - Expose des Streams pour les événements
   - Méthodes pour émettre des événements
   - Gestion de la reconnexion automatique

2. **Repository Layer**
   - Cache local avec Hive ou Isar
   - Synchronisation avec le serveur
   - Gestion des états offline/online
   - File d'attente pour messages non envoyés

3. **State Layer** (Riverpod, Bloc, Provider)
   - États de présence des utilisateurs
   - Liste des conversations
   - Messages par conversation
   - Indicateurs de frappe

### Gestion du cache

**Données à cacher localement** :

- Liste des conversations avec dernier message
- Messages de chaque conversation (pagination)
- Informations des contacts
- Statuts de présence (avec TTL de 5 minutes)

**Synchronisation** :

- À l'ouverture : Charger le cache immédiatement
- En arrière-plan : Récupérer les mises à jour du serveur
- Stratégie : Cache-first avec background sync

### Gestion des notifications

**En foreground** :

- Afficher un snackbar ou une bannière in-app
- Mettre à jour le badge de l'icône de conversation
- Jouer un son léger

**En background** :

- Utiliser Firebase Cloud Messaging (FCM)
- Le serveur envoie une notification push quand l'utilisateur est offline
- Tapper sur la notification ouvre la conversation

---

## 🔄 Gestion de la reconnexion

### Scénarios de déconnexion

1. **Perte de connexion réseau** :
   - Socket.IO tente automatiquement la reconnexion
   - Afficher un indicateur "Connexion en cours..."
   - Ré-authentifier dès la reconnexion

2. **Application mise en arrière-plan** :
   - Maintenir la connexion socket pendant 5-10 minutes
   - Après ce délai, le serveur marque l'utilisateur comme idle
   - Reconnecter immédiatement au retour en foreground

3. **Token expiré** :
   - Recevoir `auth_error` avec code spécifique
   - Rafraîchir le token avec refresh_token
   - Ré-authentifier automatiquement

### File d'attente de messages

**Problème** : Message envoyé pendant une déconnexion

**Solution** :

1. Stocker le message localement avec statut "pending"
2. Afficher le message dans l'UI avec un indicateur d'attente
3. À la reconnexion, renvoyer tous les messages "pending"
4. Mettre à jour le statut en "sent" après confirmation

---

## 🎨 Recommandations UI/UX

### Indicateurs de statut

**Tailles** :

- Liste de contacts : Petit point (8-10px)
- Barre de conversation : Point moyen (12px) + texte
- Profil utilisateur : Grand point (16px) + texte détaillé

**Couleurs** :

- Vert (#4CAF50) : En ligne
- Orange (#FF9800) : Inactif/Idle
- Gris (#9E9E9E) : Hors ligne
- Pas d'indicateur : Statut inconnu (non-contact)

### Optimisations de performance

1. **Liste de conversations** :
   - Utiliser ListView.builder avec lazy loading
   - Charger 20 conversations à la fois
   - Afficher les statuts uniquement pour conversations visibles

2. **Messages** :
   - Pagination inversée (charger les anciens en scrollant vers le haut)
   - Garder maximum 100 messages en mémoire
   - Libérer les messages hors écran

3. **Présence** :
   - Mettre à jour les statuts par batch (toutes les 2 secondes)
   - Ne pas reconstruire tout le widget pour un changement de statut
   - Utiliser ValueListenableBuilder ou similaire pour micro-updates

### Gestion des erreurs

**Affichage utilisateur** :

- Message clair et actionnable
- Bouton "Réessayer" si applicable
- Option "Contacter le support" pour erreurs persistantes

**Erreurs à anticiper** :

- Connexion internet perdue → "Pas de connexion. Vérifiez votre réseau."
- Token expiré → Reconnexion automatique, transparent pour l'utilisateur
- Message non envoyé → "Échec de l'envoi. Appuyez pour réessayer."
- Serveur indisponible → "Service temporairement indisponible. Réessai automatique..."

---

## 📊 Métriques et monitoring

### Côté client

**Événements à tracer** :

- Temps de connexion initiale
- Nombre de reconnexions par session
- Taux de succès d'envoi de messages
- Latence moyenne des messages
- Fréquence des erreurs d'authentification

**Analytics** :

- Utiliser Firebase Analytics ou similaire
- Ne jamais logger le contenu des messages
- Logger uniquement les métadonnées (IDs, timestamps, types)

---

## 🔒 Sécurité

### Protection du token

- Stocker le JWT dans FlutterSecureStorage (pas SharedPreferences)
- Ne jamais logger le token
- Implémenter un mécanisme de refresh token
- Invalider le token lors de la déconnexion

### Validation des données

- Valider tous les champs reçus du serveur
- Ne jamais faire confiance aux données du socket sans vérification
- Sanitiser le contenu des messages avant affichage
- Gérer les cas où `userId`, `conversationId` sont null

### HTTPS/WSS

- En production, utiliser HTTPS pour l'API REST
- Utiliser WSS (WebSocket Secure) pour Socket.IO
- Épingler le certificat SSL (certificate pinning) si possible

---

## 📱 Spécificités mobiles

### Gestion de la batterie

**Optimisations** :

- Réduire la fréquence de ping à 30-60 secondes
- Grouper les mises à jour de statut
- Déconnecter après 10 minutes en arrière-plan
- Utiliser WorkManager pour synchronisation périodique

### Permissions

**Android** :

- `INTERNET` : Requis pour Socket.IO
- `ACCESS_NETWORK_STATE` : Détecter les changements de connectivité
- Notifications : Demander permission runtime pour Android 13+

**iOS** :

- Background modes : "fetch", "remote-notification"
- NSAppTransportSecurity : Configurer pour permettre WebSocket

### Tests

**Scénarios à tester** :

- Mode avion activé puis désactivé
- Changement de WiFi à données mobiles
- Application tuée puis rouverte
- Notifications reçues app fermée
- Plusieurs comptes sur plusieurs devices

---

## 🚀 Checklist d'intégration

- [ ] Intégrer `socket_io_client` package
- [ ] Créer un SocketService avec gestion du cycle de vie
- [ ] Implémenter l'authentification automatique au démarrage
- [ ] Écouter les 3 événements de présence (online, offline, idle)
- [ ] Implémenter l'envoi et réception de messages
- [ ] Ajouter les indicateurs de frappe
- [ ] Gérer la reconnexion automatique
- [ ] Implémenter le cache local avec Hive/Isar
- [ ] Ajouter une file d'attente pour messages offline
- [ ] Configurer les notifications push (FCM)
- [ ] Afficher les statuts de présence dans l'UI
- [ ] Tester tous les scénarios de déconnexion
- [ ] Implémenter le refresh token
- [ ] Ajouter analytics et error tracking
- [ ] Optimiser la performance (lazy loading, pagination)

---

## 📞 Support et ressources

**Documentation Socket.IO Flutter** :

- Package officiel : socket_io_client sur pub.dev
- Exemples de reconnexion et gestion d'erreurs

**Architecture recommandée** :

- Riverpod pour state management moderne
- Freezed pour les modèles immutables
- Hive pour le cache local rapide
- Auto_route pour navigation type-safe

**Outils de debug** :

- Socket.IO Inspector (outil navigateur)
- Flutter DevTools pour performance
- Charles Proxy pour inspecter WebSocket



---
## Fichier d'origine : `MIGRATION_GUIDE.md`

# 🔄 Guide de Migration - Intégration du Service Redis Partagé

## Vue d'Ensemble

Ce guide explique comment les autres services de l'application peuvent être migrés vers l'utilisation du Service Redis Partagé, en suivant le modèle appliqué à `ResilientMessageService.js`.

---

## Principes de Migration

### 1. **Identifiez les Duplications**

Recherchez dans vos services :

- ❌ Configurations de streams locales (STREAMS, MULTI_STREAMS, STREAM_MAXLEN)
- ❌ Implémentations manuelles d'`addToStream()`, `readFromStream()`
- ❌ Monitoring manuels (intervalles pour mémoire, streams, métriques)
- ❌ Instanciations individuelles des workers (RetryWorker, FallbackWorker, etc.)

### 2. **Remplacez par des Services Partagés**

Importez depuis `@chatapp-ngomna/shared` :

```javascript
const {
  CircuitBreaker,
  StreamManager,
  WorkerManager,
} = require("@chatapp-ngomna/shared");
```

### 3. **Déléguez les Opérations**

Utilisez les wrappers du service partagé au lieu des implémentations locales.

---

## Étapes de Migration Détaillées

### Étape 1 : Audit du Service

```javascript
// ✅ Cherchez ces patterns dans votre code

// Pattern 1 : Configs dupliquées
this.STREAMS = { ... };
this.STREAM_MAXLEN = { ... };

// Pattern 2 : Monitoring manuel
this.memoryMonitorInterval = setInterval(() => { ... }, 60000);
this.metricsInterval = setInterval(() => { ... }, 3600000);

// Pattern 3 : Workers manuels
this.workers = {
  retryWorker: null,
  fallbackWorker: null,
};

// Pattern 4 : Initialisation manuelle
async addToStream(streamName, fields) {
  const streamId = await this.redis.xAdd(...);
  await this.redis.xTrim(...);
  return streamId;
}
```

### Étape 2 : Remplacez le Constructor

#### Avant

```javascript
constructor(redisClient, repo) {
  this.redis = redisClient;
  this.repo = repo;

  // ❌ Configurations dupliquées
  this.STREAMS = {
    MAIN: "stream:main",
    RETRY: "stream:retry",
    DLQ: "stream:dlq",
  };

  // ❌ Monitoring manuel
  if (this.redis) {
    this.startMemoryMonitor();
    this.startMetricsReporting();
  }
}
```

#### Après

```javascript
constructor(redisClient, repo) {
  this.redis = redisClient;
  this.repo = repo;

  // ✅ Importer le StreamManager
  const { StreamManager, WorkerManager } = require("@chatapp-ngomna/shared");

  // ✅ Instancier les composants partagés
  this.streamManager = new StreamManager(this.redis);

  // ✅ Utiliser les configs partagées
  this.STREAMS = this.streamManager.STREAMS;
  this.STREAM_MAXLEN = this.streamManager.STREAM_MAXLEN;

  // ✅ Instancier WorkerManager
  this.workerManager = new WorkerManager(this.streamManager, this.redis);

  // ✅ Initialiser les workers
  this.initializeWorkers();

  console.log("✅ Service initialisé avec les composants partagés");
}

initializeWorkers() {
  const callbacks = {
    save: this.saveData.bind(this),
    publish: this.publishEvent.bind(this),
    dlq: this.handleDLQ.bind(this),
  };
  this.workerManager.initialize(callbacks);
  this.workerManager.startAll();
}
```

### Étape 3 : Remplacez les Opérations Stream

#### Avant

```javascript
async addToStream(streamName, fields) {
  if (!this.redis) return null;

  try {
    const normalizedFields = {};
    for (const [key, value] of Object.entries(fields || {})) {
      normalizedFields[key] = String(value || "");
    }

    const streamId = await this.redis.xAdd(streamName, "*", normalizedFields);

    const maxLen = this.STREAM_MAXLEN[streamName];
    if (maxLen) {
      this.redis.xTrim(streamName, "~", maxLen).catch(() => {});
    }

    return streamId;
  } catch (err) {
    console.warn(`Erreur addToStream ${streamName}:`, err.message);
    return null;
  }
}

async readFromStream(streamName) {
  try {
    const messages = await this.redis.xRead([{ key: streamName, id: "0" }]);
    // ... parsing logic
    return messages;
  } catch (err) {
    console.error("Erreur readFromStream:", err.message);
    return [];
  }
}
```

#### Après

```javascript
async addToStream(streamName, fields) {
  return this.streamManager.addToStream(streamName, fields);
}

async readFromStream(streamName, options = {}) {
  const messages = await this.streamManager.readFromStream(streamName, options);
  return messages.map(entry => this.streamManager.parseStreamMessage(entry));
}

async deleteFromStream(streamName, messageId) {
  return this.streamManager.deleteFromStream(streamName, messageId);
}

async getStreamLength(streamName) {
  return this.streamManager.getStreamLength(streamName);
}
```

### Étape 4 : Supprimez les Monitoring Manuels

#### À Supprimer

```javascript
// ❌ À SUPPRIMER
async startMemoryMonitor() {
  this.memoryMonitorInterval = setInterval(async () => {
    const info = await this.redis.info("memory");
    // ... monitoring logic
  }, 60000);
}

async startStreamMonitoring() {
  this.monitoringInterval = setInterval(async () => {
    // ... stream monitoring logic
  }, 60000);
}

startMetricsReporting() {
  this.metricsInterval = setInterval(() => {
    // ... metrics logic
  }, 3600000);
}
```

**Raison** : Ces fonctionnalités sont maintenant fournies par les workers du `WorkerManager`.

### Étape 5 : Mettez à Jour le Cycle de Vie

#### Avant

```javascript
async startWorkers() {
  this.workerInterval = setInterval(() => this.processQueue(), 1000);
  this.dlqInterval = setInterval(() => this.processDLQ(), 5000);
}

stopWorkers() {
  clearInterval(this.workerInterval);
  clearInterval(this.dlqInterval);
  clearInterval(this.memoryMonitorInterval);
  clearInterval(this.metricsInterval);
}
```

#### Après

```javascript
async startWorkers() {
  this.workerManager.startAll();
}

stopWorkers() {
  this.workerManager.stopAll();
}

stopAll() {
  this.workerManager.stopAll();
  // Autres cleanups spécifiques
  console.log("✅ Service arrêté complètement");
}
```

### Étape 6 : Ajoutez les Méthodes de Santé

```javascript
getMetrics() {
  const workerMetrics = this.workerManager?.getAllMetrics() || {};
  return {
    service: {
      processedMessages: this.metrics.processedMessages || 0,
      errors: this.metrics.errors || 0,
      // ... autres métriques spécifiques
    },
    workers: workerMetrics.workers,
    uptime: workerMetrics.uptime,
    circuitBreakerState: this.circuitBreaker?.state,
  };
}

getHealthStatus() {
  return {
    status: this.isRunning ? "RUNNING" : "STOPPED",
    workers: this.workerManager?.getHealthStatus(),
    streams: this.getStreamStats(),
    redis: this.redis ? "CONNECTED" : "DISCONNECTED",
    timestamp: new Date().toISOString(),
  };
}
```

---

## Checklist de Migration

### Pour Chaque Service

- [ ] **Audit** : Identifiez les patterns à remplacer
- [ ] **Imports** : Importez les composants partagés
- [ ] **Constructor** : Instanciez `StreamManager` et `WorkerManager`
- [ ] **Configurations** : Utilisez les configs du `StreamManager`
- [ ] **Opérations Stream** : Remplacez par des wrappers
- [ ] **Monitoring** : Supprimez les monitoring manuels
- [ ] **Workers** : Utilisez `WorkerManager.startAll()` / `stopAll()`
- [ ] **Métriques** : Ajoutez `getMetrics()` et `getHealthStatus()`
- [ ] **Tests** : Vérifiez que la logique fonctionne
- [ ] **Validation** : Exécutez les tests d'intégration

---

## Services Candidats pour Migration

### 1. **MessageDeliveryService** 🔄

**Chemin** : `/chat-file-service/src/infrastructure/services/MessageDeliveryService.js`

**Duplications Identifiées** :

- Config streams locale
- Monitoring mémoire manuel
- processRetries() / processDLQ() manuels

**Estimé** : 2-3 heures

### 2. **MediaProcessingService** 📦

**Chemin** : `/chat-file-service/src/infrastructure/services/MediaProcessingService.js`

**Duplications Identifiées** :

- Config streams pour uploads
- Monitoring manuel

**Estimé** : 1-2 heures

### 3. **NotificationService** 🔔

**Chemin** : Si existe...

**Duplications Identifiées** :

- Gestion des événements via streams
- Monitoring des queues

**Estimé** : 1-2 heures

---

## Exemple Complet : ThumbnailService

### Avant

```javascript
class ThumbnailService {
  constructor(redisClient) {
    this.redis = redisClient;
    this.STREAMS = {
      JOBS: "thumbnail:jobs",
      RESULTS: "thumbnail:results",
    };
    this.startWorkers();
  }

  async startWorkers() {
    this.jobInterval = setInterval(() => this.processJobs(), 1000);
  }

  stopWorkers() {
    clearInterval(this.jobInterval);
  }
}
```

### Après

```javascript
const { StreamManager, WorkerManager } = require("@chatapp-ngomna/shared");

class ThumbnailService {
  constructor(redisClient) {
    this.redis = redisClient;

    // ✅ Utiliser le StreamManager
    this.streamManager = new StreamManager(this.redis);
    this.STREAMS = this.streamManager.STREAMS;

    // ✅ Utiliser le WorkerManager
    this.workerManager = new WorkerManager(this.streamManager, this.redis);
    this.initializeWorkers();
  }

  initializeWorkers() {
    const callbacks = {
      process: this.generateThumbnail.bind(this),
      dlq: this.handleFailed.bind(this),
    };
    this.workerManager.initialize(callbacks);
    this.workerManager.startAll();
  }

  async startWorkers() {
    this.workerManager.startAll();
  }

  stopWorkers() {
    this.workerManager.stopAll();
  }

  async addJob(mediaId, options) {
    return this.streamManager.addToStream(this.STREAMS.JOBS, {
      mediaId,
      options: JSON.stringify(options),
      timestamp: Date.now().toString(),
    });
  }

  getMetrics() {
    const workerMetrics = this.workerManager?.getAllMetrics() || {};
    return {
      service: {
        jobsProcessed: this.metrics.jobsProcessed || 0,
      },
      workers: workerMetrics.workers,
      uptime: workerMetrics.uptime,
    };
  }
}
```

---

## Avantages de la Migration

### Immédiats

✅ Code plus propre (-200+ lignes)  
✅ Maintenance centralisée  
✅ Une source de vérité

### À Long Terme

✅ Mise à jour simple (un seul endroit)  
✅ Monitoring cohérent  
✅ Résilience garantie  
✅ Performance optimisée

---

## Ordre Recommandé de Migration

1. **ResilientMessageService** ✅ (FAIT)
2. **MessageDeliveryService** ⏳
3. **MediaProcessingService** ⏳
4. **ThumbnailService** ⏳
5. **Autres services** ⏳

---

## Troubleshooting Migration

### Problème : "StreamManager not found"

```javascript
// Vérifier l'import
const { StreamManager, WorkerManager } = require("@chatapp-ngomna/shared");
```

### Problème : "Callbacks not working"

```javascript
// Vérifier que initializeWorkers() utilise bind()
const callbacks = {
  save: this.saveData.bind(this), // ✅ bind() requis
};
```

### Problème : "Old methods still called"

```javascript
// Remplacer les appels directs
// ❌ await this.redis.xAdd(...)
// ✅ await this.streamManager.addToStream(...)
```

---

## Ressources

- **StreamManager** : `shared/resilience/StreamManager.js`
- **WorkerManager** : `shared/redis/workers/WorkerManager.js`
- **CircuitBreaker** : `shared/resilience/CircuitBreaker.js`
- **Exemple** : `ResilientMessageService.js` (modèle de référence)

---

**Dernière Mise à Jour** : 2026-01-04  
**Auteur** : GitHub Copilot  
**Statut** : Prêt pour Migration



---
## Fichier d'origine : `DEPLOYMENT.md`

# Guide de déploiement multi-serveurs

## 📦 Architecture NPM

Le shared module est configuré pour être publié comme package NPM réel.

### Configuration actuelle

**shared/package.json**

```json
{
  "name": "@chatapp-ngomna/shared",
  "version": "1.0.0",
  "private": false
}
```

**chat-file-service/package.json**

```json
{
  "dependencies": {
    "@chatapp-ngomna/shared": "file:../shared" // Local development
  }
}
```

## 🚀 Déploiement (3 stratégies)

### Option 1 : npm Public (Recommandé pour production)

```bash
# 1️⃣ Publier le shared
cd shared
npm login
npm version patch  # Ou minor/major
npm publish

# 2️⃣ Mettre à jour chat-file-service
cd ../chat-file-service
# Dans package.json :
# "@chatapp-ngomna/shared": "^1.0.0"

npm install
npm start
```

**Avantages** ✅

- Réutilisable par tous les services
- Versionning sémantique
- Peut être sur serveurs différents
- Facile à maintenir

**Inconvénients** ❌

- Dépend de npmjs.org

---

### Option 2 : Registry Privée (Verdaccio local)

```bash
# 1️⃣ Installer Verdaccio
npm install -g verdaccio
verdaccio  # Démarre sur http://localhost:4873

# 2️⃣ Configurer npm
npm set registry http://localhost:4873/

# 3️⃣ Publier shared
cd shared
npm publish

# 4️⃣ Installer dans chat-file-service
cd ../chat-file-service
npm set registry http://localhost:4873/
npm install @chatapp-ngomna/shared
```

**Avantages** ✅

- Registry privée locale
- Pas dépendant d'internet
- Contrôle complet des versions
- Multi-serveurs possible

**Inconvénients** ❌

- Infrastructure supplémentaire à gérer
- Dépend du serveur Verdaccio

---

### Option 3 : Déploiement monolitique (Développement)

Utiliser `file:../shared` pour le développement local.

```bash
# Structure
chatapp-ngomna/
  ├── shared/
  ├── chat-file-service/
  ├── auth-service/
  └── group-service/

# Chaque service peut faire
# package.json: "@chatapp-ngomna/shared": "file:../shared"
```

**Avantages** ✅

- Zéro configuration
- Développement facile

**Inconvénients** ❌

- Pas possible sur serveurs différents
- Changements du shared = rebuild tous les services

---

## 🔄 Pipeline CI/CD avec npm

### GitHub Actions

```yaml
name: Publish Shared Module

on:
  push:
    branches: [main]
    paths:
      - "shared/**"

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - uses: actions/setup-node@v3
        with:
          node-version: "18"
          registry-url: "https://registry.npmjs.org"

      - run: cd shared && npm publish
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}

      - run: cd chat-file-service && npm install @chatapp-ngomna/shared@latest
```

---

## 📋 Checklist Déploiement Multi-Serveurs

### Avant production

- [ ] Créer compte npm ou Verdaccio
- [ ] Générer NPM_TOKEN
- [ ] Configurer `.npmrc` sur tous les serveurs
- [ ] Tester publication du shared
- [ ] Tester installation dans chat-file-service
- [ ] Valider les imports dans les services

### Configuration serveur

```bash
# Sur chaque serveur (chat-file-service)
cat > ~/.npmrc << EOF
@chatapp-ngomna:registry=https://registry.npmjs.org/
//registry.npmjs.org/:_authToken=YOUR_TOKEN_HERE
EOF

npm install
npm start
```

### Monitoring versions

```bash
# Vérifier version installée
npm list @chatapp-ngomna/shared

# Voir les mises à jour disponibles
npm outdated @chatapp-ngomna/shared

# Mettre à jour
npm update @chatapp-ngomna/shared
```

---

## 🐛 Troubleshooting

### "Cannot find module '@chatapp-ngomna/shared'"

**Solution 1** : Vérifier .npmrc

```bash
npm config list
cat ~/.npmrc
```

**Solution 2** : Réinstaller

```bash
rm -rf node_modules package-lock.json
npm install
```

**Solution 3** : Vérifier que le package est publié

```bash
npm view @chatapp-ngomna/shared versions
```

---

## 📈 Versioning

Après chaque changement dans shared :

```bash
cd shared

# Minor: Nouvelles fonctionnalités compatibles
npm version minor
npm publish

# Patch: Corrections de bugs
npm version patch
npm publish

# Major: Changements API incompatibles
npm version major
npm publish

cd ../chat-file-service
npm update @chatapp-ngomna/shared
```

---

## 🔗 Ressources

- [npm Scoped Packages](https://docs.npmjs.com/about/scoped-packages)
- [Verdaccio Documentation](https://verdaccio.org/)
- [npm Publishing](https://docs.npmjs.com/cli/v9/commands/npm-publish)

