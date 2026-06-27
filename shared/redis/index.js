/**
 * Export centralisé du module Redis
 */

// ✅ FACTORY - LE SEUL avec require("redis")
// const RedisFactory = require("./RedisFactory");
// const { RedisService, DEFAULT_CONFIG } = require("./RedisFactory");

// Configuration (legacy - pour compatibilité)
// const redisConfig = require("./redisConfig");

// ✅ MANAGER PRINCIPAL (SINGLETON INSTANCE)
const RedisManager = require("./RedisManager");

// Resilience
const CircuitBreaker = require("../resilience/CircuitBreaker");
const StreamManager = require("../resilience/StreamManager");

// ✅ MANAGERS REDIS (migrés depuis chat-file-service)
const CacheService = require("./managers/CacheService");
const OnlineUserManager = require("./managers/OnlineUserManager");
const RoomManager = require("./managers/RoomManager");
const UnreadMessageManager = require("./managers/UnreadMessageManager");

// Workers
const RetryWorker = require("./workers/RetryWorker");
const FallbackWorker = require("./workers/FallbackWorker");
const WALRecoveryWorker = require("./workers/WALRecoveryWorker");
const DLQMonitorWorker = require("./workers/DLQMonitorWorker");
const MemoryMonitorWorker = require("./workers/MemoryMonitorWorker");
const StreamMonitorWorker = require("./workers/StreamMonitorWorker");
const WorkerManager = require("./workers/WorkerManager");

module.exports = {
  // ✅ FACTORY - Point d'entrée recommandé
  // RedisFactory,
  // RedisService,
  // DEFAULT_CONFIG,

  // Configuration legacy
  // redisConfig,

  // ✅ MANAGER PRINCIPAL (L'INSTANCE SINGLETON, pas la classe)
  RedisManager, // ✅ C'est déjà une instance (ligne 525 de RedisManager.js)

  // ✅ MANAGERS REDIS
  CacheService,
  OnlineUserManager,
  RoomManager,
  UnreadMessageManager,
  
  // Résilience
  CircuitBreaker,
  StreamManager,
  
  // Workers
  RetryWorker,
  FallbackWorker,
  WALRecoveryWorker,
  DLQMonitorWorker,
  MemoryMonitorWorker,
  StreamMonitorWorker,
  WorkerManager,
};
