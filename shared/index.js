/**
 * Export centralisé du module shared
 * @chatapp-ngomna/shared
 */

// Redis
const redis = require("./redis");

// Resilience
const resilience = require("./resilience");

// User
const user = require("./user");

module.exports = {
  // Redis exports

  RedisManager: redis.RedisManager,

  DEFAULT_CONFIG: redis.DEFAULT_CONFIG,

  // ✅ MANAGERS REDIS
  CacheService: redis.CacheService,
  OnlineUserManager: redis.OnlineUserManager,
  RoomManager: redis.RoomManager,
  UnreadMessageManager: redis.UnreadMessageManager,

  // Resilience exports
  CircuitBreaker: resilience.CircuitBreaker,
  StreamManager: resilience.StreamManager,

  // Workers
  WorkerManager: redis.WorkerManager,
  RetryWorker: redis.RetryWorker,
  FallbackWorker: redis.FallbackWorker,
  WALRecoveryWorker: redis.WALRecoveryWorker,
  DLQMonitorWorker: redis.DLQMonitorWorker,
  MemoryMonitorWorker: redis.MemoryMonitorWorker,
  StreamMonitorWorker: redis.StreamMonitorWorker,

  // ✅ USER CACHE & STREAMS
  UserCache: user.UserCache,
  UserStreamConsumer: user.UserStreamConsumer,

  // Namespaces
  redis,
  resilience,
  user,
};
