
let LocalPermissionStore = require('./local-permission.store.js');
let TokenValidator = require('./valide-token.middleware.js');
let PermissionChecker = require('./require-permission.middleware.js');
const PermissionStreamConsumer = require('./permission-stream.consumer.js');
const { RedisManager } = require('../index.js');

/**
 * Initialise le système d'authentification et d'autorisation
 * 
 * @param {Object} options Options de configuration
 */
function createAuthSystem(options = {}) {
  // 1. Créer le store local SANS expiration automatique (event-driven sync)
  const store = new LocalPermissionStore({
    ttlMs: options.ttlMs || 24 * 60 * 60 * 1000,      // 24h (très long, quasi permanent)
    cleanupIntervalMs: options.cleanupIntervalMs || 0  // Pas de nettoyage automatique
  });
  
  // 2. Créer le validateur de token
  const tokenValidator = new TokenValidator({
    secret: options.jwtSecret || process.env.JWT_SECRET
  });
  
  // 3. Créer le vérificateur de permissions
  const permissionChecker = new PermissionChecker(store, {
    fallbackEnabled: options.fallbackEnabled || false,
    fallbackFetcher: options.fallbackFetcher || null
  });
  
  
  // 4. Créer le consumer si redisClient est fourni
  let permissionConsumer = null;
  
  if ( options.serviceName) {   //options.redisClient && options.serviceName
    permissionConsumer = new PermissionStreamConsumer(
      // options.redisClient,
      RedisManager.getStreamClient(), // Utiliser un client dédié pour le stream
      store,
      options.serviceName
    );
  }
  
  return {
    // Store pour synchronisation via Redis consumer
    store,
    permissionConsumer,
    
    // Middlewares
    valideToken: () => tokenValidator.valideToken(),
    valideTokenOptional: () => tokenValidator.valideTokenOptional(),
    requirePermission: (permission) => permissionChecker.requirePermission(permission),
    requireAnyPermission: (permissions) => permissionChecker.requireAnyPermission(permissions),
    requireAllPermissions: (permissions) => permissionChecker.requireAllPermissions(permissions),
    requireRole: (role) => permissionChecker.requireRole(role),
    requireAnyRole: (roles) => permissionChecker.requireAnyRole(roles),
    requireScope: (domain, getCible) => permissionChecker.requireScope(domain, getCible),
    
    // Helpers pour controllers
    hasExtendedAccess: (matricule, domain) => permissionChecker.hasExtendedAccess(matricule, domain),
    getScopeFilter: (matricule, domain) => permissionChecker.getScopeFilter(matricule, domain)
  };
}

module.exports = {
  createAuthSystem,
  LocalPermissionStore,
  TokenValidator,
  PermissionChecker
};