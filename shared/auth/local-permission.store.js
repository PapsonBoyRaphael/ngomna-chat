
// permettre que pour toutes les operations, que le prefixe rbac: soit utilise pour stocker les elements dans le cache. exemple de cle : 'rbac:1187225C'
class LocalPermissionStore {
  constructor(options = {}) {
    //Structure de donnees du store
    // Map: rbac:matricule -> { permissions: Set<string>, roles: Set<string> }
    this.users = new Map();
    this.lastSync = null;
    
    // Configuration de l'expiration (24h par défaut = quasi permanent)
    this.ttlMs = options.ttlMs || 24 * 60 * 60 * 1000; // 24 heures
    this.cleanupIntervalMs = options.cleanupIntervalMs || 0; // Pas de nettoyage auto par défaut
    
    // Statistiques pour monitoring
    this.stats = {
      hits: 0,
      misses: 0,
      fallbacks: 0,
      eventsReceived: 0
    };
    
    // Démarrer le nettoyage automatique seulement si configuré
    if (this.cleanupIntervalMs > 0) {
      this.cleanupInterval = setInterval(() => this.cleanup(), this.cleanupIntervalMs);
      console.log(`🔄 [LocalStore] Nettoyage automatique configuré: ${this.cleanupIntervalMs}ms`);
    } else {
      console.log(`♾️  [LocalStore] Mode persistant activé (pas d'expiration automatique)`);
    }
  }

  /**
   * Définit les permissions et rôles d'un utilisateur (snapshot complet)
   */
  setUser(matricule, data) {
    // Map: rbac:matricule -> { permissions: Set<string>, roles: Set<string> }
    const isUpdate = this.users.has(`rbac:${matricule}`);
    const syncSource = data.syncSource || 'event'; // 'event', 'fallback', 'manual'
    
    this.users.set(`rbac:${matricule}`, {
      permissions: new Set(data.permissions || []),
      roles: new Set(data.roles || []),
      updatedAt: Date.now(),
      lastAccess: Date.now(),
      syncSource: syncSource
    });
    
    if (isUpdate) {
      if (syncSource === 'event') {
        this.stats.eventsReceived++;
      }
      console.log(`🔄 [LocalStore] Mise à jour via ${syncSource}: ${matricule}`);
    } else {
      console.log(`➕ [LocalStore] Nouvelle entrée via ${syncSource}: ${matricule}`);
    }
  }

  /**
   * Définit uniquement les permissions d'un utilisateur
   */
  setUserPermissions(matricule, permissions) {
    console.log(`🔄 [PERMISSION] Mise à jour ${matricule}`);
    const existing = this.users.get(`rbac:${matricule}`) || { roles: new Set() };
    this.users.set(`rbac:${matricule}`, {
      permissions: new Set(permissions),
      roles: existing.roles,
      updatedAt: Date.now()
    });
  }
  
  /**
   * Définit uniquement les rôles d'un utilisateur
   */
  setUserRoles(matricule, roles) {
    console.log(`🔄 [ROLE] Mise à jour ${matricule}`);
    const existing = this.users.get(`rbac:${matricule}`) || { permissions: new Set() };
    this.users.set(`rbac:${matricule}`, {
      permissions: existing.permissions,
      roles: new Set(roles),
      updatedAt: Date.now()
    });
  }

  /**
   * Ajoute des permissions à un utilisateur
   */
  addPermissions(matricule, permissions) {
    const user = this.users.get(`rbac:${matricule}`) || { 
      permissions: new Set(), 
      roles: new Set() 
    };
    permissions.forEach(p => user.permissions.add(p));
    user.updatedAt = Date.now();
    this.users.set(`rbac:${matricule}`, user);
  }

  /**
   * Retire des permissions à un utilisateur
   */
  removePermissions(matricule, permissions) {
    const user = this.users.get(`rbac:${matricule}`);
    if (user) {
      permissions.forEach(p => user.permissions.delete(p));
      user.updatedAt = Date.now();
    }
  }

  /**
   * Vérifie si un utilisateur a une permission
   */
  hasPermission(matricule, permission) {
    const user = this.users.get(`rbac:${matricule}`);
    return user ? user.permissions.has(permission) : false;
  }

  /**
   * Vérifie si un utilisateur a au moins une des permissions
   */
  hasAnyPermission(matricule, permissions) {
    const user = this.users.get(`rbac:${matricule}`);
    if (!user) return false;
    return permissions.some(p => user.permissions.has(p));
  }

  /**
   * Vérifie si un utilisateur a toutes les permissions
   */
  hasAllPermissions(matricule, permissions) {
    const user = this.users.get(`rbac:${matricule}`);
    if (!user) return false;
    return permissions.every(p => user.permissions.has(p));
  }

  /**
   * Vérifie si un utilisateur a un rôle
   */
  hasRole(matricule, role) {
    const user = this.users.get(`rbac:${matricule}`);
    return user ? user.roles.has(role) : false;
  }

  /**
   * Vérifie si un utilisateur a au moins un des rôles
   */
  hasAnyRole(matricule, roles) {
    const user = this.users.get(`rbac:${matricule}`);
    if (!user) return false;
    return roles.some(r => user.roles.has(r));
  }
  
  /**
   * Récupère toutes les permissions d'un utilisateur
   */
  getPermissions(matricule) {
    const user = this.users.get(`rbac:${matricule}`);
    return user ? Array.from(user.permissions) : [];
  }

  /**
   * Récupère tous les rôles d'un utilisateur
   */
  getRoles(matricule) {
    const user = this.users.get(`rbac:${matricule}`);
    return user ? Array.from(user.roles) : [];
  }

  /**
   * Vérifie si un utilisateur existe dans le store et n'est pas expiré
   */
  hasUser(matricule) {
    if (!this.users.has(`rbac:${matricule}`)) {
      this.stats.misses++;
      return false;
    }
    
    // Vérifier l'expiration (rare en mode persistant)
    const user = this.users.get(`rbac:${matricule}`);
    const isExpired = Date.now() - user.updatedAt > this.ttlMs;
    
    if (isExpired) {
      this.users.delete(`rbac:${matricule}`);
      console.log(`⏰ [LocalStore] Données expirées supprimées pour ${matricule} (âge: ${Math.round((Date.now() - user.updatedAt) / 1000 / 60)} min)`);
      this.stats.misses++;
      return false;
    }
    
    // Mettre à jour lastAccess pour statistiques
    user.lastAccess = Date.now();
    this.stats.hits++;
    return true;
  }

  /**
   * Supprime un utilisateur du store
   */
  removeUser(matricule) {
    this.users.delete(`rbac:${matricule}`);
  }

  /**
   * Vide le store
   */
  clear() {
    this.users.clear();
  }

  /**
   * Marquer un fallback (appelé depuis require-permission.middleware.js)
   */
  markFallback(matricule) {
    this.stats.fallbacks++;
    console.log(`🔄 [LocalStore] Fallback DB exécuté pour ${matricule} (total: ${this.stats.fallbacks})`);
  }
  
  /**
   * Statistiques du store avec ratios
   */
  getStats() {
    const total = this.stats.hits + this.stats.misses;
    const hitRatio = total > 0 ? ((this.stats.hits / total) * 100).toFixed(1) : 0;
    
    return {
      usersCount: this.users.size,
      lastSync: this.lastSync,
      performance: {
        hits: this.stats.hits,
        misses: this.stats.misses,
        hitRatio: `${hitRatio}%`,
        fallbacks: this.stats.fallbacks,
        eventsReceived: this.stats.eventsReceived
      }
    };
  }

  /**
   * Nettoyage automatique des données expirées
   */
  cleanup() {
    const now = Date.now();
    let cleanedCount = 0;
    
    for (const [matricule, user] of this.users) {
      if (now - user.updatedAt > this.ttlMs) {
        this.users.delete(matricule);
        cleanedCount++;
      }
    }
    
    if (cleanedCount > 0) {
      console.log(`🧹 [LocalStore] ${cleanedCount} entrée(s) expirée(s) nettoyée(s)`);
    }
  }
  
  /**
   * Arrêter le nettoyage automatique (pour shutdown propre)
   */
  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
      console.log('🛑 [LocalStore] Nettoyage automatique arrêté');
    }
  }

  /**
   * Export pour debug
   */
  toJSON() {
    const result = {};
    for (const [matricule, data] of this.users) {
      const ageMs = Date.now() - data.updatedAt;
      const remainingMs = Math.max(0, this.ttlMs - ageMs);
      
      result[matricule] = {
        permissions: Array.from(data.permissions),
        roles: Array.from(data.roles),
        updatedAt: data.updatedAt,
        ageMs: ageMs,
        remainingMs: remainingMs,
        expired: ageMs > this.ttlMs
      };
    }
    return result;
  }
}

module.exports = LocalPermissionStore;