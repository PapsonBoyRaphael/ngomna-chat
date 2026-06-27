// src/auth/require-permission.middleware.js

class PermissionChecker {
  constructor(localStore, options = {}) {
    this.store = localStore;
    this.fallbackEnabled = options.fallbackEnabled || false;
    this.fallbackFetcher = options.fallbackFetcher || null; // Fonction async pour fetch HTTP si store vide
  }
  
  /**
   * Middleware : vérifie une permission spécifique
   */
  requirePermission(permission) {
    return async (req, res, next) => {
      const result = await this._checkAccess(req, res, () => {
        return this.store.hasPermission(req.user.matricule, permission);
      });
      
      if (result === false) {
        return res.status(403).json({
          success: false,
          error: 'Permission insuffisante',
          required: permission,
          code: 'INSUFFICIENT_PERMISSION'
        });
      }
      
      if (result === true) {
        return next();
      }
      // Si result est une Response, elle a déjà été envoyée
    };
  }
  
  /**
   * Middleware : vérifie au moins une des permissions (OR)
   */
  requireAnyPermission(permissions) {
    return async (req, res, next) => {
      const result = await this._checkAccess(req, res, () => {
        return this.store.hasAnyPermission(req.user.matricule, permissions);
      });
      
      if (result === false) {
        return res.status(403).json({
          success: false,
          error: 'Aucune permission valide',
          required_any: permissions,
          code: 'NO_VALID_PERMISSION'
        });
      }

      if (result === true) {
        return next();
      }
    };
  }

  /**
   * Middleware : vérifie toutes les permissions (AND)
   */
  requireAllPermissions(permissions) {
    return async (req, res, next) => {
      const result = await this._checkAccess(req, res, () => {
        return this.store.hasAllPermissions(req.user.matricule, permissions);
      });

      if (result === false) {
        const missing = permissions.filter(
          p => !this.store.hasPermission(req.user.matricule, p)
        );
        return res.status(403).json({
          success: false,
          error: 'Permissions insuffisantes',
          required_all: permissions,
          missing: missing,
          code: 'INSUFFICIENT_PERMISSIONS'
        });
      }

      if (result === true) {
        return next();
      }
    };
  }

  /**
   * Middleware : vérifie un rôle spécifique
   */
  requireRole(role) {
    return async (req, res, next) => {
      var r = this.store.hasRole(req.user.matricule, role);
      var lesroles = this.store.getRoles(req.user.matricule);
      console.log('===================user roles found in Redis store:', lesroles);
      // var ress = this.store.getPermissions(req.user.matricule);
      // console.log('===================required role:,', r);
      // console.log('===================user roles found in Redis store:,', lesroles);
      // console.log('===================user permissions found in Redis store:,', ress);
      const result = await this._checkAccess(req, res, () => {
        return this.store.hasRole(req.user.matricule, role);
      });
      
      if (result === false) {
        console.log('===================required role:,', r);
        return res.status(403).json({
          success: false,
          error: 'Rôle insuffisant',
          required: role,
          code: 'INSUFFICIENT_ROLE'
        });
      }

      if (result === true) {
        return next();
      }
    };
  }

  /**
   * Middleware : vérifie au moins un des rôles (OR)
   */
  requireAnyRole(roles) {
    return async (req, res, next) => {
      const result = await this._checkAccess(req, res, () => {
        return this.store.hasAnyRole(req.user.matricule, roles);
      });
      
      if (result === false) {
        return res.status(403).json({
          success: false,
          error: 'Aucun rôle valide',
          required_any: roles,
          code: 'NO_VALID_ROLE'
        });
      }

      if (result === true) {
        return next();
      }
    };
  }

  /**
   * Middleware : vérifie le scope (accès étendu)
   */
  requireScope(domain, getCible) {
    return async (req, res, next) => {
      const matricule = req.user?.matricule;
      const cible = getCible(req);

      // Pas de cible = action globale = accès étendu requis
      if (!cible) {
        const hasExtended = this.store.hasPermission(
          matricule, 
          `${domain}:acces_etendu`
        );

        if (!hasExtended) {
          return res.status(403).json({
            success: false,
            error: 'Accès étendu requis',
            code: 'EXTENDED_ACCESS_REQUIRED'
          });
        }
        return next();
      }

      // Même personne = OK
      if (matricule === cible) {
        return next();
      }

      // Personne différente = vérifier accès étendu
      const hasExtended = this.store.hasPermission(
        matricule, 
        `${domain}:acces_etendu`
      );

      if (!hasExtended) {
        return res.status(403).json({
          success: false,
          error: 'Vous ne pouvez effectuer cette action que pour vous-même',
          code: 'SCOPE_RESTRICTED'
        });
      }

      next();
    };
  }

  /**
   * Helper interne pour vérifier l'accès
   */
  async _checkAccess(req, res, checkFn) {
    // 1. Vérifier que l'utilisateur est authentifié
    if (!req.user?.matricule) {
      res.status(401).json({
        success: false,
        error: 'Utilisateur non authentifié',
        code: 'NOT_AUTHENTICATED'
      });
      return null; // Indique que la réponse a été envoyée
    }
    // For testing purposes
    // this.store.clear();
    const matricule = req.user.matricule;

    // 2. Si l'utilisateur n'est pas dans le store, tenter un fallback
    if (!this.store.hasUser(matricule)) {
      if (this.fallbackEnabled && this.fallbackFetcher) {
        try {
          const userData = await this.fallbackFetcher(matricule);
          // console.log('🔍 Fallback response:', userData);
          if (userData) {
            //  console.log('📊 Permissions received:', userData.permissions);
            //  console.log('👥 Roles received:', userData.roles);
            this.store.setUser(matricule, userData);
          }
        } catch (error) {
          console.error(`[PermissionChecker] Fallback failed for ${matricule}:`, error);
        }
      }

      // Si toujours pas dans le store après fallback
      if (!this.store.hasUser(matricule)) {
        console.warn(`[PermissionChecker] User ${matricule} not in store`);
        // Fail-secure : refuser l'accès
        return false;
      }
    }

    // 3. Vérifier l'accès
    return checkFn();
  }
  
  /**
   * Helper pour vérifier si accès étendu (utile dans les controllers)
   */
  hasExtendedAccess(matricule, domain) {
    return this.store.hasPermission(matricule, `${domain}:acces_etendu`);
  }

  /**
   * Helper pour filtrer les requêtes selon le scope
   */
  getScopeFilter(matricule, domain) {
    const fullAccess = this.hasExtendedAccess(matricule, domain);
    return {
      fullAccess,
      matriculeFilter: fullAccess ? null : matricule
    };
  }
}

module.exports = PermissionChecker;