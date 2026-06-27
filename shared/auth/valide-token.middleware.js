// src/auth/valide-token.middleware.js

const jwt = require('jsonwebtoken');

class TokenValidator {
  constructor(options = {}) {
    this.secret = options.secret || process.env.JWT_SECRET;
    this.algorithms = options.algorithms || ['HS256'];
    
    if (!this.secret) {
      throw new Error('JWT_SECRET is required');
    }
  }

  /**
   * Middleware de validation du token
   */
  valideToken() {
    return (req, res, next) => {
      try {
        // 1. Extraire le token
        const authHeader = req.headers.authorization || req.headers['authorization'];
        console.log('----------------------------DANS ValidateToken middleware-----------');
        // console.log('🔍 Headers reçus:', Object.keys(req.headers));
        console.log('🔍 Authorization header:', authHeader);
        console.log('🔍 URL appelée:', req.method, req.url);
        
        if (!authHeader) {
          return res.status(401).json({
            success: false,
            error: 'Token manquant',
            code: 'TOKEN_MISSING'
          });
        }
        
        const parts = authHeader.split(' ');
        
        if (parts.length !== 2 || parts[0] !== 'Bearer') {
          return res.status(401).json({
            success: false,
            error: 'Format de token invalide',
            code: 'TOKEN_FORMAT_INVALID'
          });
        }

        const token = parts[1];

        // 2. Vérifier et décoder le token
        const decoded = jwt.verify(token, this.secret, {
          algorithms: this.algorithms
        });

        // 3. Vérifier les champs requis
        if (!decoded.matricule) {
          return res.status(401).json({
            success: false,
            error: 'Token invalide: matricule manquant',
            code: 'TOKEN_INVALID_PAYLOAD'
          });
        }

        // 4. Attacher l'utilisateur à la requête
        req.user = {
          matricule: decoded.matricule,
        //   nom: decoded.nom || null,
        //   prenom: decoded.prenom || null,
        //   tokenType: decoded.type || 'access',
        //   iat: decoded.iat,
        //   exp: decoded.exp
        };
        console.log('==================Utilisateur connecte:', decoded.matricule);
        
        next();
      } catch (error) {
        if (error.name === 'TokenExpiredError') {
          return res.status(401).json({
            success: false,
            error: 'Token expiré',
            code: 'TOKEN_EXPIRED'
          });
        }

        if (error.name === 'JsonWebTokenError') {
          return res.status(401).json({
            success: false,
            error: 'Token invalide',
            code: 'TOKEN_INVALID'
          });
        }

        console.error('[TokenValidator] Unexpected error:', error);
        return res.status(500).json({
          success: false,
          error: 'Erreur de validation du token',
          code: 'TOKEN_VALIDATION_ERROR'
        });
      }
    };
  }

  /**
   * Middleware optionnel - n'échoue pas si pas de token
   */
  valideTokenOptional() {
    return (req, res, next) => {
      const authHeader = req.headers.authorization;
      
      if (!authHeader) {
        req.user = null;
        return next();
      }

      // Utiliser le middleware principal
      return this.valideToken()(req, res, next);
    };
  }
}

module.exports = TokenValidator;