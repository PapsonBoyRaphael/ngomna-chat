const express = require("express");
const  rateLimitMiddleware  = require("../middleware/rateLimitMiddleware");

function createHealthRoutes(healthController) {
  const router = express.Router();

  // **VALIDATION CRITIQUE : S'ASSURER QUE LE CONTRÔLEUR EXISTE**
  if (!healthController) {
    console.error("❌ HealthController manquant dans createHealthRoutes");
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de santé temporairement indisponible",
        error: "HealthController non initialisé",
      });
    });
    return router;
  }

  // **VALIDATION DES MÉTHODES DU CONTRÔLEUR**
  const requiredMethods = [
    "getHealth",
    "checkMongoDB",
    "checkRedis",
    "getDetailedHealth",
  ];
  const missingMethods = requiredMethods.filter(
    (method) => typeof healthController[method] !== "function"
  );

  if (missingMethods.length > 0) {
    console.error(
      `❌ Méthodes manquantes dans HealthController: ${missingMethods.join(
        ", "
      )}`
    );
    router.all("*", (req, res) => {
      res.status(503).json({
        success: false,
        message: "Service de santé incomplet",
        error: `Méthodes manquantes: ${missingMethods.join(", ")}`,
      });
    });
    return router;
  }

  /**
   * @api {get} /health Health check complet
   * @apiName HealthCheck
   * @apiGroup Health
   */
  router.get("/", rateLimitMiddleware.healthLimit, async (req, res) => {
    try {
      // ✅ CORRIGER: Utiliser getHealth au lieu de getHealthStatus
      await healthController.getHealth(req, res);
    } catch (error) {
      console.error("❌ Erreur route GET /health:", error);
      res.status(500).json({
        success: false,
        message: "Erreur lors de la vérification de santé",
        error: error.message,
      });
    }
  });

  /**
   * @api {get} /health/mongodb État MongoDB
   * @apiName MongoDBHealth
   * @apiGroup Health
   */
  router.get("/mongodb", rateLimitMiddleware.healthLimit, async (req, res) => {
    try {
      const mongoHealth = await healthController.checkMongoDB();
      res.json({
        service: "MongoDB",
        ...mongoHealth,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      console.error("❌ Erreur MongoDB health:", error);
      res.status(500).json({
        success: false,
        message: "Erreur vérification MongoDB",
        error: error.message,
      });
    }
  });

  /**
   * @api {get} /health/redis État Redis
   * @apiName RedisHealth
   * @apiGroup Health
   */
  router.get("/redis", rateLimitMiddleware.healthLimit, async (req, res) => {
    try {
      const redisHealth = await healthController.checkRedis();
      res.json({
        service: "Redis",
        ...redisHealth,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      console.error("❌ Erreur Redis health:", error);
      res.status(500).json({
        success: false,
        message: "Erreur vérification Redis",
        error: error.message,
      });
    }
  });
  

  /**
   * @api {get} /health/detailed Health check détaillé avec métriques
   * @apiName DetailedHealthCheck
   * @apiGroup Health
   */
  router.get("/detailed", rateLimitMiddleware.healthLimit, async (req, res) => {
    try {
      await healthController.getDetailedHealth(req, res);
    } catch (error) {
      console.error("❌ Erreur detailed health:", error);
      res.status(500).json({
        success: false,
        message: "Erreur lors de la vérification détaillée",
        error: error.message,
      });
    }
  });
  
  /**
   * @api {get} /health/redis-keys Voir les clés et valeurs du cache Redis
   * @apiName RedisKeys
   * @apiGroup Health
   */
  router.get("/redis-keys", async (req, res) => {
    try {
      // Récupérer le client Redis depuis app.locals ou le controller
      const redisClient =
        req.app?.locals?.redisClient ||
        (healthController.redisClient ? healthController.redisClient : null);

      if (!redisClient) {
        return res.status(503).json({
          success: false,
          message: "Client Redis non disponible",
        });
      }

      // Récupérer toutes les clés (attention: peut être lent si beaucoup de clés)
      const keys = await redisClient.keys("*");
      const result = {};

      // Limiter à 100 clés pour éviter les ralentissements
      const limitedKeys = keys.slice(0, 100);

      for (const key of limitedKeys) {
        try {
          const value = await redisClient.get(key);
          result[key] = value;
        } catch (err) {
          result[key] = `⚠️ Erreur lecture: ${err.message}`;
        }
      }

      res.json({
        success: true,
        totalKeys: keys.length,
        keys: limitedKeys,
        values: result,
        warning:
          keys.length > 100
            ? "Limité à 100 clés pour la performance"
            : undefined,
      });
    } catch (error) {
      console.error("❌ Erreur lecture clés Redis:", error);
      res.status(500).json({
        success: false,
        message: "Erreur lors de la lecture des clés Redis",
        error: error.message,
      });
    }
  });

  /**
   * @api {post} /health/redis-flush Réinitialiser tout le cache Redis
   * @apiName RedisFlush
   * @apiGroup Health
   */
  router.post("/redis-flush", async (req, res) => {
    try {
      // Récupérer le client Redis depuis app.locals ou le controller
      const redisClient =
        req.app?.locals?.redisClient ||
        (healthController.redisClient ? healthController.redisClient : null);

      if (!redisClient) {
        return res.status(503).json({
          success: false,
          message: "Client Redis non disponible",
        });
      }

      // Suppression complète de toutes les données Redis
      await redisClient.flushDb();

      res.json({
        success: true,
        message: "Cache Redis vidé avec succès (flushDb)",
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      console.error("❌ Erreur flushDb Redis:", error);
      res.status(500).json({
        success: false,
        message: "Erreur lors du flushDb Redis",
        error: error.message,
      });
    }
  });

  return router;
}

module.exports = createHealthRoutes;
