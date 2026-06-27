class HealthController {
  constructor(
    redisClient = null,
    kafkaConfig = null,
    onlineUserManager = null,
    roomManager = null
  ) {
    this.redisClient = redisClient;
    this.kafkaConfig = kafkaConfig;
    this.onlineUserManager = onlineUserManager;
    this.roomManager = roomManager;

    console.log("✅ HealthController initialisé avec:", {
      redis: !!redisClient,
      kafka: !!kafkaConfig,
      onlineUserManager: !!onlineUserManager,
      roomManager: !!roomManager,
    });
  }

  // ✅ MÉTHODE PRINCIPALE ATTENDUE PAR LES ROUTES
  async getHealth(req, res) {
    try {
      const startTime = Date.now();

      // Vérifier MongoDB
      const mongoHealth = await this.checkMongoDB();

      // Vérifier Redis
      const redisHealth = await this.checkRedis();

      // Vérifier Kafka
      const kafkaHealth = await this.checkKafka();

      // Vérifier le service utilisateurs
      const userServiceHealth = await this.checkUserService();

      const processingTime = Date.now() - startTime;

      const overallStatus = this.determineOverallStatus([
        mongoHealth,
        redisHealth,
        kafkaHealth,
        userServiceHealth,
      ]);

      const healthData = {
        service: "CENADI Chat-File-Service",
        version: "1.0.0",
        status: overallStatus,
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        processingTime: `${processingTime}ms`,
        services: {
          mongodb: mongoHealth,
          redis: redisHealth,
          kafka: kafkaHealth,
          userService: userServiceHealth,
        },
        system: {
          nodeVersion: process.version,
          platform: process.platform,
          arch: process.arch,
          memory: {
            used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
            total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
            external: Math.round(process.memoryUsage().external / 1024 / 1024),
          },
          cpu: process.cpuUsage(),
        },
        endpoints: {
          files: "/files",
          messages: "/messages",
          conversations: "/conversations",
          health: "/health",
        },
      };

      // Statut HTTP basé sur la santé globale
      const statusCode =
        overallStatus === "healthy"
          ? 200
          : overallStatus === "degraded"
          ? 207
          : 503;

      res.status(statusCode).json(healthData);
    } catch (error) {
      console.error("❌ Erreur health check:", error);
      res.status(500).json({
        service: "CENADI Chat-File-Service",
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      });
    }
  }

  async checkMongoDB() {
    try {
      const mongoose = require("mongoose");

      if (mongoose.connection.readyState === 1) {
        // Tester une requête simple
        await mongoose.connection.db.admin().ping();

        return {
          status: "healthy",
          message: "Connecté et opérationnel",
          responseTime: "< 10ms",
          details: {
            readyState: "connected",
            host: mongoose.connection.host,
            name: mongoose.connection.name,
          },
        };
      } else {
        return {
          status: "error",
          message: "Non connecté",
          details: {
            readyState: mongoose.connection.readyState,
          },
        };
      }
    } catch (error) {
      return {
        status: "error",
        message: error.message,
        details: { error: error.name },
      };
    }
  }

  async checkRedis() {
    if (!this.redisClient) {
      return {
        status: "disabled",
        message: "Redis non configuré",
        details: { mode: "memory-only" },
      };
    }

    try {
      const start = Date.now();
      await this.redisClient.ping();
      const responseTime = Date.now() - start;

      return {
        status: "healthy",
        message: "Connecté et opérationnel",
        responseTime: `${responseTime}ms`,
        details: {
          connected: true,
          ready: this.redisClient.status === "ready",
        },
      };
    } catch (error) {
      return {
        status: "error",
        message: error.message,
        details: {
          connected: false,
          error: error.name,
        },
      };
    }
  }

  async checkKafka() {
    if (!this.kafkaConfig) {
      return {
        status: "disabled",
        message: "Kafka non configuré",
        details: { mode: "development" },
      };
    }

    try {
      const healthStatus = await this.kafkaConfig.getHealthStatus();

      if (healthStatus.status === "connected") {
        return {
          status: "healthy",
          message: "Connecté et opérationnel",
          details: {
            topics: healthStatus.topics || 0,
            brokers: healthStatus.brokers || 0,
            connected: true,
          },
        };
      } else if (healthStatus.status === "error") {
        return {
          status: "error",
          message: healthStatus.error,
          details: { connected: false },
        };
      } else {
        return {
          status: "error",
          message: "Non connecté",
          details: { connected: false },
        };
      }
    } catch (error) {
      return {
        status: "error",
        message: error.message,
        details: {
          connected: false,
          error: error.name,
        },
      };
    }
  }

  async checkUserService() {
    try {
      const start = Date.now();
      const response = await fetch("http://localhost:8000/api/users/all", {
        method: "GET",
        headers: { "Content-Type": "application/json" },
        signal: AbortSignal.timeout(5000), // Timeout de 5 secondes
      });

      const responseTime = Date.now() - start;

      if (response.ok) {
        const data = await response.json();

        return {
          status: "healthy",
          message: "Service utilisateurs opérationnel",
          responseTime: `${responseTime}ms`,
          details: {
            url: "http://localhost:8000/api/users/all",
            statusCode: response.status,
            usersCount: data.data?.length || 0,
          },
        };
      } else {
        return {
          status: "degraded",
          message: `Service utilisateurs retourne HTTP ${response.status}`,
          responseTime: `${responseTime}ms`,
          details: {
            url: "http://localhost:8000/api/users/all",
            statusCode: response.status,
          },
        };
      }
    } catch (error) {
      return {
        status: "error",
        message: "Service utilisateurs inaccessible",
        details: {
          url: "http://localhost:8000/api/users/all",
          error: error.message,
          type: error.name,
        },
      };
    }
  }

  // ✅ MÉTHODE POUR DÉTERMINER LE STATUT GLOBAL
  determineOverallStatus(healthChecks) {
    const errorCount = healthChecks.filter((h) => h.status === "error").length;
    const degradedCount = healthChecks.filter(
      (h) => h.status === "degraded"
    ).length;

    if (errorCount > 0) {
      return "error";
    } else if (degradedCount > 0) {
      return "degraded";
    } else {
      return "healthy";
    }
  }

  async getDetailedHealth(req, res) {
    try {
      const health = await this.getHealth(req, {
        status: () => ({ json: (data) => data }),
        json: (data) => data,
      });

      // Ajouter des métriques supplémentaires
      const detailedHealth = {
        ...health,
        metrics: {
          requestsPerSecond: 0, // À implémenter
          averageResponseTime: 0, // À implémenter
          errorRate: 0, // À implémenter
          activeConnections: 0, // À implémenter
        },
        dependencies: [
          {
            name: "MongoDB",
            required: true,
            status: health.services.mongodb.status,
          },
          {
            name: "Redis",
            required: false,
            status: health.services.redis.status,
          },
          {
            name: "Kafka",
            required: false,
            status: health.services.kafka.status,
          },
          {
            name: "User Service",
            required: true,
            status: health.services.userService.status,
          },
        ],
      };

      res.json(detailedHealth);
    } catch (error) {
      res.status(500).json({
        error: "Erreur lors de la vérification de santé détaillée",
        message: error.message,
      });
    }
  }

  // ✅ MÉTHODES COMPATIBLES AVEC L'INDEX.JS
  async getHealthStatus() {
    try {
      const mongoHealth = await this.checkMongoDB();
      const redisHealth = await this.checkRedis();
      const kafkaHealth = await this.checkKafka();
      const userServiceHealth = await this.checkUserService();

      return {
        status: this.determineOverallStatus([
          mongoHealth,
          redisHealth,
          kafkaHealth,
          userServiceHealth,
        ]),
        services: {
          mongodb: mongoHealth,
          redis: redisHealth,
          kafka: kafkaHealth,
          userService: userServiceHealth,
        },
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      return {
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      };
    }
  }

  async getMongoDBStatus(req, res) {
    try {
      const mongoHealth = await this.checkMongoDB();
      res.json({
        service: "MongoDB",
        ...mongoHealth,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      res.status(500).json({
        service: "MongoDB",
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      });
    }
  }

  async getRedisStatus(req, res) {
    try {
      const redisHealth = await this.checkRedis();
      res.json({
        service: "Redis",
        ...redisHealth,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      res.status(500).json({
        service: "Redis",
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      });
    }
  }

  async getKafkaStatus(req, res) {
    try {
      const kafkaHealth = await this.checkKafka();
      res.json({
        service: "Kafka",
        ...kafkaHealth,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      res.status(500).json({
        service: "Kafka",
        status: "error",
        error: error.message,
        timestamp: new Date().toISOString(),
      });
    }
  }
}

module.exports = HealthController;
