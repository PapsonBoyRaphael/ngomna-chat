const rateLimit = require("express-rate-limit");

// Configuration de base
const createLimiter = (windowMs, max, message) => {
  return rateLimit({
    windowMs,
    max,
    message: {
      success: false,
      message,
      code: "RATE_LIMIT_EXCEEDED",
    },
    standardHeaders: true,
    legacyHeaders: false,
    skip: (req) => {
      // Exclure les requêtes de health check
      return req.path === "/health" || req.path === "/api/health";
    },
  });
};

class RateLimitMiddleware {
  constructor() {
    this.requests = new Map();

    // Nettoyer toutes les 5 minutes
    setInterval(() => {
      this.cleanup();
    }, 5 * 60 * 1000);
  }

  createLimiter(maxRequests, windowMs, message) {
    return (req, res, next) => {
      const key = req.ip || req.connection.remoteAddress || "unknown";
      const now = Date.now();

      if (!this.requests.has(key)) {
        this.requests.set(key, []);
      }

      const userRequests = this.requests.get(key);
      const validRequests = userRequests.filter(
        (time) => now - time < windowMs
      );

      if (validRequests.length >= maxRequests) {
        return res.status(429).json({
          success: false,
          message: message || "Trop de requêtes",
          code: "RATE_LIMIT_EXCEEDED",
        });
      }

      validRequests.push(now);
      this.requests.set(key, validRequests);
      next();
    };
  }

  cleanup() {
    const now = Date.now();
    for (const [key, requests] of this.requests.entries()) {
      if (
        requests.length === 0 ||
        requests.every((time) => now - time > 60000)
      ) {
        this.requests.delete(key);
      }
    }
  }

  get apiLimit() {
    return this.createLimiter(100, 60000, "Limite API dépassée");
  }

  get createLimit() {
    return this.createLimiter(10, 60000, "Limite création dépassée");
  }

  get reactionLimit() {
    return this.createLimiter(30, 60000, "Limite réactions dépassée");
  }

  get healthLimit() {
    return this.createLimiter(20, 60000, "Limite health check dépassée");
  }

  get adminLimit() {
    return this.createLimiter(5, 60000, "Limite admin dépassée");
  }
}

module.exports = new RateLimitMiddleware();
