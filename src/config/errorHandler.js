// src/config/errorHandler.js
class ErrorHandler {
  static handleControllerError(error, req, res, context = "Controller") {
    console.error(`❌ Erreur ${context}:`, error);

    const isDevelopment = process.env.NODE_ENV === "development";

    res.status(error.status || 500).json({
      success: false,
      message: error.message || "Erreur interne du serveur",
      code: error.code || "INTERNAL_ERROR",
      ...(isDevelopment && { stack: error.stack }),
      timestamp: new Date().toISOString(),
    });
  }

  static handleRepositoryError(error, operation = "Database operation") {
    console.error(`❌ Erreur Repository ${operation}:`, error);
    throw new Error(`${operation} failed: ${error.message}`);
  }

  static handleKafkaError(error, eventType = "Unknown") {
    console.warn(`⚠️ Erreur Kafka ${eventType}:`, error.message);
    // Ne pas faire d'erreur fatale pour Kafka
    return false;
  }

  static handleRedisError(error, operation = "Redis operation") {
    console.warn(`⚠️ Erreur Redis ${operation}:`, error.message);
    // Ne pas faire d'erreur fatale pour Redis
    return null;
  }
}

module.exports = ErrorHandler;
