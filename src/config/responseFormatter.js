// src/config/responseFormatter.js
class ResponseFormatter {
  static success(data, message = "Opération réussie", metadata = {}) {
    return {
      success: true,
      data,
      message,
      metadata: {
        timestamp: new Date().toISOString(),
        ...metadata,
      },
    };
  }

  static error(message, code = "ERROR", details = null) {
    return {
      success: false,
      message,
      code,
      details,
      timestamp: new Date().toISOString(),
    };
  }

  static paginated(items, pagination, metadata = {}) {
    return {
      success: true,
      data: items,
      pagination,
      metadata: {
        timestamp: new Date().toISOString(),
        ...metadata,
      },
    };
  }
}

module.exports = ResponseFormatter;
