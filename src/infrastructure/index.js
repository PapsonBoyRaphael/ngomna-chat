// Export centralisé de toute l'infrastructure
const connectDB = require("./mongodb/connection");
const redisConfig = require("./redis/redisConfig");

// Gestionnaires Redis
const OnlineUserManager = require("./redis/OnlineUserManager");
const RoomManager = require("./redis/RoomManager");

// Repositories
const MongoMessageRepository = require("./repositories/MongoMessageRepository");
const MongoConversationRepository = require("./repositories/MongoConversationRepository");
const MongoFileRepository = require("./repositories/MongoFileRepository");

// Modèles (optionnel, généralement importés directement)
const MessageModel = require("./mongodb/models/MessageModel");
const ConversationModel = require("./mongodb/models/ConversationModel");
const FileModel = require("./mongodb/models/FileModel");
const UserPresenceManager = require("./redis/UserPresenceManager");

// Services
const TypingIndicatorService = require("./services/TypingIndicatorService");

module.exports = {
  // Connexions
  connectDB,

  // Gestionnaires
  UserPresenceManager,

  // Repositories
  MongoMessageRepository,
  MongoConversationRepository,
  MongoFileRepository,

  // Modèles
  MessageModel,
  ConversationModel,
  FileModel,

  // Services
  TypingIndicatorService,
};
