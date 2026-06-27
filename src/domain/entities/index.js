// Export centralisé de toutes les entités du domaine
const Message = require("./Message");
const Conversation = require("./Conversation");
const File = require("./File");
const Event = require("./Event");

module.exports = {
  Message,
  Conversation,
  File,
  Event,
};
