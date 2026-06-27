/**
 * Use-case wrapper for AutoGroupSyncService
 * Expose a consistent `execute` API used by handlers/controllers.
 */
class AutoGroupSync {
  constructor(autoGroupSyncService) {
    this.service = autoGroupSyncService;
  }

  async execute(userId, senderSocketId = null) {
    if (!this.service || typeof this.service.syncUserGroups !== "function") {
      throw new Error("AutoGroupSyncService non initialisé");
    }
    return this.service.syncUserGroups(userId, senderSocketId);
  }

  async syncAll(connectedUsers) {
    if (
      !this.service ||
      typeof this.service.syncAllConnectedUsers !== "function"
    ) {
      throw new Error("AutoGroupSyncService non initialisé");
    }
    return this.service.syncAllConnectedUsers(connectedUsers);
  }

  async removeUser(userId) {
    if (
      !this.service ||
      typeof this.service.removeUserFromAutoGroups !== "function"
    ) {
      throw new Error("AutoGroupSyncService non initialisé");
    }
    return this.service.removeUserFromAutoGroups(userId);
  }
}

module.exports = AutoGroupSync;
