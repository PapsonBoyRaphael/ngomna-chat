/**
 * DeleteFile - Supprime un fichier (logique et/ou physique)
 * Publie l'événement file.deleted dans Redis Streams
 */
const fs = require("fs").promises;
const path = require("path");

class DeleteFile {
  constructor(
    fileRepository,
    kafkaProducer = null,
    resilientMessageService = null,
  ) {
    this.fileRepository = fileRepository;
    this.kafkaProducer = kafkaProducer;
    this.resilientMessageService = resilientMessageService;
  }

  /**
   * Supprime un fichier
   * @param {Object} params
   * @param {string} params.fileId - ID du fichier à supprimer
   * @param {string} params.userId - ID de l'utilisateur demandant la suppression
   * @param {boolean} params.physicalDelete - Si true, supprime aussi le fichier physique (défaut: true)
   * @returns {Promise<Object>} Résultat de la suppression
   */
  async execute({ fileId, userId, physicalDelete = true }) {
    if (!fileId || !userId) {
      throw new Error("fileId et userId sont requis");
    }

    // Récupérer le fichier
    const file = await this.fileRepository.findById(fileId);
    if (!file) {
      throw new Error("Fichier introuvable");
    }

    // Vérifier les permissions (seul l'uploader peut supprimer)
    if (String(file.uploadedBy) !== String(userId)) {
      throw new Error("Suppression non autorisée");
    }

    const fileInfo = {
      fileId: file._id,
      conversationId: file.conversationId?.toString(),
      uploadedBy: file.uploadedBy?.toString(),
      originalName: file.originalName,
      fileName: file.fileName,
      mimeType: file.mimeType,
      size: file.size,
      url: file.url,
      path: file.path,
    };

    // Supprimer le fichier physique si demandé
    if (physicalDelete && file.path) {
      try {
        await fs.unlink(file.path);
        console.log(`🗑️ Fichier physique supprimé: ${file.path}`);
      } catch (fsErr) {
        console.warn(
          `⚠️ Impossible de supprimer le fichier physique: ${fsErr.message}`,
        );
        // Non-bloquant, continue avec la suppression logique
      }
    }

    // Suppression logique (marquer comme supprimé) ou physique de la DB
    // Sauvegarder les modifications OU supprimer complètement
    let result;
    if (physicalDelete) {
      // Suppression complète de la base de données
      result = await this.fileRepository.deleteFile(fileId, false); // false = hard delete
    } else {
      // Suppression logique (marqué comme supprimé)
      result = await this.fileRepository.deleteFile(fileId, true); // true = soft delete (status: DELETED)
    }

    // ✅ PUBLIER DANS REDIS STREAMS chat:stream:events:files
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:files",
          {
            event: "file.deleted",
            userId: userId, // ✅ REQUIS : l'utilisateur qui a supprimé le fichier
            fileId: fileInfo.fileId,
            fileName: fileInfo.fileName,
            fileSize: fileInfo.size.toString(),
            conversationId: fileInfo.conversationId || "unknown",
            originalName: fileInfo.originalName,
            mimeType: fileInfo.mimeType,
            deletedAt: new Date().toISOString(),
            physicalDelete: physicalDelete.toString(),
            timestamp: Date.now().toString(),
          },
        );
        console.log(`📤 [file.deleted] publié dans chat:stream:events:files`);
      } catch (streamErr) {
        console.error(
          "❌ Erreur publication stream file.deleted:",
          streamErr.message,
        );
      }
    }

    return {
      success: true,
      fileId: fileInfo.fileId,
      deletedAt: new Date(),
      physicalDelete,
      message: physicalDelete
        ? "Fichier supprimé définitivement"
        : "Fichier marqué comme supprimé",
    };
  }
}

module.exports = DeleteFile;
