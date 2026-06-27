const archiver = require("archiver");
const stream = require("stream");

class DownloadFile {
  constructor(
    fileRepository,
    fileStorageService,
    resilientMessageService = null,
  ) {
    this.fileRepository = fileRepository;
    this.fileStorageService = fileStorageService;
    this.resilientMessageService = resilientMessageService;
    this.devConfigMode = process.env.DEV_CONFIG_MODE || "simple"; // 'simple' ou 'advanced'
  }

  /**
   * Téléchargement d'un fichier unique
   * @param {string} fileId
   * @param {string} userId
   * @returns {Promise<{file, fileStream?, downloadUrl?}>}
   */
  async executeSingle(fileId, userId) {
    // Optionnel : lecture du cache si pertinent (ex: file metadata)
    const file = await this.fileRepository.findById(fileId);
    if (!file) throw new Error("Fichier non trouvé");

    // ✅ VÉRIFIER QUE LE FICHIER N'EST PAS SUPPRIMÉ
    if (file.status === "DELETED" || file.isDeleted) {
      throw new Error("Ce fichier a été supprimé et n'est plus disponible");
    }

    // Vérifier les droits de téléchargement
    if (
      typeof file.canBeDownloadedBy === "function" &&
      !file.canBeDownloadedBy(userId)
    ) {
      throw new Error("Accès refusé à ce fichier");
    }

    // Incrémenter le compteur de téléchargements
    await this.fileRepository.incrementDownloadCount(fileId, userId);

    // ✅ PUBLIER ÉVÉNEMENT DE TÉLÉCHARGEMENT DANS REDIS STREAMS chat:stream:events:files
    if (this.resilientMessageService) {
      try {
        await this.resilientMessageService.addToStream(
          "chat:stream:events:files",
          {
            event: "file.downloaded",
            fileId: fileId,
            fileName: file.fileName,
            fileSize: file.fileSize,
            userId: userId,
            timestamp: Date.now(),
          },
        );
        console.log(
          `📥 [file.downloaded] publié dans chat:stream:events:files pour ${fileId}`,
        );
      } catch (error) {
        console.warn("⚠️ Échec publication événement download:", error.message);
      }
    }

    // Basculer selon le mode DEV
    if (
      process.env.NODE_ENV === "development" &&
      this.devConfigMode === "advanced"
    ) {
      // Mode DEV simple : retourner l'URL signée pour téléchargement direct
      const downloadUrl = await this.fileStorageService.getDownloadUrl(
        file.fileName,
      );
      return { file, downloadUrl };
    } else {
      // Mode actuel : retourner le stream
      const fileStream = await this.fileStorageService.download(
        file.fileName,
        file.fileName,
      );
      return { file, fileStream };
    }
  }

  /**
   * Téléchargement multiple (avec file queue)
   * @param {string[]} fileIds
   * @param {string} userId
   * @returns {Promise<{zipStream, files}>}
   */
  async executeMultiple(fileIds, userId) {
    // Mettre en attente la demande (file queue Redis via CacheService)

    // Récupérer les fichiers
    const files = [];
    for (const fileId of fileIds) {
      const file = await this.fileRepository.findById(fileId);
      if (file && (!file.canBeDownloadedBy || file.canBeDownloadedBy(userId))) {
        files.push(file);
      }
    }
    if (files.length === 0) throw new Error("Aucun fichier téléchargeable");

    // Générer un ZIP à la volée
    const zipStream = archiver("zip", { zlib: { level: 9 } });
    const passThrough = new stream.PassThrough();

    zipStream.pipe(passThrough);

    for (const file of files) {
      const fileStream = await this.fileStorageService.download(
        file.fileName,
        file.fileName,
      );
      zipStream.append(fileStream, {
        name: file.originalName || file.fileName,
      });
      await this.fileRepository.incrementDownloadCount(file._id, userId);

      // ✅ PUBLIER ÉVÉNEMENT DE TÉLÉCHARGEMENT DANS REDIS STREAMS chat:stream:events:files
      if (this.resilientMessageService) {
        try {
          await this.resilientMessageService.addToStream(
            "chat:stream:events:files",
            {
              event: "file.downloaded",
              fileId: file._id,
              fileName: file.fileName,
              fileSize: file.fileSize,
              userId: userId,
              timestamp: Date.now(),
            },
          );
          console.log(
            `📥 [file.downloaded] publié dans chat:stream:events:files pour ${file._id}`,
          );
        } catch (error) {
          console.warn(
            "⚠️ Échec publication événement download:",
            error.message,
          );
        }
      }
    }
    zipStream.finalize();

    return { zipStream: passThrough, files };
  }
}

module.exports = DownloadFile;
