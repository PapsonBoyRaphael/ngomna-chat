const File = require("../../domain/entities/File");
const musicMetadata = require("music-metadata");
const path = require("path");

class UploadFile {
  constructor(
    fileRepository,
    kafkaProducer = null,
    resilientMessageService = null,
  ) {
    this.fileRepository = fileRepository;
    this.kafkaProducer = kafkaProducer;
    this.resilientMessageService = resilientMessageService;
  }

  async execute(fileData) {
    try {
      console.log("📤 Démarrage de l'upload du fichier:", fileData);

      // ✅ VALIDATION DES DONNÉES
      if (!fileData.originalName || !fileData.fileName) {
        throw new Error("Données de fichier incomplètes");
      }

      // ✅ EXTRAIRE L'UUID DU FILENAME (sans extension)
      const ext = path.extname(fileData.fileName);
      const fileId = fileData.fileName.replace(ext, "");
      console.log(`🆔 ID fichier extrait du fileName: ${fileId}`);
      console.log(`📝 Nom de fichier reçu: ${fileData.fileName}`);

      // ✅ CRÉER UNE INSTANCE DE L'ENTITÉ FILE AVEC LES MÉTADONNÉES
      const fileEntity = new File({
        _id: fileId, // Assigner l'ID custom (string) extrait du fileName
        originalName: fileData.originalName,
        fileName: fileData.fileName, // Utiliser le nom reçu du contrôleur
        mimeType: fileData.mimeType,
        size: fileData.size,
        path: fileData.path, // ✅ Utiliser le path reçu du contrôleur (qui utilise déjà fileName)
        url: fileData.url, // ✅ Utiliser l'url reçue du contrôleur
        uploadedBy: fileData.uploadedBy,
        conversationId: fileData.conversationId,
        status: "COMPLETED",
        metadata: {
          technical: fileData.metadata?.technical,
          content: fileData.metadata?.content,
          processing: fileData.metadata?.processing,
          kafkaMetadata: fileData.metadata?.kafkaMetadata,
          redisMetadata: fileData.metadata?.redisMetadata,
          security: fileData.metadata?.security,
          storage: fileData.metadata?.storage,
          usage: fileData.metadata?.usage,
        },
        isClientRecorded:
          fileData.isClientRecorded === true ||
          fileData.isClientRecorded === "true",
      });

      // ✅ SAUVEGARDER VIA LE REPOSITORY
      const savedFile = await this.fileRepository.save(fileEntity);

      if (!savedFile) {
        throw new Error("Échec de la sauvegarde du fichier");
      }

      console.log(`✅ Fichier sauvé avec ID custom: ${fileId}`);

      // ✅ PUBLIER DANS REDIS STREAMS chat:stream:events:files
      if (this.resilientMessageService) {
        try {
          await this.resilientMessageService.addToStream(
            "chat:stream:events:files",
            {
              event: "file.uploaded",
              userId: savedFile.uploadedBy, // ✅ REQUIS : l'utilisateur qui a uploadé le fichier
              fileId: savedFile._id,
              fileName: savedFile.fileName,
              fileSize: savedFile.size.toString(),
              conversationId: savedFile.conversationId?.toString() || "unknown",
              originalName: savedFile.originalName,
              mimeType: savedFile.mimeType,
              metadata: JSON.stringify(savedFile.metadata || {}),
              url: savedFile.url,
              timestamp: Date.now().toString(),
            },
          );
          console.log(
            `📤 [file.uploaded] publié dans chat:stream:events:files`,
          );
        } catch (streamErr) {
          console.error(
            "❌ Erreur publication stream file.uploaded:",
            streamErr.message,
          );
        }
      }

      // ✅ TRAITEMENT DES MÉTADONNÉES AUDIO SI BESOIN
      // if (fileEntity.mimeType && fileEntity.mimeType.startsWith("audio/")) {
      //   await processAudioFile(savedFile);
      // }

      return {
        id: savedFile._id, // Retourne l'ID custom
        originalName: savedFile.originalName,
        fileName: savedFile.fileName,
        size: savedFile.size,
        mimeType: savedFile.mimeType,
        uploadedAt: savedFile.createdAt,
        url: savedFile.url,
        status: savedFile.status,
      };
    } catch (error) {
      console.error("❌ Erreur UploadFile use case:", error);
      throw error;
    }
  }
}

module.exports = UploadFile;
