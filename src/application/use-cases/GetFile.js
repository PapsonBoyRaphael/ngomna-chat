class GetFile {
  constructor(fileRepository) {
    this.fileRepository = fileRepository;
  }

  /**
   * Retourne uniquement le document mongoose File (pas de stream, pas de download)
   */
  async execute(fileId, userId) {
    try {
      // Récupérer depuis la base
      const file = await this.fileRepository.findById(fileId);

      if (!file) {
        throw new Error("Fichier non trouvé");
      }

      // Interdire l'accès si le fichier est supprimé
      if (file.status === "DELETED") {
        throw new Error("Ce fichier a été supprimé");
      }

      return file;
    } catch (error) {
      console.error("❌ Erreur GetFile use case:", error);
      throw error;
    }
  }
}

module.exports = GetFile;
