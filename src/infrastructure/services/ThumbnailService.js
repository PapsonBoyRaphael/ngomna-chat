const sharp = require("sharp");
const path = require("path");
const fs = require("fs-extra");
const { v4: uuidv4 } = require("uuid");

class ThumbnailService {
  constructor(fileStorageService) {
    this.fileStorageService = fileStorageService;
    this.thumbnailSizes = [
      { name: "small", width: 150, height: 150 },
      { name: "medium", width: 300, height: 300 },
      { name: "large", width: 600, height: 600 },
    ];
    this.maxRetries = 3;
    this.timeout = 10000; // 10s max per thumbnail
    this.metrics = { generated: 0, errors: 0 };
  }

  // Vérif si processable (ajoutée)
  isProcessable(mimeType) {
    return this.isImageFile(mimeType); // Conserve ta méthode
  }

  async generateThumbnails(originalFilePath, originalFileName, fileId) {
    try {
      const thumbnails = [];
      const fileExtension = path.extname(originalFileName);
      const baseFileName = path.basename(originalFileName, fileExtension);

      for (const size of this.thumbnailSizes) {
        const thumbnailFileName = `thumbnail_${size.name}_${fileId}_${baseFileName}.webp`;

        // Sharp avec buffer in-memory (évite /tmp)
        const inputBuffer = await fs.readFile(originalFilePath);
        const thumbnailBuffer = await sharp(inputBuffer)
          .resize(size.width, size.height, {
            fit: "cover",
            position: "center",
          })
          .webp({ quality: 80 })
          .toBuffer();

        // Upload buffer direct
        const remoteThumbnailPath =
          await this.fileStorageService.uploadFromBuffer(
            thumbnailBuffer,
            `thumbnails/${thumbnailFileName}`,
            "image/webp",
          );

        thumbnails.push({
          size: size.name,
          width: size.width,
          height: size.height,
          path: remoteThumbnailPath,
          url: this.generateThumbnailUrl(remoteThumbnailPath),
          fileName: thumbnailFileName,
        });
      }

      console.log(
        `✅ ${thumbnails.length} thumbnails générés pour ${originalFileName}`,
      );
      this.metrics.generated += thumbnails.length;
      return thumbnails;
    } catch (error) {
      this.metrics.errors++;
      console.error(
        `❌ Erreur génération thumbnails pour ${originalFileName}:`,
        error,
      );
      throw error;
    }
  }

  generateThumbnailUrl(remotePath) {
    const config = require("../../config/envValidator");

    // ✅ Conserver le chemin complet (ex: "thumbnails/thumb_medium_xxx.webp")
    // Supprimer uniquement le préfixe bucket s'il est présent
    const objectKey = remotePath.startsWith(`${config.s3Bucket}/`)
      ? remotePath.substring(config.s3Bucket.length + 1)
      : remotePath;

    if (config.env === "development") {
      return `${config.s3Endpoint}/${config.s3Bucket}/${objectKey}`;
    } else {
      return `/api/files/thumbnail/${path.basename(remotePath)}`;
    }
  }

  isImageFile(mimeType) {
    return (
      mimeType && mimeType.startsWith("image/") && !mimeType.includes("svg")
    );
  }

  async downloadImageForProcessing(remotePath) {
    try {
      const stream = await this.fileStorageService.download(null, remotePath);
      const buffer = await this.streamToBuffer(stream);

      // ✅ CRÉER UN FICHIER TEMPORAIRE AVEC LE BUFFER
      const tempDir = "./storage/temp";
      await fs.ensureDir(tempDir);
      const tempFilePath = path.join(
        tempDir,
        `temp_${Date.now()}_${Math.random().toString(36).substring(7)}.tmp`,
      );
      await fs.writeFile(tempFilePath, buffer);

      return tempFilePath; // Retourne le chemin du fichier temporaire
    } catch (error) {
      console.error("❌ Erreur téléchargement image pour traitement:", error);
      throw error;
    }
  }

  // Helper pour stream to buffer (ajouté)
  async streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on("data", (chunk) => chunks.push(chunk));
      stream.on("error", reject);
      stream.on("end", () => resolve(Buffer.concat(chunks)));
    });
  }

  // Get metrics pour monitoring (ajouté)
  getMetrics() {
    return this.metrics;
  }
}

module.exports = ThumbnailService;
