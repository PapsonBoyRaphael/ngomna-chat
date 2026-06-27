// services/MediaProcessingService.js
const ffmpeg = require("fluent-ffmpeg");
const mm = require("music-metadata");
const pdfParse = require("pdf-parse");
const { exec } = require("child_process");
const util = require("util");
const path = require("path");
const fs = require("fs").promises;
const mime = require("mime-types");
const crypto = require("crypto");
const sharp = require("sharp");

const execAsync = util.promisify(exec);

class MediaProcessingService {
  constructor() {
    // ✅ AJOUTER TOUS LES TIMEOUTS MANQUANTS
    this.timeout = 30000; // 30 secondes pour processFile
    this.imageProcessTimeout = 15000; // 15s pour images
    this.audioProcessTimeout = 20000; // 20s pour audio
    this.videoProcessTimeout = 45000; // 45s pour vidéo
    this.documentProcessTimeout = 15000; // 15s pour documents

    this.supportedFormats = {
      IMAGE: ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "svg"],
      AUDIO: ["mp3", "wav", "ogg", "flac", "aac", "m4a", "wma"],
      VIDEO: ["mp4", "avi", "mov", "mkv", "webm", "flv", "wmv", "mpeg"],
      DOCUMENT: ["pdf", "doc", "docx", "txt", "rtf", "odt"],
    };
    this.metrics = { processed: 0, errors: 0 };
    this.maxBufferSize = 500 * 1024 * 1024; // 500MB max
  }

  // Validation buffer/MIME
  validateBuffer(buffer, mimeType) {
    if (!buffer || buffer.length === 0) {
      throw new Error("Buffer vide");
    }
    if (buffer.length > this.maxBufferSize) {
      throw new Error(
        `Buffer trop grand (${buffer.length} > ${this.maxBufferSize})`,
      );
    }
    if (!this.isSupportedMimeType(mimeType)) {
      throw new Error(`Type MIME non supporté: ${mimeType}`);
    }
  }

  /**
   * ✅ TRAITE UN FICHIER ET EXTRAIT SES MÉTADONNÉES (SANS THUMBNAILS)
   */
  async processFile(buffer, originalName, mimeType) {
    this.validateBuffer(buffer, mimeType);

    try {
      console.log(`🔍 Traitement du fichier: ${originalName}`);
      const fileType = this.getFileType(mimeType, originalName);

      let metadata = {
        technical: {
          extension: path.extname(originalName).toLowerCase(),
          fileType: fileType,
          category: this.getFileCategory(fileType),
          encoding: "binary",
        },
        content: {},
      };

      // ✅ UTILISER LE TIMEOUT APPROPRIÉ SELON LE TYPE
      let timeoutValue = this.timeout;
      switch (fileType) {
        case "IMAGE":
          timeoutValue = this.imageProcessTimeout;
          break;
        case "AUDIO":
          timeoutValue = this.audioProcessTimeout;
          break;
        case "VIDEO":
          timeoutValue = this.videoProcessTimeout;
          break;
        case "DOCUMENT":
          timeoutValue = this.documentProcessTimeout;
          break;
        default:
          timeoutValue = this.timeout;
      }

      console.log(`⏱️ Timeout défini: ${timeoutValue}ms pour type ${fileType}`);

      // Traitement spécifique selon le type avec timeout CORRIGÉ
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error(`Timeout processing ${fileType} (${timeoutValue}ms)`),
            ),
          timeoutValue, // ✅ UTILISER LA VARIABLE CORRECTE
        ),
      );

      let processingPromise;
      switch (fileType) {
        case "AUDIO":
          processingPromise = this.processAudio(buffer, metadata);
          break;
        case "IMAGE":
          processingPromise = this.processImage(buffer, metadata);
          break;
        case "VIDEO":
          processingPromise = this.processVideo(buffer, metadata);
          break;
        case "DOCUMENT":
          processingPromise = this.processDocument(buffer, metadata);
          break;
        default:
          processingPromise = this.processOtherFile(buffer, metadata);
      }

      metadata = await Promise.race([timeoutPromise, processingPromise]);

      // Générer les checksums depuis le buffer
      metadata.technical.checksums = {
        md5: crypto.createHash("md5").update(buffer).digest("hex"),
        sha1: crypto.createHash("sha1").update(buffer).digest("hex"),
        sha256: crypto.createHash("sha256").update(buffer).digest("hex"),
      };

      this.metrics.processed++;
      console.log(`✅ Traitement réussi: ${originalName}`);
      return metadata;
    } catch (error) {
      console.error(`❌ Erreur traitement fichier ${originalName}:`, error);
      this.metrics.errors++;
      return { error: error.message, technical: {}, content: {} }; // Fallback metadata
    }
  }

  /**
   * ✅ TRAITEMENT DES FICHIERS AUDIO
   */
  async processAudio(buffer, metadata) {
    try {
      // ✅ AJOUTER UN TIMEOUT INTERNE POUR mm.parseBuffer
      const audioMetadata = await Promise.race([
        mm.parseBuffer(buffer),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Audio parsing timeout")),
            this.audioProcessTimeout - 1000, // 1s avant le timeout global
          ),
        ),
      ]);

      metadata.content = {
        duration: audioMetadata.format.duration || null,
        bitrate: audioMetadata.format.bitrate || null,
        sampleRate: audioMetadata.format.sampleRate || null,
        channels: audioMetadata.format.numberOfChannels || null,
        codec: audioMetadata.format.codec || null,
      };

      // Métadonnées ID3 si disponibles
      if (audioMetadata.common) {
        metadata.content = {
          ...metadata.content,
          title: audioMetadata.common.title || null,
          artist: audioMetadata.common.artist || null,
          album: audioMetadata.common.album || null,
          genre: audioMetadata.common.genre?.[0] || null,
          year: audioMetadata.common.year || null,
        };
      }

      console.log(`✅ Audio traité: ${metadata.content.duration}s`);
      return metadata;
    } catch (error) {
      console.warn("⚠️ Erreur traitement audio:", error.message);
      // Retourner metadata partielle plutôt que de lever une erreur
      metadata.content = {
        duration: null,
        bitrate: null,
        sampleRate: null,
        channels: null,
        codec: null,
      };
      return metadata;
    }
  }

  /**
   * ✅ TRAITEMENT DES IMAGES (SANS THUMBNAILS)
   */
  async processImage(buffer, metadata) {
    try {
      const imageInfo = await Promise.race([
        sharp(buffer).metadata(),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(new Error("Image metadata timeout")),
            this.imageProcessTimeout - 1000,
          ),
        ),
      ]);

      metadata.content = {
        dimensions: {
          width: imageInfo.width || null,
          height: imageInfo.height || null,
        },
        format: imageInfo.format || null,
        space: imageInfo.space || "RGB",
        hasAlpha: imageInfo.hasAlpha || false,
        channels: imageInfo.channels || null,
      };

      console.log(`✅ Image traitée: ${imageInfo.width}x${imageInfo.height}`);
      return metadata;
    } catch (error) {
      console.warn("⚠️ Erreur traitement image:", error.message);
      metadata.content = {
        dimensions: { width: null, height: null },
        format: null,
        space: "RGB",
        hasAlpha: false,
        channels: null,
      };
      return metadata;
    }
  }

  /**
   * ✅ TRAITEMENT DES VIDÉOS AVEC EXTRACTION DE FRAME ET MINIATURE
   */
  async processVideo(buffer, metadata) {
    const tempId = `${Date.now()}_${Math.random().toString(36).substring(7)}`;
    const tempPath = `/tmp/video_${tempId}`;
    const framePath = `/tmp/frame_${tempId}.jpg`;

    try {
      // Écrire buffer temporairement pour ffprobe/ffmpeg
      await fs.writeFile(tempPath, buffer);

      // 1. Extraire les métadonnées via ffprobe
      const probeData = await this._ffprobeAsync(tempPath);

      if (probeData) {
        const videoStream = probeData.streams.find(
          (s) => s.codec_type === "video",
        );
        const audioStream = probeData.streams.find(
          (s) => s.codec_type === "audio",
        );

        if (videoStream) {
          metadata.content.dimensions = {
            width: videoStream.width || null,
            height: videoStream.height || null,
          };
          metadata.content.duration =
            parseFloat(videoStream.duration) ||
            parseFloat(probeData.format?.duration) ||
            null;
          metadata.content.bitrate = parseInt(videoStream.bit_rate) || null;
          metadata.content.fps =
            this.parseFps(videoStream.r_frame_rate) || null;
          metadata.content.aspectRatio =
            videoStream.display_aspect_ratio || null;
          metadata.content.videoCodec = videoStream.codec_name || null;
        }

        if (audioStream) {
          metadata.content.audioCodec = audioStream.codec_name || null;
          metadata.content.audioChannels = audioStream.channels || null;
          metadata.content.audioSampleRate =
            parseInt(audioStream.sample_rate) || null;
        }
      }

      // 2. Extraire une frame et générer les miniatures
      try {
        const thumbnailData = await this._extractFrameAndGenerateThumbnails(
          tempPath,
          framePath,
          metadata.content.duration,
        );

        if (thumbnailData) {
          metadata.content.thumbnail = thumbnailData;
          console.log(
            `🖼️ Miniature vidéo générée: ${thumbnailData.thumbnails.length} taille(s) à ${thumbnailData.extractedAtSecond}s`,
          );
        }
      } catch (thumbError) {
        console.warn(
          "⚠️ Erreur extraction miniature vidéo:",
          thumbError.message,
        );
        metadata.content.thumbnail = {
          generated: false,
          error: thumbError.message,
        };
      }

      console.log(`✅ Vidéo traitée avec miniature`);
      return metadata;
    } catch (error) {
      console.warn("⚠️ Erreur traitement vidéo:", error.message);
      return metadata;
    } finally {
      // Nettoyage garanti des fichiers temporaires
      await this._cleanupTempFiles(tempPath, framePath);
    }
  }

  /**
   * ✅ TRAITEMENT DES DOCUMENTS
   */
  async processDocument(buffer, metadata) {
    try {
      // ✅ AJOUTER TIMEOUT POUR PDF PARSING
      const processingPromise = (async () => {
        // 1. Pour les PDF
        if (metadata.technical.extension === ".pdf") {
          const pdfData = await Promise.race([
            pdfParse(buffer),
            new Promise((_, reject) =>
              setTimeout(
                () => reject(new Error("PDF parsing timeout")),
                this.documentProcessTimeout - 1000,
              ),
            ),
          ]);

          metadata.content = {
            pageCount: pdfData.numpages || 0,
            text: pdfData.text ? pdfData.text.substring(0, 1000) : null,
            wordCount: pdfData.text ? pdfData.text.split(/\s+/).length : 0,
            hasImages: pdfData.text ? pdfData.text.includes("/Image") : false,
            author: pdfData.info?.Author || null,
            title: pdfData.info?.Title || null,
            creator: pdfData.info?.Creator || null,
            size: buffer.length,
            encoding: "binary",
          };
        }
        // 2. Pour les fichiers texte
        else if (metadata.technical.extension.match(/\.(txt|rtf|md)$/)) {
          try {
            const text = buffer.toString("utf8");
            metadata.content = {
              text: text.substring(0, 1000),
              wordCount: text.split(/\s+/).length,
              lineCount: text.split("\n").length,
              encoding: "utf8",
              size: buffer.length,
            };
          } catch {
            metadata.content = {
              size: buffer.length,
              encoding: "binary",
            };
          }
        }
        // 3. Pour les documents Office
        else if (
          metadata.technical.extension.match(/\.(doc|docx|xls|xlsx|ppt|pptx)$/)
        ) {
          metadata.content = {
            size: buffer.length,
            encoding: "binary",
            type: metadata.technical.extension.substring(1).toUpperCase(),
          };
        }
        // 4. Pour tout autre type de document
        else {
          metadata.content = {
            size: buffer.length,
            encoding: "binary",
          };
        }

        return metadata;
      })();

      return await processingPromise;
    } catch (error) {
      console.warn("⚠️ Erreur traitement document:", error.message);
      metadata.content = {
        size: buffer.length,
        encoding: "binary",
      };
      return metadata;
    }
  }

  // ===============================
  // MÉTHODES UTILITAIRES FILETYPE
  // ===============================

  /**
   * Fallback pour l'audio avec FFprobe
   */
  async processAudioWithFFprobe(filePath, metadata) {
    return new Promise((resolve, reject) => {
      ffmpeg.ffprobe(filePath, (err, data) => {
        if (err) {
          reject(err);
          return;
        }

        const stream = data.streams.find((s) => s.codec_type === "audio");
        if (stream) {
          metadata.content.duration = parseFloat(stream.duration);
          metadata.content.bitrate = parseInt(stream.bit_rate);
          metadata.content.sampleRate = parseInt(stream.sample_rate);
          metadata.content.channels = stream.channels;
          metadata.content.codec = stream.codec_name;
        }

        resolve(metadata);
      });
    });
  }

  /**
   * Traitement des PDF (filePath)
   */
  async processPDF(filePath, metadata) {
    try {
      const dataBuffer = await fs.readFile(filePath);
      const pdfData = await pdfParse(dataBuffer);

      metadata.content.pageCount = pdfData.numpages;
      metadata.content.text = pdfData.text.substring(0, 1000); // Extraire les premiers caractères
      metadata.content.wordCount = pdfData.text.split(/\s+/).length;
      metadata.content.hasImages = pdfData.text.includes("/Image");
      metadata.content.author = pdfData.info?.Author;
      metadata.content.title = pdfData.info?.Title;
      metadata.content.creator = pdfData.info?.Creator;

      return metadata;
    } catch (error) {
      console.warn("⚠️ Erreur traitement PDF:", error.message);
      return metadata;
    }
  }

  /**
   * Traitement des fichiers texte
   */
  async processTextFile(filePath, metadata) {
    try {
      const content = await fs.readFile(filePath, "utf8");
      metadata.content.text = content.substring(0, 2000); // Limiter la taille
      metadata.content.wordCount = content.split(/\s+/).length;
      metadata.content.lineCount = content.split("\n").length;
      metadata.content.encoding = "utf8";

      return metadata;
    } catch (error) {
      console.warn("⚠️ Erreur traitement fichier texte:", error.message);
      return metadata;
    }
  }

  /**
   * Traitement des documents génériques
   */
  async processGenericDocument(filePath, metadata) {
    // Pour les documents non supportés, on se contente des infos basiques
    const stats = await fs.stat(filePath);
    metadata.content.size = stats.size;

    return metadata;
  }

  /**
   * Traitement des autres types de fichiers
   */
  async processOtherFile(buffer, metadata) {
    // Métadonnées basiques pour les types non supportés
    metadata.content.size = buffer.length;
    return metadata;
  }

  /**
   * Génère un waveform basique pour l'audio
   */
  async generateAudioWaveform(filePath) {
    // Implémentation simplifiée - retourne des données simulées
    const waveform = [];
    for (let i = 0; i < 50; i++) {
      waveform.push(Math.random() * 0.8 + 0.2); // Valeurs entre 0.2 et 1.0
    }
    return waveform;
  }

  /**
   * Extrait les données EXIF des images
   */
  async extractExifData(filePath) {
    try {
      // Pour une implémentation légère sans Sharp
      // On pourrait utiliser 'exif-reader' si nécessaire
      const exif = {};

      // Extraction basique via commande système
      try {
        const { stdout } = await execAsync(
          `exiftool -j "${filePath}" 2>/dev/null || echo "{}"`,
        );
        const exifData = JSON.parse(stdout)[0];

        if (exifData) {
          exif.orientation = exifData.Orientation;
          exif.camera = exifData.Model;
          exif.software = exifData.Software;

          // Extraction GPS si disponible
          if (exifData.GPSLatitude && exifData.GPSLongitude) {
            exif.location = {
              latitude: this.convertExifGps(
                exifData.GPSLatitude,
                exifData.GPSLatitudeRef,
              ),
              longitude: this.convertExifGps(
                exifData.GPSLongitude,
                exifData.GPSLongitudeRef,
              ),
            };
          }
        }
      } catch (exifError) {
        console.warn("⚠️ exiftool non disponible:", exifError.message);
      }

      return exif;
    } catch (error) {
      console.warn("⚠️ Erreur extraction EXIF:", error.message);
      return {};
    }
  }

  /**
   * Convertit les coordonnées GPS EXIF
   */
  convertExifGps(coordinate, ref) {
    if (!coordinate) return null;

    try {
      // Format: "40 deg 44' 54.00" N" -> 40.748333
      const parts = coordinate.toString().split(" ");
      const degrees = parseFloat(parts[0]);
      const minutes = parseFloat(parts[2]);
      const seconds = parseFloat(parts[3]);

      const decimal = degrees + minutes / 60 + seconds / 3600;

      if (ref === "S" || ref === "W") {
        return -decimal;
      }
      return decimal;
    } catch (error) {
      return null;
    }
  }

  // ===============================
  // MÉTHODES HELPER VIDÉO
  // ===============================

  /**
   * ✅ Wrapper async pour ffprobe (callback → Promise)
   */
  _ffprobeAsync(filePath) {
    return new Promise((resolve) => {
      ffmpeg.ffprobe(filePath, (err, data) => {
        if (err) {
          console.warn("⚠️ Erreur ffprobe:", err.message);
          resolve(null);
        } else {
          resolve(data);
        }
      });
    });
  }

  /**
   * ✅ Extrait une frame de la vidéo et génère les miniatures (small, medium, large)
   * @param {string} videoPath - Chemin du fichier vidéo temporaire
   * @param {string} framePath - Chemin de sortie pour la frame extraite
   * @param {number|null} duration - Durée de la vidéo en secondes
   * @returns {Object} Données de miniature avec buffers
   */
  async _extractFrameAndGenerateThumbnails(videoPath, framePath, duration) {
    // Calculer le timestamp de capture (25% de la durée, max 10s, min 0.5s)
    const captureTime = duration
      ? Math.max(0.5, Math.min(duration * 0.25, 10))
      : 1;

    // Extraire une frame avec ffmpeg (timeout 15s)
    await execAsync(
      `ffmpeg -ss ${captureTime} -i "${videoPath}" -vframes 1 -q:v 2 "${framePath}" -y 2>/dev/null`,
      { timeout: 15000 },
    );

    // Lire la frame extraite
    const frameBuffer = await fs.readFile(framePath);
    if (!frameBuffer || frameBuffer.length === 0) {
      throw new Error("Frame vidéo extraite vide");
    }

    // Récupérer les dimensions de la frame originale
    const frameMeta = await sharp(frameBuffer).metadata();

    // Générer les miniatures avec sharp (mêmes tailles que ThumbnailService)
    const thumbnailSizes = [
      { name: "small", width: 150, height: 150 },
      { name: "medium", width: 300, height: 300 },
      { name: "large", width: 600, height: 600 },
    ];

    const thumbnails = [];
    for (const size of thumbnailSizes) {
      const thumbBuffer = await sharp(frameBuffer)
        .resize(size.width, size.height, {
          fit: "cover",
          position: "center",
        })
        .webp({ quality: 80 })
        .toBuffer();

      thumbnails.push({
        name: size.name,
        width: size.width,
        height: size.height,
        buffer: thumbBuffer,
        mimeType: "image/webp",
        byteSize: thumbBuffer.length,
      });
    }

    console.log(
      `🎬 Frame extraite à ${captureTime.toFixed(1)}s → ${thumbnails.length} miniatures générées (${frameMeta.width}x${frameMeta.height})`,
    );

    return {
      generated: true,
      extractedAtSecond: parseFloat(captureTime.toFixed(1)),
      frameWidth: frameMeta.width || null,
      frameHeight: frameMeta.height || null,
      thumbnails,
    };
  }

  /**
   * ✅ Nettoyage sécurisé des fichiers temporaires
   */
  async _cleanupTempFiles(...paths) {
    for (const filePath of paths) {
      try {
        await fs.access(filePath);
        await fs.unlink(filePath);
      } catch {
        // Fichier n'existe pas ou déjà supprimé, ignorer silencieusement
      }
    }
  }

  // ===============================
  // MÉTHODES UTILITAIRES
  // ===============================

  /**
   * Parse les FPS vidéo
   */
  parseFps(fpsString) {
    if (!fpsString) return null;

    try {
      const [numerator, denominator] = fpsString.split("/");
      return denominator ? numerator / denominator : parseFloat(numerator);
    } catch (error) {
      return null;
    }
  }

  /**
   * Génère les checksums du fichier
   */
  async generateChecksums(filePath) {
    try {
      const fileBuffer = await fs.readFile(filePath);

      return {
        md5: crypto.createHash("md5").update(fileBuffer).digest("hex"),
        sha1: crypto.createHash("sha1").update(fileBuffer).digest("hex"),
        sha256: crypto.createHash("sha256").update(fileBuffer).digest("hex"),
      };
    } catch (error) {
      console.warn("⚠️ Erreur génération checksums:", error.message);
      return {};
    }
  }

  /**
   * Détermine le type de fichier
   */
  getFileType(mimeType, fileName) {
    const extension = path.extname(fileName).toLowerCase().replace(".", "");

    if (
      mimeType.startsWith("image/") ||
      this.supportedFormats.IMAGE.includes(extension)
    ) {
      return "IMAGE";
    }
    if (
      mimeType.startsWith("audio/") ||
      this.supportedFormats.AUDIO.includes(extension)
    ) {
      return "AUDIO";
    }
    if (
      mimeType.startsWith("video/") ||
      this.supportedFormats.VIDEO.includes(extension)
    ) {
      return "VIDEO";
    }
    if (
      mimeType.includes("pdf") ||
      mimeType.includes("text/") ||
      mimeType.includes("application/") ||
      this.supportedFormats.DOCUMENT.includes(extension)
    ) {
      return "DOCUMENT";
    }

    return "OTHER";
  }

  /**
   * Détermine la catégorie du fichier
   */
  getFileCategory(fileType) {
    const categories = {
      IMAGE: "media",
      AUDIO: "media",
      VIDEO: "media",
      DOCUMENT: "document",
      OTHER: "other",
    };

    return categories[fileType] || "other";
  }

  /**
   * Vérifie si un type MIME est supporté
   */
  isSupportedMimeType(mimeType) {
    // Vérification par préfixe — cohérente avec getFileType()
    if (mimeType.startsWith("image/")) return true;
    if (mimeType.startsWith("audio/")) return true;
    if (mimeType.startsWith("video/")) return true;
    if (mimeType.startsWith("text/")) return true;
    if (mimeType.includes("pdf")) return true;
    if (
      mimeType.includes("word") ||
      mimeType.includes("excel") ||
      mimeType.includes("powerpoint") ||
      mimeType.includes("document") ||
      mimeType.includes("spreadsheet") ||
      mimeType.includes("presentation")
    ) {
      return true;
    }
    return false;
  }

  /**
   * Récupère des informations basiques sur un fichier
   */
  async getBasicFileInfo(filePath) {
    try {
      const stats = await fs.stat(filePath);
      const mimeType = mime.lookup(filePath) || "application/octet-stream";

      return {
        size: stats.size,
        mimeType: mimeType,
        created: stats.birthtime,
        modified: stats.mtime,
        fileType: this.getFileType(mimeType, filePath),
      };
    } catch (error) {
      throw new Error(
        `Impossible d'obtenir les infos du fichier: ${error.message}`,
      );
    }
  }

  getMetrics() {
    return this.metrics;
  }
}

module.exports = MediaProcessingService;
