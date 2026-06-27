const fs = require("fs-extra");
const path = require("path");
const { v4: uuidv4 } = require("uuid");

/**
 * ✅ ChunkedUploadService
 * Gère l'upload par morceaux pour les fichiers > 100 MB
 *
 * Flux :
 *   1. POST /upload/init       → crée une session, retourne uploadId
 *   2. POST /upload/chunk/:id  → stocke chaque morceau sur disque
 *   3. POST /upload/complete/:id → assemble, uploade vers MinIO, crée en DB
 *
 * Stockage temporaire : ./storage/chunks/{uploadId}/chunk_000, chunk_001, …
 * Session Redis        : chat:cache:upload:chunked:{uploadId}  (TTL 2h)
 * Token Redis          : chat:cache:upload:token:{token} → fileId (TTL 1h)
 */
class ChunkedUploadService {
  constructor(redisClient, fileStorageService) {
    this.redis = redisClient;
    this.fileStorageService = fileStorageService;

    // ✅ Préfixe centralisé pour toutes les clés Redis d'upload
    this.keyPrefix = "chat:cache:upload";

    this.chunksBaseDir = path.resolve("./storage/chunks");
    this.sessionTTL = 7200; // 2h pour terminer un upload chunké
    this.tokenTTL = 3600; // 1h pour le mapping token → fileId
    this.maxChunkSize = 5 * 1024 * 1024; // 5 MB par chunk
    this.maxFileSize = 500 * 1024 * 1024; // 500 MB max total

    // Créer le dossier de base des chunks
    fs.ensureDirSync(this.chunksBaseDir);

    console.log("✅ ChunkedUploadService initialisé:", {
      chunksBaseDir: this.chunksBaseDir,
      maxChunkSize: `${this.maxChunkSize / 1024 / 1024} MB`,
      maxFileSize: `${this.maxFileSize / 1024 / 1024} MB`,
    });
  }

  // =============================================
  // 1. INITIALISATION D'UN UPLOAD CHUNKÉ
  // =============================================

  /**
   * Crée une session d'upload chunké
   * @param {Object} params - { fileName, fileSize, mimeType, totalChunks, uploadToken, userId, conversationId }
   * @returns {Object} - { uploadId, chunkSize, totalChunks, expiresAt }
   */
  async initUpload({
    fileName,
    fileSize,
    mimeType,
    totalChunks,
    uploadToken,
    userId,
    conversationId,
  }) {
    // ✅ VALIDATION
    if (!fileName || !fileSize || !mimeType || !totalChunks) {
      throw new Error(
        "Paramètres requis : fileName, fileSize, mimeType, totalChunks",
      );
    }

    if (fileSize > this.maxFileSize) {
      throw new Error(
        `Taille fichier (${Math.round(fileSize / 1024 / 1024)} MB) dépasse la limite de ${this.maxFileSize / 1024 / 1024} MB`,
      );
    }

    if (!userId) {
      throw new Error("userId requis");
    }

    // ✅ IDEMPOTENCE : si le token existe déjà, retourner la session existante
    if (uploadToken && this.redis) {
      const existingFileId = await this.redis.get(
        `${this.keyPrefix}:token:${uploadToken}`,
      );
      if (existingFileId) {
        const existingData = JSON.parse(existingFileId);
        return {
          uploadId: existingData.uploadId || existingData.id,
          status: "already_completed",
          data: existingData,
        };
      }

      // Vérifier si une session chunked existe déjà pour ce token
      const existingSession = await this.redis.get(
        `${this.keyPrefix}:token:session:${uploadToken}`,
      );
      if (existingSession) {
        const session = JSON.parse(existingSession);
        return {
          uploadId: session.uploadId,
          chunkSize: this.maxChunkSize,
          totalChunks: session.totalChunks,
          uploadedChunks: session.uploadedChunks,
          status: "in_progress",
          expiresAt: session.expiresAt,
        };
      }
    }

    // ✅ CRÉER LA SESSION
    const uploadId = uuidv4().replace(/-/g, "");
    const chunkDir = path.join(this.chunksBaseDir, uploadId);
    await fs.ensureDir(chunkDir);

    const expiresAt = new Date(Date.now() + this.sessionTTL * 1000);

    const session = {
      uploadId,
      fileName,
      fileSize,
      mimeType,
      totalChunks: parseInt(totalChunks),
      uploadedChunks: [],
      chunkSize: this.maxChunkSize,
      userId,
      conversationId: conversationId || null,
      uploadToken: uploadToken || null,
      status: "uploading",
      chunkDir,
      createdAt: new Date().toISOString(),
      expiresAt: expiresAt.toISOString(),
    };

    // ✅ STOCKER EN REDIS
    if (this.redis) {
      await this.redis.setEx(
        `${this.keyPrefix}:chunked:${uploadId}`,
        this.sessionTTL,
        JSON.stringify(session),
      );

      // Mapping token → session
      if (uploadToken) {
        await this.redis.setEx(
          `${this.keyPrefix}:token:session:${uploadToken}`,
          this.sessionTTL,
          JSON.stringify(session),
        );
      }
    }

    console.log(`📦 Session upload chunké créée:`, {
      uploadId,
      fileName,
      fileSize: `${Math.round(fileSize / 1024 / 1024)} MB`,
      totalChunks,
      userId,
    });

    return {
      uploadId,
      chunkSize: this.maxChunkSize,
      totalChunks: session.totalChunks,
      uploadedChunks: [],
      status: "uploading",
      expiresAt: session.expiresAt,
    };
  }

  // =============================================
  // 2. RÉCEPTION D'UN CHUNK
  // =============================================

  /**
   * Stocke un chunk sur disque
   * @param {string} uploadId
   * @param {number} chunkIndex
   * @param {Buffer} chunkBuffer
   * @returns {Object} - { received, total, remaining, uploadedChunks }
   */
  async storeChunk(uploadId, chunkIndex, chunkBuffer) {
    // ✅ RÉCUPÉRER LA SESSION
    const session = await this._getSession(uploadId);
    if (!session) {
      throw new Error(`Session d'upload ${uploadId} introuvable ou expirée`);
    }

    if (session.status !== "uploading") {
      throw new Error(
        `Upload ${uploadId} n'est pas en cours (${session.status})`,
      );
    }

    const idx = parseInt(chunkIndex);
    if (isNaN(idx) || idx < 0 || idx >= session.totalChunks) {
      throw new Error(
        `Index de chunk invalide: ${chunkIndex} (attendu 0-${session.totalChunks - 1})`,
      );
    }

    if (chunkBuffer.length > this.maxChunkSize + 1024) {
      // +1KB de marge
      throw new Error(
        `Chunk trop volumineux: ${chunkBuffer.length} octets (max ${this.maxChunkSize})`,
      );
    }

    // ✅ ÉCRIRE SUR DISQUE
    const chunkPath = path.join(
      session.chunkDir,
      `chunk_${String(idx).padStart(5, "0")}`,
    );
    await fs.writeFile(chunkPath, chunkBuffer);

    // ✅ SADD ATOMIQUE : évite la race condition quand plusieurs chunks arrivent en parallèle
    // (SADD est une opération atomique Redis, contrairement au read-modify-write du tableau)
    let uploadedCount = session.totalChunks; // fallback si pas de Redis
    if (this.redis) {
      const chunkSetKey = `${this.keyPrefix}:chunks:set:${uploadId}`;
      await this.redis.sAdd(chunkSetKey, String(idx));
      await this.redis.expire(chunkSetKey, this.sessionTTL);
      uploadedCount = await this.redis.sCard(chunkSetKey);

      // Mettre à jour le token:session avec juste le compteur (pas le tableau complet)
      if (session.uploadToken) {
        await this.redis.setEx(
          `${this.keyPrefix}:token:session:${session.uploadToken}`,
          this.sessionTTL,
          JSON.stringify({ ...session, uploadedCount }),
        );
      }
    }

    const remaining = session.totalChunks - uploadedCount;

    console.log(
      `📥 Chunk ${idx}/${session.totalChunks - 1} reçu pour ${uploadId} (${remaining} restant(s))`,
    );

    return {
      received: idx,
      total: session.totalChunks,
      remaining,
      uploadedCount,
      isComplete: remaining === 0,
    };
  }

  // =============================================
  // 3. ASSEMBLAGE ET FINALISATION
  // =============================================

  /**
   * Assemble les chunks en un fichier, uploade vers MinIO, retourne le chemin
   * @param {string} uploadId
   * @returns {Object} - { assembledFilePath, remotePath, session }
   */
  async completeUpload(uploadId) {
    // ✅ RÉCUPÉRER ET VALIDER LA SESSION
    const session = await this._getSession(uploadId);
    if (!session) {
      throw new Error(`Session d'upload ${uploadId} introuvable ou expirée`);
    }

    // ✅ VÉRIFIER QUE TOUS LES CHUNKS SONT PRÉSENTS — SOURCE DE VÉRITÉ = DISQUE
    // On ne se fie PAS à session.uploadedChunks (sujet à race condition Redis)
    const missingChunks = [];
    for (let i = 0; i < session.totalChunks; i++) {
      const chunkPath = path.join(
        session.chunkDir,
        `chunk_${String(i).padStart(5, "0")}`,
      );
      if (!(await fs.pathExists(chunkPath))) {
        missingChunks.push(i);
      }
    }

    if (missingChunks.length > 0) {
      throw new Error(
        `Chunks manquants: ${missingChunks.join(", ")} (${missingChunks.length}/${session.totalChunks})`,
      );
    }

    // ✅ MARQUER EN COURS D'ASSEMBLAGE
    session.status = "assembling";
    if (this.redis) {
      await this.redis.setEx(
        `${this.keyPrefix}:chunked:${uploadId}`,
        this.sessionTTL,
        JSON.stringify(session),
      );
    }

    console.log(
      `🔧 Assemblage de ${session.totalChunks} chunks pour ${uploadId}...`,
    );

    // ✅ ASSEMBLER LES CHUNKS EN UN FICHIER UNIQUE SUR DISQUE
    const ext = path.extname(session.fileName) || ".bin";
    const safeFileName = `${uploadId}${ext.toLowerCase()}`;
    const assembledFilePath = path.join(session.chunkDir, safeFileName);

    const writeStream = fs.createWriteStream(assembledFilePath);

    for (let i = 0; i < session.totalChunks; i++) {
      const chunkPath = path.join(
        session.chunkDir,
        `chunk_${String(i).padStart(5, "0")}`,
      );
      const chunkData = await fs.readFile(chunkPath);
      writeStream.write(chunkData);
    }

    await new Promise((resolve, reject) => {
      writeStream.end(resolve);
      writeStream.on("error", reject);
    });

    console.log(`✅ Fichier assemblé: ${assembledFilePath}`);

    // ✅ UPLOAD VERS MINIO/SFTP VIA fPutObject (PAS de chargement en RAM)
    const remotePath = await this.fileStorageService.upload(
      assembledFilePath,
      safeFileName,
    );

    console.log(`✅ Fichier uploadé vers stockage: ${remotePath}`);

    // ✅ MARQUER COMME COMPLÉTÉ
    session.status = "uploaded";
    session.remotePath = remotePath;
    session.safeFileName = safeFileName;

    if (this.redis) {
      await this.redis.setEx(
        `${this.keyPrefix}:chunked:${uploadId}`,
        this.sessionTTL,
        JSON.stringify(session),
      );
    }

    return {
      assembledFilePath,
      remotePath,
      safeFileName,
      session,
    };
  }

  // =============================================
  // 4. NETTOYAGE DES CHUNKS
  // =============================================

  /**
   * Supprime les fichiers temporaires d'un upload
   */
  async cleanup(uploadId) {
    try {
      const chunkDir = path.join(this.chunksBaseDir, uploadId);
      if (await fs.pathExists(chunkDir)) {
        await fs.remove(chunkDir);
        console.log(`🗑️ Chunks nettoyés: ${uploadId}`);
      }

      // Supprimer la session Redis
      if (this.redis) {
        const session = await this._getSession(uploadId);
        await this.redis.del(`${this.keyPrefix}:chunked:${uploadId}`);
        if (session?.uploadToken) {
          await this.redis.del(
            `${this.keyPrefix}:token:session:${session.uploadToken}`,
          );
        }
      }
    } catch (error) {
      console.warn(`⚠️ Erreur nettoyage chunks ${uploadId}:`, error.message);
    }
  }

  /**
   * Nettoie les uploads expirés (à appeler périodiquement)
   */
  async cleanupExpired() {
    try {
      const dirs = await fs.readdir(this.chunksBaseDir);
      let cleaned = 0;

      for (const dir of dirs) {
        const chunkDir = path.join(this.chunksBaseDir, dir);
        const stat = await fs.stat(chunkDir);

        // Supprimer si plus vieux que 2h
        if (Date.now() - stat.mtimeMs > this.sessionTTL * 1000) {
          await fs.remove(chunkDir);
          cleaned++;
        }
      }

      if (cleaned > 0) {
        console.log(`🗑️ ${cleaned} upload(s) chunké(s) expiré(s) nettoyé(s)`);
      }
    } catch (error) {
      console.warn("⚠️ Erreur nettoyage uploads expirés:", error.message);
    }
  }

  // =============================================
  // 5. VÉRIFICATION DU STATUT
  // =============================================

  /**
   * Vérifie le statut d'un upload par token ou uploadId
   * @param {string} tokenOrId
   * @returns {Object|null}
   */
  async getUploadStatus(tokenOrId) {
    if (!this.redis) return null;

    // ✅ D'ABORD : vérifier si c'est un upload monolithique terminé (token → fileId)
    const completedData = await this.redis.get(
      `${this.keyPrefix}:token:${tokenOrId}`,
    );
    if (completedData) {
      return {
        status: "completed",
        data: JSON.parse(completedData),
      };
    }

    // ✅ ENSUITE : vérifier si c'est une session chunked en cours (token → session)
    const sessionByToken = await this.redis.get(
      `${this.keyPrefix}:token:session:${tokenOrId}`,
    );
    if (sessionByToken) {
      const session = JSON.parse(sessionByToken);
      const uploadedCount = await this._getUploadedCount(session.uploadId);
      return {
        status: session.status,
        uploadId: session.uploadId,
        totalChunks: session.totalChunks,
        uploadedCount,
        remaining: session.totalChunks - uploadedCount,
        progress: Math.round((uploadedCount / session.totalChunks) * 100),
        expiresAt: session.expiresAt,
      };
    }

    // ✅ ENFIN : vérifier directement par uploadId
    const sessionById = await this.redis.get(
      `${this.keyPrefix}:chunked:${tokenOrId}`,
    );
    if (sessionById) {
      const session = JSON.parse(sessionById);
      const uploadedCount = await this._getUploadedCount(session.uploadId);
      return {
        status: session.status,
        uploadId: session.uploadId,
        totalChunks: session.totalChunks,
        uploadedCount,
        remaining: session.totalChunks - uploadedCount,
        progress: Math.round((uploadedCount / session.totalChunks) * 100),
        expiresAt: session.expiresAt,
      };
    }

    return { status: "not_found" };
  }

  // =============================================
  // 6. TOKEN MANAGEMENT (pour upload monolithique)
  // =============================================

  /**
   * Vérifie si un upload_token correspond à un upload déjà terminé
   * @returns {Object|null} - Les données du fichier si déjà uploadé
   */
  async checkToken(uploadToken) {
    if (!this.redis || !uploadToken) return null;

    const data = await this.redis.get(`${this.keyPrefix}:token:${uploadToken}`);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Enregistre le mapping token → résultat de l'upload
   */
  async storeTokenResult(uploadToken, resultData) {
    if (!this.redis || !uploadToken) return;

    await this.redis.setEx(
      `${this.keyPrefix}:token:${uploadToken}`,
      this.tokenTTL,
      JSON.stringify(resultData),
    );

    // Supprimer la session chunked associée si elle existe
    await this.redis.del(`${this.keyPrefix}:token:session:${uploadToken}`);
  }

  // =============================================
  // MÉTHODES PRIVÉES
  // =============================================

  async _getSession(uploadId) {
    if (!this.redis) return null;

    const data = await this.redis.get(`${this.keyPrefix}:chunked:${uploadId}`);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Retourne le nombre de chunks reçus via le Set Redis atomique
   * Fallback sur 0 si Redis absent
   */
  async _getUploadedCount(uploadId) {
    if (!this.redis) return 0;
    return await this.redis.sCard(`${this.keyPrefix}:chunks:set:${uploadId}`);
  }
}

module.exports = ChunkedUploadService;
