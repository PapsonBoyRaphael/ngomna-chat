"use strict";

const crypto = require("crypto");
const UserEncryptionKeyModel = require("../mongodb/models/UserEncryptionKeyModel");

/**
 * KeyManagementService
 *
 * Gère les clés publiques RSA des utilisateurs pour le chiffrement E2EE.
 *
 * Architecture :
 *  - Cache Redis (TTL 1h) pour les lectures fréquentes (clé active par userId)
 *  - MongoDB pour la persistance et l'historique des versions (rotation)
 *
 * RÈGLE DE SÉCURITÉ : Ce service ne stocke et ne manipule JAMAIS de clés privées.
 * Les clés privées restent exclusivement côté client (Keychain / SecureStorage).
 *
 * Cohérence avec EncryptionService :
 *  - getPublicKey(userId) → retourne un PEM string utilisable directement dans
 *    encryptionService.encryptText(text, publicKey) et encryptionService.encryptFile(buffer, publicKey)
 *  - Le fingerprint est calculé avec le même algorithme SHA-256 que
 *    encryptionService.getPublicKeyFingerprint()
 */

/** Préfixe des clés Redis pour les clés publiques actives */
const REDIS_KEY_PREFIX = "chat:encryption:pubkey";

/** TTL cache Redis en secondes (1 heure) */
const REDIS_TTL = 3600;

class KeyManagementService {
  /**
   * @param {import('redis').RedisClientType} redisClient - Client Redis (node-redis v4)
   * @param {object} [options]
   * @param {number}  [options.cacheTTL=3600]         - TTL cache Redis en secondes
   * @param {string}  [options.redisKeyPrefix]        - Préfixe des clés Redis
   * @param {object}  [options.logger=console]        - Logger { info, warn, error }
   */
  constructor(redisClient, options = {}) {
    this._redis = redisClient;
    this._cacheTTL = options.cacheTTL ?? REDIS_TTL;
    this._prefix = options.redisKeyPrefix ?? REDIS_KEY_PREFIX;
    this._logger = options.logger ?? console;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Enregistrement / Mise à jour
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Enregistre ou met à jour la clé publique d'un utilisateur.
   *
   * Si l'utilisateur n'a pas encore de clé → version "1", isActive: true.
   * Si l'utilisateur a déjà une clé active → rotation automatique
   *   (ancienne version désactivée, nouvelle version créée).
   *
   * @param {string} userId          - Identifiant de l'utilisateur (matricule)
   * @param {string} publicKeyPem    - Clé publique RSA au format PEM
   * @param {object} [deviceInfo]    - { platform, deviceId, deviceName }
   * @returns {Promise<KeyRegistrationResult>}
   */
  async registerPublicKey(userId, publicKeyPem, deviceInfo = {}) {
    this._assertUserId(userId);
    this._assertPublicKeyPem(publicKeyPem);

    const fingerprint = this._computeFingerprint(publicKeyPem);

    // Récupérer la version active courante
    const existing = await UserEncryptionKeyModel.findOne({
      userId,
      isActive: true,
    }).lean();

    // Si même fingerprint → clé déjà à jour, rien à faire
    if (existing && existing.fingerprint === fingerprint) {
      this._logger.info(
        `[KeyManagementService] Clé déjà à jour pour userId=${userId} v${existing.keyVersion}`,
      );
      return {
        userId,
        keyVersion: existing.keyVersion,
        fingerprint,
        isRotation: false,
      };
    }

    // Calculer la nouvelle version
    const newVersion = existing ? String(Number(existing.keyVersion) + 1) : "1";

    // Désactiver l'ancienne version si elle existe
    if (existing) {
      await UserEncryptionKeyModel.updateMany(
        { userId, isActive: true },
        { $set: { isActive: false } },
      );
      this._logger.info(
        `[KeyManagementService] 🔄 Rotation de clé pour userId=${userId}: v${existing.keyVersion} → v${newVersion}`,
      );
    }

    // Créer la nouvelle version
    await UserEncryptionKeyModel.create({
      userId,
      publicKey: publicKeyPem,
      keyVersion: newVersion,
      fingerprint,
      isActive: true,
      deviceInfo: {
        platform: deviceInfo.platform ?? null,
        deviceId: deviceInfo.deviceId ?? null,
        deviceName: deviceInfo.deviceName ?? null,
      },
    });

    // Mettre à jour le cache Redis
    await this._cachePublicKey(userId, publicKeyPem);

    this._logger.info(
      `[KeyManagementService] ✅ Clé enregistrée pour userId=${userId} v${newVersion}`,
    );

    return {
      userId,
      keyVersion: newVersion,
      fingerprint,
      isRotation: !!existing,
      previousVersion: existing?.keyVersion ?? null,
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Lecture
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Retourne la clé publique PEM active d'un utilisateur.
   * Stratégie : cache Redis → MongoDB.
   *
   * Le PEM retourné peut être passé directement à :
   *   encryptionService.encryptText(text, publicKey)
   *   encryptionService.encryptFile(buffer, publicKey)
   *
   * @param {string} userId
   * @returns {Promise<string>} Clé publique PEM
   * @throws {Error} Si aucune clé n'est trouvée
   */
  async getPublicKey(userId) {
    this._assertUserId(userId);

    // 1. Essayer le cache Redis
    const cached = await this._getCachedPublicKey(userId);
    if (cached) return cached;

    // 2. Fallback MongoDB
    const keyDoc = await UserEncryptionKeyModel.findOne({
      userId,
      isActive: true,
    }).lean();

    if (!keyDoc) {
      throw new Error(
        `[KeyManagementService] Aucune clé publique active pour userId: ${userId}`,
      );
    }

    // Remettre en cache
    await this._cachePublicKey(userId, keyDoc.publicKey);

    return keyDoc.publicKey;
  }

  /**
   * Retourne les métadonnées de la clé active (sans la clé PEM complète).
   *
   * @param {string} userId
   * @returns {Promise<KeyMetadata|null>}
   */
  async getKeyMetadata(userId) {
    this._assertUserId(userId);

    const keyDoc = await UserEncryptionKeyModel.findOne(
      { userId, isActive: true },
      { publicKey: 0 }, // Ne pas retourner le PEM complet
    ).lean();

    if (!keyDoc) return null;

    return {
      userId: keyDoc.userId,
      keyVersion: keyDoc.keyVersion,
      fingerprint: keyDoc.fingerprint,
      isActive: keyDoc.isActive,
      deviceInfo: keyDoc.deviceInfo,
      createdAt: keyDoc.createdAt,
      updatedAt: keyDoc.updatedAt,
    };
  }

  /**
   * Retourne une version spécifique d'une clé (pour déchiffrer d'anciens messages).
   *
   * @param {string} userId
   * @param {string} keyVersion
   * @returns {Promise<string>} Clé publique PEM
   */
  async getPublicKeyByVersion(userId, keyVersion) {
    this._assertUserId(userId);

    const keyDoc = await UserEncryptionKeyModel.findOne({
      userId,
      keyVersion,
    }).lean();

    if (!keyDoc) {
      throw new Error(
        `[KeyManagementService] Clé v${keyVersion} introuvable pour userId: ${userId}`,
      );
    }

    return keyDoc.publicKey;
  }

  /**
   * Retourne toutes les versions de clé d'un utilisateur (sans les PEM).
   *
   * @param {string} userId
   * @returns {Promise<KeyMetadata[]>}
   */
  async getKeyHistory(userId) {
    this._assertUserId(userId);

    const docs = await UserEncryptionKeyModel.find({ userId }, { publicKey: 0 })
      .sort({ keyVersion: -1 })
      .lean();

    return docs.map((d) => ({
      userId: d.userId,
      keyVersion: d.keyVersion,
      fingerprint: d.fingerprint,
      isActive: d.isActive,
      deviceInfo: d.deviceInfo,
      createdAt: d.createdAt,
    }));
  }

  /**
   * Vérifie si un utilisateur a une clé publique enregistrée.
   *
   * @param {string} userId
   * @returns {Promise<boolean>}
   */
  async hasPublicKey(userId) {
    this._assertUserId(userId);
    const count = await UserEncryptionKeyModel.countDocuments({
      userId,
      isActive: true,
    });
    return count > 0;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Révocation / Suppression
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Révoque la clé active d'un utilisateur (désactive sans supprimer l'historique).
   * À utiliser en cas de compromission suspectée.
   *
   * @param {string} userId
   * @returns {Promise<{ revoked: boolean, keyVersion: string|null }>}
   */
  async revokeKey(userId) {
    this._assertUserId(userId);

    const result = await UserEncryptionKeyModel.updateMany(
      { userId, isActive: true },
      { $set: { isActive: false } },
    );

    // Invalider le cache
    await this._invalidateCache(userId);

    const revoked = result.modifiedCount > 0;
    this._logger.warn(
      `[KeyManagementService] 🚫 Clé révoquée pour userId=${userId} (${result.modifiedCount} doc(s))`,
    );

    return { revoked, revokedCount: result.modifiedCount };
  }

  /**
   * Supprime définitivement TOUTES les clés d'un utilisateur.
   * À utiliser uniquement lors de la suppression du compte.
   *
   * @param {string} userId
   * @returns {Promise<{ deletedCount: number }>}
   */
  async deleteAllKeys(userId) {
    this._assertUserId(userId);

    const result = await UserEncryptionKeyModel.deleteMany({ userId });

    await this._invalidateCache(userId);

    this._logger.warn(
      `[KeyManagementService] 🗑️ ${result.deletedCount} clé(s) supprimée(s) pour userId=${userId}`,
    );

    return { deletedCount: result.deletedCount };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Validation
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Vérifie qu'un fingerprint correspond à la clé active d'un utilisateur.
   * Utilisé par le client pour valider l'identité (TOFU).
   *
   * @param {string} userId
   * @param {string} fingerprint - Fingerprint SHA-256 hex à vérifier
   * @returns {Promise<boolean>}
   */
  async verifyFingerprint(userId, fingerprint) {
    this._assertUserId(userId);

    const keyDoc = await UserEncryptionKeyModel.findOne(
      { userId, isActive: true },
      { fingerprint: 1 },
    ).lean();

    if (!keyDoc) return false;
    return keyDoc.fingerprint === fingerprint;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Méthodes privées — Cache Redis
  // ─────────────────────────────────────────────────────────────────────────────

  _redisKey(userId) {
    return `${this._prefix}:${userId}`;
  }

  async _getCachedPublicKey(userId) {
    if (!this._redis) return null;
    try {
      return await this._redis.get(this._redisKey(userId));
    } catch (err) {
      this._logger.warn(
        `[KeyManagementService] Cache Redis get error: ${err.message}`,
      );
      return null;
    }
  }

  async _cachePublicKey(userId, publicKeyPem) {
    if (!this._redis) return;
    try {
      await this._redis.set(this._redisKey(userId), publicKeyPem, {
        EX: this._cacheTTL,
      });
    } catch (err) {
      this._logger.warn(
        `[KeyManagementService] Cache Redis set error: ${err.message}`,
      );
    }
  }

  async _invalidateCache(userId) {
    if (!this._redis) return;
    try {
      await this._redis.del(this._redisKey(userId));
    } catch (err) {
      this._logger.warn(
        `[KeyManagementService] Cache Redis del error: ${err.message}`,
      );
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Méthodes privées — Utilitaires
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Calcule le fingerprint SHA-256 d'une clé publique PEM.
   * Identique à EncryptionService.getPublicKeyFingerprint().
   */
  _computeFingerprint(publicKeyPem) {
    const keyBuffer = Buffer.from(
      publicKeyPem
        .replace(/-----BEGIN PUBLIC KEY-----/, "")
        .replace(/-----END PUBLIC KEY-----/, "")
        .replace(/\s+/g, ""),
      "base64",
    );
    return crypto.createHash("sha256").update(keyBuffer).digest("hex");
  }

  _assertUserId(userId) {
    if (!userId || typeof userId !== "string") {
      throw new Error("[KeyManagementService] userId invalide ou manquant");
    }
  }

  _assertPublicKeyPem(pem) {
    if (!pem || typeof pem !== "string" || !pem.includes("BEGIN PUBLIC KEY")) {
      throw new Error(
        "[KeyManagementService] publicKey doit être un PEM valide (format SPKI)",
      );
    }
  }
}

module.exports = KeyManagementService;

/**
 * @typedef {object} KeyRegistrationResult
 * @property {string}       userId
 * @property {string}       keyVersion        - Version enregistrée
 * @property {string}       fingerprint       - SHA-256 hex de la clé
 * @property {boolean}      isRotation        - true si rotation (ancienne clé désactivée)
 * @property {string|null}  [previousVersion] - Version précédente (si rotation)
 *
 * @typedef {object} KeyMetadata
 * @property {string}  userId
 * @property {string}  keyVersion
 * @property {string}  fingerprint
 * @property {boolean} isActive
 * @property {object}  deviceInfo
 * @property {Date}    createdAt
 * @property {Date}    [updatedAt]
 */
