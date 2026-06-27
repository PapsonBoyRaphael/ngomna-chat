"use strict";

const crypto = require("crypto");

/**
 * EncryptionService
 *
 * Service de chiffrement unifié supportant deux modes :
 *  - 'none' : pas de chiffrement applicatif (transport en clair ou TLS géré par le réseau)
 *  - 'e2ee' : chiffrement de bout en bout (AES-256-GCM + RSA-OAEP 4096)
 *
 * Le switch de mode est dynamique (sans redémarrage du serveur).
 *
 * Algorithmes :
 *  - Symétrique  : AES-256-GCM (authentifié, 256-bit key, 16-byte IV)
 *  - Asymétrique : RSA-OAEP avec SHA-256 (4096-bit)
 *  - Hachage     : SHA-256 (intégrité / fingerprint)
 *
 * NOTE : En mode E2EE, le serveur chiffre le message avec la clé publique
 *        du destinataire récupérée via KeyManagementService.
 *        Le serveur NE stocke JAMAIS les clés privées des utilisateurs.
 */

const VALID_MODES = Object.freeze(["none", "e2ee"]);

const ALGORITHM = "aes-256-gcm";
const IV_LENGTH = 16; // bytes
const KEY_LENGTH = 32; // bytes (256 bits)
const AUTH_TAG_LENGTH = 16; // bytes (128 bits — max pour GCM)
const RSA_PADDING = crypto.constants.RSA_PKCS1_OAEP_PADDING;
const RSA_OAEP_HASH = "SHA-256";

class EncryptionService {
  /**
   * @param {object} options
   * @param {string} [options.mode='none']       - Mode initial : 'none' | 'e2ee'
   * @param {object} [options.logger=console]    - Logger compatible { info, warn, error }
   */
  constructor({ mode = "none", logger = console } = {}) {
    this._validateMode(mode);
    this._mode = mode;
    this._logger = logger;

    this._logger.info(`[EncryptionService] Initialisé en mode: ${this._mode}`);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Accesseurs
  // ─────────────────────────────────────────────────────────────────────────────

  /** Retourne le mode actuel */
  getMode() {
    return this._mode;
  }

  /** Indique si le mode E2EE est actif */
  isE2EEEnabled() {
    return this._mode === "e2ee";
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Switch de mode
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Change le mode de chiffrement à chaud (sans redémarrage).
   * @param {string} newMode - 'none' | 'e2ee'
   * @returns {{ previousMode: string, newMode: string }}
   */
  switchMode(newMode) {
    this._validateMode(newMode);
    const previousMode = this._mode;
    this._mode = newMode;
    this._logger.info(
      `[EncryptionService] 🔄 Mode changé: ${previousMode} → ${newMode}`,
    );
    return { previousMode, newMode };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Chiffrement de texte (messages)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Chiffre un contenu texte.
   *
   * Mode 'none' → retourne le texte en clair avec flag encrypted: false.
   * Mode 'e2ee' → AES-256-GCM, clé symétrique chiffrée avec la clé publique RSA
   *               du destinataire.
   *
   * @param {string} plaintext          - Texte à chiffrer
   * @param {string} recipientPublicKey - Clé publique PEM du destinataire (requis pour e2ee)
   * @returns {Promise<EncryptedTextResult>}
   */
  async encryptText(plaintext, recipientPublicKey = null) {
    if (this._mode === "none") {
      return {
        encrypted: false,
        mode: "none",
        content: plaintext,
      };
    }

    // Mode e2ee
    this._assertPublicKey(recipientPublicKey, "encryptText");

    const symmetricKey = crypto.randomBytes(KEY_LENGTH);
    const iv = crypto.randomBytes(IV_LENGTH);

    const cipher = crypto.createCipheriv(ALGORITHM, symmetricKey, iv, {
      authTagLength: AUTH_TAG_LENGTH,
    });

    const encryptedContent =
      cipher.update(plaintext, "utf8", "base64") + cipher.final("base64");
    const authTag = cipher.getAuthTag();

    const encryptedKey = crypto.publicEncrypt(
      {
        key: recipientPublicKey,
        padding: RSA_PADDING,
        oaepHash: RSA_OAEP_HASH,
      },
      symmetricKey,
    );

    return {
      encrypted: true,
      mode: "e2ee",
      encryptedContent: encryptedContent,
      encryptionIV: iv.toString("base64"),
      encryptionTag: authTag.toString("base64"),
      encryptedKey: encryptedKey.toString("base64"),
    };
  }

  /**
   * Déchiffre un contenu texte précédemment chiffré par encryptText().
   *
   * @param {EncryptedTextResult} encryptedData
   * @param {string}              privateKey  - Clé privée PEM du destinataire (requis pour e2ee)
   * @returns {Promise<string>} Texte en clair
   */
  async decryptText(encryptedData, privateKey = null) {
    if (!encryptedData || encryptedData.encrypted === false) {
      return encryptedData?.content ?? encryptedData;
    }

    this._assertPrivateKey(privateKey, "decryptText");

    const symmetricKey = crypto.privateDecrypt(
      { key: privateKey, padding: RSA_PADDING, oaepHash: RSA_OAEP_HASH },
      Buffer.from(encryptedData.encryptedKey, "base64"),
    );

    const decipher = crypto.createDecipheriv(
      ALGORITHM,
      symmetricKey,
      Buffer.from(encryptedData.encryptionIV, "base64"),
      { authTagLength: AUTH_TAG_LENGTH },
    );

    decipher.setAuthTag(Buffer.from(encryptedData.encryptionTag, "base64"));

    const plaintext =
      decipher.update(encryptedData.encryptedContent, "base64", "utf8") +
      decipher.final("utf8");

    return plaintext;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Chiffrement de fichier (buffer binaire)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Chiffre un buffer de fichier.
   *
   * Mode 'none' → retourne le buffer original avec flag encrypted: false.
   * Mode 'e2ee' → AES-256-GCM, clé symétrique chiffrée avec la clé publique RSA.
   *
   * @param {Buffer} fileBuffer           - Buffer du fichier original
   * @param {string} recipientPublicKey   - Clé publique PEM (requis pour e2ee)
   * @returns {Promise<EncryptedFileResult>}
   */
  async encryptFile(fileBuffer, recipientPublicKey = null) {
    if (this._mode === "none") {
      return {
        encrypted: false,
        mode: "none",
        buffer: fileBuffer,
      };
    }

    this._assertPublicKey(recipientPublicKey, "encryptFile");

    const symmetricKey = crypto.randomBytes(KEY_LENGTH);
    const iv = crypto.randomBytes(IV_LENGTH);

    const cipher = crypto.createCipheriv(ALGORITHM, symmetricKey, iv, {
      authTagLength: AUTH_TAG_LENGTH,
    });

    const encryptedBuffer = Buffer.concat([
      cipher.update(fileBuffer),
      cipher.final(),
    ]);
    const authTag = cipher.getAuthTag();

    const encryptedKey = crypto.publicEncrypt(
      {
        key: recipientPublicKey,
        padding: RSA_PADDING,
        oaepHash: RSA_OAEP_HASH,
      },
      symmetricKey,
    );

    return {
      encrypted: true,
      mode: "e2ee",
      buffer: encryptedBuffer,
      iv: iv.toString("base64"),
      tag: authTag.toString("base64"),
      encryptedKey: encryptedKey.toString("base64"),
    };
  }

  /**
   * Déchiffre un buffer de fichier précédemment chiffré par encryptFile().
   *
   * @param {Buffer}              encryptedBuffer
   * @param {FileEncryptionMeta}  meta         - { iv, tag, encryptedKey }
   * @param {string}              privateKey   - Clé privée PEM (requis pour e2ee)
   * @returns {Promise<Buffer>} Buffer en clair
   */
  async decryptFile(encryptedBuffer, meta, privateKey = null) {
    if (!meta || meta.encrypted === false) {
      return encryptedBuffer;
    }

    this._assertPrivateKey(privateKey, "decryptFile");

    const symmetricKey = crypto.privateDecrypt(
      { key: privateKey, padding: RSA_PADDING, oaepHash: RSA_OAEP_HASH },
      Buffer.from(meta.encryptedKey, "base64"),
    );

    const decipher = crypto.createDecipheriv(
      ALGORITHM,
      symmetricKey,
      Buffer.from(meta.iv, "base64"),
      { authTagLength: AUTH_TAG_LENGTH },
    );

    decipher.setAuthTag(Buffer.from(meta.tag, "base64"));

    return Buffer.concat([decipher.update(encryptedBuffer), decipher.final()]);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Utilitaires
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Génère une paire de clés RSA-4096 (usage : tests, génération initiale côté serveur).
   * En production, les clés privées des utilisateurs sont générées côté client.
   *
   * @returns {{ publicKey: string, privateKey: string }} Clés au format PEM
   */
  generateKeyPair() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
      modulusLength: 4096,
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
    });
    return { publicKey, privateKey };
  }

  /**
   * Calcule le fingerprint SHA-256 d'une clé publique PEM.
   * Utile pour la vérification d'identité côté client (TOFU — Trust On First Use).
   *
   * @param {string} publicKeyPem
   * @returns {string} fingerprint hex
   */
  getPublicKeyFingerprint(publicKeyPem) {
    const keyBuffer = Buffer.from(
      publicKeyPem
        .replace(/-----BEGIN PUBLIC KEY-----/, "")
        .replace(/-----END PUBLIC KEY-----/, "")
        .replace(/\s+/g, ""),
      "base64",
    );
    return crypto.createHash("sha256").update(keyBuffer).digest("hex");
  }

  /**
   * Retourne un résumé de la configuration courante (sans données sensibles).
   * @returns {object}
   */
  getConfig() {
    return {
      mode: this._mode,
      algorithm: ALGORITHM,
      keyLength: KEY_LENGTH * 8, // bits
      ivLength: IV_LENGTH * 8,
      authTagLength: AUTH_TAG_LENGTH * 8,
      rsaPadding: "OAEP",
      oaepHash: RSA_OAEP_HASH,
    };
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Méthodes privées
  // ─────────────────────────────────────────────────────────────────────────────

  _validateMode(mode) {
    if (!VALID_MODES.includes(mode)) {
      throw new Error(
        `[EncryptionService] Mode invalide: "${mode}". Modes acceptés: ${VALID_MODES.join(", ")}`,
      );
    }
  }

  _assertPublicKey(key, methodName) {
    if (!key || typeof key !== "string") {
      throw new Error(
        `[EncryptionService] ${methodName}() requiert une clé publique PEM valide en mode e2ee`,
      );
    }
  }

  _assertPrivateKey(key, methodName) {
    if (!key || typeof key !== "string") {
      throw new Error(
        `[EncryptionService] ${methodName}() requiert une clé privée PEM valide pour le déchiffrement`,
      );
    }
  }
}

module.exports = EncryptionService;

/**
 * @typedef {object} EncryptedTextResult
 * @property {boolean}  encrypted        - true si chiffré
 * @property {string}   mode             - 'none' | 'e2ee'
 * @property {string}   [content]        - Texte en clair (mode none)
 * @property {string}   [encryptedContent] - Texte chiffré base64 (mode e2ee)
 * @property {string}   [encryptionIV]   - IV base64
 * @property {string}   [encryptionTag]  - Auth tag GCM base64
 * @property {string}   [encryptedKey]   - Clé symétrique chiffrée RSA base64
 *
 * @typedef {object} EncryptedFileResult
 * @property {boolean}  encrypted        - true si chiffré
 * @property {string}   mode             - 'none' | 'e2ee'
 * @property {Buffer}   buffer           - Buffer (chiffré ou original)
 * @property {string}   [iv]             - IV base64
 * @property {string}   [tag]            - Auth tag GCM base64
 * @property {string}   [encryptedKey]   - Clé symétrique chiffrée RSA base64
 *
 * @typedef {object} FileEncryptionMeta
 * @property {string}   iv               - IV base64
 * @property {string}   tag              - Auth tag GCM base64
 * @property {string}   encryptedKey     - Clé symétrique chiffrée RSA base64
 */
