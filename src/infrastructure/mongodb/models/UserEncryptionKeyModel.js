"use strict";

const mongoose = require("mongoose");

/**
 * UserEncryptionKeyModel
 *
 * Stocke les clés publiques RSA des utilisateurs pour le chiffrement E2EE.
 *
 * Règles importantes :
 *  - La clé PRIVÉE n'est JAMAIS stockée côté serveur.
 *  - Chaque utilisateur peut avoir plusieurs versions de clé (rotation).
 *  - La version active est identifiée par `isActive: true`.
 *  - Les anciennes versions sont conservées pour déchiffrer les anciens messages.
 */

const encryptionKeySchema = new mongoose.Schema(
  {
    // userId (matricule / string ID) — correspond à senderId/receiverId dans Message
    userId: {
      type: String,
      required: true,
      index: true,
      trim: true,
    },

    // Clé publique RSA au format PEM (SPKI)
    publicKey: {
      type: String,
      required: true,
    },

    // Version de la clé (incrémentée à chaque rotation)
    keyVersion: {
      type: String,
      required: true,
      default: "1",
    },

    // Fingerprint SHA-256 de la clé publique (vérification côté client — TOFU)
    fingerprint: {
      type: String,
      required: true,
      index: true,
    },

    // Indique si cette version est la clé active pour chiffrer de nouveaux messages
    isActive: {
      type: Boolean,
      default: true,
      index: true,
    },

    // Date d'expiration optionnelle (null = pas d'expiration automatique)
    expiresAt: {
      type: Date,
      default: null,
      index: { expireAfterSeconds: 0, sparse: true },
    },

    // Métadonnées optionnelles (device info, plateforme client…)
    deviceInfo: {
      platform: { type: String, default: null }, // 'flutter', 'web', 'desktop'
      deviceId: { type: String, default: null },
      deviceName: { type: String, default: null },
    },
  },
  {
    timestamps: true, // createdAt, updatedAt
    collection: "user_encryption_keys",
  },
);

// Index composé : un seul enregistrement actif par userId
encryptionKeySchema.index({ userId: 1, isActive: 1 });

// Index composé : retrouver une version spécifique rapidement
encryptionKeySchema.index({ userId: 1, keyVersion: 1 }, { unique: true });

const UserEncryptionKeyModel = mongoose.model(
  "UserEncryptionKey",
  encryptionKeySchema,
);

module.exports = UserEncryptionKeyModel;
