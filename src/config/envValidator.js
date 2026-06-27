const fs = require("fs");
const path = require("path");

class EnvironmentValidator {
  constructor() {
    this.requiredVars = ["NODE_ENV", "MONGODB_URI", "PORT", "JWT_SECRET"];

    this.optionalVars = ["REDIS_HOST" , "AUTH_SERVICE_URL"];

    this.warnings = [];
    this.errors = [];
  }

  validate() {
    console.log("🔍 Validation de la configuration environnement...");

    // Vérifier les variables obligatoires
    this.checkRequiredVars();

    // Vérifier les variables optionnelles
    this.checkOptionalVars();

    // Vérifier les dossiers de storage
    this.checkStoragePaths();

    // Vérifier la cohérence
    this.checkConsistency();

    // Afficher les résultats
    this.displayResults();

    return this.errors.length === 0;
  }

  checkRequiredVars() {
    this.requiredVars.forEach((varName) => {
      if (!process.env[varName]) {
        this.errors.push(`❌ Variable obligatoire manquante: ${varName}`);
      } else {
        console.log(`✅ ${varName} configuré`);
      }
    });
  }

  checkOptionalVars() {
    this.optionalVars.forEach((varName) => {
      if (!process.env[varName]) {
        this.warnings.push(
          `⚠️ Variable optionnelle non configurée: ${varName}`
        );
      } else {
        console.log(`✅ ${varName} configuré`);
      }
    });
  }

  checkStoragePaths() {
    const storagePaths = [
      process.env.STORAGE_BASE_PATH || "./storage",
      process.env.STORAGE_UPLOAD_PATH || "./storage/uploads",
      process.env.STORAGE_TEMP_PATH || "./storage/temp",
      process.env.LOG_DIR || "./logs",
    ];

    storagePaths.forEach((pathStr) => {
      try {
        if (!fs.existsSync(pathStr)) {
          fs.mkdirSync(pathStr, { recursive: true });
          console.log(`📁 Dossier créé: ${pathStr}`);
        } else {
          console.log(`✅ Dossier existe: ${pathStr}`);
        }
      } catch (error) {
        this.errors.push(
          `❌ Erreur création dossier ${pathStr}: ${error.message}`
        );
      }
    });
  }

  checkConsistency() {
    // Vérifier JWT en production
    if (process.env.NODE_ENV === "production") {
      if (process.env.JWT_SECRET === "CHATAPP_NGOMNA_PRIVATE_KEY") {
        this.errors.push("❌ JWT_SECRET par défaut en production!");
      }

      if (process.env.REDIS_PASSWORD === "") {
        this.warnings.push("⚠️ Redis sans mot de passe en production");
      }
    }

    // Vérifier les ports
    const port = parseInt(process.env.PORT);
    if (isNaN(port) || port < 1000 || port > 65535) {
      this.errors.push("❌ PORT invalide (doit être entre 1000-65535)");
    }

    // Vérifier ENABLE flags
   
    const enableRedis = process.env.ENABLE_REDIS === "true";
    
    
    if (enableRedis && !process.env.REDIS_HOST) {
      this.warnings.push("⚠️ ENABLE_REDIS=true mais REDIS_HOST non configuré");
    }
  }

  displayResults() {
    console.log("\n📋 Résultats de la validation:");

    if (this.errors.length > 0) {
      console.log("\n❌ ERREURS:");
      this.errors.forEach((error) => console.log(error));
    }

    if (this.warnings.length > 0) {
      console.log("\n⚠️ AVERTISSEMENTS:");
      this.warnings.forEach((warning) => console.log(warning));
    }

    if (this.errors.length === 0 && this.warnings.length === 0) {
      console.log("✅ Configuration environnement parfaite!");
    }

    console.log(
      `\n📊 Résumé: ${this.errors.length} erreur(s), ${this.warnings.length} avertissement(s)\n`
    );
  }
}

module.exports = EnvironmentValidator;
