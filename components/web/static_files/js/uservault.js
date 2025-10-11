// Constants
const CRYPTO_CONSTANTS = {
  CURVE: "P-256",
  ALGORITHM: "ECDH",
  AES_ALGORITHM: "AES-GCM",
  AES_KEY_LENGTH: 256,
  KDF_ALGORITHM: "PBKDF2",
  KDF_ITERATIONS: 100000,
  KDF_HASH: "SHA-256",
  SALT_LENGTH: 16,
  IV_LENGTH: 12,
  EPHEMERAL_KEY_LENGTH: 65, // uncompressed EC point for P-256
  PEM_LINE_LENGTH: 64,
};

// Utility functions
const Base64Utils = {
  arrayBufferToBase64(buffer) {
    return btoa(String.fromCharCode(...new Uint8Array(buffer)));
  },

  base64ToUint8Array(base64) {
    return Uint8Array.from(atob(base64), c => c.charCodeAt(0));
  },

  formatPEM(base64, type) {
    const lines = base64.match(new RegExp(`.{1,${CRYPTO_CONSTANTS.PEM_LINE_LENGTH}}`, 'g')).join("\n");
    return `-----BEGIN ${type}-----\n${lines}\n-----END ${type}-----`;
  },

  stripPEM(pem) {
    return pem.replace(/-----.*?-----|\n/g, "");
  }
};

class UserCryptoVault {
  constructor() {
    this.encoder = new TextEncoder();
    this.decoder = new TextDecoder();
    this.keyPair = null;
    this.salt = null;
    this.iv = null;
    this.wrappedPrivateKey = null;
  }

  // Key pair generation
  async generateKeyPair() {
    this.keyPair = await crypto.subtle.generateKey(
      { name: CRYPTO_CONSTANTS.ALGORITHM, namedCurve: CRYPTO_CONSTANTS.CURVE },
      true,
      ["deriveKey", "deriveBits"]
    );
  }

  // Key export methods
  async exportPublicKeyPEM() {
    const spki = await crypto.subtle.exportKey("spki", this.keyPair.publicKey);
    const b64 = Base64Utils.arrayBufferToBase64(spki);
    return Base64Utils.formatPEM(b64, "PUBLIC KEY");
  }

  async exportPrivateKeyPEM() {
    this._ensureUnlocked("Cannot export private key");
    const pkcs8 = await crypto.subtle.exportKey("pkcs8", this.keyPair.privateKey);
    const b64 = Base64Utils.arrayBufferToBase64(pkcs8);
    return Base64Utils.formatPEM(b64, "PRIVATE KEY");
  }

  // Password-based key encryption/decryption
  async _deriveKeyFromPassword(password, salt, operation) {
    const baseKey = await crypto.subtle.importKey(
      "raw",
      this.encoder.encode(password),
      CRYPTO_CONSTANTS.KDF_ALGORITHM,
      false,
      ["deriveKey"]
    );

    return await crypto.subtle.deriveKey(
      {
        name: CRYPTO_CONSTANTS.KDF_ALGORITHM,
        salt,
        iterations: CRYPTO_CONSTANTS.KDF_ITERATIONS,
        hash: CRYPTO_CONSTANTS.KDF_HASH,
      },
      baseKey,
      { name: CRYPTO_CONSTANTS.AES_ALGORITHM, length: CRYPTO_CONSTANTS.AES_KEY_LENGTH },
      false,
      [operation]
    );
  }

  async wrapPrivateKeyWithPassword(password) {
    this.salt = crypto.getRandomValues(new Uint8Array(CRYPTO_CONSTANTS.SALT_LENGTH));
    this.iv = crypto.getRandomValues(new Uint8Array(CRYPTO_CONSTANTS.IV_LENGTH));

    const kek = await this._deriveKeyFromPassword(password, this.salt, "encrypt");
    const privateKeyRaw = await crypto.subtle.exportKey("pkcs8", this.keyPair.privateKey);

    this.wrappedPrivateKey = await crypto.subtle.encrypt(
      { name: CRYPTO_CONSTANTS.AES_ALGORITHM, iv: this.iv },
      kek,
      privateKeyRaw
    );
  }

  async unlockPrivateKey(wrappedPrivateKey, salt, iv, password, publicKeyPem) {
    const kek = await this._deriveKeyFromPassword(password, salt, "decrypt");

    let rawPrivateKey;
    try {
      rawPrivateKey = await crypto.subtle.decrypt(
        { name: CRYPTO_CONSTANTS.AES_ALGORITHM, iv },
        kek,
        wrappedPrivateKey
      );
    } catch (error) {
      throw new Error("Failed to unlock vault: invalid password or corrupted data");
    }

    const privateKey = await crypto.subtle.importKey(
      "pkcs8",
      rawPrivateKey,
      { name: CRYPTO_CONSTANTS.ALGORITHM, namedCurve: CRYPTO_CONSTANTS.CURVE },
      true,
      ["deriveKey", "deriveBits"]
    );

    const publicKey = await this._importPublicKeyFromPEM(publicKeyPem);
    this.keyPair = { privateKey, publicKey };
  }

  async _importPublicKeyFromPEM(publicKeyPem) {
    const stripped = Base64Utils.stripPEM(publicKeyPem);
    const spkiBytes = Base64Utils.base64ToUint8Array(stripped);

    return await crypto.subtle.importKey(
      "spki",
      spkiBytes.buffer,
      { name: CRYPTO_CONSTANTS.ALGORITHM, namedCurve: CRYPTO_CONSTANTS.CURVE },
      true,
      []
    );
  }

  // Data encryption/decryption
  async encryptData(message) {
    this._ensureUnlocked("Cannot encrypt data");

    const ephemeral = await crypto.subtle.generateKey(
      { name: CRYPTO_CONSTANTS.ALGORITHM, namedCurve: CRYPTO_CONSTANTS.CURVE },
      true,
      ["deriveKey"]
    );

    const sharedKey = await crypto.subtle.deriveKey(
      { name: CRYPTO_CONSTANTS.ALGORITHM, public: this.keyPair.publicKey },
      ephemeral.privateKey,
      { name: CRYPTO_CONSTANTS.AES_ALGORITHM, length: CRYPTO_CONSTANTS.AES_KEY_LENGTH },
      false,
      ["encrypt"]
    );

    const iv = crypto.getRandomValues(new Uint8Array(CRYPTO_CONSTANTS.IV_LENGTH));
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
      { name: CRYPTO_CONSTANTS.AES_ALGORITHM, iv },
      sharedKey,
      this.encoder.encode(message)
    ));

    const ephemeralRaw = new Uint8Array(await crypto.subtle.exportKey("raw", ephemeral.publicKey));

    // Combine [ephemeral | iv | ciphertext]
    const combined = new Uint8Array(ephemeralRaw.length + iv.length + ciphertext.length);
    combined.set(ephemeralRaw, 0);
    combined.set(iv, ephemeralRaw.length);
    combined.set(ciphertext, ephemeralRaw.length + iv.length);

    return "uv:" + Base64Utils.arrayBufferToBase64(combined);
  }

  async decryptData(blobBase64) {
    this._ensureUnlocked("Cannot decrypt data");

    const combined = Base64Utils.base64ToUint8Array(blobBase64.replace(/^uv:/, ""));

    const ephemeralRaw = combined.slice(0, CRYPTO_CONSTANTS.EPHEMERAL_KEY_LENGTH);
    const iv = combined.slice(CRYPTO_CONSTANTS.EPHEMERAL_KEY_LENGTH, CRYPTO_CONSTANTS.EPHEMERAL_KEY_LENGTH + CRYPTO_CONSTANTS.IV_LENGTH);
    const ciphertext = combined.slice(CRYPTO_CONSTANTS.EPHEMERAL_KEY_LENGTH + CRYPTO_CONSTANTS.IV_LENGTH);

    const ephemeralPubKey = await crypto.subtle.importKey(
      "raw",
      ephemeralRaw.buffer,
      { name: CRYPTO_CONSTANTS.ALGORITHM, namedCurve: CRYPTO_CONSTANTS.CURVE },
      true,
      []
    );

    const sharedKey = await crypto.subtle.deriveKey(
      { name: CRYPTO_CONSTANTS.ALGORITHM, public: ephemeralPubKey },
      this.keyPair.privateKey,
      { name: CRYPTO_CONSTANTS.AES_ALGORITHM, length: CRYPTO_CONSTANTS.AES_KEY_LENGTH },
      false,
      ["decrypt"]
    );

    let plaintext;
    try {
      plaintext = await crypto.subtle.decrypt(
        { name: CRYPTO_CONSTANTS.AES_ALGORITHM, iv },
        sharedKey,
        ciphertext
      );
    } catch (error) {
      throw new Error("Failed to decrypt data: invalid data or wrong vault");
    }

    return this.decoder.decode(plaintext);
  }

  // Vault state management
  isUnlocked() {
    return !!this.keyPair?.privateKey;
  }

  lock() {
    this.keyPair = null;
    this.salt = null;
    this.iv = null;
    this.wrappedPrivateKey = null;
  }

  _ensureUnlocked(message) {
    if (!this.keyPair?.privateKey) {
      throw new Error(`${message}: vault is locked`);
    }
  }

  // Password management
  async changePassword(oldPassword, newPassword, wrappedPrivateKey, salt, iv, publicKeyPem) {
    await this.unlockPrivateKey(wrappedPrivateKey, salt, iv, oldPassword, publicKeyPem);
    await this.wrapPrivateKeyWithPassword(newPassword);
  }

  // Export vault data
  async exportPayload() {
    return {
      public_key_pem: await this.exportPublicKeyPEM(),
      wrapped_private_key: Base64Utils.arrayBufferToBase64(this.wrappedPrivateKey),
      salt: Base64Utils.arrayBufferToBase64(this.salt),
      iv: Base64Utils.arrayBufferToBase64(this.iv),
    };
  }
}

// Global vault instance
window.vault = new UserCryptoVault();

// Helper functions for vault operations
async function VaultSetupUserCryptoAndSend(password) {
  await window.vault.generateKeyPair();
  await window.vault.wrapPrivateKeyWithPassword(password);
  return await window.vault.exportPayload();
}

async function VaultUnlockPrivateKey(password, keyData) {
  await window.vault.unlockPrivateKey(
    Base64Utils.base64ToUint8Array(keyData.wrapped_private_key),
    Base64Utils.base64ToUint8Array(keyData.salt),
    Base64Utils.base64ToUint8Array(keyData.iv),
    password,
    keyData.public_key_pem
  );
}

async function VaultChangePassword(old_password, new_password, keyData) {
  await window.vault.changePassword(
    old_password,
    new_password,
    Base64Utils.base64ToUint8Array(keyData.wrapped_private_key),
    Base64Utils.base64ToUint8Array(keyData.salt),
    Base64Utils.base64ToUint8Array(keyData.iv),
    keyData.public_key_pem
  );
}
