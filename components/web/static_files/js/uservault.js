class UserCryptoVault {
  constructor() {
    this.encoder = new TextEncoder();
    this.decoder = new TextDecoder();
    this.keyPair = null;
    this.salt = null;
    this.iv = null;
    this.wrappedPrivateKey = null;
  }

  async generateKeyPair() {
    this.keyPair = await crypto.subtle.generateKey(
      { name: "ECDH", namedCurve: "P-256" },
      true,
      ["deriveKey", "deriveBits"]
    );
  }

  async exportPublicKeyPEM() {
    const spki = await crypto.subtle.exportKey("spki", this.keyPair.publicKey);
    const b64 = btoa(String.fromCharCode(...new Uint8Array(spki)));
    const lines = b64.match(/.{1,64}/g).join("\n");
    return `-----BEGIN PUBLIC KEY-----\n${lines}\n-----END PUBLIC KEY-----`;
  }

  async wrapPrivateKeyWithPassword(password) {
    this.salt = crypto.getRandomValues(new Uint8Array(16));
    this.iv = crypto.getRandomValues(new Uint8Array(12));

    const baseKey = await crypto.subtle.importKey(
      "raw",
      this.encoder.encode(password),
      "PBKDF2",
      false,
      ["deriveKey"]
    );

    const kek = await crypto.subtle.deriveKey(
      {
        name: "PBKDF2",
        salt: this.salt,
        iterations: 100000,
        hash: "SHA-256",
      },
      baseKey,
      { name: "AES-GCM", length: 256 },
      false,
      ["encrypt"]
    );

    const privateKeyRaw = await crypto.subtle.exportKey("pkcs8", this.keyPair.privateKey);
    this.wrappedPrivateKey = await crypto.subtle.encrypt({ name: "AES-GCM", iv: this.iv }, kek, privateKeyRaw);
  }

  async unlockPrivateKey(wrappedPrivateKey, salt, iv, password, publicKeyPem) {
    const baseKey = await crypto.subtle.importKey(
      "raw",
      this.encoder.encode(password),
      "PBKDF2",
      false,
      ["deriveKey"]
    );

    const kek = await crypto.subtle.deriveKey(
      {
        name: "PBKDF2",
        salt,
        iterations: 100000,
        hash: "SHA-256",
      },
      baseKey,
      { name: "AES-GCM", length: 256 },
      false,
      ["decrypt"]
    );

    const rawPrivateKey = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, kek, wrappedPrivateKey);

    const privateKey = await crypto.subtle.importKey(
      "pkcs8",
      rawPrivateKey,
      { name: "ECDH", namedCurve: "P-256" },
      true,
      ["deriveKey", "deriveBits"]
    );

    const stripped = publicKeyPem.replace(/-----.*?-----|\n/g, "");
    const spkiBytes = Uint8Array.from(atob(stripped), c => c.charCodeAt(0));
    const publicKey = await crypto.subtle.importKey(
      "spki",
      spkiBytes.buffer,
      { name: "ECDH", namedCurve: "P-256" },
      true,
      []
    );

    this.keyPair = { privateKey, publicKey };
  }

  async exportPrivateKeyPEM() {
    if (!this.keyPair?.publicKey || !this.keyPair?.privateKey) throw new Error("Vault not unlocked");
    const pkcs8 = await crypto.subtle.exportKey("pkcs8", this.keyPair.privateKey);
    const b64 = btoa(String.fromCharCode(...new Uint8Array(pkcs8)));
    const lines = b64.match(/.{1,64}/g).join("\n");
    return `-----BEGIN PRIVATE KEY-----\n${lines}\n-----END PRIVATE KEY-----`;
  }

  async encryptData(message) {
    if (!this.keyPair?.publicKey || !this.keyPair?.privateKey) throw new Error("Vault not unlocked");

    const ephemeral = await crypto.subtle.generateKey(
      { name: "ECDH", namedCurve: "P-256" },
      true,
      ["deriveKey"]
    );

    const sharedKey = await crypto.subtle.deriveKey(
      {
        name: "ECDH",
        public: this.keyPair.publicKey,
      },
      ephemeral.privateKey,
      { name: "AES-GCM", length: 256 },
      false,
      ["encrypt"]
    );

    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
      { name: "AES-GCM", iv },
      sharedKey,
      this.encoder.encode(message)
    ));

    const ephemeralRaw = new Uint8Array(await crypto.subtle.exportKey("raw", ephemeral.publicKey));

    // Combine [ephemeral | iv | ciphertext]
    const combined = new Uint8Array(ephemeralRaw.length + iv.length + ciphertext.length);
    combined.set(ephemeralRaw, 0);
    combined.set(iv, ephemeralRaw.length);
    combined.set(ciphertext, ephemeralRaw.length + iv.length);

    return "uv:" + btoa(String.fromCharCode(...combined));
  }

  async decryptData(blobBase64) {
    if (!this.keyPair?.privateKey) throw new Error("Vault not unlocked");

    const combined = Uint8Array.from(atob(blobBase64.replace(/^uv:/, "")), c => c.charCodeAt(0));

    const ephemeralLength = 65; // uncompressed EC point for P-256
    const ivLength = 12;

    const ephemeralRaw = combined.slice(0, ephemeralLength);
    const iv = combined.slice(ephemeralLength, ephemeralLength + ivLength);
    const ciphertext = combined.slice(ephemeralLength + ivLength);

    const ephemeralPubKey = await crypto.subtle.importKey(
      "raw",
      ephemeralRaw.buffer,
      { name: "ECDH", namedCurve: "P-256" },
      true,
      []
    );

    const sharedKey = await crypto.subtle.deriveKey(
      {
        name: "ECDH",
        public: ephemeralPubKey,
      },
      this.keyPair.privateKey,
      { name: "AES-GCM", length: 256 },
      false,
      ["decrypt"]
    );

    const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, sharedKey, ciphertext);
    return this.decoder.decode(plaintext);
  }

  isUnlocked() {
    return !!this.keyPair?.privateKey;
  }

  lock() {
    this.keyPair = null;
  }

  async changePassword(oldPassword, newPassword, wrappedPrivateKey, salt, iv, publicKeyPem) {
    await this.unlockPrivateKey(wrappedPrivateKey, salt, iv, oldPassword, publicKeyPem);
    await this.wrapPrivateKeyWithPassword(newPassword);
  }

  async exportPayload() {
    return {
      public_key_pem: await this.exportPublicKeyPEM(),
      wrapped_private_key: btoa(String.fromCharCode(...new Uint8Array(this.wrappedPrivateKey))),
      salt: btoa(String.fromCharCode(...this.salt)),
      iv: btoa(String.fromCharCode(...this.iv)),
    };
  }
}

window.vault = new UserCryptoVault();

async function VaultSetupUserCryptoAndSend(password) {
    await window.vault.generateKeyPair();
    await window.vault.wrapPrivateKeyWithPassword(password);
    const payload = await window.vault.exportPayload();
    return payload;
}

async function VaultUnlockPrivateKey(password, keyData) {
    await window.vault.unlockPrivateKey(
      Uint8Array.from(atob(keyData.wrapped_private_key), c => c.charCodeAt(0)),
      Uint8Array.from(atob(keyData.salt), c => c.charCodeAt(0)),
      Uint8Array.from(atob(keyData.iv), c => c.charCodeAt(0)),
      password,
      keyData.public_key_pem
    );
}

async function VaultChangePassword(old_password, new_password, keyData) {
    await window.vault.changePassword(old_password, new_password,
      Uint8Array.from(atob(keyData.wrapped_private_key), c => c.charCodeAt(0)),
      Uint8Array.from(atob(keyData.salt), c => c.charCodeAt(0)),
      Uint8Array.from(atob(keyData.iv), c => c.charCodeAt(0)),
      keyData.public_key_pem
    );
}

