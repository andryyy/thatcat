// passkeys-client-global.js
// Minimal WebAuthn client for registration/authentication.
// - No imports, no deps
// - Global API for hyperscript: window.Passkeys.create / window.Passkeys.get
// - Converts base64url <-> ArrayBuffer
// - Passes through server options; supports resident keys and extensions

(function () {
  "use strict";

  // ---- base64url helpers ----
  function b64urlToBuf(b64url) {
    const pad = "=".repeat((4 - (b64url.length % 4)) % 4);
    const b64 = (b64url + pad).replace(/-/g, "+").replace(/_/g, "/");
    const bin = atob(b64);
    const buf = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buf[i] = bin.charCodeAt(i);
    return buf.buffer;
  }

  function bufToB64url(buf) {
    const bytes = buf instanceof ArrayBuffer ? new Uint8Array(buf) : new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    let bin = "";
    for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  }

  // ---- clone helper (avoid mutating server payloads) ----
  function clone(obj) {
    if (typeof structuredClone === "function") return structuredClone(obj);
    return JSON.parse(JSON.stringify(obj));
  }

  // ---- map request options from server (strings) -> WebAuthn (ArrayBuffers) ----
  function toCreationOptions(optionsFromServer) {
    const o = clone(optionsFromServer);
    o.challenge = b64urlToBuf(o.challenge);
    if (o.user && typeof o.user.id === "string") {
      o.user = { ...o.user, id: b64urlToBuf(o.user.id) };
    }
    if (Array.isArray(o.excludeCredentials)) {
      o.excludeCredentials = o.excludeCredentials.map((c) => ({
        ...c,
        id: b64urlToBuf(c.id),
      }));
    }
    return o;
  }

  function toRequestOptions(optionsFromServer) {
    const o = clone(optionsFromServer);
    o.challenge = b64urlToBuf(o.challenge);
    if (Array.isArray(o.allowCredentials)) {
      o.allowCredentials = o.allowCredentials.map((c) => ({
        ...c,
        id: b64urlToBuf(c.id),
      }));
    }
    return o;
  }

  // ---- normalize responses for server (ArrayBuffers -> base64url strings) ----
  function attestationToJSON(cred) {
    const resp = cred.response;
    return {
      id: cred.id,
      type: cred.type,
      authenticatorAttachment: cred.authenticatorAttachment || null,
      clientExtensionResults: cred.getClientExtensionResults ? cred.getClientExtensionResults() : {},
      rawId: bufToB64url(cred.rawId),
      response: {
        clientDataJSON: bufToB64url(resp.clientDataJSON),
        attestationObject: bufToB64url(resp.attestationObject),
        transports:
          typeof resp.getTransports === "function" ? resp.getTransports() : undefined,
      },
    };
  }

  function assertionToJSON(cred) {
    const resp = cred.response;
    return {
      id: cred.id,
      type: cred.type,
      authenticatorAttachment: cred.authenticatorAttachment || null,
      clientExtensionResults: cred.getClientExtensionResults ? cred.getClientExtensionResults() : {},
      rawId: bufToB64url(cred.rawId),
      response: {
        clientDataJSON: bufToB64url(resp.clientDataJSON),
        authenticatorData: bufToB64url(resp.authenticatorData),
        signature: bufToB64url(resp.signature),
        userHandle: resp.userHandle ? bufToB64url(resp.userHandle) : undefined,
      },
    };
  }

  // ---- optional: AbortController timeout wrapper ----
  function withTimeout(ms) {
    if (!ms) return undefined;
    const ac = new AbortController();
    setTimeout(() => ac.abort("timeout"), ms);
    return ac.signal;
  }

  // ---- public API ----
  async function create(options, timeoutMs) {
    if (!("PublicKeyCredential" in window)) throw new Error("WebAuthn not supported");
    const publicKey = toCreationOptions(options);
    const signal = withTimeout(timeoutMs);
    const cred = await navigator.credentials.create({ publicKey, signal });
    if (!cred) throw new Error("Creation returned null");
    return attestationToJSON(cred);
  }

  async function get(options, timeoutMs) {
    if (!("PublicKeyCredential" in window)) throw new Error("WebAuthn not supported");
    const publicKey = toRequestOptions(options);
    const signal = withTimeout(timeoutMs);
    const cred = await navigator.credentials.get({ publicKey, signal });
    if (!cred) throw new Error("Assertion returned null");
    return assertionToJSON(cred);
  }

  // expose helpers too (handy in hyperscript)
  window.Passkeys = {
    // main ops
    create,         // Passkeys.create({options}) -> attestation JSON (send to /verify)
    get,            // Passkeys.get({options})     -> assertion JSON (send to /verify)
    // utils
    b64urlToBuf,
    bufToB64url,
  };
})();
