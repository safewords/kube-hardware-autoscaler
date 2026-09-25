//! Sipeed NanoKVM driver (ATX power via the NanoKVM web API).
//!
//! * `POST /api/auth/login` with `{"username", "password"}`; the password is
//!   encrypted the way the NanoKVM web UI does it (OpenSSL-compatible
//!   AES-256-CBC, MD5 key derivation, fixed passphrase `nanokvm-sipeed-2024`,
//!   base64, then URL-encoded). The session token comes back in the
//!   `nano-kvm-token` cookie.
//! * `GET /api/vm/gpio` returns `{"pwr": bool, "hdd": bool}` (LED states).
//! * `POST /api/vm/gpio` with `{"type": "power", "duration": ms}` presses the
//!   power button.
//!
//! Responses use the envelope `{"code": 0, "msg": "...", "data": ...}`.
//! The power button toggles, so the driver reads the power LED first and only
//! presses when the machine is not already in the requested state. The power
//! LED header of the motherboard must be wired to the NanoKVM ATX board,
//! otherwise the machine always reads as off.

use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
use async_trait::async_trait;
use base64::Engine as _;
use md5::{Digest, Md5};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use super::{Credentials, DriverError, DriverInit, DriverKind, PowerDriver, Result, base_url, http_client};
use crate::crd::PowerState;

/// Passphrase hardcoded in the NanoKVM server and web UI.
const NANOKVM_PASSPHRASE: &[u8] = b"nanokvm-sipeed-2024";
const TOKEN_COOKIE: &str = "nano-kvm-token";

/// `config` of the `nanokvm` driver.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NanoKvmConfig {
    /// Base URL of the NanoKVM, e.g. `http://192.168.1.50`.
    pub endpoint: String,
    /// Skip TLS certificate verification (when TLS is enabled on the NanoKVM).
    #[serde(default)]
    pub insecure_skip_verify: bool,
    /// Button press for power on / graceful shutdown, in milliseconds.
    #[serde(default = "default_short_press")]
    pub short_press_ms: u32,
    /// Button hold for a forced power off, in milliseconds (ATX needs > 4 s).
    #[serde(default = "default_long_press")]
    pub long_press_ms: u32,
}

fn default_short_press() -> u32 {
    800
}
fn default_long_press() -> u32 {
    6000
}

pub struct NanoKvmDriver {
    base: String,
    config: NanoKvmConfig,
    creds: Credentials,
    http: reqwest::Client,
    /// Cached session token; refreshed on authentication failure.
    token: Mutex<Option<String>>,
}

impl DriverKind for NanoKvmDriver {
    const NAME: &'static str = "nanokvm";
    const DESCRIPTION: &'static str = "Sipeed NanoKVM ATX power control (web API)";
    type Config = NanoKvmConfig;

    fn build(config: NanoKvmConfig, init: &DriverInit<'_>) -> Result<Self> {
        Ok(Self {
            base: base_url(&config.endpoint)?,
            http: http_client(config.insecure_skip_verify, init.ctx.op_timeout)?,
            creds: init.credentials()?,
            config,
            token: Mutex::new(None),
        })
    }
}

/// OpenSSL `enc -aes-256-cbc -md md5` key/IV derivation (EVP_BytesToKey, 1 round).
fn derive_key_iv(passphrase: &[u8], salt: &[u8; 8]) -> ([u8; 32], [u8; 16]) {
    let mut material = Vec::with_capacity(48);
    let mut prev: Vec<u8> = Vec::new();
    while material.len() < 48 {
        let mut h = Md5::new();
        h.update(&prev);
        h.update(passphrase);
        h.update(salt);
        prev = h.finalize().to_vec();
        material.extend_from_slice(&prev);
    }
    let mut key = [0u8; 32];
    let mut iv = [0u8; 16];
    key.copy_from_slice(&material[..32]);
    iv.copy_from_slice(&material[32..48]);
    (key, iv)
}

/// Raw AES-256-CBC/PKCS#7 ciphertext for `plain` with the given salt.
fn encrypt_raw(plain: &[u8], salt: &[u8; 8]) -> Vec<u8> {
    let (key, iv) = derive_key_iv(NANOKVM_PASSPHRASE, salt);
    cbc::Encryptor::<aes::Aes256>::new(&key.into(), &iv.into()).encrypt_padded_vec::<Pkcs7>(plain)
}

/// Encrypts a password exactly like the NanoKVM web UI:
/// base64("Salted__" + salt + ciphertext), URL-encoded.
fn encrypt_password(password: &str, salt: &[u8; 8]) -> String {
    let mut blob = b"Salted__".to_vec();
    blob.extend_from_slice(salt);
    blob.extend(encrypt_raw(password.as_bytes(), salt));
    url_encode(&base64::engine::general_purpose::STANDARD.encode(blob))
}

/// Percent-encodes everything except RFC 3986 unreserved characters
/// (matches JavaScript's encodeURIComponent for base64 input).
fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn token_from_set_cookie(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find_map(|c| {
            let first = c.split(';').next()?.trim();
            let (name, value) = first.split_once('=')?;
            (name == TOKEN_COOKIE && !value.is_empty()).then(|| value.to_string())
        })
}

/// Checks the `{code, msg, data}` envelope; returns `data`.
fn envelope(body: &Value, what: &str) -> Result<Value> {
    match body["code"].as_i64() {
        Some(0) => Ok(body["data"].clone()),
        Some(code) => Err(DriverError::Interface(format!(
            "{what}: NanoKVM returned code {code}: {}",
            body["msg"].as_str().unwrap_or("")
        ))),
        None => Err(DriverError::Interface(format!("{what}: unexpected response {body}"))),
    }
}

impl NanoKvmDriver {
    async fn login(&self) -> Result<Option<String>> {
        let mut salt = [0u8; 8];
        getrandom::fill(&mut salt).map_err(|e| DriverError::Interface(format!("random salt: {e}")))?;
        let resp = self
            .http
            .post(format!("{}/api/auth/login", self.base))
            .json(&json!({
                "username": self.creds.username,
                "password": encrypt_password(&self.creds.password, &salt),
            }))
            .send()
            .await?;
        let token = token_from_set_cookie(resp.headers());
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        envelope(&body, "login")?;
        // With authentication disabled on the NanoKVM there is no cookie.
        Ok(token)
    }

    async fn token(&self, refresh: bool) -> Result<Option<String>> {
        let mut guard = self.token.lock().await;
        if refresh || guard.is_none() {
            *guard = self.login().await?;
        }
        Ok(guard.clone())
    }

    /// Sends an authenticated request, logging in again once if the session expired.
    async fn call(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value> {
        for attempt in 0..2 {
            let token = self.token(attempt > 0).await?;
            let mut rb = self.http.request(method.clone(), format!("{}{}", self.base, path));
            if let Some(t) = &token {
                rb = rb.header(reqwest::header::COOKIE, format!("{TOKEN_COOKIE}={t}"));
            }
            if let Some(b) = &body {
                rb = rb.json(b);
            }
            let resp = rb.send().await?;
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
                continue;
            }
            let status = resp.status();
            let value: Value = resp.json().await.unwrap_or(Value::Null);
            if !status.is_success() {
                return Err(DriverError::Interface(format!("{path} returned {status}: {value}")));
            }
            return envelope(&value, path);
        }
        Err(DriverError::Interface(format!(
            "{path}: NanoKVM rejected the session after re-login"
        )))
    }

    async fn is_on(&self) -> Result<bool> {
        let data = self.call(reqwest::Method::GET, "/api/vm/gpio", None).await?;
        data["pwr"]
            .as_bool()
            .ok_or_else(|| DriverError::Interface(format!("unexpected gpio state: {data}")))
    }

    async fn press(&self, duration_ms: u32) -> Result<()> {
        self.call(
            reqwest::Method::POST,
            "/api/vm/gpio",
            Some(json!({"type": "power", "duration": duration_ms})),
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl PowerDriver for NanoKvmDriver {
    async fn power_state(&self) -> Result<PowerState> {
        Ok(if self.is_on().await? {
            PowerState::On
        } else {
            PowerState::Off
        })
    }

    async fn power_on(&self) -> Result<()> {
        if self.is_on().await? {
            return Ok(());
        }
        self.press(self.config.short_press_ms).await
    }

    async fn power_off(&self, force: bool) -> Result<()> {
        if !self.is_on().await? {
            return Ok(());
        }
        let ms = if force {
            self.config.long_press_ms
        } else {
            self.config.short_press_ms
        };
        self.press(ms).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Vectors from `openssl enc -aes-256-cbc -md md5 -S <salt> -pass pass:nanokvm-sipeed-2024 -base64 -A`
    /// (OpenSSL 3 prints the raw ciphertext when the salt is given explicitly).
    #[test]
    fn matches_openssl_vectors() {
        let b64 = |v: Vec<u8>| base64::engine::general_purpose::STANDARD.encode(v);
        assert_eq!(
            b64(encrypt_raw(b"admin", &[1, 2, 3, 4, 5, 6, 7, 8])),
            "OqpDa3FQkTKaK482DKBwZA=="
        );
        assert_eq!(
            b64(encrypt_raw(
                b"correct horse battery staple",
                &[0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, 0x18]
            )),
            "bTQ7ysO/tIxKdvFZWmR/HuZl/zv2/7bVpmCuziPtu58="
        );
    }

    #[test]
    fn password_blob_has_openssl_header_and_is_url_encoded() {
        let salt = [1, 2, 3, 4, 5, 6, 7, 8];
        let enc = encrypt_password("admin", &salt);
        assert!(!enc.contains(['+', '/', '=']), "{enc}");
        let decoded: String = {
            let mut out = String::new();
            let mut bytes = enc.bytes();
            while let Some(b) = bytes.next() {
                if b == b'%' {
                    let hex: String = [bytes.next().unwrap() as char, bytes.next().unwrap() as char]
                        .iter()
                        .collect();
                    out.push(u8::from_str_radix(&hex, 16).unwrap() as char);
                } else {
                    out.push(b as char);
                }
            }
            out
        };
        let raw = base64::engine::general_purpose::STANDARD.decode(decoded).unwrap();
        assert_eq!(&raw[..8], b"Salted__");
        assert_eq!(&raw[8..16], &salt);
        assert_eq!(b64_tail(&raw[16..]), "OqpDa3FQkTKaK482DKBwZA==");
    }

    fn b64_tail(ct: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(ct)
    }

    #[test]
    fn parses_session_cookie_and_envelope() {
        let mut h = reqwest::header::HeaderMap::new();
        h.append(reqwest::header::SET_COOKIE, "other=1; Path=/".parse().unwrap());
        h.append(
            reqwest::header::SET_COOKIE,
            "nano-kvm-token=abc.def; Path=/; HttpOnly".parse().unwrap(),
        );
        assert_eq!(token_from_set_cookie(&h).as_deref(), Some("abc.def"));
        assert_eq!(
            envelope(&json!({"code": 0, "data": {"pwr": true}}), "x").unwrap()["pwr"],
            true
        );
        assert!(envelope(&json!({"code": -2, "msg": "invalid username or password"}), "login").is_err());
    }
}
