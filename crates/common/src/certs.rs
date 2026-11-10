use crate::prelude::*;
// Note: Modified from: https://github.com/grimvoodoo/cert-gen

// Do I need openssl?
// extern crate openssl;

use lazy_static::lazy_static;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedKey, DistinguishedName, DnType, Ia5String, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Mutex;

lazy_static! {
    static ref CERT_IO_LOCK: Mutex<()> = Mutex::new(());
}

#[allow(dead_code)]
pub fn read_root_cert(pem_path: String, key_path: String) -> R<CertifiedKey> {
    let _guard = CERT_IO_LOCK.lock().expect("cert IO lock poisoned");

    // Open the PEM file containing both the certificate and private key

    let pem_cert_file = File::open(pem_path)?;
    let mut pem_cert_reader = BufReader::new(pem_cert_file);

    let mut cert_string = String::new();
    pem_cert_reader.read_to_string(&mut cert_string)?;

    let pem_key_file = File::open(key_path)?;
    let mut pem_key_reader = BufReader::new(pem_key_file);

    let mut key_pair_sting = String::new();
    pem_key_reader.read_to_string(&mut key_pair_sting)?;

    let key_pair = KeyPair::from_pem(key_pair_sting.as_str())?;
    // Parse the PEM file and create a new CertificateParams object
    let ca_cert_params = CertificateParams::from_ca_cert_pem(cert_string.as_str())?;

    // Reconstruct a certificate using the parsed parameters and key pair
    let ca_cert = ca_cert_params.self_signed(&key_pair)?;

    Ok(CertifiedKey {
        cert: ca_cert,
        key_pair,
    })
}

#[allow(dead_code)]
pub fn create_ca_cert(out_dir: &Path) -> R<CertifiedKey> {
    let _guard = CERT_IO_LOCK.lock().expect("cert IO lock poisoned");

    let key_pair = KeyPair::generate()?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "localhost");
    dn.push(DnType::CommonName, "legion");

    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = dn;
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365 * 20);

    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let cert = params.self_signed(&key_pair)?;

    let cert_pem = cert.pem();

    std::fs::create_dir_all(out_dir)?;

    let cert_path = out_dir.join("rootca.pem");
    let key_path = out_dir.join("rootca.key");
    let cert_tmp = out_dir.join("rootca.pem.tmp");
    let key_tmp = out_dir.join("rootca.key.tmp");

    fs::write(&cert_tmp, cert_pem.as_bytes())?;
    fs::write(&key_tmp, key_pair.serialize_pem().as_bytes())?;

    fs::rename(&cert_tmp, &cert_path)?;
    fs::rename(&key_tmp, &key_path)?;

    Ok(CertifiedKey { cert, key_pair })
}

pub fn signed_cert_with_ca(
    ca_cert: &CertifiedKey,
    dn_name: &str,
) -> R<(String, String, CertifiedKey)> {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, dn_name.to_string());

    let mut params = CertificateParams::default();

    params.distinguished_name = dn;
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365 * 20);

    params.subject_alt_names = vec![
        SanType::DnsName(Ia5String::try_from(dn_name)?),
        SanType::DnsName(Ia5String::try_from("legion")?),
        SanType::DnsName(Ia5String::try_from("localhost")?),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
        SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::new(
            0, 0, 0, 0, 0, 0, 0, 1,
        ))),
    ];
    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, &ca_cert.cert, &ca_cert.key_pair)?;

    let cert_signed = cert.pem();
    let key = key_pair.serialize_pem();

    Ok((cert_signed, key, CertifiedKey { cert, key_pair }))
}

#[allow(dead_code)]
pub fn write_signed_cert_with_ca(
    ca_cert: &CertifiedKey,
    dn_name: &str,
    out_dir: &Path,
) -> R<CertifiedKey> {
    let (cert_signed, key, ca_cert_again) = signed_cert_with_ca(ca_cert, dn_name)?;

    let path = out_dir.join(dn_name);
    let cert_path = path.join(format!("{dn_name}.pem"));
    let key_path = path.join(format!("{dn_name}.key"));

    std::fs::create_dir_all(&path)?;
    fs::write(cert_path, cert_signed)?;
    fs::write(key_path, key)?;

    Ok(ca_cert_again)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a unique, ephemeral directory for cert IO in tests so we never
    /// clobber the checked-in fixtures under `configs/tls/`. Cleaned up on drop.
    struct TmpCertDir(std::path::PathBuf);

    impl TmpCertDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);

            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "legion-certs-test-{}-{nanos}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("failed to create temp cert dir");
            TmpCertDir(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TmpCertDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn create_ca_cert_runs() -> R<()> {
        let tmp = TmpCertDir::new();
        let _cert = create_ca_cert(tmp.path())?;
        assert!(tmp.path().join("rootca.pem").exists());
        assert!(tmp.path().join("rootca.key").exists());

        Ok(())
    }

    #[test]
    fn read_root_cert_works() -> R<()> {
        let tmp = TmpCertDir::new();
        let _generated = create_ca_cert(tmp.path())?;

        let ca_pem_path = tmp.path().join("rootca.pem");
        let ca_key_path = tmp.path().join("rootca.key");

        let cert = read_root_cert(
            ca_pem_path.to_string_lossy().into_owned(),
            ca_key_path.to_string_lossy().into_owned(),
        )?;

        assert!(!cert.cert.pem().is_empty());
        assert!(!cert.key_pair.serialize_pem().is_empty());

        Ok(())
    }

    #[test]
    fn signed_cert_with_ca_works() -> R<()> {
        let tmp = TmpCertDir::new();
        let cert = create_ca_cert(tmp.path())?;
        let (cert_signed, key, _) = signed_cert_with_ca(&cert, "test")?;
        assert!(!cert_signed.is_empty());
        assert!(!key.is_empty());

        Ok(())
    }

    #[test]
    fn write_signed_cert_with_ca_works() -> R<()> {
        let tmp = TmpCertDir::new();
        let cert = create_ca_cert(tmp.path())?;
        write_signed_cert_with_ca(&cert, "server", tmp.path())?;
        assert!(tmp.path().join("server/server.pem").exists());
        assert!(tmp.path().join("server/server.key").exists());
        Ok(())
    }
}
