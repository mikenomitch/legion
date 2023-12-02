use crate::util::R;

// Note: Modified from: https://github.com/grimvoodoo/cert-gen

/// This is a Rust script that generates SSL certificates for use in local development environments.
///
/// It takes two command-line arguments: the first is either the path to an existing CA file or the
/// string "new" to create a new CA; the second is the domain name you wish to create a certificate for.
///
/// The script uses the `rcgen` crate to generate a new certificate and key pair, then signs the certificate
/// with the specified CA. It saves the certificate and key files to disk in the `certs` directory.
///
/// Usage:
/// ```
/// $ cargo run -- args...
/// ```
// Do I need openssl?
// extern crate openssl;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair,
    KeyUsagePurpose, SanType, PKCS_ECDSA_P256_SHA256,
};

use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};

pub fn read_root_cert(
    pem_path: String,
    key_path: String,
) -> R<Certificate> {
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
    let ca_cert_params = CertificateParams::from_ca_cert_pem(cert_string.as_str(), key_pair)?;

    // Create a new certificate using the CertificateParams object
    let ca_cert = Certificate::from_params(ca_cert_params)?;

    Ok(ca_cert)
}

pub fn create_ca_cert() -> R<Certificate> {
    let key_pair = KeyPair::generate(&PKCS_ECDSA_P256_SHA256)?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "localhost");
    dn.push(DnType::CommonName, "legion");

    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = dn;
    params.key_pair = Some(key_pair);

    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365 * 20);

    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    let cert = Certificate::from_params(params)?;

    let cert_pem = cert.serialize_pem()?;

    std::fs::create_dir_all("configs/tls/")?;
    fs::write("configs/tls/rootca.pem", &cert_pem.as_bytes())?;
    fs::write(
        "configs/tls/rootca.key",
        &cert.serialize_private_key_pem().as_bytes(),
    )?;

    Ok(cert)
}

pub fn signed_cert_with_ca(ca_cert: Certificate, dn_name: String) -> R<(String, String, Certificate)> {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, dn_name.clone());

    let mut params = CertificateParams::default();

    params.distinguished_name = dn;
    params.not_before = time::OffsetDateTime::now_utc();
    params.not_after = time::OffsetDateTime::now_utc() + time::Duration::days(365 * 20);

    params.subject_alt_names = vec![
        SanType::DnsName(dn_name.clone()),
        SanType::DnsName(String::from("legion")),
        SanType::DnsName(String::from("localhost")),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
        SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::new(
            0, 0, 0, 0, 0, 0, 0, 1,
        ))),
    ];
    let cert = Certificate::from_params(params)?;

    let cert_signed = cert.serialize_pem_with_signer(&ca_cert)?;
    let key = cert.serialize_private_key_pem();

    Ok((cert_signed, key, cert))
}

pub fn write_signed_cert_with_ca(ca_cert: Certificate, dn_name: String) -> R<Certificate> {
    let (cert_signed, key, ca_cert_again) = signed_cert_with_ca(ca_cert, dn_name.clone())?;

    let path = format!("configs/tls/{dn_name}/");
    let cert_path = format!("configs/tls/{dn_name}/{dn_name}.pem");
    let key_path = format!("configs/tls/{dn_name}/{dn_name}.key");

    std::fs::create_dir_all(path)?;
    fs::write(cert_path, cert_signed)?;
    fs::write(key_path, key)?;

    Ok(ca_cert_again)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_ca_cert_runs() -> R<()> {
        let _cert = create_ca_cert()?;
        assert!(true);

        Ok(())
    }

    #[test]
    fn read_root_cert_works() {
        // TODO: Replace with better files
        let ca_pem_path = "/Users/mike/code/legion/configs/tls/rootca.pem".to_string();
        let ca_key_path = "/Users/mike/code/legion/configs/tls/rootca.key".to_string();

        let res = read_root_cert(ca_pem_path, ca_key_path);

        match res {
            Ok(_cert) => {
                // assert_eq!(cert.is_ca(), IsCa::Ca(BasicConstraints::Unconstrained));
            }
            Err(e) => {
                println!("Error: {}", e);
                assert!(false);
            }
        }
    }

    #[test]
    fn signed_cert_with_ca_works() -> R<()> {
        let cert = create_ca_cert()?;
        let (cert_signed, key, _) = signed_cert_with_ca(cert, "test".to_string())?;
        assert!(cert_signed.len() > 0);
        assert!(key.len() > 0);

        Ok(())
    }

    #[test]
    fn write_signed_cert_with_ca_works() -> R<()> {
        let cert = create_ca_cert()?;
        write_signed_cert_with_ca(cert, "server".to_string())?;
        Ok(())
    }
}
