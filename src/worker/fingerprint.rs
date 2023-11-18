use crate::cmd::structs::WorkerConfig;
// use log::error;
use std::error;
use std::collections::HashMap;

extern crate libloading;
// use std::env;
// use libloading::{Library, Symbol};

// TODO: USE THIS GUIDE
// https://michael-f-bryan.github.io/rust-ffi-guide/dynamic_loading.html
// using dylib crate type: https://doc.rust-lang.org/reference/linkage.html


pub fn fingerprint_node(config: &WorkerConfig) -> HashMap<String, String> {
    let fps : Vec<Box<dyn Fingerprinter>> = vec![
        Box::new(ArchFingerprinter{}),
        Box::new(OSFingerprinter{}),
    ];

    let mut ret = HashMap::new();

    fps.iter().for_each( |f| {
        ret.extend(f.fingerprint());
    });

    config.worker_config.additional_fingerprinters.iter().for_each(|path| {
        match call_dynamic(path) {
            Ok(fp) => {
                println!("\n\n\nYEAAAAHHHH IT LOADED!!!!\n\n\n");
                ret.extend(fp.fingerprint());
            },
            Err(e) => {
                println!("Error loading fingerprinter: {}", e);
            }
        }
    });

    ret
}


// let p = "../plugins/fingerprinters/basic.so";
fn call_dynamic(path: &str) -> Result<Box<dyn Fingerprinter>, Box<dyn error::Error>> {
    println!("call: {:?}", path);
    unsafe {
        let lib = libloading::Library::new(path)?;
        println!("lib: {:?}", lib);
        let func: libloading::Symbol<unsafe extern fn() -> Box<dyn Fingerprinter>> = lib.get(b"get")?;
        println!("func: {:?}", func);
        Ok(func())
    }
}

trait Fingerprinter {
    fn name(&self) -> &'static str;

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([("foo".to_string(), "bar".to_string())])
    }
}

struct OSFingerprinter {}

impl Fingerprinter for OSFingerprinter {
    fn name(&self) -> &'static str { "os" }

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([
            ("os".to_string(), std::env::consts::OS.to_string())
        ])
    }
}

struct ArchFingerprinter {}

impl Fingerprinter for ArchFingerprinter {
    fn name(&self) -> &'static str { "arch" }

    fn fingerprint(&self) -> HashMap<String, String> {
        HashMap::from([
            ("arch".to_string(), std::env::consts::ARCH.to_string())
        ])
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::structs::{WorkerConfigFromManager, ServerConfig};

    use super::*;

    #[test]
    fn fingerprint_node_works() {
        let config = &WorkerConfig{
            server: ServerConfig{
                port: 1234,
                address: "duper".to_string(),
            },
            worker_config: WorkerConfigFromManager{
                additional_fingerprinters: vec!["/Users/mike/Code/legion/src/plugins/fingerprinters/example/target/release/libexample.dylib".to_string()],
            }
        };
        let res = fingerprint_node(config);

        println!("ressy {:?}", res);

        assert_eq!(res.get("os"), Some(&"macos".to_string()));
        assert_eq!(res.get("arch"), Some(&"aarch64".to_string()));
        // assert_eq!(res.get("basic"), Some(&"output".to_string()));
    }
}

// SOME OF THE FINGERPRINTERS TO ADD

// arch
    // arch
// storage
    // bytes total, bytes free, volumes
// signals
    // os.signals
// CNI
    // various CNI plugins available
// Network
    // speed, interfaces, IP addresses, etc
// CPU
    // cores, speed, etc
// Memory
    // total, free, etc
// Landlock
    // available
// CPU
    // cores, modelname, numcores, numcores.perf, numcores.efficiency, totalcompute, 
// Fingerprinters specific to cloud providers
