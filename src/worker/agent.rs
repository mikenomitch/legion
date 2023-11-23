use crate::cmd::structs::JoinTokenClaims;
use crate::constants::TMP_SECRET_KEY;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};

use crate::worker::fingerprint::fingerprint_node;

use log::debug;
use std::error;

pub async fn run_worker(token: &str) -> Result<(), Box<dyn error::Error>> {
    // let addr: String = format!("{}:{}", DEFAULT_ADDR, DEFAULT_PORT)
    //     .parse()
    //     .unwrap();

    println!("Starting worker node with token: {}", token);

    let token_data = match decode::<JoinTokenClaims>(
        &token,
        &DecodingKey::from_secret(&TMP_SECRET_KEY[..]),
        &Validation::new(Algorithm::HS512),
    ) {
        Ok(c) => c,
        Err(err) => {
            // print error message
            println!("Error: {}", err);

            match *err.kind() {
            ErrorKind::InvalidToken => panic!(), // Example on how to handle a specific error
            _ => panic!(),
        }
    },
    };
    println!("{:?}", token_data.claims.address);
    println!("{:?}", token_data.claims.config);

    let fingerprint_data = fingerprint_node(&token_data.claims.config);
    let os = fingerprint_data.get("os").unwrap().as_str().unwrap();
    println!("OS fingerprinted as: {:?}", os);

    // TODO:
    // - Read the config file (if there)??

    // - Register with the manager
        // - parse JWT from config or cmd
        // - send request to manager
        // - receive config bundle from manager

    // - Fingerprint the node (after or before?)
        // - Run custom fingerprinters

    // - Boot client RPC server
    // - Really register with manager
    // - Start the heartbeat loop

    debug!("Cleaning Up");
    debug!("Worker node starting on {}", token);

    Ok(())
}

