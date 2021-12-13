use common::transaction::{Transaction, TransactionState};
use std::collections::HashMap;
use std::net::UdpSocket;
use structopt::StructOpt;

use common::helper::id_to_microservice;
use rand::Rng;

fn id_to_microservice_name(id: usize) -> String {
    let result = match id {
        0 => "Bank",
        1 => "Airline",
        2 => "Hotel",
        _ => {
            panic!("Unknown microservice")
        }
    };

    result.to_string()
}

/// Receives the id of the new AlGlobo instance.
#[derive(StructOpt)]
struct Cli {
    /// The new worker id (Type u32).
    id: usize,
}

fn main() {
    // Gets arguments
    let args = Cli::from_args();
    let id = args.id;

    let name = id_to_microservice_name(id);

    let mut log = HashMap::new();
    let mut response;

    let socket = UdpSocket::bind(id_to_microservice(id)).expect("Could not bind socket");

    println!("{} service is up", name);

    loop {
        let mut buf = [0; 13];
        let (size, from) = socket.recv_from(&mut buf).expect("Receive socket error");

        println!("[{}] received {} bytes", name, size);

        let transaction = Transaction::deserialize(buf);

        match transaction.transaction_state {
            TransactionState::Prepare => {
                println!(
                    "[{}] received PREPARE for {}",
                    name, transaction.transaction_id
                );
                let state = match log.get(&transaction.transaction_id) {
                    Some(TransactionState::Accepted) | Some(TransactionState::Commit) => {
                        TransactionState::Commit
                    }
                    Some(TransactionState::Abort) => TransactionState::Abort,
                    None => {
                        let is_success = rand::thread_rng().gen_bool(0.75);
                        if is_success {
                            log.insert(transaction.transaction_id, TransactionState::Accepted);
                            TransactionState::Commit
                        } else {
                            log.insert(transaction.transaction_id, TransactionState::Abort);
                            TransactionState::Abort
                        }
                    }
                    _ => {
                        panic!("[{}] impossible to respond to prepare", name)
                    }
                };

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: state,
                    service: id as i32,
                };

                socket.send_to(&response.serialize(), from).expect("Error sending prepare response");
            }
            TransactionState::Commit => {
                println!(
                    "[{}] received COMMIT from {}",
                    name, transaction.transaction_id
                );
                log.insert(transaction.transaction_id, TransactionState::Commit);

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: TransactionState::Commit,
                    service: id as i32,
                };

                socket.send_to(&response.serialize(), from).expect("Error sending commit response");
            }
            TransactionState::Abort => {
                println!(
                    "[{}] received ABORT from {}",
                    name, transaction.transaction_id
                );
                log.insert(transaction.transaction_id, TransactionState::Abort);

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: TransactionState::Abort,
                    service: id as i32,
                };

                socket.send_to(&response.serialize(), from).expect("Error sending abort response");
            }
            _ => {
                println!("[{}] ??? {}", name, transaction.transaction_id);
            }
        }
    }
}
