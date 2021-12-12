use std::collections::HashMap;
use std::net::UdpSocket;
use structopt::StructOpt;

use rand::Rng;

pub enum TransactionState {
    Accepted,
    Commit,
    Abort,
    Prepare,
    Wait
}

struct Transaction {
    transaction_state: TransactionState,
    transaction_id: i32,
    amount: i32,
    service: i32,
}

impl Transaction {
    pub fn serialize(&mut self) -> [u8; 13] {

        let mut serialize: [u8; 13] = [0;13];

        serialize[0] = match self.transaction_state {
            TransactionState::Prepare => {b'P'}
            TransactionState::Abort => {b'A'}
            TransactionState::Commit => {b'C'}
            _ => {panic!("Unrecognized TransactionState")}
        };

        let bin_transaction_id = self.transaction_id.to_le_bytes();
        serialize[1..5].copy_from_slice(&bin_transaction_id);

        let bin_amount = self.amount.to_le_bytes();
        serialize[5..9].copy_from_slice(&bin_amount);

        let bin_service = self.service.to_le_bytes();
        serialize[9..13].copy_from_slice(&bin_service);


        serialize
    }

    pub fn deserialize(buf: [u8; 13]) -> Transaction {

        let state = match buf[0] {
            b'P' => { TransactionState::Prepare }
            b'C' => { TransactionState::Commit }
            b'A' => { TransactionState::Abort }
            _ => {panic!("Invalid transaction state")}
        };

        let mut transaction_id_b:[u8; 4] = [0;4];
        transaction_id_b.clone_from_slice(&buf[1..5]);

        let mut amount_b:[u8; 4] = [0;4];
        amount_b.clone_from_slice(&buf[5..9]);

        let mut service_b:[u8; 4] = [0;4];
        service_b.clone_from_slice(&buf[9..13]);

        Transaction {
            transaction_id: i32::from_le_bytes(transaction_id_b),
            amount: i32::from_le_bytes(amount_b),
            transaction_state: state,
            service: i32::from_le_bytes(service_b),
        }
    }
}

fn id_to_microservice(id: usize) -> String {
    let result = match id {
        0 => { "127.0.0.1:1111" }
        1 => { "127.0.0.1:2222" }
        2 => { "127.0.0.1:3333" }
        _=> {panic!("Unknown microservice")}
    };

    result.to_string()
}

fn id_to_microservice_name(id: usize) -> String {
    let result = match id {
        0 => { "Bank" }
        1 => { "Airline" }
        2 => { "Hotel" }
        _=> {panic!("Unknown microservice")}
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
        let mut buf = [0;13];
        let (size, from) = socket.recv_from(&mut buf).expect("Receive socket error");

        println!("[{}] received {} bytes", name, size);

        let transaction = Transaction::deserialize(buf);

        match transaction.transaction_state {
            TransactionState::Prepare => {
                println!("[{}] received PREPARE for {}", name, transaction.transaction_id);
                let state = match log.get(&transaction.transaction_id) {
                    Some(TransactionState::Accepted) | Some(TransactionState::Commit) => TransactionState::Commit,
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
                    _ => {panic!("[{}] impossible to respond to prepare", name)}
                };

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: state,
                    service: id as i32
                };

                socket.send_to(&response.serialize(), from).unwrap();
            }
            TransactionState::Commit => {
                println!("[{}] received COMMIT from {}", name, transaction.transaction_id);
                log.insert(transaction.transaction_id, TransactionState::Commit);

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: TransactionState::Commit,
                    service: id as i32
                };

                socket.send_to(&response.serialize(), from).unwrap();
            }
            TransactionState::Abort => {
                println!("[{}] received ABORT from {}", name, transaction.transaction_id);
                log.insert(transaction.transaction_id, TransactionState::Abort);

                response = Transaction {
                    transaction_id: transaction.transaction_id,
                    amount: 0,
                    transaction_state: TransactionState::Abort,
                    service: id as i32
                };

                socket.send_to(&response.serialize(), from).unwrap();
            }
            _ => {
                println!("[{}] ??? {}", name, transaction.transaction_id);
            }
        }

    }
}
