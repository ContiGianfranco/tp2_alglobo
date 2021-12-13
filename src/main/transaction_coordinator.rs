use common::helper::id_to_microservice;
use common::transaction::{Transaction, TransactionState};
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::payment::Payment;

/// The address of the COORDINATOR_ADDR minus the last digit which will be the id of the alGlobo instance
const TRANSACTION_COORDINATOR_ADDR: &str = "127.0.0.1:123";
/// The amount of stakeholders
const STAKEHOLDERS: usize = 3;
/// Time before decide that microservice is down
const TIMEOUT: Duration = Duration::from_secs(5);

/// Struct that represents the transaction logic of the alGlobo leader
pub struct TransactionCoordinator {
    id: usize,
    log: HashMap<i32, TransactionState>,
    socket: UdpSocket,
    responses: Arc<(Mutex<Vec<Option<TransactionState>>>, Condvar)>,
}

impl TransactionCoordinator {
    /// Creates a new alGlobo TransactionCoordinator fo the given id.
    pub fn new(id: usize) -> TransactionCoordinator {
        let coordinator = TransactionCoordinator {
            id,
            log: HashMap::new(),
            socket: UdpSocket::bind(format!("{}{}", TRANSACTION_COORDINATOR_ADDR, id))
                .expect("Error binding socket for transaction coordinator"),
            responses: Arc::new((Mutex::new(vec![None; STAKEHOLDERS]), Condvar::new())),
        };

        let mut clone = coordinator.clone();
        thread::spawn(move || clone.responder());

        coordinator
    }

    /// Receives a transaction id and a payment and communicates with microservices to commit the transaction
    /// if it's successfully committed return true if it's aborted returns false.
    pub fn submit(&mut self, t: i32, r: Payment) -> bool {
        match self.log.get(&t) {
            None => self.full_protocol(t, r),
            Some(TransactionState::Wait) => self.full_protocol(t, r),
            Some(TransactionState::Commit) => self.commit(t, r),
            Some(TransactionState::Abort) => self.abort(t, r),
            _ => {
                panic!("No match for transaction {}", t)
            }
        }
    }

    /// Is called if the transaction was not preciously logged
    fn full_protocol(&mut self, t: i32, r: Payment) -> bool {
        let clone = r;
        if self.prepare(t, r) {
            self.commit(t, clone)
        } else {
            self.abort(t, clone)
        }
    }

    /// Sends a prepare message and the corresponding transaction info to each  microservice
    fn prepare(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Wait);
        println!("[COORDINATOR] prepare {}", t);
        self.broadcast_and_wait(b'P', t, r, TransactionState::Commit)
    }

    /// Sends a commit message and the corresponding transaction info to each  microservice
    fn commit(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Commit);
        println!("[COORDINATOR] commit {}", t);
        self.broadcast_and_wait(b'C', t, r, TransactionState::Commit)
    }

    /// Sends an abort message and the corresponding transaction info to each  microservice
    fn abort(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Abort);
        println!("[COORDINATOR] abort {}", t);
        !self.broadcast_and_wait(b'A', t, r, TransactionState::Abort)
    }

    /// Broadcasts the specified transaction to all microservices, returns true if every microservice
    /// responded whit the expected state, it returns false in other cases
    fn broadcast_and_wait(
        &self,
        message: u8,
        t: i32,
        r: Payment,
        expected: TransactionState,
    ) -> bool {
        *self.responses.0.lock().expect("Responses is poisoned") = vec![None; STAKEHOLDERS];

        for stakeholder in 0..STAKEHOLDERS {
            let amount = match stakeholder {
                0 => r.bank,
                1 => r.airline,
                2 => r.hotel,
                _ => {
                    panic!("Unknown stakeholder")
                }
            };

            let state = match message {
                b'P' => TransactionState::Prepare,
                b'C' => TransactionState::Commit,
                b'A' => TransactionState::Abort,
                _ => {
                    panic!("Unknown stakeholder")
                }
            };

            let mut msg = Transaction {
                transaction_id: t,
                transaction_state: state,
                service: stakeholder as i32,
                amount,
            };

            println!(
                "[COORDINATOR] sending {} id {} a {}",
                message, t, stakeholder
            );

            self.socket
                .send_to(&msg.serialize(), id_to_microservice(stakeholder))
                .expect("Error sending msg to stakeholder");
        }

        let responses = self.responses.1.wait_timeout_while(
            self.responses.0.lock().expect("Responses is poisoned"),
            TIMEOUT,
            |responses| responses.iter().any(Option::is_none),
        );

        return match responses {
            Ok(wait_result) => {
                if wait_result.1.timed_out() {
                    println!("[COORDINATOR] timeout {}", t);
                    if expected == TransactionState::Abort {
                        return true;
                    }
                    false
                } else {
                    wait_result
                        .0
                        .iter()
                        .all(|opt| opt.is_some() && (opt.expect("opt is poisoned") == expected))
                }
            }
            Err(e) => {
                println!("Error at broadcast_and_wait {}", e);
                false
            }
        };
    }

    /// Receives the responses form the microservices and stores it in responses
    fn responder(&mut self) {
        loop {
            let mut buf = [0; 13];
            let (size, from) = self
                .socket
                .recv_from(&mut buf)
                .expect("Error receiving message in responder");
            println!("[COORDINATOR] received {} bytes from {}", size, from);

            let transaction = Transaction::deserialize(buf);

            match transaction.transaction_state {
                TransactionState::Commit => {
                    println!("[COORDINATOR] received COMMIT from {}", transaction.service);
                    self.responses.0.lock().expect("Responses is poisoned")
                        [transaction.service as usize] = Some(TransactionState::Commit);
                    self.responses.1.notify_all();
                }
                TransactionState::Abort => {
                    println!("[COORDINATOR] received ABORT from {}", transaction.service);
                    self.responses.0.lock().expect("Responses is poisoned")
                        [transaction.service as usize] = Some(TransactionState::Abort);
                    self.responses.1.notify_all();
                }
                _ => {
                    println!("[COORDINATOR] ??? {}", transaction.service);
                }
            }
        }
    }

    /// Clones the TransactionCoordinator
    fn clone(&self) -> Self {
        TransactionCoordinator {
            id: self.id,
            log: HashMap::new(),
            socket: self.socket.try_clone().expect("Error cloning socket"),
            responses: self.responses.clone(),
        }
    }
}
