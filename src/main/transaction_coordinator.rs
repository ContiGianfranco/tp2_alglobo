use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;
use crate::payment::Payment;

const TRANSACTION_COORDINATOR_ADDR: &str = "127.0.0.1:123";
const STAKEHOLDERS: usize = 3;
const TIMEOUT: Duration = Duration::from_secs(10);

fn id_to_microservice(id: usize) -> String {
    let result = match id {
        0 => { "127.0.0.1:1111" }
        1 => { "127.0.0.1:2222" }
        2 => { "127.0.0.1:3333" }
        _=> {panic!("Unknown microservice")}
    };

    result.to_string()
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransactionState {
    Accepted,
    Commit,
    Abort,
    Prepare,
    Wait
}

pub struct Transaction {
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

pub struct TransactionCoordinator {
    id: usize,
    log: HashMap<i32, TransactionState>,
    socket: UdpSocket,
    responses: Arc<(Mutex<Vec<Option<TransactionState>>>, Condvar)>,
}

impl TransactionCoordinator {
    pub fn new(id: usize) -> TransactionCoordinator {
        let coordinator = TransactionCoordinator {
            id,
            log: HashMap::new(),
            socket: UdpSocket::bind(format!("{}{}",TRANSACTION_COORDINATOR_ADDR, id)).unwrap(),
            responses: Arc::new((Mutex::new(vec![None; STAKEHOLDERS]), Condvar::new())),
        };

        let mut clone = coordinator.clone();
        thread::spawn(move || clone.responder());

        coordinator
    }

    pub fn submit(&mut self, t: i32, r: Payment) -> bool {
        match self.log.get(&t) {
            None => self.full_protocol(t, r),
            Some(TransactionState::Wait) => self.full_protocol(t, r),
            Some(TransactionState::Commit) => self.commit(t, r),
            Some(TransactionState::Abort) => self.abort(t, r),
            _ => {
                panic!("No match for transaction {}",t)
            }
        }
    }

    fn full_protocol(&mut self, t: i32, r: Payment) -> bool {
        let clone = r;
        if self.prepare(t, r) {
            self.commit(t, clone)
        } else {
            self.abort(t, clone)
        }
    }

    fn prepare(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Wait);
        println!("[COORDINATOR] prepare {}", t);
        self.broadcast_and_wait(b'P', t, r, TransactionState::Commit)
    }

    fn commit(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Commit);
        println!("[COORDINATOR] commit {}", t);
        self.broadcast_and_wait(b'C', t, r, TransactionState::Commit)
    }

    fn abort(&mut self, t: i32, r: Payment) -> bool {
        self.log.insert(t, TransactionState::Abort);
        println!("[COORDINATOR] abort {}", t);
        !self.broadcast_and_wait(b'A', t, r, TransactionState::Abort)
    }

    fn broadcast_and_wait(&self, message: u8, t: i32, r: Payment, expected: TransactionState) -> bool {
        *self.responses.0.lock().unwrap() = vec![None; STAKEHOLDERS];

        for stakeholder in 0..STAKEHOLDERS {
            let amount = match stakeholder {
                0 => { r.bank }
                1 => { r.airline }
                2 => { r.hotel }
                _=> {panic!("Unknown stakeholder")}
            };

            let state = match message {
                b'P' => { TransactionState::Prepare }
                b'C' => { TransactionState::Commit }
                b'A' => { TransactionState::Abort }
                _=> {panic!("Unknown stakeholder")}
            };

            let mut msg = Transaction {
                transaction_id: t,
                transaction_state: state,
                service: stakeholder as i32,
                amount
            };

            println!("[COORDINATOR] sending {} id {} a {}", message, t, stakeholder);

            self.socket.send_to(&msg.serialize(), id_to_microservice(stakeholder)).unwrap();
        }

        let responses = self.responses.1.wait_timeout_while(self.responses.0.lock().unwrap(), TIMEOUT, |responses| responses.iter().any(Option::is_none));

        if responses.is_err() {
            println!("[COORDINATOR] timeout {}", t);
            false
        } else {
            responses.unwrap().0.iter().all(|opt| opt.is_some() && (opt.unwrap() == expected))
        }
    }

    fn responder(&mut self) {
        loop {
            let mut buf = [0; 13];
            let (size, from) = self.socket.recv_from(&mut buf).unwrap();
            println!("[COORDINATOR] received {} bytes from {}", size, from);

            let transaction = Transaction::deserialize(buf);

            match transaction.transaction_state {
                TransactionState::Commit => {
                    println!("[COORDINATOR] received COMMIT from {}", transaction.service);
                    self.responses.0.lock().unwrap()[transaction.service as usize] = Some(TransactionState::Commit);
                    self.responses.1.notify_all();
                }
                TransactionState::Abort => {
                    println!("[COORDINATOR] received ABORT from {}", transaction.service);
                    self.responses.0.lock().unwrap()[transaction.service as usize] = Some(TransactionState::Abort);
                    self.responses.1.notify_all();
                }
                _ => {
                    println!("[COORDINATOR] ??? {}", transaction.service);
                }
            }
        }
    }

    fn clone(&self) -> Self {
        TransactionCoordinator {
            id: self.id,
            log: HashMap::new(),
            socket: self.socket.try_clone().unwrap(),
            responses: self.responses.clone(),
        }
    }
}
