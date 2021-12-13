/// This enum represent all possible states of a transaction
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransactionState {
    Accepted,
    Commit,
    Abort,
    Prepare,
    Wait,
}

/// This struct is made to represent a transaction between the leader and a microservice. It's
/// formed by the transaction_state(TransactionState), transaction_id, amount, and service(as it's id)
pub struct Transaction {
    pub transaction_state: TransactionState,
    pub transaction_id: i32,
    pub amount: i32,
    pub service: i32,
}

impl Transaction {
    /// Converts the transaction into a bytes array to be send through a socket
    pub fn serialize(&mut self) -> [u8; 13] {
        let mut serialize: [u8; 13] = [0; 13];

        serialize[0] = match self.transaction_state {
            TransactionState::Prepare => b'P',
            TransactionState::Abort => b'A',
            TransactionState::Commit => b'C',
            _ => {
                panic!("Unrecognized TransactionState")
            }
        };

        let bin_transaction_id = self.transaction_id.to_le_bytes();
        serialize[1..5].copy_from_slice(&bin_transaction_id);

        let bin_amount = self.amount.to_le_bytes();
        serialize[5..9].copy_from_slice(&bin_amount);

        let bin_service = self.service.to_le_bytes();
        serialize[9..13].copy_from_slice(&bin_service);

        serialize
    }

    /// Converts serialized transaction into a transaction again
    pub fn deserialize(buf: [u8; 13]) -> Transaction {
        let state = match buf[0] {
            b'P' => TransactionState::Prepare,
            b'C' => TransactionState::Commit,
            b'A' => TransactionState::Abort,
            _ => {
                panic!("Invalid transaction state")
            }
        };

        let mut transaction_id_b: [u8; 4] = [0; 4];
        transaction_id_b.clone_from_slice(&buf[1..5]);

        let mut amount_b: [u8; 4] = [0; 4];
        amount_b.clone_from_slice(&buf[5..9]);

        let mut service_b: [u8; 4] = [0; 4];
        service_b.clone_from_slice(&buf[9..13]);

        Transaction {
            transaction_id: i32::from_le_bytes(transaction_id_b),
            amount: i32::from_le_bytes(amount_b),
            transaction_state: state,
            service: i32::from_le_bytes(service_b),
        }
    }
}
