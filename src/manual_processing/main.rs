use common::payment::Payment;
use common::transaction_coordinator;
use std::io;


/// Receives transaction amount
pub fn get_amount(service: &str) -> i32 {
    let mut line = String::new();
    let error_message = "[Main] Expected a number greater than zero.";

    println!("[Main] Enter {} amount", service);

    io::stdin()
        .read_line(&mut line)
        .expect("failed to read from stdin");
    if line.trim().is_empty() {
        println!("{}", error_message);
        get_amount(service)
    } else {
        match line.trim().parse::<u32>() {
            Ok(last_transaction) => {
                if last_transaction > 0 {
                    last_transaction as i32
                } else {
                    println!("{}", error_message);
                    get_amount(service)
                }
            }
            Err(..) => {
                println!("{}", error_message);
                get_amount(service)
            }
        }
    }
}

/// Receives transaction id
pub fn get_last_transaction_id() -> i32 {
    let mut line = String::new();
    let error_message = "[Main] Expected a number greater than zero.";

    println!("[Main] Enter number of lines processed by alGlobo");

    io::stdin()
        .read_line(&mut line)
        .expect("failed to read from stdin");
    if line.trim().is_empty() {
        println!("{}", error_message);
        get_last_transaction_id()
    } else {
        match line.trim().parse::<u32>() {
            Ok(last_transaction) => {
                if last_transaction > 0 {
                    last_transaction as i32
                } else {
                    println!("{}", error_message);
                    get_last_transaction_id()
                }
            }
            Err(..) => {
                println!("{}", error_message);
                get_last_transaction_id()
            }
        }
    }
}

/// Manual processing main
fn main() {
    let mut coordinator = transaction_coordinator::TransactionCoordinator::new(0);

    let mut transaction_id = get_last_transaction_id() + 1;

    loop {
        let bank = get_amount("bank");
        let airline = get_amount("airline");
        let hotel = get_amount("hotel");

        let payment = Payment {
            line: transaction_id as usize,
            bank,
            airline,
            hotel,
        };

        let result = coordinator.submit(transaction_id, payment);

        if result {
            println!("Successful transaction")
        } else {
            println!("Transaction failed")
        }

        transaction_id += 1;
    }
}
