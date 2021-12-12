mod transaction_coordinator;
mod record;

use std::fs;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use structopt::StructOpt;

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

    let failed_transactions_path = "src/main_activity/failed_transactions.csv";
    let mut failed_transactions_file;

    match fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(failed_transactions_path) {
        Ok(file) => {
            println!("Failed transactions already existed, will append on it");
            failed_transactions_file = file;
        }
        Err(_) => {
            println!("Failed transactions file does not exist, will create it");
            failed_transactions_file = fs::File::create(failed_transactions_path)
                .expect("Error creating logger file");
        },
    };

    let mut transaction_id = 0;

    let csv = fs::read_to_string("src/main_activity/transactions.csv")
        .expect("Something went wrong reading the file");

    let mut reader = csv::Reader::from_reader(csv.as_bytes());

    let mut coordinator = transaction_coordinator::TransactionCoordinator::new(id);

    for record in reader.deserialize() {
        let record: record::Record = record.expect("Error getting record from csv");

        let result = coordinator.submit(transaction_id ,record);

        if !result {
            let data = format!("{},{},{}\n",record.hotel, record.airline, record.bank);
            failed_transactions_file.write_all(data.as_ref()).expect("Error writing to logger file")
        }

        transaction_id += 1;
    }
}
