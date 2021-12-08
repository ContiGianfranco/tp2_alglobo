mod transaction_coordinator;
mod record;

use std::fs;
use std::io::Write;

fn main(){
    let mut error_file = fs::File::create("src/main_activity/failed_transactions.csv")
        .expect("Error creating logger file");

    let mut transaction_id = 0;

    let csv = fs::read_to_string("src/main_activity/transactions.csv")
        .expect("Something went wrong reading the file");

    let mut reader = csv::Reader::from_reader(csv.as_bytes());

    let mut coordinator = transaction_coordinator::TransactionCoordinator::new();

    for record in reader.deserialize() {
        let record: record::Record = record.expect("Error getting record from csv");

        let result = coordinator.submit(transaction_id ,record);

        if !result {
            let data = format!("{},{},{}\n",record.hotel, record.airline, record.bank);
            error_file.write_all(data.as_ref()).expect("Error writing to logger file")
        }

        transaction_id += 1;
    }
}
