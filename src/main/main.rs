mod leader_election;
mod payment;
mod transaction_coordinator;

use structopt::StructOpt;

use crate::leader_election::{LeaderElection, TEAM_MEMBERS, TIMEOUT};
use common::helper::id_to_dataaddr;
use std::fs::File;
use std::io::Write;
use std::net::UdpSocket;
use std::time::Duration;
use std::{fs, thread};

use crate::payment::Payment;

/// Receives the id of the new AlGlobo instance.
#[derive(StructOpt)]
struct Cli {
    /// The new worker id (Type u32).
    id: usize,
}

/// AlGlobo instance main loop
fn main() {
    let args = Cli::from_args();
    let id = args.id;
    println!("[{}] Start", id);

    let socket = UdpSocket::bind(id_to_dataaddr(id)).unwrap();
    let csv = fs::read_to_string("./resources/payments.csv")
        .expect("Something went wrong reading the file");
    let reader = csv::Reader::from_reader(csv.as_bytes());
    let mut iter = reader.into_deserialize();
    let mut scrum_master = LeaderElection::new(id);
    let mut buf = [0; 8];
    let mut last_record: usize = 0;
    let mut failed_transactions_file =
        get_failed_transactions_file("src/main/failed_transactions.csv");
    let mut coordinator = transaction_coordinator::TransactionCoordinator::new(id);

    loop {
        if scrum_master.am_i_leader() {
            if let Some(result) = iter.next() {
                let mut record: Payment = result.unwrap();
                while record.line <= last_record {
                    if let Some(result) = iter.next() {
                        match result {
                            Err(e) => {
                                println!("[Reading record threw error] {}", e);
                            }
                            Ok(r) => {
                                record = r;
                            }
                        }
                    }
                }

                println!(
                    "\n\n\n[Record | {},{},{},{}]",
                    record.line, record.hotel, record.airline, record.bank
                );

                let is_successful = coordinator.submit(record.line as i32, record);

                println!("result was {}", is_successful);

                if !is_successful {
                    let data = format!("{},{},{}\n", record.bank, record.airline, record.hotel);
                    failed_transactions_file
                        .write_all(data.as_ref())
                        .expect("Error writing to error file")
                }

                last_record = record.line;
            } else {
                println!("[Reached EOF]");
            }

            socket
                .set_read_timeout(Some(Duration::new(1, 0)))
                .expect("set_read_timeout error occurred");

            for peer_id in 0..TEAM_MEMBERS {
                if peer_id != id {
                    println!("[{}] Sending to peer last record", id);
                    socket
                        .send_to(&last_record.to_be_bytes(), id_to_dataaddr(peer_id))
                        .unwrap();
                }
            }
        } else {
            println!(
                "[{}] Last time I checked last line was {}",
                id,
                last_record.to_string()
            );

            let leader_id = scrum_master.get_leader_id();

            if leader_id != id {
                socket.set_read_timeout(Some(TIMEOUT)).unwrap();
                if let Ok((_size, from)) = socket.recv_from(&mut buf) {
                    last_record = usize::from_be_bytes(buf);
                    if leader_id == TEAM_MEMBERS {
                        let new_leader = from
                            .port()
                            .to_string()
                            .chars()
                            .last()
                            .unwrap()
                            .to_digit(10)
                            .unwrap() as usize;
                        scrum_master.set_leader(new_leader);
                        println!(
                            "[{}] Leader is ({}) and last line is {}",
                            id,
                            new_leader,
                            last_record.to_string()
                        );
                    } else {
                        println!(
                            "[{}] Received from leader ({}) that last line is {}",
                            id,
                            leader_id,
                            last_record.to_string()
                        );
                    }
                    thread::sleep(Duration::from_millis(500));
                } else {
                    scrum_master.find_new()
                }
            }
        }
    }
}

fn get_failed_transactions_file(failed_transactions_path: &str) -> File {
    return match fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(failed_transactions_path)
    {
        Ok(file) => {
            println!("Failed transactions already existed, will append on it");
            file
        }
        Err(_) => {
            println!("Failed transactions file does not exist, will create it");
            fs::File::create(failed_transactions_path).expect("Error creating logger file")
        }
    };
}
