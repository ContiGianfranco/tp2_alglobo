mod payment;

use structopt::StructOpt;



use std::mem::size_of;
use std::net::{UdpSocket};
use std::sync::{Arc, Condvar, Mutex};

use std::{fs, thread};
use std::time::Duration;



use std::convert::TryInto;
use crate::payment::Payment;

fn id_to_ctrladdr(id: usize) -> String { "127.0.0.1:1234".to_owned() + &*id.to_string() }
fn id_to_dataaddr(id: usize) -> String { "127.0.0.1:1235".to_owned() + &*id.to_string() }

const TEAM_MEMBERS: usize = 5;
const TIMEOUT: Duration = Duration::from_secs(10);

struct LeaderElection {
    id: usize,
    socket: UdpSocket,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
    stop: Arc<(Mutex<bool>, Condvar)>,
}

impl LeaderElection {
    fn new(id: usize) -> LeaderElection {
        let mut ret = LeaderElection {
            id,
            socket: UdpSocket::bind(id_to_ctrladdr(id)).unwrap(),
            leader_id: Arc::new((Mutex::new(Some(id)), Condvar::new())),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
            stop: Arc::new((Mutex::new(false), Condvar::new()))
        };

        let mut clone = ret.clone();
        thread::spawn(move || clone.responder());

        ret.find_new();
        ret
    }

    fn am_i_leader(&self) -> bool {
        self.get_leader_id() == self.id
    }

    fn get_leader_id(&self) -> usize {
        self.leader_id.1.wait_while(self.leader_id.0.lock().unwrap(), |leader_id| leader_id.is_none()).unwrap().unwrap()
    }

    fn find_new(&mut self) {
        if *self.stop.0.lock().unwrap() {
            return
        }
        if self.leader_id.0.lock().unwrap().is_none() {
            return
        }
        println!("[{}] Searching for new leader", self.id);
        *self.got_ok.0.lock().unwrap() = false;
        *self.leader_id.0.lock().unwrap() = None;
        self.send_election();
        let got_ok = self.got_ok.1.wait_timeout_while(self.got_ok.0.lock().unwrap(), TIMEOUT, |got_it| !*got_it );
        if !*got_ok.unwrap().0 {
            self.make_me_leader()
        } else {
            self.leader_id.1.wait_while(self.leader_id.0.lock().unwrap(), |leader_id| leader_id.is_none() );
        }

    }

    fn id_to_msg(&self, header:u8) -> Vec<u8> {
        let mut msg = vec!(header);
        msg.extend_from_slice(&self.id.to_le_bytes());
        msg
    }

    fn send_election(&self) {
        let msg = self.id_to_msg(b'E');
        for peer_id in (self.id+1)..TEAM_MEMBERS {
            self.socket.send_to(&msg, id_to_ctrladdr(peer_id)).unwrap();
        }
    }

    fn make_me_leader(&self) {
        println!("[{}] Announce coordinator", self.id);
        let msg = self.id_to_msg(b'C');
        for peer_id in 0..TEAM_MEMBERS {
            if peer_id != self.id {
                self.socket.send_to(&msg, id_to_ctrladdr(peer_id)).unwrap();
            }
        }
        *self.leader_id.0.lock().unwrap() = Some(self.id);
    }

    fn responder(&mut self) {
        while !*self.stop.0.lock().unwrap() {
            let mut buf = [0; size_of::<usize>() + 1];
            let (_size, _from) = self.socket.recv_from(&mut buf).unwrap();
            let id_from = usize::from_le_bytes(buf[1..].try_into().unwrap());
            if *self.stop.0.lock().unwrap() {
                break;
            }
            match &buf[0] {
                b'O' => {
                    println!("[{}] Received OK from {}", self.id, id_from);
                    *self.got_ok.0.lock().unwrap() = true;
                    self.got_ok.1.notify_all();
                }
                b'E' => {
                    println!("[{}] Received election from {}", self.id, id_from);
                    if id_from < self.id {
                        self.socket.send_to(&self.id_to_msg(b'O'), id_to_ctrladdr(id_from)).unwrap();
                        let mut me = self.clone();
                        thread::spawn(move || me.find_new());
                    }
                }
                b'C' => {
                    println!("[{}] Received new coordinator {}", self.id, id_from);
                    *self.leader_id.0.lock().unwrap() = Some(id_from);
                    self.leader_id.1.notify_all();
                }
                _ => {
                    println!("[{}] Unknown message from {}", self.id, id_from);
                }
            }
        }
        *self.stop.0.lock().unwrap() = false;
        self.stop.1.notify_all();
    }

    fn stop(&mut self) {
        *self.stop.0.lock().unwrap() = true;
        self.stop.1.wait_while(self.stop.0.lock().unwrap(), |should_stop| *should_stop);
    }

    fn clone(&self) -> LeaderElection {
        LeaderElection {
            id: self.id,
            socket: self.socket.try_clone().unwrap(),
            leader_id: self.leader_id.clone(),
            got_ok: self.got_ok.clone(),
            stop: self.stop.clone(),
        }
    }
}

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

    loop {
        println!("[{}] Start", id);
        let csv = fs::read_to_string("./resources/payments.csv").expect("Something went wrong reading the file");
        let reader = csv::Reader::from_reader(csv.as_bytes());
        let mut iter = reader.into_deserialize();
        let mut scrum_master = LeaderElection::new(id);
        let socket = UdpSocket::bind(id_to_dataaddr(id)).unwrap();
        let mut buf = [0; 8];

        loop {

            let mut last_record: usize = 0;

            if scrum_master.am_i_leader() {

                //println!("[{}] I'm leader and last line is {}", id, last_record.to_string());

                if let Some(result) = iter.next() {
                    if result.is_err() {
                        println!("[Reading record threw error]");
                    } else {
                        let record: Payment = result.unwrap();
                        println!("[Record number {}]", record.line);
                        last_record = record.line
                    }
                } else {
                    println!("[Reached EOF]");
                }

                socket.set_read_timeout(Some(Duration::new(1, 0)));
                if let Ok((_size, from)) = socket.recv_from(&mut buf) {
                    println!("[{}] Received PING, sending NUMBER OF LAST PROCESSED LINE", id);
                    socket.send_to(&last_record.to_be_bytes(), from).unwrap();
                }
            } else {
                let leader_id = scrum_master.get_leader_id();
                println!("[{}] Asking leader ({}) last line via PING", id, leader_id);
                //println!("[{}] Last time I checked last line was {}", id, last_record.to_string());
                socket.send_to("PING".as_bytes(), id_to_dataaddr(leader_id)).unwrap();
                socket.set_read_timeout(Some(TIMEOUT)).unwrap();
                if let Ok((_size, _from)) = socket.recv_from(&mut buf) {
                    last_record = usize::from_be_bytes(buf);
                    println!("[{}] Received from leader ({}) that last line is {}", id, leader_id,last_record.to_string());
                    thread::sleep(Duration::from_millis(1000));
                } else {
                    scrum_master.find_new()
                }
            }
        }
    }
}
