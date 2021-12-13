use common::helper::id_to_ctrladdr;
use std::convert::TryInto;
use std::mem::size_of;
use std::net::UdpSocket;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// The max amount of members
pub const TEAM_MEMBERS: usize = 5;
/// The timme before considering that the leader has fall
pub const TIMEOUT: Duration = Duration::from_secs(20);

/// struct used to replace the leader election protocol
pub struct LeaderElection {
    id: usize,
    socket: UdpSocket,
    leader_id: Arc<(Mutex<Option<usize>>, Condvar)>,
    got_ok: Arc<(Mutex<bool>, Condvar)>,
    stop: Arc<(Mutex<bool>, Condvar)>,
}

impl LeaderElection {
    /// Creates a new instance of LeaderElection
    pub fn new(id: usize) -> LeaderElection {
        let ret = LeaderElection {
            id,
            socket: UdpSocket::bind(id_to_ctrladdr(id))
                .expect("Unable to bind socket for LeaderElection"),
            leader_id: Arc::new((Mutex::new(Some(TEAM_MEMBERS)), Condvar::new())),
            got_ok: Arc::new((Mutex::new(false), Condvar::new())),
            stop: Arc::new((Mutex::new(false), Condvar::new())),
        };

        let mut clone = ret.clone();
        thread::spawn(move || clone.responder());

        ret
    }

    /// Returns true if the instance is the leader
    pub fn am_i_leader(&self) -> bool {
        self.get_leader_id() == self.id
    }

    /// Returns the id of the current leader, if it's unknown it returns TEAM_MEMBERS
    pub fn get_leader_id(&self) -> usize {
        self.leader_id
            .1
            .wait_while(
                self.leader_id.0.lock().expect("leader_id is poisoned"),
                |leader_id| leader_id.is_none(),
            )
            .expect("leader_id is poisoned")
            .expect("Leader is yet none")
    }

    /// Starts search for a new leader
    pub fn find_new(&mut self) {
        if *self.stop.0.lock().expect("Poisoned stop") {
            return;
        }
        if self
            .leader_id
            .0
            .lock()
            .expect("leader_id is poisoned")
            .is_none()
        {
            return;
        }
        println!("[{}] Searching for new leader", self.id);
        *self.got_ok.0.lock().expect("got_ok is poisoned") = false;
        *self.leader_id.0.lock().expect("leader_id is poisoned") = None;
        self.send_election();
        let got_ok = self.got_ok.1.wait_timeout_while(
            self.got_ok.0.lock().expect("got_ok is poisoned"),
            TIMEOUT,
            |got_it| !*got_it,
        );
        if !*got_ok.expect("got_ok is poisoned").0 {
            self.make_me_leader()
        } else {
            let leader_id = self.leader_id.1.wait_while(
                self.leader_id.0.lock().expect("leader_id is poisoned"),
                |leader_id| leader_id.is_none(),
            );
            match leader_id {
                Ok(_t) => {}
                Err(e) => {
                    panic!("leader lock is poisoned: {}", e)
                }
            }
        }
    }

    /// Forms a message whit the id of the peer and a byte that represents the message
    fn id_to_msg(&self, header: u8) -> Vec<u8> {
        let mut msg = vec![header];
        msg.extend_from_slice(&self.id.to_le_bytes());
        msg
    }

    /// Tells other peers to start an election
    fn send_election(&self) {
        let msg = self.id_to_msg(b'E');
        for peer_id in (self.id + 1)..TEAM_MEMBERS {
            self.socket
                .send_to(&msg, id_to_ctrladdr(peer_id))
                .expect("Error sending election to peer");
        }
    }

    /// Informs the other peers that this instance is the leader
    fn make_me_leader(&self) {
        println!("[{}] Announce coordinator", self.id);
        let msg = self.id_to_msg(b'C');
        for peer_id in 0..TEAM_MEMBERS {
            if peer_id != self.id {
                self.socket
                    .send_to(&msg, id_to_ctrladdr(peer_id))
                    .expect("Error sending make_me_leader to peer");
            }
        }
        *self.leader_id.0.lock().expect("Poisoned leader_id") = Some(self.id);
        self.leader_id.1.notify_all();
    }

    /// Receives the responses form the peer and responds accordingly, if receives ok makes got_ok
    /// true and notify_all, if received Election spawns a tread for find_new, and if it receives
    /// coordinator makes the received peer the leader
    fn responder(&mut self) {
        while !*self.stop.0.lock().expect("Stop is poisoned") {
            let mut buf = [0; size_of::<usize>() + 1];
            let (_size, _from) = self
                .socket
                .recv_from(&mut buf)
                .expect("responder found an error at recv_from");
            let id_from = usize::from_le_bytes(buf[1..].try_into().expect("Error getting id_from"));
            if *self.stop.0.lock().expect("Stop is poisoned") {
                break;
            }
            match &buf[0] {
                b'O' => {
                    println!("[{}] Received OK from {}", self.id, id_from);
                    *self.got_ok.0.lock().expect("got_ok is poisoned") = true;
                    self.got_ok.1.notify_all();
                }
                b'E' => {
                    println!("[{}] Received election from {}", self.id, id_from);
                    if id_from < self.id {
                        self.socket
                            .send_to(&self.id_to_msg(b'O'), id_to_ctrladdr(id_from))
                            .expect("Error sending ok");
                        let mut me = self.clone();
                        thread::spawn(move || me.find_new());
                    }
                }
                b'C' => {
                    println!("[{}] Received new coordinator {}", self.id, id_from);
                    *self.leader_id.0.lock().expect("leader_id is poisoned") = Some(id_from);
                    self.leader_id.1.notify_all();
                }
                _ => {
                    println!("[{}] Unknown message from {}", self.id, id_from);
                }
            }
        }
        *self.stop.0.lock().expect("stop is poisoned") = false;
        self.stop.1.notify_all();
    }

    /// clones the LeaderElection
    fn clone(&self) -> LeaderElection {
        LeaderElection {
            id: self.id,
            socket: self.socket.try_clone().expect("Error while cloning socket"),
            leader_id: self.leader_id.clone(),
            got_ok: self.got_ok.clone(),
            stop: self.stop.clone(),
        }
    }

    pub fn set_leader(&mut self, id: usize) {
        *self.leader_id.0.lock().expect("leader_id is poisoned") = Some(id);
    }
}
