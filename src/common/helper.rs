/// It receives de id of a peer as a Usize and returns de control address of that peer
pub fn id_to_ctrladdr(id: usize) -> String {
    "127.0.0.1:1234".to_owned() + &*id.to_string()
}

/// It receives de id of a peer as a Usize and returns de data address of that peer
pub fn id_to_dataaddr(id: usize) -> String {
    "127.0.0.1:1235".to_owned() + &*id.to_string()
}

/// It receives de id of a microservice as a Usize and returns de address of that microservice
pub fn id_to_microservice(id: usize) -> String {
    let result = match id {
        0 => "127.0.0.1:1111",
        1 => "127.0.0.1:2222",
        2 => "127.0.0.1:3333",
        _ => {
            panic!("Unknown microservice")
        }
    };

    result.to_string()
}
