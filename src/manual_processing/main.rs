use std::net::UdpSocket;

let ADDRESS = "127.0.0.1:12345";

fn main(){

    let socket = UdpSocket::bind(ADDRESS).expect("Unable to bind socket in main");
}