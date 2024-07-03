use std::io::{self, BufRead};
use std::net::{TcpListener, TcpStream}; // Ipv4Addr, SocketAddrV4
//use portpicker::pick_unused_port;

fn handle_client(stream: TcpStream) -> io::Result<()> {
    let buffer_size = 1024 * 1024 * 1024 * 1024;
    let reader = io::BufReader::with_capacity(buffer_size, stream);

    for line in reader.lines() {
        let line = line?;
        println!("Received: {:?}\n", line);
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("192.168.100.52:41000")?;
    let local_addr = listener.local_addr()?;
    println!("Server listening on {}", local_addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
               std::thread::spawn(move || handle_client(stream));
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
