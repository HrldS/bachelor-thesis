use std::io::{self, BufRead};
use std::net::{TcpListener, TcpStream}; // Ipv4Addr, SocketAddrV4
//use portpicker::pick_unused_port;

fn handle_client(stream: TcpStream) -> io::Result<()> {
    let reader = io::BufReader::new(stream);
    let mut count = 1;
    for line in reader.lines() {
        match line {
            Ok(line) => {
                println!("Received: {:?}", line);
                println!("Called times {}", count);
                count += 1;
            }
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }
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
               std::thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
    Ok(())
}
