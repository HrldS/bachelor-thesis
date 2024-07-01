use std::io::{self, BufRead};
use std::net::{TcpListener, TcpStream};
use std::thread;


fn handle_client(stream: TcpStream) -> io::Result<()> {
    let reader = io::BufReader::new(stream);

    for line in reader.lines() {
        let line = line?;
        println!("Received: {}", line);
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("192.168.100.51:0")?;
    let local_addr = listener.local_addr()?;
    println!("Server listening on {}", local_addr);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
               std::thread::spawn(||handle_client(stream));
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }

    Ok(())
}
