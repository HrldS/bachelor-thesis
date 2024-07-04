extern crate csv;

use csv::{StringRecord, ReaderBuilder};
use std::error::Error;
use std::fs::File;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    alloc::Layout,
    io::{self, BufRead, Write, Error as IOError, ErrorKind}, 
    net::{TcpListener, TcpStream, Ipv4Addr, SocketAddrV4},
    time::Duration 
};

trait WriteLine {
    fn write_csv_record(&mut self, line: &StringRecord) -> io::Result<usize>;
}

trait ReadLine {
    fn read_line(&self) -> io::Result<StringRecord>;
}

impl ReadLine for [u8] {
    fn read_line(&self) -> io::Result<StringRecord> {
        // Convert bytes back to a string
        let line_str = match std::str::from_utf8(self) {
            Ok(s) => s,
            Err(e) => return Err(IOError::new(ErrorKind::InvalidData, e)),
        };
        
        println!("Debug ReadLine: {:?}", line_str);
        println!();
        //Parse the string into a CSV record
        let mut reader = csv::ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(line_str.as_bytes());
        
        if let Some(result) = reader.records().next() {
            result.map_err(|e| IOError::new(ErrorKind::InvalidData, e))
        } else {
            Err(IOError::new(ErrorKind::UnexpectedEof, "No CSV record found"))
        }
    }
}

impl WriteLine for [u8] {
    fn write_csv_record(&mut self, line: &StringRecord) -> io::Result<usize> {
        let mut this = self;
        let line_str = line.iter().collect::<Vec<_>>().join(";"); // Convert the line to a semicolon-separated string

        println!("Debug WriteLine as String: {:?}", line_str);
        println!();

        let bytes = line_str.as_bytes(); // Convert the string to bytes

        println!("Debug WriteLine as Bytes: {:?}", bytes);
        println!();

        this.write(bytes) // Write the bytes to the memory region
    }
}

fn data_formating(size: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let file = File::open("src/data/test_data.csv")?;
    let mut reader = ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(file);
    let reset_string = String::new();
    let mut result: Vec<String> = Vec::new();

    match size {
        "1" => {
            for line in reader.records() {
                let record = line?;
                let string = record.iter().collect::<Vec<_>>().join(";");
                result.push(string);
            }
        },
        "2" => {
            let mut index = 0;
            for line in reader.records() {
                let mut temp = String::new();
                if index < 500 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";") + "\n";
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string;
                    index = 0;
                }
            }
        },
        "3" => {
            let mut index = 0;
            for line in reader.records() {
                let mut temp = String::new();
                if index < 1000 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";") + "\n";
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string;
                    index = 0;
                }
            }
        },
        "4" => {
            let mut index = 0;
            for line in reader.records() {
                let mut temp = String::new();
                if index < 1250 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";") + "\n";
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string;
                    index = 0;
                }
            }
        },
        "5" => {
            let mut index = 0;
            for line in reader.records() {
                let mut temp = String::new();
                if index < 2500 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";") + "\n";
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string;
                    index = 0;
                }
            }
        },
        "6" => {
            let mut index = 0;
            for line in reader.records() {
                let mut temp = String::new();
                if index < 5000 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";") + "\n";
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string;
                    index = 0;
                }
            }
        },
        _ => {

        }
    }
    Ok(result)
}

fn read_file() -> Result<Vec<(String, i32, i32, i32)>, Box<dyn Error>>{
    let file = File::open("src/data/test_data.csv")?;  //? try reading file
    let mut contant = ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(file); // Disable headers assumption to not skip first row

    let mut records = Vec::new();
    for line in contant.records() {
        let record = line?;

        if record.len() != 4 {
            println!("Record length:{:?}",record.len());
            return Err("Incorrect number of fields in record".into());
        }

        let name = record[0].to_string();
        let a: i32 = record[1].parse()?;
        let b: i32 = record[2].parse()?;
        let c: i32 = record[3].parse()?;

        records.push((name, a, b, c));
    }
    Ok(records)
}

async fn client_rdma(addr: SocketAddrV4, rdma_type: &str) -> io::Result<()> {
    let rdma = Rdma::connect(addr, 1, 1, 512).await?;

    let file = File::open("src/data/test_data.csv")?;  //? try reading file
    let mut contant = csv::ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(file); // Disable headers assumption to not skip first row

    for line in contant.records() {
        let line = line?;

        if rdma_type == "write" {
            let layout = Layout::for_value(&line);

            let mut lmr = rdma.alloc_local_mr(layout)?;
            let mut rmr = rdma.request_remote_mr(layout).await?;
                
            println!("Debug: Client about to write {:?}", line);
            println!();

            let _num = lmr.as_mut_slice().write_csv_record(&line)?;
            rdma.write(&lmr, &mut rmr).await?;

            rdma.send_remote_mr(rmr).await?;
        } else {
            println!("not write");
        }
    }
    
    Ok(())
}

fn client_tcp(size: &str) -> io::Result<()> {
    let mut stream = TcpStream::connect("192.168.100.52:41000")?;

    let data = data_formating(size);

    for line in data {
        let message = line;
        stream.write_all(message.as_bytes())?;
        stream.flush()?;
    }
    Ok(())
}

fn handle_tcp_client(stream: TcpStream) -> io::Result<()> {
    let reader = io::BufReader::new(stream);

    for line in reader.lines() {
        match line {
            Ok(line) => println!("Received: {:?}", line),
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn server(addr: SocketAddrV4) -> io::Result<()> {
    let rdma_listener = RdmaListener::bind(addr).await?;
    let rdma = rdma_listener.accept(1, 1, 512).await?;
    // run here after client connect
    let lmr = rdma.receive_local_mr().await?;

    println!("Debug Server: {:?}", lmr.as_slice());
    println!();

    let lmr_contant = lmr.as_slice().read_line()?; 
    println!("Server received: {:?}", lmr_contant);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    loop {
        let mut size_selected = String::new();
        let mut input = String::new();
        println!("Hallo! This programm tests the latency and bandwidth of the TCP and RDMA transport protocol by sending the content of a csv file to a server for processing.");
        println!("Afterwards the server sends the processed data back to you.");
        println!("");
        println!("Please enter the transportation Protocol you want to test:");
        println!("Protocols available: 'rdma' or 'tcp'");

        io::stdin().read_line(&mut input).expect("failed to read");

        let protocol = input.trim();

        if protocol == "rdma" {
            loop {
                println!("Please choose RDMA transmission Type: send, write or atomic");
                let mut rdma_type = String::new();
                io::stdin().read_line(&mut rdma_type).expect("failed to read");
        
                let rdma_type = rdma_type.trim();
        
                if rdma_type == "send" {
                    let data = read_file()?;
        
                    for tupel in data {
                        println!("{:?}", tupel);
                    }
                    break;
                } else if rdma_type == "write" {
                    let addr = SocketAddrV4::new(Ipv4Addr::new(192, 168, 100, 51), pick_unused_port().unwrap());
                    std::thread::spawn(move || server(addr));
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    client_rdma(addr, rdma_type).await.map_err(|err| println!("{}", err)).unwrap();
                    break;
                } else if rdma_type == "atomic" {
                    println!("{:?}", rdma_type);
                    break;
                } else {
                    println!("Transmission type:{:?} does not exist!", rdma_type);
                }
            }
            break;
        } else if protocol == "tcp" {
            println!("Please select message size:");
            println!("1: each row individually");
            println!("2: 10% size");
            println!("3: 20% size");
            println!("4: 25% size");
            println!("5: 50% size");
            println!("6: entire file at once");

            let mut size = String::new();
            io::stdin().read_line(&mut size).expect("failed to read");
            size_selected = size.trim().to_string();

            let client_thread = std::thread::spawn(move || client_tcp(&size_selected));   //spawn worker thread to handle the tcp client

            let listener = TcpListener::bind("192.168.100.51:40999")?;
            let local_addr = listener.local_addr()?;

            println!("Server listening on {}", local_addr);

            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                       std::thread::spawn(|| handle_tcp_client(stream));  // spawn worker thread to handle incomming tcp requests
                    }
                    Err(e) => {
                        eprintln!("Connection failed: {}", e);
                    }
                }
            }
            client_thread.join().unwrap();  //wait for the worker thread to finish his work
            println!("Worker has finished");
            break;
        } else {
            println!("Protocol: {:?} does not exist!", protocol);
        }
    }
    Ok(())
}

