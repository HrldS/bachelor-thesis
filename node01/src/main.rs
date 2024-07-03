extern crate csv;

use csv::{StringRecord, ReaderBuilder};
use std::error::Error;
use std::fs::File;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    alloc::Layout,
    io::{self, Write, Error as IOError, ErrorKind}, //BufWriter,
    net::{TcpStream, Ipv4Addr, SocketAddrV4}, //TcpListener,
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

fn client_tcp() -> io::Result<()> {
    let mut stream = TcpStream::connect("192.168.100.52:41000")?;

    let file = File::open("src/data/test_data.csv")?;  //? try reading file
    let mut contant = ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(file); // Disable headers assumption to not skip first row

    let mut records = Vec::new();
    for line in contant.records() {
        let record = line?;

        let name = record[0].to_string();
        let a = record[1].to_string();
        let b = record[2].to_string();
        let c = record[3].to_string();

        records.push((name, a, b, c));
        println!("Debug: {:?}",records);
    }
    //let file = File::open("src/data/test_data.csv")?;  //? try reading file
    //let mut content = ReaderBuilder::new().delimiter(b';').has_headers(false).from_reader(file); // Disable headers assumption to not skip first row
    let mut record_string = String::new();
    //let mut iterator = 0;

   // for line in content.records() {
      //  let record = line?;
     //   println!("Debug: {:?}", record);
        //println!("Debug: runde {}", iterator);
        //iterator += 1;
        //let record = line?;
        //println!("Debug Record: {:?}", record)
        //let concat_record = record.iter().collect::<Vec<&str>>().join(";");
       // println!("Debug Recordstring: {:?}", concat_record);
        //record_string += &concat_record;
        //println!("Debug String: {:?}", record_string);
        // Write the record to the TCP stream
    stream.write_all(record_string.as_bytes())?;
    println!("Debug: Something was flushed");
    stream.flush()?;
    println!("Debug: Something was written");
    println!("");
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
        let mut input = String::new();
        println!("Enter transportation Protocol:");
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
            //let _handle = std::thread::spawn(|| {  //move
               // if let Err(e) = client_tcp() {
                  //  eprintln!("Error occurred: {}", e);
              //  }
              client_tcp();
                //});
            break;
        } else {
            println!("Protocol: {:?} does not exist!", protocol);
        }
    }
    Ok(())
}
