extern crate csv;
extern crate tokio;
/*
use std::io::{self, BufRead};
use std::net::{TcpListener, TcpStream}; // Ipv4Addr, SocketAddrV4
//use portpicker::pick_unused_port;
*/
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener,TcpStream};
use csv::Writer;
use std::io;


async fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    println!("first line");

    let mut data_buffer = Vec::new();

    stream.read_to_end(&mut data_buffer).await?;        //write the data from the stream into the data_buffer
    println!("after read_to_end");
    let mut reader = csv::Reader::from_reader(data_buffer.as_slice());  //read the data from the data_buffer

    let mut message_buffer = Writer::from_writer(Vec::new()); //create the buffer to write the processed records into

    for content in reader.records() {  // calculate the volume for each record in the received csv file
        let record = content?;
        
        // get the 3 necessary values from the csv record to calculate the volume
        let col1: i32 = record[1].parse()?;     
        let col2: i32 = record[2].parse()?;
        let col3: i32 = record[3].parse()?;
        let object_volume = col1 * col2 * col3;
        
        let mut new_record = record.clone();    // take the original record
        new_record.push_field(&object_volume.to_string()); // create the processed record by adding the volume to the original record
        
        message_buffer.write_record(&new_record)?; //write the processed records into the message buffer
    }

    let send_message = message_buffer.into_inner()?; // return the bytes of the processed record

    stream.write_all(&send_message).await?; //send the processed message bytes back to client

    stream.flush().await?; // ensure that the entire message is send

    stream.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    /*
    let listener = TcpListener::bind("192.168.100.52:41000")?;
    let local_addr = listener.local_addr()?;
    println!("Server listening on: {}", local_addr);

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
    */
    
    let mut server_type = String::new();
    loop {
        let mut input = String::new();
        println!("Please select one of these Transportation protocol types: rdma or tcp");
        io::stdin().read_line(&mut input).expect("failed to read server_type");

        server_type = input.trim().to_string();
    
        if server_type == "rdma" {
            break;
        } else if server_type == "tcp" {
            break;
        } else {
            println!("The Transportation protocol: {:?} is not supported", server_type);
        }
    
    }
        
    if server_type == "tcp" {
        let listener = TcpListener::bind("192.168.100.52:41000").await?;        //192.168.100.52:41000
        let local_addr = listener.local_addr()?;
        println!("Server listening on: {}", local_addr);
                
        while let Ok((stream, _)) = listener.accept().await {
            println!("New connection: {:?}", stream.peer_addr());  //print incoming client ip address
                    
                    // Spawn a new tokio task to handle each client
            tokio::spawn(async move {
                println!("inside tokyo spawn");
                if let Err(err) = handle_client(stream).await {
                    eprintln!("Error handling client: {}", err); 
                }
            });
        }   
        ()
    }
    println!("The Transportation protocol: {:?} choosen!", server_type);
    Ok(())
}

    /*
    handle_client:
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
    */
