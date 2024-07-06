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
use csv::StringRecord;

async fn handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    /*
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
    let mut data_buffer = Vec::new();

    stream.read_to_end(&mut data_buffer).await?;

    let mut reader = csv::Reader::from_reader(data_buffer.as_slice());  //read the data from the buffer

    let mut message_buffer = Writer::from_writer(Vec::new()); //the buffer to write the processed records to

    for content in reader.records() {  // calculate the volume for each record in the received csv file
        let record = content?;
        
        // get the 3 necessary values from the csv record to calculate the volume
        let col1: i32 = record[1].parse()?;     
        let col2: i32 = record[2].parse()?;
        let col3: i32 = record[3].parse()?;
        let object_volume = col1 * col2 * col3;
        
        let mut new_record = record.clone();    // take the original record
        new_record.push_field(&object_volume.to_string()); // create the processed record by adding the volume to the original record
        
        writer.write_record(&new_record)?; //write all records into the message buffer
    }

    let send_message = writer.into_inner()?; // return the bytes of the processed record

    stream.write_all(&send_message).await?; //send the processed message bytes back to client

    stream.flush().await?; // ensure that the entire message is send

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
    let listener = TcpListener::bind("192.168.100.52:41000").await?;
    let local_addr = listener.local_addr()?;
    println!("Server listening on: {}", local_addr);
        
    while let Ok((stream, _)) = listener.accept().await {
        println!("New connection: {:?}", stream.peer_addr());  //print incoming client ip address
            
            // Spawn a new tokio task to handle each client
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream).await {
                eprintln!("Error handling client: {}", err); 
            }
        });
    }
        
    Ok(())
}
