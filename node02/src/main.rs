extern crate csv;
extern crate tokio;

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener,TcpStream};
use csv::{Writer,ReaderBuilder};
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener, RdmaBuilder};
use std::io;

async fn tcp_handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    println!("first line");

    let mut data_buffer = Vec::new();
    stream.read_to_end(&mut data_buffer).await?;  // Write the data from the stream into the data_buffer

    let processed_data = process_data(data_buffer)?;  // Process the data
    println!("Data Processed");

    stream.write_all(&processed_data).await?; // Send the processed message bytes back to client
    stream.flush().await?; // Ensure that the entire message is sent
    stream.shutdown().await?;
    Ok(())
}

async fn rdma_handle_client(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let rdma = RdmaBuilder::default().listen(&addr).await?;
    let mut lmr = rdma.receive_local_mr().await?;

    let lmr_contents = lmr.as_slice().to_vec();

    let processed_data = process_data(lmr_contents);
    println!("Data processed");

    let lmr_contant = lmr.as_slice(); 
    println!("Server received: {:?}", lmr_contant);
    Ok(())
}

fn process_data(data_buffer: Vec<u8>) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut reader = ReaderBuilder::new().has_headers(false).from_reader(data_buffer.as_slice());  // Read the data from the data_buffer
    let mut message_buffer = Writer::from_writer(Vec::new()); // Create the buffer to write the processed records into

    for content in reader.records() {  // Calculate the volume for each record in the received CSV file
        let record = content?;

        // Get the 3 necessary values from the CSV record to calculate the volume
        let col1: i32 = record[1].parse()?;     
        let col2: i32 = record[2].parse()?;
        let col3: i32 = record[3].parse()?;
        let object_volume = col1 * col2 * col3;
        
        let mut new_record = record.clone();  // Take the original record
        new_record.push_field(&object_volume.to_string()); // Create the processed record by adding the volume to the original record
        
        message_buffer.write_record(&new_record)?; // Write the processed records into the message buffer
    }

    let send_message = message_buffer.into_inner()?; // Return the bytes of the processed record
    Ok(send_message)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
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
                if let Err(err) = tcp_handle_client(stream).await {
                    eprintln!("Error handling client: {}", err); 
                }
            });
        }   
        ()
    } else if server_type == "rdma" {
        tokio::spawn(async move {
            if let Err(err) = rdma_handle_client("192.168.100.52:41000".to_string()).await {
                eprintln!("Error handling client: {}", err); 
            }
        }).await?;
    }  
    Ok(())
}