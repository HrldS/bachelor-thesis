extern crate csv;
extern crate tokio;

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener,TcpStream};
use csv::{Writer,ReaderBuilder};
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder, MrAccess};
use std::{
    alloc::Layout,
    io::{Write},
    io,
};

async fn tcp_handle_client(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut data_buffer = Vec::new();
    stream.read_to_end(&mut data_buffer).await?;  // Write the data from the stream into the data_buffer

    let processed_data = process_data(data_buffer)?;  // Process the data

    stream.write_all(&processed_data).await?; // Send the processed message bytes back to client
    stream.flush().await?; // Ensure that the entire message is sent
    stream.shutdown().await?;
    
    Ok(())
}

async fn rdma_send_handle_client(addr: String) -> Result<(), Box<dyn std::error::Error>> { //addr: String
    let rdma = RdmaBuilder::default().listen(addr).await?;
    
    let message = rdma.receive_remote_mr().await?;
    let data_size = message.length();

    println!("Size: {}", data_size);
    
    let layout = Layout::from_size_align(data_size, std::mem::align_of::<u8>()).expect("Failed to create layout");
    let mut lmr = rdma.alloc_local_mr(layout)?;

    rdma.read(&mut lmr, &message).await?;

    let message_contents = lmr.as_slice().to_vec();

    let processed_data = match process_data(message_contents) {  
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error processing data: {}", e);
            return Err(e.into());
        }
    };

    let layout = Layout::from_size_align(processed_data.len(), std::mem::align_of::<u8>()).expect("Failed to create layout");

    let mut lmr = rdma.alloc_local_mr(layout)?;

    let _num = lmr.as_mut_slice().write(&processed_data)?;

    println!("Size: {}", lmr.length());

    rdma.send_local_mr(lmr).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

async fn rdma_write_handle_client(addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let rdma = RdmaBuilder::default().listen(&addr).await?;
    let lmr = rdma.receive_local_mr().await?; //mut

    let data_size = lmr.length();
    println!("Size: {}", data_size);

    let lmr_contents = lmr.as_slice().to_vec();

    let processed_data = match process_data(lmr_contents) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error processing data: {}", e);
            return Err(e.into());
        }
    };

//send back
    let layout = Layout::from_size_align(processed_data.len(), std::mem::align_of::<u8>()).expect("Failed to create layout");

    let mut lmr_response = rdma.alloc_local_mr(layout)?;
    let mut rmr_response = rdma.request_remote_mr(layout).await?;

    let _num = lmr_response.as_mut_slice().copy_from_slice(&processed_data);
    rdma.write(&lmr_response, &mut rmr_response).await?;

    println!("Size: {}", rmr_response.length());

    rdma.send_remote_mr(rmr_response).await?;

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
        println!("Please select one of these Transportation protocol types: rdma_write, rdma_send or tcp");
        io::stdin().read_line(&mut input).expect("failed to read server_type");

        server_type = input.trim().to_string();
    
        if server_type == "rdma_write" || server_type == "tcp" || server_type == "rdma_send" {
            break;
        } else {
            println!("The Transportation protocol: {:?} is not supported", server_type);
        }
    }
        
    if server_type == "tcp" {
        let listener = TcpListener::bind("192.168.100.52:41000").await?;       
        let local_addr = listener.local_addr()?;
        println!("Server listening on: {}", local_addr);
                
        while let Ok((stream, _)) = listener.accept().await {
                    
            tokio::spawn(async move {
                if let Err(err) = tcp_handle_client(stream).await {
                    eprintln!("Error handling client: {}", err); 
                }
            });
        }   
        ()
    } else if server_type == "rdma_write" {
        tokio::spawn(async move {
            if let Err(err) = rdma_write_handle_client("192.168.100.52:41000".to_string()).await {
                eprintln!("Error handling client: {}", err); 
            }
        }).await?;
    } else {
        tokio::spawn(async move {
            if let Err(err) = rdma_send_handle_client("192.168.100.52:41000".to_string()).await {
                eprintln!("Error handling client: {}", err); 
            }
        }).await?;
    }
    Ok(())
}