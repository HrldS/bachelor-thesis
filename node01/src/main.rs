extern crate csv;

use csv::{StringRecord, ReaderBuilder};
use std::error::Error;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener, RdmaBuilder};
use portpicker::pick_unused_port;
use std::{
    fs::File,
    alloc::Layout,
    io::{self, Error as IOError, ErrorKind, Write,Read},
    net::SocketAddrV4,
    time::{Instant, Duration},
};
use tokio::{
    fs::File as OtherFile,  // Import Tokio's File here
    io::{AsyncWriteExt,AsyncReadExt, AsyncBufReadExt, BufReader},
    net::TcpStream,
};
use std::ptr;

fn valid_size(size: &str) -> bool {
    println!("valid_size");
    matches!(size, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" | "12" | "13" | "14" | "15" | "16" | "17" | "18" | "19" | "20" | "21" | "22" | "23" | "24" | "25") 
}

fn message_size_info() {
    println!("Please select one of these message sizes:");
    println!("1: 100kb size");
    println!("2: 200kb size");
    println!("3: 500kb size");
    println!("4: 1MB size");
    println!("5: 2MB size");
    println!("6: 3.5MB size");
    println!("7: 4.5MB size");
    println!("8: 5.5MB size");
    println!("9: 6MB size");
    println!("10: 7MB size");
    println!("11: 8MB size");
    println!("12: 8.5MB size");
    println!("13: 9MB size");
    println!("14: 9.5MB size");
    println!("15: 10.5MB size");
    println!("16: 11.5MB size");
    println!("17: 12MB size");
    println!("18: 13MB size");
    println!("19: 14MB size");
    println!("20: 15MB size");
    println!("21: 16MB size");
    println!("22: 17MB size");
    println!("23: 18MB size");
    println!("24: 19MB size");
    println!("25: 20MB size");
}

async fn data_path(size: &str) -> Result<String, Box<dyn Error>> {
    let file_path = match size {
        "1" => "src/data/test_data_100kb.csv",
        "2" => "src/data/test_data_200kb.csv",
        "3" => "src/data/test_data_500kb.csv",
        "4" => "src/data/test_data_1mb.csv",
        "5" => "src/data/test_data_2mb.csv",
        "6" => "src/data/test_data_3,5mb.csv",
        "7" => "src/data/test_data_4,5mb.csv",
        "8" => "src/data/test_data_5,5mb.csv",
        "9" => "src/data/test_data_6mb.csv",
        "10" => "src/data/test_data_7mb.csv",
        "11" => "src/data/test_data_8mb.csv",
        "12" => "src/data/test_data_8,5mb.csv",
        "13" => "src/data/test_data_9mb.csv",
        "14" => "src/data/test_data_9,5mb.csv",
        "15" => "src/data/test_data_10,5mb.csv",
        "16" => "src/data/test_data_11,5mb.csv",
        "17" => "src/data/test_data_12mb.csv",
        "18" => "src/data/test_data_13mb.csv",
        "19" => "src/data/test_data_14mb.csv",
        "20" => "src/data/test_data_15mb.csv",
        "21" => "src/data/test_data_16mb.csv",
        "22" => "src/data/test_data_17mb.csv",
        "23" => "src/data/test_data_18mb.csv",
        "24" => "src/data/test_data_19mb.csv",
        "25" => "src/data/test_data_20mb.csv",
        _ => return Err("Invalid size selected".into()),
    };

    Ok(file_path.to_string())
}

async fn client_rdma(addr: &str, rdma_type: &str, size: &str) -> io::Result<()> {

    if rdma_type == "write" {
        let start_time = Instant::now();
        let rdma = RdmaBuilder::default().connect(addr).await?;

        let file_path = match data_path(size).await {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error: {}", e);
                return Ok(()); 
            }
        };
    
        let mut file = File::open(&file_path)?;  //? try reading file
        let file_size = file.metadata()?.len() as usize;
        
        let mut file_data = Vec::with_capacity(file_size);
        file.read_to_end(&mut file_data)?;

        let layout = Layout::from_size_align(file_size, std::mem::align_of::<u8>()).expect("Failed to create layout");

        let mut lmr = rdma.alloc_local_mr(layout)?;
        let mut rmr = rdma.request_remote_mr(layout).await?;

        let _num = lmr.as_mut_slice().write(&file_data)?;

        rdma.write(&lmr, &mut rmr).await?;

        rdma.send_remote_mr(rmr).await?;

    //server response

        let mut server_response = rdma.receive_local_mr().await?;
        let lmr_contents = server_response.as_slice().to_vec();

        println!("Contents of lmr_contents as string: {:?}", String::from_utf8_lossy(&lmr_contents));
        let elapsed_time = start_time.elapsed();
        println!("Time needed: {:?} ms", elapsed_time.as_millis());

    } else if rdma_type == "send" {

        let start_time = Instant::now();
        let rdma = RdmaBuilder::default().connect(addr).await?;

        let file_path = match data_path(size).await {
            Ok(data) => data,
            Err(e) => {
                eprintln!("Error: {}", e);
                return Ok(()); 
            }
        };

        let mut file = File::open(&file_path)?;  //? try reading file
        let file_size = file.metadata()?.len() as usize;
        
        let mut file_data = Vec::with_capacity(file_size);
        file.read_to_end(&mut file_data)?;

        let layout = Layout::from_size_align(file_size, std::mem::align_of::<u8>()).expect("Failed to create layout");

        let mut lmr = rdma.alloc_local_mr(layout)?;

        //let _num = lmr.as_mut_slice().write(&file_data)?;

        unsafe {
            let lmr_ptr = lmr.as_mut_ptr();
            ptr::copy_nonoverlapping(file_data.as_ptr(), *lmr_ptr, file_size);
        }

        println!("Received data: {} bytes", lmr.length());
        rdma.send(&lmr).await?;

        // server response
        let server_response = rdma.receive().await?;
        let response_contents = server_response.as_slice().to_vec();

        println!("Contents of response_contents as string: {:?}", String::from_utf8_lossy(&response_contents));
        let elapsed_time = start_time.elapsed();
        println!("Time needed: {:?} ms", elapsed_time.as_millis());
        
    } else {
        println!("not write");
    }

    Ok(())
}

async fn client_tcp(size: &str) -> io::Result<()> {
    let start_time = Instant::now();
    let stream = TcpStream::connect("192.168.100.52:41000").await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let file_path = match data_path(size).await {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error: {}", e);
            return Ok(()); 
        }
    };

    println!("Debug: filepath: {:?}", file_path);
    let mut file = OtherFile::open(&file_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    writer.write_all(&buffer).await?;
    writer.flush().await?;

    writer.shutdown().await?;

    let mut server_response = Vec::new();
    reader.read_to_end(&mut server_response).await?;

    let response_str = String::from_utf8_lossy(&server_response);
    println!("Received the following response form the server: {}", response_str);
    let elapsed_time = start_time.elapsed();
    println!("Time needed: {:?} ms", elapsed_time.as_millis());
    Ok(())
}

async fn handle_tcp_protocol() -> Result<(), Box<dyn Error>> {
    loop {
        
        message_size_info();
        let mut size_selected = String::new();
        io::stdin().read_line(&mut size_selected)?;
        let size_selected = size_selected.trim().to_string();

        if valid_size(&size_selected) {
            let handle = tokio::spawn(async move {
                client_tcp(&size_selected).await.unwrap_or_else(|err| {
                    eprintln!("Client error: {:?}", err);
                });
            });

            handle.await.unwrap_or_else(|err| {
                eprintln!("Handle error: {:?}", err);
            });

            println!("Worker has finished");
            break;
        } else {
            println!("Invalid size option: {:?}", size_selected);
        }
    }
    Ok(())
}

async fn handle_rdma_protocol() -> Result<(), Box<dyn Error>> {
    let mut size = String::new();
    loop {
        message_size_info();
        let mut size_selected = String::new();
        io::stdin().read_line(&mut size_selected)?;
        let size_selected = size_selected.trim().to_string();

        if valid_size(&size_selected) {
            size = size_selected;
            break;
        } else {
            println!("Invalid size option: {:?}", size_selected);
        }
    }
    loop {
        println!("Please choose which RDMA transmission Type you want to use:");
        println!("send, write or atomic");

        let mut rdma_type = String::new();
        io::stdin().read_line(&mut rdma_type)?;

        let rdma_type = rdma_type.trim();

        match rdma_type {
            "send" => {
                let addr = "192.168.100.52:41000";
                client_rdma(addr, rdma_type, &size).await.map_err(|err| println!("{}", err)).unwrap();
                break;
            }
            "write" => {
                let addr = "192.168.100.52:41000";
                client_rdma(addr, rdma_type, &size).await.map_err(|err| println!("{}", err)).unwrap();
                break;
            }
            "atomic" => {
                println!("{:?}", rdma_type);
                break;
            }
            _ => {
                println!("Transmission type: '{}' does not exist!", rdma_type);
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    loop {
        println!("Hallo! This programm tests the latency and bandwidth of the TCP and RDMA transport protocol by sending the content of a csv file to a server for processing.");
        println!("Afterwards the server sends the processed data back to you.");
        println!("");
        println!("Please enter the transportation Protocol you want to test:");
        println!("Protocols available: 'rdma' or 'tcp'");

        let mut protocol = String::new();
        io::stdin().read_line(&mut protocol)?;
        let protocol = protocol.trim();

        match protocol {
            "rdma" => {
                handle_rdma_protocol().await?;
                break;
            }
            "tcp" => {
                handle_tcp_protocol().await?;
                break;
            }
            _ => {
                println!("Protocol '{}' does not exist!", protocol);
            }
        }
    }

    Ok(())
}