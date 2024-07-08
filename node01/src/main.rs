extern crate csv;

use csv::{StringRecord, ReaderBuilder};
use std::error::Error;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    fs::File,
    alloc::Layout,
    io::{self, Error as IOError, ErrorKind, Write},
    net::SocketAddrV4,
    time::{Instant, Duration},
};
use tokio::{
    fs::File as OtherFile,  // Import Tokio's File here
    io::{AsyncWriteExt,AsyncReadExt, AsyncBufReadExt, BufReader},
    net::TcpStream,
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

/*
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
            let mut temp = String::new();
            for line in reader.records() {
                if index < 501 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";");
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string.clone();
                    index = 0;
                }
            }
            result.push(temp);
        },
        "3" => {
            let mut index = 0;
            let mut temp = String::new();
            for line in reader.records() {
                if index < 1001 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";");
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string.clone();
                    index = 0;
                }
            }
            result.push(temp);
        },
        "4" => {
            let mut index = 0;
            let mut temp = String::new();
            for line in reader.records() {
                if index < 1251 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";");
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string.clone();
                    index = 0;
                }
            }
            result.push(temp);
        },
        "5" => {
            let mut index = 0;
            let mut temp = String::new();
            for line in reader.records() {
                if index < 2501 {
                    let record = line?;
                    let string = record.iter().collect::<Vec<_>>().join(";");
                    temp = temp + &string;
                    index += 1;
                } else {
                    result.push(temp);
                    temp = reset_string.clone();
                    index = 0;
                }
            }
            result.push(temp);
        },
        "6" => {
            let mut temp = String::new();
            for line in reader.records() {
                let record = line?;
                let string = record.iter().collect::<Vec<_>>().join(";");
                temp = temp + &string;
            }
            result.push(temp);
        },
        _ => {

        }
    }
    Ok(result)
}
*/

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

fn valid_size(size: &str) -> bool {
    println!("valid_size");
    matches!(size, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" | "12" | "13" | "14" | "15" | "16" | "17" | "18" | "19" | "20" | "21" | "22" | "23" | "24" | "25") 
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
    println!("past shutdown of writer");

    let mut server_response = Vec::new();
    reader.read_to_end(&mut server_response).await?;
    let response_str = String::from_utf8_lossy(&server_response);
    println!("Received the following response form the server: {}", response_str);
    let elapsed_time = start_time.elapsed();
    println!("Time needed: {:?} ms", elapsed_time.as_millis());
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

async fn handle_tcp_protocol() -> Result<(), Box<dyn Error>> {
    loop {
        println!("Please select one of these message sizes:");
        println!("1: 100kb");
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
    loop {
        println!("Please choose which RDMA transmission Type you want to use:");
        println!("send, write or atomic");

        let mut rdma_type = String::new();
        io::stdin().read_line(&mut rdma_type)?;

        let rdma_type = rdma_type.trim();

        match rdma_type {
            "send" => {
                let data = read_file()?;
                for tupel in data {
                    println!("{:?}", tupel);
                }
                break;
            }
            "write" => {
                let addr = std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(192, 168, 100, 51), pick_unused_port().unwrap());
                std::thread::spawn(move || server(addr));
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                client_rdma(addr, rdma_type).await.map_err(|err| println!("{}", err)).unwrap();
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

/*
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
                println!("Please choose which RDMA transmission Type you want to use:");
                println!("SEND, write or atomic");

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
            loop {
                println!("Please select one of these message sizes:");
                println!("1: 100kb");
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
                
                let mut size = String::new();
                io::stdin().read_line(&mut size).expect("failed to read");
                let size_selected = size.trim().to_string();

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
                }
            } lese {

            }

        } else {
            println!("Protocol: {:?} does not exist!", protocol);
        }
    }
    Ok(())
}
*/
