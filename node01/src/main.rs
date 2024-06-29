extern crate csv;

use std::error::Error;
use std::fs::File;
use async_rdma::{Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration 
};

fn read_file() -> Result<Vec<(String, i32, i32, i32)>, Box<dyn Error>>{
    let file = File::open("src/data/test_data.csv")?;  //? try reading file
    let mut contant = csv::ReaderBuilder::new().has_headers(false).delimiter(b';').from_reader(file); // Disable headers assumption not not skip first row

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

async fn client(addr: SocketAddrV4) -> io::Result<()> {
    let _rdma = Rdma::connect(addr, 1, 1, 512).await?;
    Ok(())
}

#[tokio::main]
async fn server(addr: SocketAddrV4) -> io::Result<()> {
    let rdma_listener = RdmaListener::bind(addr).await?;
    let _rdma = rdma_listener.accept(1, 1, 512).await?;
    // run here after client connect
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{

    let mut input = String::new();
    println!("Enter transportation Protol:");
    println!("Protocols available: 'rdma' or 'tcp'");

    io::stdin().read_line(&mut input).expect("failed to read");

    let input = input.trim();

    if input == "rdma" {
        let data = read_file()?;

        for tupel in data {
            println!("{:?}", tupel);
        }

    } else {
        println!("{}",input);
    }

    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    std::thread::spawn(move || server(addr));
    tokio::time::sleep(Duration::from_secs(3)).await;
    client(addr).await.map_err(|err| println!("{}", err)).unwrap();

    Ok(())
}
