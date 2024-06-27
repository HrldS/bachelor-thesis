extern crate csv;

use std::error::Error;
use std::fs::File;
use async_rdma::Rdma;
use std::io::{self, Write};

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

fn main() -> Result<(), Box<dyn Error>>{

    let mut line = String::new();
    println!("Enter transportation Protol:");
    println!("Protocols available: rdma or tcp");
    let input = io::stdin().read_line(&mut line).unwrap();

    if input == "rdma" {
        let data = read_file()?;

        for tupel in data {
            println!("{:?}", tupel);
        }

    } else {
        println!("TCP FOUND");
    }

    Ok(())
}
