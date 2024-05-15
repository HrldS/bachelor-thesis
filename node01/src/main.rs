extern crate csv;

use std::error::Error;
use std::fs::File;

fn read_file() -> Result<(), Box<dyn Error>>{
    let file = File::open("src/data/test_data.csv")?;  //? try reading file
    let mut contant = csv::ReaderBuilder::new().has_headers(false).from_reader(file); // Disable headers assumption not not skip first row

    for tupel in contant.records() {
        let record = tupel?;

        for value in record.iter() {
            print!("{} ", value);
        }
        println!();
    }
    Ok(())
}

fn main() {
   let _ = read_file();
}
