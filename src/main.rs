use std::io::{Read, Seek};

use riskless::coordinator::default_impl::Index;



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{

    let topic = "example-topic";
    let partition: u64 = 1;

    
    let mut index = std::env::current_dir()?;

    index.push("index");
    index.push(topic);
    index.push(format!("{:0>20}.index", partition.to_string()));


    let mut file = std::fs::File::open(index)?;

    let mut bytes = vec![];

    file.seek(std::io::SeekFrom::Start(0));

    file.read_to_end(&mut bytes);


    println!("file contents: {:#?}", bytes);




    let index = Index::try_from(&bytes[0..28])?;

    println!("{:#?}", index);

    let index = Index::try_from(&bytes[28..56])?;

    println!("{:#?}", index);

    Ok(())
}