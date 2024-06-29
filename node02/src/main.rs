use async_rdma::{Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};


#[tokio::main]
async fn server(addr: SocketAddrV4) -> io::Result<()> {
    let rdma_listener = RdmaListener::bind(addr).await?;
    let _rdma = rdma_listener.accept(1, 1, 512).await?;

    Ok(())
}

fn main() {
    let addr = SocketAddrV4::new(Ipv4Addr::new(192, 168, 100, 51), pick_unused_port().unwrap());
    server(addr);
    println!("Hello, world!");
}
