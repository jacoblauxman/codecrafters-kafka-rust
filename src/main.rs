use redis_starter_rust::handle_connection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;

    match listener.accept().await {
        Ok((stream, addr)) => {
            println!("New connection accepted: {}", addr);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream).await {
                    eprintln!("Error handling connection: {e}");
                }
            });
        }
        Err(e) => eprintln!("Error accepting connection: {e}"),
    }

    Ok(())
}
