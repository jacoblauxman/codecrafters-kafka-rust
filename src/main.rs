use redis_starter_rust::handle_connection;
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                if let Err(e) = handle_connection(stream) {
                    eprintln!("Error handling connection: {e}");
                };
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
