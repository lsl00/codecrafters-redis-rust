#![allow(unused_imports)]
use anyhow::Result;
use resp::Value;
use std::{any, io};
use tokio::{
   io::{AsyncReadExt, AsyncWriteExt},
   net::{TcpListener, TcpStream},
   stream,
};

mod resp;

#[tokio::main]
async fn main() -> io::Result<()> {
   let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

   loop {
      let stream = listener.accept().await;
      match stream {
         Ok((mut stream, _)) => {
            tokio::spawn(async move { handle_conn(stream).await });
         }
         Err(e) => {
            eprintln!("Error {} happened!", e);
         }
      }
   }
   Ok(())
}

async fn handle_conn(mut stream: TcpStream) {
   let mut hanlder = resp::RespHanlder::new(stream);
   let mut storage = std::collections::HashMap::<String,String>::new();
   println!("Starting read loop");

   loop {
      let value = hanlder.read_value().await.unwrap();
      println!("Got value {:?}", value);

      let response = if let Some(v) = value {
         let (command, args) = extract_command(v).unwrap();
         match command.to_ascii_lowercase().as_str() {
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => args.first().unwrap().clone(),
            "set" => set(&mut storage, unpack_bulk_str(args[0].clone()).unwrap() , unpack_bulk_str(args[1].clone()).unwrap()),
            "get" => get(&storage, unpack_bulk_str(args[0].clone()).unwrap()),
            c => panic!("Cannot handle command {}", c),
         }
      } else {
         break;
      };

      println!("Sending value {:?}", response);

      hanlder.write_value(response).await.unwrap();
   }
}

fn get(storage: &std::collections::HashMap<String,String>, key: String) -> Value {
   for (k,v) in storage.iter() {
      println!("{} {}",k,v);
   }
   match storage.get(&key) {
      Some(v) => Value::BulkString(v.to_string()),
      None => Value::Null,
   }
}

fn set(storage: &mut std::collections::HashMap<String,String>, key: String,value: String) -> Value {
   storage.insert(key, value);
   Value::SimpleString("OK".to_string())
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
   match value {
      Value::Array(a) => Ok((
         unpack_bulk_str(a.first().unwrap().clone())?,
         a.into_iter().skip(1).collect(),
      )),
      _ => Err(anyhow::anyhow!("Unexpected command format")),
   }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
   match value {
      Value::BulkString(s) => Ok(s),
      _ => Err(anyhow::anyhow!("Expected command to be a bulk string")),
   }
}
