use std::env;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::TryRecvError::{Disconnected, Empty};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self};

use crate::Message::NewJob;

fn main() {
    let args: Vec<String> = env::args().collect();
    let forwarder = TcpForwarder::parse(&args[..]).unwrap();

    println!("forwarder: {:?}", forwarder);

    forwarder.bind();

    println!("Shutting down.");
}

#[derive(Debug)]
pub struct TcpForwarder {
    local_port: u32,
    remote_host: String,
    remote_port: u32,
}

impl TcpForwarder {
    pub fn parse(args: &[String]) -> Result<TcpForwarder, &'static str> {
        if args.len() != 4 {
            return Err("not enough arguments");
        }

        let local_port = args[1].parse().expect(&format!(
            "arg1 should be local port number, but actual is {}",
            &args[1]
        ));
        let remote_host = args[2].clone();
        let remote_port = args[3].parse().expect(&format!(
            "arg3 should be remote port number, but actual is {}",
            &args[3]
        ));

        Ok(TcpForwarder {
            local_port,
            remote_host,
            remote_port,
        })
    }

    fn local_addr(&self) -> String {
        format!("127.0.0.1:{}", self.local_port)
    }

    fn remote_addr(&self) -> String {
        format!("{}:{}", self.remote_host, self.remote_port)
    }

    pub fn bind(&self) {
        let listener = TcpListener::bind(self.local_addr()).unwrap();

        let pool = ThreadPool::new(10);

        for stream in listener.incoming() {
            let stream = stream.unwrap();

            let forwarder = TcpForwarder {
                local_port: self.local_port,
                remote_host: self.remote_host.clone(),
                remote_port: self.remote_port,
            };
            pool.execute(|| {
                forward(forwarder, stream);
            });
        }
    }
}

enum Mes {
    Data(Vec<u8>),
    EOF,
}

fn forward(forwarder: TcpForwarder, mut in_stream: TcpStream) {
    let (sender_rx, receiver_tx): (Sender<Mes>, Receiver<Mes>) = mpsc::channel();
    let (receiver_rx, sender_tx): (Sender<Mes>, Receiver<Mes>) = mpsc::channel();

    let receiver_thread = thread::spawn(move || {
        let mut out_stream = TcpStream::connect(forwarder.remote_addr()).unwrap();

        let mut is_sender_eof = false;
        let mut is_receiver_eof = false;
        let mut input_buffer = [0; 1024];
        loop {
            out_stream.set_nonblocking(true).unwrap();
            match receiver_tx.try_recv() {
                Result::Ok(input) => match input {
                    Mes::Data(input) => {
                        out_stream.set_nonblocking(false).unwrap();
                        out_stream.write_all(input.as_slice()).unwrap()
                    }
                    Mes::EOF => is_sender_eof = true,
                },
                Result::Err(Empty) => {
                    // no-op
                }
                Result::Err(Disconnected) => {
                    is_sender_eof = true;
                    println!("sender is disconnected");
                }
            };

            let nbytes = match out_stream.read(&mut input_buffer) {
                Ok(nbytes) => nbytes,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    is_receiver_eof = true;
                    continue;
                }
                _ => {
                    is_receiver_eof = true;
                    continue;
                }
            };

            if nbytes == 0 {
                is_receiver_eof = true;
                receiver_rx.send(Mes::EOF);
                continue;
            }

            let data = String::from_utf8_lossy(&input_buffer);
            println!("received from receiver: {}", data);

            receiver_rx
                .send(Mes::Data(Vec::from(input_buffer)))
                .unwrap();

            if is_sender_eof && is_receiver_eof {
                break;
            }

            if nbytes < 1024 {
                is_receiver_eof = true;
                receiver_rx.send(Mes::EOF);
                continue;
            }
        }
    });

    let mut is_sender_eof = false;
    let mut is_receiver_eof = false;
    let mut input_buffer = [0; 1024];
    loop {
        in_stream.set_nonblocking(true).unwrap();
        match sender_tx.try_recv() {
            Result::Ok(input) => match input {
                Mes::Data(input) => {
                    in_stream.set_nonblocking(false).unwrap();
                    in_stream.write_all(input.as_slice()).unwrap()
                }
                Mes::EOF => is_sender_eof = true,
            },
            Result::Err(Empty) => {
                // no-op
            }
            Result::Err(Disconnected) => {
                is_receiver_eof = true;
                println!("receiver is disconnected");
            }
        }

        let nbytes = match in_stream.read(&mut input_buffer) {
            Ok(nbytes) => nbytes,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                is_receiver_eof = true;
                continue;
            }
            _ => {
                is_receiver_eof = true;
                continue;
            }
        };

        if nbytes == 0 {
            is_sender_eof = true;
            sender_rx.send(Mes::EOF);
            continue;
        }

        let data = String::from_utf8_lossy(&input_buffer);
        println!("received from sender: {}", data);

        sender_rx.send(Mes::Data(Vec::from(input_buffer))).unwrap();

        if is_sender_eof && is_receiver_eof {
            break;
        }

        if nbytes < 1024 {
            is_sender_eof = true;
            sender_rx.send(Mes::EOF);
            continue;
        }
    }

    receiver_thread.join().unwrap();
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

enum Message {
    NewJob(Job),
    Terminate,
}

type Job = Box<dyn FnBox + Send + 'static>;

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.send(NewJob(job)).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    println!("Worker {} got a job; executing.", id);

                    job.call_box();
                }
                Message::Terminate => {
                    println!("Worker {} was told to terminate.", id);

                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
