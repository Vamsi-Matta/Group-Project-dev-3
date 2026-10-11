use chrono::Local;
use std::io::{self, Write};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug)]
struct User {
    id: u32,
    username: String,
}

#[derive(Clone, Debug)]
struct Message {
    id: u32,
    sender_id: u32,
    sender_name: String,
    receiver_id: u32,
    receiver_name: String,
    content: String,
    timestamp: String,
}

enum ChatCommand {
    Send(Message),
    History(oneshot::Sender<Vec<Message>>),
    FilterUser(u32, oneshot::Sender<Vec<Message>>),
    SearchKeyword(String, oneshot::Sender<Vec<Message>>),
    Shutdown,
}

struct ChatServer {
    history: Vec<Message>,
}

impl ChatServer {
    fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }

    fn run(&mut self, mut rx: mpsc::Receiver<ChatCommand>) {
        while let Some(command) = rx.blocking_recv() {
            match command {
                ChatCommand::Send(message) => {
                    println!(
                        "\n[SERVER] {} -> {}: {}",
                        message.sender_name,
                        message.receiver_name,
                        message.content
                    );

                    self.history.push(message);
                }

                ChatCommand::History(reply) => {
                    let _ = reply.send(self.history.clone());
                }

                ChatCommand::FilterUser(user_id, reply) => {
                    let result = self
                        .history
                        .iter()
                        .filter(|message| {
                            message.sender_id == user_id
                                || message.receiver_id == user_id
                        })
                        .cloned()
                        .collect();

                    let _ = reply.send(result);
                }

                ChatCommand::SearchKeyword(keyword, reply) => {
                    let keyword = keyword.to_lowercase();

                    let result = self
                        .history
                        .iter()
                        .filter(|message| {
                            message
                                .content
                                .to_lowercase()
                                .contains(&keyword)
                        })
                        .cloned()
                        .collect();

                    let _ = reply.send(result);
                }

                ChatCommand::Shutdown => {
                    println!("\n[SERVER] Chat server shutting down.");
                    break;
                }
            }
        }
    }
}

fn input(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().unwrap();

    let mut value = String::new();

    io::stdin()
        .read_line(&mut value)
        .expect("Input error");

    value.trim().to_string()
}

fn display_users(users: &[User]) {
    println!("\nAvailable Users:");

    for user in users {
        println!("{} - {}", user.id, user.username);
    }
}

fn display_messages(messages: &[Message]) {
    if messages.is_empty() {
        println!("No messages found.");
        return;
    }

    println!("\n---------------- MESSAGE HISTORY ----------------");

    for message in messages {
        println!(
            "[{}] #{} {} ({}) -> {} ({}): {}",
            message.timestamp,
            message.id,
            message.sender_name,
            message.sender_id,
            message.receiver_name,
            message.receiver_id,
            message.content
        );
    }

    println!("--------------------------------------------------");
}

fn find_user(users: &[User], id: u32) -> Option<User> {
    users.iter().find(|user| user.id == id).cloned()
}

#[tokio::main]
async fn main() {
    println!("==========================================");
    println!("       RUST TEXT-BASED CHAT APP");
    println!("==========================================");

    let users = vec![
        User {
            id: 1,
            username: "Alice".to_string(),
        },
        User {
            id: 2,
            username: "Bob".to_string(),
        },
        User {
            id: 3,
            username: "Charlie".to_string(),
        },
    ];

    let (tx, rx) = mpsc::channel::<ChatCommand>(100);

    let server_thread = std::thread::spawn(move || {
        let mut server = ChatServer::new();
        server.run(rx);
        server
    });

    let mut message_id = 1;

    loop {
        println!("\n========== MENU ==========");
        println!("1. View users");
        println!("2. Send message");
        println!("3. View message history");
        println!("4. Filter by user");
        println!("5. Search by keyword");
        println!("6. Exit");
        println!("==========================");

        let choice = input("Enter choice: ");

        match choice.as_str() {
            "1" => {
                display_users(&users);
            }

            "2" => {
                display_users(&users);

                let sender_id = match input("Sender ID: ").parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        println!("Invalid sender ID.");
                        continue;
                    }
                };

                let receiver_id = match input("Receiver ID: ").parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        println!("Invalid receiver ID.");
                        continue;
                    }
                };

                let sender = match find_user(&users, sender_id) {
                    Some(user) => user,
                    None => {
                        println!("Sender does not exist.");
                        continue;
                    }
                };

                let receiver = match find_user(&users, receiver_id) {
                    Some(user) => user,
                    None => {
                        println!("Receiver does not exist.");
                        continue;
                    }
                };

                let content = input("Message: ");

                if content.is_empty() {
                    println!("Message cannot be empty.");
                    continue;
                }

                let message = Message {
                    id: message_id,
                    sender_id: sender.id,
                    sender_name: sender.username,
                    receiver_id: receiver.id,
                    receiver_name: receiver.username,
                    content,
                    timestamp: Local::now()
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string(),
                };

                if tx.send(ChatCommand::Send(message)).await.is_ok() {
                    println!("Message delivered to chat server.");
                    message_id += 1;
                } else {
                    println!("Unable to send message.");
                }
            }

            "3" => {
                let (reply_tx, reply_rx) = oneshot::channel();

                tx.send(ChatCommand::History(reply_tx))
                    .await
                    .unwrap();

                match reply_rx.await {
                    Ok(messages) => display_messages(&messages),
                    Err(_) => println!("Unable to retrieve history."),
                }
            }

            "4" => {
                let user_id = match input("User ID: ").parse::<u32>() {
                    Ok(id) => id,
                    Err(_) => {
                        println!("Invalid user ID.");
                        continue;
                    }
                };

                let (reply_tx, reply_rx) = oneshot::channel();

                tx.send(ChatCommand::FilterUser(user_id, reply_tx))
                    .await
                    .unwrap();

                match reply_rx.await {
                    Ok(messages) => display_messages(&messages),
                    Err(_) => println!("Unable to filter messages."),
                }
            }

            "5" => {
                let keyword = input("Keyword: ");

                let (reply_tx, reply_rx) = oneshot::channel();

                tx.send(ChatCommand::SearchKeyword(keyword, reply_tx))
                    .await
                    .unwrap();

                match reply_rx.await {
                    Ok(messages) => display_messages(&messages),
                    Err(_) => println!("Unable to search messages."),
                }
            }

            "6" => {
                tx.send(ChatCommand::Shutdown).await.unwrap();

                let _server = server_thread.join().unwrap();

                println!("\nChat application closed.");
                break;
            }

            _ => {
                println!("Invalid choice.");
            }
        }
    }
}
