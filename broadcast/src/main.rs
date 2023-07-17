use std::io::BufRead;
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use uuid::Uuid;
use std::collections::HashMap;


#[derive(Debug, Serialize, Deserialize)]
struct RequestMessage {
    src: String,
    dest: String,
    body: RequestBody,
}

#[derive(Debug, Serialize, Deserialize)]
struct RequestBody {
    #[serde(rename = "type")]
    type_field: String,
    msg_id: Option<usize>,
    echo: Option<String>,
    message: Option<i32>,
    topology: Topology,
}

type Other = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Serialize, Deserialize)]
struct Topology {
    #[serde(flatten)]
    other: Other,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseBody {
    #[serde(rename = "type")]
    type_field: String,
    id: Option<String>,
    in_reply_to: Option<usize>,
    messages: Option<std::vec::Vec<i32>>,
}


#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    
    let mut messages_list: std::vec::Vec<i32> = Vec::new();

    let  _: HashMap<String, std::vec::Vec<String>> = HashMap::new();

    tokio::spawn(async move {
        let stdin = std::io::stdin();
        let reader = std::io::BufReader::new(stdin);
        let mut lines = reader.lines();
        while let Some(line) = lines.next() {
            let msg: RequestMessage = serde_json::from_str(&line.unwrap()).expect("wrong format of the request message");
            let _ = tx.send(msg).await;
        }
    });


    while let Some(req_msg) = rx.recv().await {
        let response_body: ResponseBody;

        match req_msg.body.type_field.as_str() {
            "echo" => {
                response_body = ResponseBody { 
                    type_field: "echo_ok".to_string(),
                    id: None,
                    in_reply_to: None,
                    messages: None,
                }
            },
            "init" => {
                response_body = ResponseBody { 
                    type_field: "init_ok".to_string(),
                    id: None,
                    in_reply_to: req_msg.body.msg_id,
                    messages: None,
                }
            },
            "generate" => {
                let uid = Uuid::new_v4();
                //println!("{}", uid);

                response_body = ResponseBody {
                    type_field: "generate_ok".to_string(),
                    id: Some(uid.to_string()),
                    in_reply_to: req_msg.body.msg_id,
                    messages: None,
                }
            },
            "broadcast" => {
                if let Some(val) = req_msg.body.message {
                    messages_list.push(val);
                }

                response_body = ResponseBody {
                    type_field: "broadcast_ok".to_string(),
                    id: None,
                    in_reply_to: req_msg.body.msg_id,
                    messages: None,
                }
            },
            "read" => {
                response_body = ResponseBody {
                    type_field: "read_ok".to_string(),
                    id: None,
                    in_reply_to: req_msg.body.msg_id,
                    messages: Some(messages_list.clone()),
                }
            },
            "topology" => {

                for (key, value) in &req_msg.body.topology.other {
                    let test = serde_json::to_string(&value).unwrap();
                    println!("{:?}" , test);
                }
                  
                response_body = ResponseBody {
                    type_field: "topology_ok".to_string(),
                    id: None,
                    in_reply_to: req_msg.body.msg_id,
                    messages: None,
                }
            },
            _ => {
                panic!("wrong message type");
            }
        }

        let response = ResponseMessage {
            src: req_msg.dest,
            dest: req_msg.src,
            body: response_body,
        };

        let response_str = serde_json::to_string(&response).expect("wrong format of response message");
        println!("{}", response_str);
    }

}
