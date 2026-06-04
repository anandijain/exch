use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

#[test]
fn websocket_order_and_feed_cycle() {
    let order_addr = "127.0.0.1:17111";
    let feed_addr = "127.0.0.1:17112";
    let mut server = spawn_server(order_addr, feed_addr);

    let result = run_cycle(order_addr, feed_addr);
    stop_server(&mut server);

    let (sell_ack, feed_event_1, buy_ack, feed_event_2, replay_event_1, replay_event_2, revenue) =
        result;
    assert!(sell_ack.contains("accepted:1:order=1"));
    assert!(sell_ack.contains("rested:2:order=1"));
    assert!(feed_event_1.contains("rested:2:order=1"));
    assert!(buy_ack.contains("accepted:3:order=2"));
    assert!(buy_ack.contains("executed:4:resting=1:aggressing=2"));
    assert!(feed_event_2.contains("executed:4:resting=1:aggressing=2"));
    assert!(replay_event_1.contains("rested:2:order=1"));
    assert!(replay_event_2.contains("executed:4:resting=1:aggressing=2"));
    assert!(revenue.contains("ok revenue asset=USD amount=10"));
}

fn spawn_server(order_addr: &str, feed_addr: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_exchange_ws"))
        .env("EXCH_WS_ORDER_ADDR", order_addr)
        .env("EXCH_WS_FEED_ADDR", feed_addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn exchange_ws")
}

fn run_cycle(
    order_addr: &str,
    feed_addr: &str,
) -> (String, String, String, String, String, String, String) {
    let mut feed = connect_with_retry(&format!("ws://{feed_addr}"));
    assert!(read_text(&mut feed).contains("exch-ws-feed"));
    feed.send(Message::Text("subscribe 0 10".to_string()))
        .expect("send feed subscribe");
    assert!(read_text(&mut feed).contains("ok subscribed"));
    assert!(read_text(&mut feed).contains("snapshot instrument=0"));

    let mut order = connect_with_retry(&format!("ws://{order_addr}"));
    assert!(read_text(&mut order).contains("exch-ws-order"));
    order
        .send(Message::Text("auth dev-key-100".to_string()))
        .expect("send auth");
    assert!(read_text(&mut order).contains("ok auth account=100"));

    order
        .send(Message::Text("order 0 1 sell 10000 25".to_string()))
        .expect("send sell");
    let sell_ack = read_text(&mut order);
    let feed_event_1 = read_text(&mut feed);

    order
        .send(Message::Text("auth dev-key-101".to_string()))
        .expect("send auth 101");
    assert!(read_text(&mut order).contains("ok auth account=101"));
    order
        .send(Message::Text("order 0 2 buy 10000 10".to_string()))
        .expect("send buy");
    let buy_ack = read_text(&mut order);
    let feed_event_2 = read_text(&mut feed);

    let mut replay = connect_with_retry(&format!("ws://{feed_addr}"));
    assert!(read_text(&mut replay).contains("exch-ws-feed"));
    replay
        .send(Message::Text("replay 0 0".to_string()))
        .expect("send replay");
    assert!(read_text(&mut replay).contains("ok replay"));
    let replay_event_1 = read_text(&mut replay);
    let replay_event_2 = read_text(&mut replay);

    order
        .send(Message::Text("revenue USD".to_string()))
        .expect("send revenue");
    let revenue = read_text(&mut order);

    (
        sell_ack,
        feed_event_1,
        buy_ack,
        feed_event_2,
        replay_event_1,
        replay_event_2,
        revenue,
    )
}

fn connect_with_retry(url: &str) -> WebSocket<MaybeTlsStream<std::net::TcpStream>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match connect(url) {
            Ok((socket, _)) => return socket,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("connect {url}: {error}"),
        }
    }
}

fn read_text(socket: &mut WebSocket<MaybeTlsStream<std::net::TcpStream>>) -> String {
    socket
        .read()
        .expect("read websocket message")
        .into_text()
        .expect("text websocket message")
}

fn stop_server(server: &mut Child) {
    let _ = server.kill();
    let _ = server.wait();
}
