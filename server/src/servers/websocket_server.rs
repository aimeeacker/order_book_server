use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    types::{
        L2Book, L4Book, L4BookLiteDepthUpdate, L4BookLiteSnapshot, L4BookLiteUpdates, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ChannelResponse, ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    sync::Arc,
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

#[derive(Serialize)]
#[serde(tag = "method", rename_all = "camelCase")]
enum SubscriptionResponseData {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
}

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);
    let active_symbols = Arc::new(Mutex::new(HashMap::<String, usize>::new()));

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot, active_symbols.clone())
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let app = Router::new().route(
        "/ws",
        get({
            let internal_message_tx = internal_message_tx.clone();
            let active_symbols = active_symbols.clone();
            async move |ws_upgrade| {
                ws_handler(
                    ws_upgrade,
                    internal_message_tx.clone(),
                    listener.clone(),
                    ignore_spot,
                    websocket_opts,
                    active_symbols.clone(),
                )
            }
        }),
    );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, ignore_spot, active_symbols).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !listener.lock().await.is_ready() {
        loop {
            match internal_message_rx.recv().await {
                Ok(msg) => {
                    if let InternalMessage::Snapshot { l2_snapshots, .. } = msg.as_ref() {
                        universe = new_universe(l2_snapshots, ignore_spot);
                        break;
                    }
                }
                Err(err) => {
                    error!("Receiver error: {err}");
                    prune_active_symbols(&mut manager, &active_symbols).await;
                    return;
                }
            }
        }
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                }
                            },
                            InternalMessage::L4Snapshot { snapshot } => {
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_l4_snapshot(&mut socket, sub, snapshot).await;
                                }
                            }
                            InternalMessage::L4BookLiteUpdates { updates } => {
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_l4_book_lite_updates(&mut socket, sub, updates).await;
                                }
                            }
                            InternalMessage::L4BookLiteDepthUpdates { updates } => {
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_l4_book_lite_depth_updates(&mut socket, sub, updates).await;
                                }
                            }
                        }

                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        prune_active_symbols(&mut manager, &active_symbols).await;
                        return;
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    prune_active_symbols(&mut manager, &active_symbols).await;
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(
                                    &mut socket,
                                    &mut manager,
                                    value,
                                    &universe,
                                    listener.clone(),
                                    active_symbols.clone(),
                                )
                                .await;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            prune_active_symbols(&mut manager, &active_symbols).await;
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    prune_active_symbols(&mut manager, &active_symbols).await;
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription, .. }
        | ClientMessage::Subscribe { subscription, .. }
        | ClientMessage::GetSnapshot { subscription, .. } => subscription.clone(),
    };
    let req_id = client_message.req_id();
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_error_response(socket, req_id, msg).await;
        return;
    }
    if let ClientMessage::GetSnapshot { .. } = &client_message {
        if let Subscription::L4Book { coin } = &subscription {
            if !Subscription::is_l4_snapshot_coin(coin) {
                let msg = ServerResponse::Error(format!("Invalid subscription: L4 snapshot disabled for {coin}"));
                send_error_response(socket, req_id, msg).await;
                return;
            }
        }
        if let Subscription::L4BookLite { coin } = &subscription {
            if !Subscription::is_l4_snapshot_coin(coin) {
                let msg = ServerResponse::Error(format!("Invalid subscription: L4 book lite snapshot disabled for {coin}"));
                send_error_response(socket, req_id, msg).await;
                return;
            }
            let snapshot = listener.lock().await.l4_book_lite_snapshot(coin.clone());
            if let Some(snapshot) = snapshot {
                send_snapshot_data_for_get_snapshot(socket, snapshot, req_id).await;
            } else {
                let msg = ServerResponse::Error("Snapshot Failed".to_string());
                send_error_response(socket, req_id, msg).await;
            }
            return;
        }
        let snapshot_msg = subscription.handle_immediate_snapshot(listener).await;
        match snapshot_msg {
            Ok(Some(msg)) => send_get_snapshot_response(socket, msg, req_id).await,
            Ok(None) => {
                let msg = ServerResponse::Error("Snapshot Failed".to_string());
                send_error_response(socket, req_id, msg).await;
            }
            Err(err) => {
                let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                send_error_response(socket, req_id, msg).await;
            }
        }
        return;
    }
    let is_subscribe = matches!(client_message, ClientMessage::Subscribe { .. });
    let (word, success) = if is_subscribe {
        ("", manager.subscribe(subscription.clone()))
    } else {
        ("un", manager.unsubscribe(subscription.clone()))
    };
    if success {
        if let Some(coin) = subscription_coin(&subscription) {
            update_active_symbols(active_symbols, coin, is_subscribe).await;
        }
        send_subscription_response(socket, &client_message).await;
        if let ClientMessage::Subscribe { subscription, .. } = &client_message {
            match subscription {
                Subscription::L4BookLite { coin } => {
                    if let Some(snapshot) = listener.lock().await.l4_book_lite_snapshot(coin.clone()) {
                        send_ws_data_from_l4_book_lite_snapshot(socket, subscription, &snapshot, None).await;
                    }
                }
                _ => {
                    let msg = subscription.handle_immediate_snapshot(listener).await;
                    match msg {
                        Ok(Some(msg)) => send_snapshot_response(socket, msg, None).await,
                        Ok(None) => {}
                        Err(err) => {
                            manager.unsubscribe(subscription.clone());
                            let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                            send_error_response(socket, req_id, msg).await;
                            return;
                        }
                    }
                }
            }
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_error_response(socket, req_id, msg).await;
    }
}

async fn update_active_symbols(active_symbols: Arc<Mutex<HashMap<String, usize>>>, coin: String, is_subscribe: bool) {
    let mut active_symbols = active_symbols.lock().await;
    let entry = active_symbols.entry(coin).or_insert(0);
    if is_subscribe {
        *entry = entry.saturating_add(1);
    } else if *entry > 0 {
        *entry -= 1;
    }
}

async fn prune_active_symbols(manager: &mut SubscriptionManager, active_symbols: &Arc<Mutex<HashMap<String, usize>>>) {
    let subscriptions = manager.subscriptions().clone();
    for subscription in subscriptions {
        if let Some(coin) = subscription_coin(&subscription) {
            update_active_symbols(active_symbols.clone(), coin, false).await;
        }
    }
}

fn subscription_coin(subscription: &Subscription) -> Option<String> {
    match subscription {
        Subscription::Trades { coin } => Some(coin.clone()),
        Subscription::L4Book { coin } => Some(coin.clone()),
        Subscription::L4BookLite { coin } => Some(coin.clone()),
        _ => None,
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

async fn send_channel_response<T: Serialize>(socket: &mut WebSocket, response: ChannelResponse<T>) {
    let msg = serde_json::to_string(&response);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

async fn send_subscription_response(socket: &mut WebSocket, msg: &ClientMessage) {
    let data = SubscriptionResponseData::from_client_message(msg);
    let response = ChannelResponse { channel: "subscriptionResponse".to_string(), req_id: msg.req_id(), data };
    send_channel_response(socket, response).await;
}

async fn send_snapshot_response(socket: &mut WebSocket, msg: ServerResponse, req_id: Option<u64>) {
    match msg {
        ServerResponse::L2Book(book) => {
            let response = ChannelResponse { channel: "l2Book".to_string(), req_id, data: book };
            send_channel_response(socket, response).await;
        }
        ServerResponse::L4Book(book) => {
            let response = ChannelResponse { channel: "l4Book".to_string(), req_id, data: book };
            send_channel_response(socket, response).await;
        }
        other => send_socket_message(socket, other).await,
    }
}

async fn send_get_snapshot_response(socket: &mut WebSocket, msg: ServerResponse, req_id: Option<u64>) {
    match msg {
        ServerResponse::L2Book(book) => {
            let response = ChannelResponse { channel: "snapshot".to_string(), req_id, data: book };
            send_channel_response(socket, response).await;
        }
        ServerResponse::L4Book(book) => {
            let response = ChannelResponse { channel: "snapshot".to_string(), req_id, data: book };
            send_channel_response(socket, response).await;
        }
        other => send_socket_message(socket, other).await,
    }
}

async fn send_snapshot_data_for_get_snapshot(
    socket: &mut WebSocket,
    snapshot: L4BookLiteSnapshot,
    req_id: Option<u64>,
) {
    let response = ChannelResponse { channel: "snapshot".to_string(), req_id, data: snapshot };
    send_channel_response(socket, response).await;
}

// derive it from l2_snapshots because thats convenient
fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let snapshot = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time);
            let msg = ServerResponse::L2Book(l2_book);
            send_socket_message(socket, msg).await;
        } else {
            error!("Coin {coin} not found");
        }
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut fills = batch.clone().events();
    let mut trades = HashMap::new();
    while fills.len() >= 2 {
        let f2 = fills.pop();
        let f1 = fills.pop();
        if let Some(f1) = f1 {
            if let Some(f2) = f2 {
                let mut fills = HashMap::new();
                fills.insert(f1.1.side, f1);
                fills.insert(f2.1.side, f2);
                let trade = Trade::from_fills(fills);
                let coin = trade.coin.clone();
                trades.entry(coin).or_insert_with(Vec::new).push(trade);
            }
        }
    }
    for list in trades.values_mut() {
        list.reverse();
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_l4_snapshot(socket: &mut WebSocket, subscription: &Subscription, snapshot: &TimedSnapshots) {
    if let Subscription::L4Book { coin } = subscription {
        let book = snapshot
            .snapshot
            .clone()
            .value()
            .into_iter()
            .filter(|(c, _)| *c == Coin::new(coin))
            .collect::<Vec<_>>()
            .pop();
        if let Some((coin, book)) = book {
            let levels = book.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
            let msg = ServerResponse::L4Book(L4Book::Snapshot {
                coin: coin.value(),
                time: snapshot.time,
                height: snapshot.height,
                levels,
            });
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_l4_book_lite_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &L4BookLiteSnapshot,
    req_id: Option<u64>,
) {
    if let Subscription::L4BookLite { coin } = subscription {
        if coin == &snapshot.coin {
            let response = ChannelResponse { channel: "l4BookLite".to_string(), req_id, data: snapshot.clone() };
            send_channel_response(socket, response).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_l4_book_lite_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    updates: &L4BookLiteUpdates,
) {
    if let Subscription::L4BookLite { coin } = subscription {
        if coin == &updates.coin {
            let response = ChannelResponse { channel: "l4BookLite".to_string(), req_id: None, data: updates.clone() };
            send_channel_response(socket, response).await;
        }
    }
}

async fn send_ws_data_from_l4_book_lite_depth_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    updates: &L4BookLiteDepthUpdate,
) {
    if let Subscription::L4BookLite { coin } = subscription {
        if coin == &updates.coin {
            let response = ChannelResponse { channel: "l4BookLiteDepth".to_string(), req_id: None, data: updates.clone() };
            send_channel_response(socket, response).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(&self, listener: Arc<Mutex<OrderBookListener>>) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            if !Self::is_l4_snapshot_coin(coin) {
                return Ok(None);
            }
            let snapshot = listener.lock().await.l4_snapshot_for_coin(coin);
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

async fn send_error_response(socket: &mut WebSocket, req_id: Option<u64>, msg: ServerResponse) {
    if let ServerResponse::Error(message) = msg {
        let response = ChannelResponse { channel: "error".to_string(), req_id, data: message };
        send_channel_response(socket, response).await;
    } else {
        send_socket_message(socket, msg).await;
    }
}

impl SubscriptionResponseData {
    fn from_client_message(msg: &ClientMessage) -> Self {
        match msg {
            ClientMessage::Subscribe { subscription, .. } => Self::Subscribe { subscription: subscription.clone() },
            ClientMessage::Unsubscribe { subscription, .. } => Self::Unsubscribe { subscription: subscription.clone() },
            ClientMessage::GetSnapshot { .. } => {
                unreachable!("subscription responses are only sent for subscribe/unsubscribe requests")
            }
        }
    }
}

impl ClientMessage {
    fn req_id(&self) -> Option<u64> {
        match self {
            Self::Subscribe { req_id, .. } | Self::Unsubscribe { req_id, .. } | Self::GetSnapshot { req_id, .. } => {
                *req_id
            }
        }
    }
}
