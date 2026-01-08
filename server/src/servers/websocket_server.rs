use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
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

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let active_symbols = Arc::new(Mutex::new(HashMap::new()));
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        let active_symbols = active_symbols.clone();
        OrderBookListener::new(Some(internal_message_tx), Some(active_symbols), ignore_spot)
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
                    active_symbols,
                    ignore_spot,
                    websocket_opts,
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
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
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

        handle_socket(ws, internal_message_tx, listener, active_symbols, ignore_spot).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    active_symbols: Arc<Mutex<HashMap<String, usize>>>,
    ignore_spot: bool,
) {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
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
                            InternalMessage::SnapshotRebuilt { time, height, l4_snapshot, lite_snapshots } => {
                                let mut l4book_payloads: HashMap<String, serde_json::Value> = HashMap::new();
                                for (coin, snapshot) in l4_snapshot.as_ref() {
                                    let levels =
                                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                                    let payload = L4Book::Snapshot {
                                        coin: coin.value(),
                                        time: *time,
                                        height: *height,
                                        levels,
                                    };
                                    if let Ok(val) = serde_json::to_value(payload) {
                                        l4book_payloads.insert(coin.value().to_string(), val);
                                    }
                                }

                                let mut lite_payloads: HashMap<String, serde_json::Value> = HashMap::new();
                                for (coin, snapshot) in lite_snapshots {
                                    if let Ok(val) = serde_json::to_value(snapshot) {
                                        lite_payloads.insert(coin.value().to_string(), val);
                                    }
                                }

                                for sub in manager.subscriptions() {
                                    match sub {
                                        Subscription::L4Book { coin } => {
                                            if let Some(data) = l4book_payloads.get(coin) {
                                                let channel = format!("{}@l4Book", coin.to_lowercase());
                                                send_stream_snapshot_message(&mut socket, &channel, data.clone()).await;
                                            }
                                        }
                                        Subscription::L4Lite { coin } => {
                                            if let Some(data) = lite_payloads.get(coin) {
                                                let channel = format!("{}@l4Lite", coin.to_lowercase());
                                                send_stream_snapshot_message(&mut socket, &channel, data.clone()).await;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
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
                            InternalMessage::L4Lite{ updates, analysis_b, analysis_a, height } => {
                                let mut updates_by_coin: HashMap<String, crate::listeners::order_book::lite::L2BlockUpdate> = HashMap::new();
                                for u in updates {
                                    updates_by_coin.insert(u.coin.clone(), u.clone());
                                }
                                
                                let mut analysis_by_coin_b: HashMap<String, Vec<crate::listeners::order_book::lite::AnalysisUpdate>> = HashMap::new();
                                for a in analysis_b {
                                    analysis_by_coin_b.entry(a.coin.value()).or_default().push(a.clone());
                                }
                                let mut analysis_by_coin_a: HashMap<String, Vec<crate::listeners::order_book::lite::AnalysisUpdate>> = HashMap::new();
                                for a in analysis_a {
                                    analysis_by_coin_a.entry(a.coin.value()).or_default().push(a.clone());
                                }

                                for sub in manager.subscriptions() {
                                    send_ws_data_from_lite_updates(&mut socket, sub, &mut updates_by_coin, &mut analysis_by_coin_b, &mut analysis_by_coin_a, *height).await;
                                }
                            },
                        }


                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        break;
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
                                    break;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::GetSnapshot { snapshot, req_id } => {
                                        handle_snapshot_request(&mut socket, snapshot, req_id, listener.clone()).await;
                                    }
                                    other => {
                                        receive_client_message(
                                            &mut socket,
                                            &mut manager,
                                            other,
                                            &universe,
                                            listener.clone(),
                                            active_symbols.clone(),
                                        )
                                        .await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            break;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    break;
                }
            }
        }
    }

    // Cleanup subscriptions
    let mut guard = active_symbols.lock().await;
    for sub in manager.subscriptions() {
        let coin = match sub {
            Subscription::Trades { coin } => coin,
            Subscription::L2Book { coin, .. } => coin,
            Subscription::L4Book { coin } => coin,
            Subscription::L4Lite { coin } => coin,
        };
        if let Some(count) = guard.get_mut(coin) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                guard.remove(coin);
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
    // Helper to convert subscription to a stream string like "btc@l4Book"
    fn sub_to_stream(s: &Subscription) -> String {
        match s {
            Subscription::Trades { coin } => format!("{}@trade", coin.to_lowercase()),
            Subscription::L2Book { coin, .. } => format!("{}@l2Book", coin.to_lowercase()),
            Subscription::L4Book { coin } => format!("{}@l4Book", coin.to_lowercase()),
            Subscription::L4Lite { coin } => format!("{}@l4Lite", coin.to_lowercase()),
        }
    }

    // parse "btc@l4Book" into Subscription
    fn parse_stream_string(s: &str) -> Option<Subscription> {
        let mut parts = s.splitn(2, '@');
        let coin = parts.next()?.to_uppercase();
        let kind = parts.next()?.to_lowercase();
        match kind.as_str() {
            "l4book" => Some(Subscription::L4Book { coin }),
            "l4lite" => Some(Subscription::L4Lite { coin }),
            "l2book" => Some(Subscription::L2Book { coin, n_sig_figs: None, n_levels: None, mantissa: None }),
            "trade" | "trades" => Some(Subscription::Trades { coin }),
            _ => None,
        }
    }

    // Build list of subscriptions and streams + req_id
    let (subscriptions, streams, req_id, is_subscribe) = match client_message {
        ClientMessage::Subscribe { payload } => match payload {
            crate::types::subscription::SubscribePayload::Single { subscription } => {
                (vec![subscription.clone()], vec![sub_to_stream(&subscription)], None, true)
            }
            crate::types::subscription::SubscribePayload::Streams { streams, req_id } => {
                let mut subs = Vec::new();
                let mut valid_streams = Vec::new();
                for s in streams {
                    if let Some(sub) = parse_stream_string(&s) {
                        valid_streams.push(s);
                        subs.push(sub);
                    }
                }
                (subs, valid_streams, req_id, true)
            }
        },
        ClientMessage::Unsubscribe { payload } => match payload {
            crate::types::subscription::SubscribePayload::Single { subscription } => {
                (vec![subscription.clone()], vec![sub_to_stream(&subscription)], None, false)
            }
            crate::types::subscription::SubscribePayload::Streams { streams, req_id } => {
                let mut subs = Vec::new();
                let mut valid_streams = Vec::new();
                for s in streams {
                    if let Some(sub) = parse_stream_string(&s) {
                        valid_streams.push(s);
                        subs.push(sub);
                    }
                }
                (subs, valid_streams, req_id, false)
            }
        },
        ClientMessage::GetSnapshot { .. } => {
            let msg = ServerResponse::Error("Use getSnapshot with req_id on the request path".to_string());
            send_socket_message(socket, msg).await;
            return;
        }
    };

    // Validate all subscriptions
    for sub in &subscriptions {
        if !sub.validate(universe) {
            let sub_str = serde_json::to_string(sub).unwrap_or_default();
            let msg = ServerResponse::Error(format!("Invalid subscription: {sub_str}"));
            send_socket_message(socket, msg).await;
            return;
        }
    }

    // Apply subscriptions/unsubscriptions
    // We will batch apply and if any error occurs revert changes
    let mut applied_coins: Vec<String> = Vec::new();
    let mut applied_subs: Vec<Subscription> = Vec::new();
    {
        let mut guard = active_symbols.lock().await;
        for sub in &subscriptions {
            let coin = match sub {
                Subscription::Trades { coin } => coin,
                Subscription::L2Book { coin, .. } => coin,
                Subscription::L4Book { coin } => coin,
                Subscription::L4Lite { coin } => coin,
            };
            if is_subscribe {
                *guard.entry(coin.clone()).or_insert(0) += 1;
                applied_coins.push(coin.clone());
                applied_subs.push(sub.clone());
            } else {
                if let Some(count) = guard.get_mut(coin) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        guard.remove(coin);
                    }
                }
                applied_subs.push(sub.clone());
            }
        }
    }

    // Now register with manager for each subscription
    for sub in &applied_subs {
        let success = if is_subscribe { manager.subscribe(sub.clone()) } else { manager.unsubscribe(sub.clone()) };
        if !success {
            // revert active_symbols changes
            let mut guard = active_symbols.lock().await;
            for coin in applied_coins.iter() {
                if let Some(count) = guard.get_mut(coin) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        guard.remove(coin);
                    }
                }
            }
            // drop the guard before awaiting
            drop(guard);
            let sub_repr = serde_json::to_string(sub).unwrap_or_default();
            let word = if is_subscribe { "" } else { "un" };
            let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub_repr}"));
            send_socket_message(socket, msg).await;
            return;
        }
    }

    // For subscribe, attempt to gather immediate snapshots for L4Book subs
    let mut snapshot_msgs: Vec<ServerResponse> = Vec::new();
    if is_subscribe {
        for sub in &subscriptions {
            // Check if subscription type supports snapshots
            let should_try_snapshot = matches!(sub, Subscription::L4Book { .. } | Subscription::L4Lite { .. });
            if should_try_snapshot {
                // Build a subscription object to call handle_immediate_snapshot
                let maybe = sub.handle_immediate_snapshot(listener.clone()).await;
                match maybe {
                    Ok(Some(msg)) => snapshot_msgs.push(msg),
                    Ok(None) => {}
                    Err(err) => {
                        // revert changes
                        let mut guard = active_symbols.lock().await;
                        for coin in applied_coins.iter() {
                            if let Some(count) = guard.get_mut(coin) {
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    guard.remove(coin);
                                }
                            }
                        }
                        for s in &applied_subs {
                            manager.unsubscribe(s.clone());
                        }
                        // drop the guard before awaiting
                        drop(guard);
                        let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                        send_socket_message(socket, msg).await;
                        return;
                    }
                }
            }
        }
    }

    // Success: send subscription response with streams and req_id
    let msg = ServerResponse::SubscriptionResponse { streams, req_id };
    send_socket_message(socket, msg).await;

    // send any immediate snapshot messages
    for sm in snapshot_msgs {
        send_socket_message(socket, sm).await;
    }
}

enum SnapshotKind {
    L4Book,
    L4Lite,
}

fn parse_snapshot_request(snapshot: &str) -> Option<(String, SnapshotKind, String)> {
    let trimmed = snapshot.trim().trim_end_matches('/');
    let mut parts = trimmed.splitn(2, '@');
    let coin = parts.next()?.trim();
    let kind = parts.next()?.trim().to_lowercase();
    if coin.is_empty() {
        return None;
    }
    let kind = match kind.as_str() {
        "l4book" => SnapshotKind::L4Book,
        "l4lite" => SnapshotKind::L4Lite,
        _ => return None,
    };
    Some((coin.to_uppercase(), kind, trimmed.to_string()))
}

async fn handle_snapshot_request(
    socket: &mut WebSocket,
    snapshot: String,
    req_id: Option<i64>,
    listener: Arc<Mutex<OrderBookListener>>,
) {
    let trimmed = snapshot.trim().trim_end_matches('/');
    let fallback_channel = if trimmed.is_empty() { "snapshot" } else { trimmed };
    let (coin, kind, channel) = match parse_snapshot_request(trimmed) {
        Some(parts) => parts,
        None => {
            let payload = serde_json::json!({ "error": "Invalid snapshot format" });
            send_snapshot_response(socket, fallback_channel, req_id, payload).await;
            return;
        }
    };

    match kind {
        SnapshotKind::L4Book => {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().find(|(c, _)| *c == Coin::new(&coin));
                if let Some((coin, snapshot)) = snapshot {
                    let levels =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    let payload = L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels,
                    };
                    if let Ok(data) = serde_json::to_value(payload) {
                        send_snapshot_response(socket, &channel, req_id, data).await;
                        return;
                    }
                }
            }
            let payload = serde_json::json!({ "error": "Snapshot not available" });
            send_snapshot_response(socket, &channel, req_id, payload).await;
        }
        SnapshotKind::L4Lite => {
            let listener = listener.lock().await;
            if let Some(lb) = listener.lite_books.get(&Coin::new(&coin)) {
                if !lb.is_initialized() {
                    let payload = serde_json::json!({ "error": "Lite snapshot not initialized" });
                    send_snapshot_response(socket, &channel, req_id, payload).await;
                    return;
                }
                let snap = lb.get_snapshot();
                if let Ok(data) = serde_json::to_value(snap) {
                    send_snapshot_response(socket, &channel, req_id, data).await;
                    return;
                }
            }
            let payload = serde_json::json!({ "error": "Lite snapshot not available" });
            send_snapshot_response(socket, &channel, req_id, payload).await;
        }
    }
}

async fn send_snapshot_response(
    socket: &mut WebSocket,
    channel: &str,
    req_id: Option<i64>,
    data: serde_json::Value,
) {
    let obj = serde_json::json!({
        "channel": channel,
        "req_id": req_id,
        "data": data,
    });
    match serde_json::to_string(&obj) {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => error!("Server response serialization error: {err}"),
    }
}

async fn send_stream_snapshot_message(
    socket: &mut WebSocket,
    channel: &str,
    data: serde_json::Value,
) {
    let obj = serde_json::json!({
        "channel": channel,
        "data": data,
    });
    match serde_json::to_string(&obj) {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => error!("Server response serialization error: {err}"),
    }
}


async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    // Special-case subscription response so `req_id` is placed at top-level
    match msg {
        ServerResponse::SubscriptionResponse { streams, req_id } => {
            // Return without data wrapper: top-level streams and req_id
            let obj = serde_json::json!({
                "channel": "subscriptionResponse",
                "streams": streams,
                "req_id": req_id,
            });
            match serde_json::to_string(&obj) {
                Ok(msg) => {
                    if let Err(err) = socket.send(FrameView::text(msg)).await {
                        error!("Failed to send: {err}");
                    }
                }
                Err(err) => error!("Server response serialization error: {err}"),
            }
        }
        other => {
            let msg = serde_json::to_string(&other);
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
    }
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

async fn send_ws_data_from_lite_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    updates: &mut HashMap<String, crate::listeners::order_book::lite::L2BlockUpdate>,
    analysis_b: &mut HashMap<String, Vec<crate::listeners::order_book::lite::AnalysisUpdate>>,
    analysis_a: &mut HashMap<String, Vec<crate::listeners::order_book::lite::AnalysisUpdate>>,
    block_height: u64,
) {
    if let Subscription::L4Lite { coin } = subscription {
        use crate::types::subscription::AnalysisData;
        
        let lite = updates.get(coin).cloned().unwrap_or_else(|| {
             crate::listeners::order_book::lite::L2BlockUpdate {
                 coin: coin.clone(),
                 b: Vec::new(),
                 a: Vec::new(),
                 block_height
             }
        });
        
        // Send analysis frame 
        let a_b = analysis_b.remove(coin).unwrap_or_default();
        let a_a = analysis_a.remove(coin).unwrap_or_default();

        let analysis = AnalysisData {
             bids: a_b,
             asks: a_a,
             block_height,
        };
        
        // Combined Update
        let msg = ServerResponse::L4LiteUpdate {
             lite,
             analysis
        };
        
        send_socket_message(socket, msg).await;
    }
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

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        match self {
            Self::L4Book { coin } => {
                let snapshot = listener.lock().await.compute_snapshot();
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
            Self::L4Lite { coin } => {
                // Fetch L2 Snapshot from LiteBook
                let listener = listener.lock().await;
                if let Some(lb) = listener.lite_books.get(&Coin::new(coin)) {
                     let snap = lb.get_snapshot();
                     return Ok(Some(ServerResponse::L2Snapshot(snap)));
                } else {
                     return Err("LiteBook not ready".into());
                }
            }
            _ => Ok(None)
        }
    }
}
