use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use url::Url;
use dashmap::DashMap;
use tracing::{info, error, warn, debug, instrument};
use tracing_subscriber::{fmt, EnvFilter};
use std::time::Duration;
use rust_decimal::Decimal;
use thiserror::Error;
use reqwest::Client as HttpClient;
use std::cmp::Ordering;
use std::collections::BTreeMap;

// Configuration constants
const BINANCE_WS_URL: &str = "wss://fstream.binance.com/stream";
const BINANCE_API_URL: &str = "https://fapi.binance.com";
const RECONNECT_DELAY_MS: u64 = 1000;
const MAX_RECONNECT_ATTEMPTS: u8 = 5;
const BATCH_SIZE: usize = 50;
const BATCH_INTERVAL_MS: u64 = 100;
const ORDER_BOOK_DEPTH: usize = 1000; // Depth to fetch for initial snapshot

#[derive(Debug, Error)]
enum WebSocketError {
    #[error("Connection error: {0}")]
    ConnectionError(#[from] url::ParseError),
    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("Max reconnect attempts reached")]
    MaxReconnectAttempts,
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

// Data structures for Binance WebSocket messages
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BookTickerData {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b", deserialize_with = "decimal_from_str")]
    bid_price: Decimal,
    #[serde(rename = "B", deserialize_with = "decimal_from_str")]
    bid_qty: Decimal,
    #[serde(rename = "a", deserialize_with = "decimal_from_str")]
    ask_price: Decimal,
    #[serde(rename = "A", deserialize_with = "decimal_from_str")]
    ask_qty: Decimal,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct TradeData {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p", deserialize_with = "decimal_from_str")]
    price: Decimal,
    #[serde(rename = "q", deserialize_with = "decimal_from_str")]
    quantity: Decimal,
    #[serde(rename = "b")]
    buyer_order_id: u64,
    #[serde(rename = "a")]
    seller_order_id: u64,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    is_buyer_market_maker: bool,
    #[serde(rename = "M")]
    ignore: bool,
}

// New struct for depth update data
#[derive(Debug, Deserialize, Clone)]
struct DepthUpdateData {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T")]
    transaction_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

// New struct for orderbook snapshot
#[derive(Debug, Deserialize, Clone)]
struct OrderBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

// New struct to hold order book state
#[derive(Debug, Clone)]
struct OrderBook {
    symbol: String,
    last_update_id: u64,
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    synced: bool,
}

impl OrderBook {
    fn new(symbol: String) -> Self {
        Self {
            symbol,
            last_update_id: 0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            synced: false,
        }
    }

    // Initialize from snapshot
    fn apply_snapshot(&mut self, snapshot: OrderBookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        self.last_update_id = snapshot.last_update_id;

        for bid in snapshot.bids {
            let price = Decimal::from_str_exact(&bid[0]).unwrap_or_default();
            let qty = Decimal::from_str_exact(&bid[1]).unwrap_or_default();
            if qty > Decimal::ZERO {
                self.bids.insert(price, qty);
            }
        }

        for ask in snapshot.asks {
            let price = Decimal::from_str_exact(&ask[0]).unwrap_or_default();
            let qty = Decimal::from_str_exact(&ask[1]).unwrap_or_default();
            if qty > Decimal::ZERO {
                self.asks.insert(price, qty);
            }
        }

        self.synced = false;
        debug!("Applied snapshot for {} with lastUpdateId: {}", self.symbol, self.last_update_id);
    }

    // Apply depth update based on Binance's documentation
    fn apply_update(&mut self, update: &DepthUpdateData) -> bool {
        if !self.synced {
            // Step 4: Drop any event where u is < lastUpdateId in the snapshot
            if update.final_update_id < self.last_update_id {
                return false;
            }

            // Step 5: The first processed event should have U <= lastUpdateId AND u >= lastUpdateId
            if update.first_update_id <= self.last_update_id && update.final_update_id >= self.last_update_id {
                self.synced = true;
            } else {
                return false;
            }
        } else {
            // Step 6: While listening to the stream, each new event's pu should be equal to the previous event's u
            if update.prev_final_update_id != self.last_update_id {
                return false;
            }
        }

        // Update the last update ID
        self.last_update_id = update.final_update_id;

        // Process the bid updates
        for bid in &update.bids {
            let price = Decimal::from_str_exact(&bid[0]).unwrap_or_default();
            let qty = Decimal::from_str_exact(&bid[1]).unwrap_or_default();
            
            if qty == Decimal::ZERO {
                // Step 8: If the quantity is 0, remove the price level
                self.bids.remove(&price);
            } else {
                // Step 7: Apply the update
                self.bids.insert(price, qty);
            }
        }

        // Process the ask updates
        for ask in &update.asks {
            let price = Decimal::from_str_exact(&ask[0]).unwrap_or_default();
            let qty = Decimal::from_str_exact(&ask[1]).unwrap_or_default();
            
            if qty == Decimal::ZERO {
                // Step 8: If the quantity is 0, remove the price level
                self.asks.remove(&price);
            } else {
                // Step 7: Apply the update
                self.asks.insert(price, qty);
            }
        }

        true
    }

    // Get the best bid (highest price)
    fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(k, v)| (*k, *v))
    }

    // Get the best ask (lowest price)
    fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(k, v)| (*k, *v))
    }

    // Get a sorted vec of top N bids
    fn top_bids(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids.iter()
            .rev()
            .take(n)
            .map(|(k, v)| (*k, *v))
            .collect()
    }

    // Get a sorted vec of top N asks
    fn top_asks(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter()
            .take(n)
            .map(|(k, v)| (*k, *v))
            .collect()
    }
}

fn decimal_from_str<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Decimal::from_str_exact(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize)]
struct StreamMessage {
    stream: String,
    data: serde_json::Value,
}

// Cache for storing latest market data
struct MarketDataCache {
    book_tickers: DashMap<String, BookTickerData>,
    last_trades: DashMap<String, TradeData>,
    order_books: DashMap<String, OrderBook>,
}

impl MarketDataCache {
    fn new() -> Self {
        Self {
            book_tickers: DashMap::with_capacity(100),
            last_trades: DashMap::with_capacity(100),
            order_books: DashMap::with_capacity(100),
        }
    }

    fn batch_update_book_tickers(&self, tickers: Vec<BookTickerData>) {
        for ticker in tickers {
            let symbol = ticker.symbol.to_uppercase();
            self.book_tickers.insert(symbol, ticker);
        }
    }

    fn batch_update_trades(&self, trades: Vec<TradeData>) {
        for trade in trades {
            let symbol = trade.symbol.to_uppercase();
            self.last_trades.insert(symbol, trade);
        }
    }

    fn update_order_book(&self, update: DepthUpdateData) {
        let symbol = update.symbol.to_uppercase();
        
        match self.order_books.entry(symbol.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let book = entry.get_mut();
                if !book.apply_update(&update) {
                    warn!("Order book {} needs resyncing", symbol);
                    // Mark for resync
                    book.synced = false;
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let mut book = OrderBook::new(symbol.clone());
                if !book.apply_update(&update) {
                    debug!("Created new order book for {}, waiting for sync", symbol);
                }
                entry.insert(book);
            }
        }
    }
}

// WebSocket client for Binance API
struct BinanceWebSocketClient {
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    symbols: Vec<String>,
    cache: Arc<MarketDataCache>,
    reconnect_attempts: u8,
    http_client: HttpClient,
}

impl BinanceWebSocketClient {
    fn new(symbols: Vec<String>) -> Self {
        Self {
            ws_stream: None,
            symbols,
            cache: Arc::new(MarketDataCache::new()),
            reconnect_attempts: 0,
            http_client: HttpClient::new(),
        }
    }

    async fn fetch_order_book_snapshot(&self, symbol: &str) -> Result<OrderBookSnapshot, WebSocketError> {
        let url = format!(
            "{}/fapi/v1/depth?symbol={}&limit={}",
            BINANCE_API_URL, symbol.to_uppercase(), ORDER_BOOK_DEPTH
        );
        
        debug!("Fetching order book snapshot for {}", symbol);
        let response = self.http_client.get(&url).send().await?;
        let snapshot: OrderBookSnapshot = response.json().await?;
        
        Ok(snapshot)
    }

    async fn initialize_order_books(&self) -> Result<(), WebSocketError> {
        for symbol in &self.symbols {
            let snapshot = self.fetch_order_book_snapshot(symbol).await?;
            match self.cache.order_books.entry(symbol.to_uppercase()) {
                dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                    entry.get_mut().apply_snapshot(snapshot);
                }
                dashmap::mapref::entry::Entry::Vacant(entry) => {
                    let mut book = OrderBook::new(symbol.to_uppercase());
                    book.apply_snapshot(snapshot);
                    entry.insert(book);
                }
            }
            info!("Initialized order book for {}", symbol);
            
            // Avoid rate limiting
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn connect(&mut self) -> Result<(), WebSocketError> {
        // First initialize order books with snapshots
        self.initialize_order_books().await?;

        let streams = self.symbols
            .iter()
            .flat_map(|symbol| {
                let s = symbol.to_lowercase();
                vec![
                    format!("{}@bookTicker", s),
                    format!("{}@trade", s),
                    format!("{}@depth", s), // Add depth stream
                ]
            })
            .collect::<Vec<_>>()
            .join("/");

        let ws_url = format!("{}?streams={}", BINANCE_WS_URL, streams);
        let url = Url::parse(&ws_url)?;
        
        let (ws_stream, _) = connect_async(url).await?;
        self.ws_stream = Some(ws_stream);
        self.reconnect_attempts = 0;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn process_messages(&mut self) -> Result<(), WebSocketError> {
        let mut book_ticker_batch = Vec::with_capacity(BATCH_SIZE);
        let mut trade_batch = Vec::with_capacity(BATCH_SIZE);
        let mut last_flush = tokio::time::Instant::now();
        
        let (write, mut read) = self.ws_stream.as_mut().unwrap().split();
        let write = Arc::new(tokio::sync::Mutex::new(write));

        loop {
            tokio::select! {
                _ = flush_batch(
                    &mut book_ticker_batch,
                    &mut trade_batch,
                    &mut last_flush,
                    self.cache.clone(),
                ) => {}

                msg = read.next() => {
                    let msg = match msg.transpose()? {
                        Some(m) => m,
                        None => break Ok(()),
                    };

                    match msg {
                        Message::Text(text) => handle_message(
                            &text,
                            &mut book_ticker_batch,
                            &mut trade_batch,
                            BATCH_SIZE,
                            self.cache.clone(),
                        ).await?,
                        Message::Ping(data) => {
                            write.lock().await.send(Message::Pong(data)).await?;
                        }
                        Message::Close(_) => {
                            debug!("Received close frame");
                            break Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn run(&mut self) -> Result<(), WebSocketError> {
        loop {
            if self.reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                return Err(WebSocketError::MaxReconnectAttempts);
            }

            match self.connect().await {
                Ok(_) => {
                    info!("Connected to Binance WebSocket");
                    if let Err(e) = self.process_messages().await {
                        error!("Error processing messages: {}", e);
                    }
                    self.reconnect_attempts += 1;
                    warn!("Reconnecting attempt {}/{}", self.reconnect_attempts, MAX_RECONNECT_ATTEMPTS);
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_DELAY_MS)).await;
                }
                Err(e) => {
                    error!("Connection failed: {}", e);
                    self.reconnect_attempts += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_DELAY_MS)).await;
                }
            }
        }
    }

    #[allow(dead_code)]
    fn get_book_ticker(&self, symbol: &str) -> Option<BookTickerData> {
        let symbol = symbol.to_uppercase();
        self.cache.book_tickers
            .get(&symbol)
            .map(|r| r.value().clone())
    }

    #[allow(dead_code)]
    fn get_last_trade(&self, symbol: &str) -> Option<TradeData> {
        let symbol = symbol.to_uppercase();
        self.cache.last_trades
            .get(&symbol)
            .map(|r| r.value().clone())
    }

    #[allow(dead_code)]
    fn get_order_book(&self, symbol: &str) -> Option<OrderBook> {
        let symbol = symbol.to_uppercase();
        self.cache.order_books
            .get(&symbol)
            .map(|r| r.value().clone())
    }
}

#[instrument(skip_all)]
async fn handle_message(
    text: &str,
    book_batch: &mut Vec<BookTickerData>,
    trade_batch: &mut Vec<TradeData>,
    batch_size: usize,
    cache: Arc<MarketDataCache>,
) -> Result<(), WebSocketError> {
    if let Ok(msg) = serde_json::from_str::<StreamMessage>(text) {
        let parts: Vec<&str> = msg.stream.split('@').collect();
        if parts.len() != 2 {
            return Ok(());
        }

        match parts[1] {
            "bookTicker" => {
                if let Ok(ticker) = serde_json::from_value::<BookTickerData>(msg.data) {
                    book_batch.push(ticker);
                    if book_batch.len() >= batch_size {
                        debug!("Batch limit reached for book tickers");
                    }
                }
            }
            "trade" => {
                match serde_json::from_value::<TradeData>(msg.data) {
                    Ok(trade) => {
                        trade_batch.push(trade);
                        if trade_batch.len() >= batch_size {
                            debug!("Trade batch ready for flushing");
                        }
                    }
                    Err(e) => warn!("Failed to parse trade: {}", e),
                }
            }
            "depth" => {
                match serde_json::from_value::<DepthUpdateData>(msg.data) {
                    Ok(depth_update) => {
                        // Process depth update immediately rather than batching
                        cache.update_order_book(depth_update);
                    }
                    Err(e) => warn!("Failed to parse depth update: {}", e),
                }
            }
            _ => warn!("Unknown stream type: {}", parts[1]),
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn flush_batch(
    book_batch: &mut Vec<BookTickerData>,
    trade_batch: &mut Vec<TradeData>,
    last_flush: &mut tokio::time::Instant,
    cache: Arc<MarketDataCache>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(BATCH_INTERVAL_MS));
    loop {
        interval.tick().await;
        if book_batch.len() >= BATCH_SIZE || trade_batch.len() >= BATCH_SIZE 
            || last_flush.elapsed() >= Duration::from_millis(BATCH_INTERVAL_MS)
        {
            if !book_batch.is_empty() {
                let count = book_batch.len();
                cache.batch_update_book_tickers(std::mem::take(book_batch));
                debug!("Flushed {} book tickers", count);
            }
            if !trade_batch.is_empty() {
                let count = trade_batch.len();
                cache.batch_update_trades(std::mem::take(trade_batch));
                debug!("Flushed {} trades", count);
            }
            *last_flush = tokio::time::Instant::now();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), WebSocketError> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();

    let symbols = vec!["BTCUSDT".into(), "ETHUSDT".into()];
    let mut client = BinanceWebSocketClient::new(symbols);
    let cache_clone = client.cache.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            info!(
                "Cache stats: BookTickers={}, Trades={}, OrderBooks={}",
                cache_clone.book_tickers.len(),
                cache_clone.last_trades.len(),
                cache_clone.order_books.len()
            );
            
            // Print order book statistics
            for entry in cache_clone.order_books.iter() {
                let book = entry.value();
                if let (Some((best_bid_price, best_bid_qty)), Some((best_ask_price, best_ask_qty))) = 
                    (book.best_bid(), book.best_ask()) {
                    info!(
                        "{} Order Book: Best Bid: {} @ {}, Best Ask: {} @ {}, Spread: {}, Synced: {}",
                        book.symbol,
                        best_bid_qty, best_bid_price,
                        best_ask_qty, best_ask_price,
                        best_ask_price - best_bid_price,
                        book.synced
                    );
                } else {
                    info!(
                        "{} Order Book: No valid bids/asks available, Synced: {}",
                        book.symbol, book.synced
                    );
                }
            }
        }
    });

    client.run().await
}