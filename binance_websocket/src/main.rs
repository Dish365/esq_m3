use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream};
use url::Url;
use std::error::Error;
use dashmap::DashMap;
use deadpool::managed::Pool;
use tracing::{info, error, warn};
use tracing_subscriber;
use async_trait;

// Configuration constants
const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";
const RECONNECT_DELAY_MS: u64 = 1000;
const MAX_RECONNECT_ATTEMPTS: u8 = 5;

// Data structures for Binance WebSocket messages
#[derive(Debug, Deserialize, Serialize, Clone)]
struct BookTickerData {
    u: String,    // Order book updateId
    s: String,    // Symbol
    b: String,    // Best bid price
    B: String,    // Best bid qty
    a: String,    // Best ask price
    A: String,    // Best ask qty
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TradeData {
    e: String,    // Event type
    E: u64,       // Event time
    s: String,    // Symbol
    t: String,    // Trade ID
    p: String,    // Price
    q: String,    // Quantity
    b: String,    // Buyer order ID
    a: String,    // Seller order ID
    T: u64,       // Trade time
    m: bool,      // Is the buyer the market maker?
    M: bool,      // Ignore
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
}

impl MarketDataCache {
    fn new() -> Self {
        Self {
            book_tickers: DashMap::new(),
            last_trades: DashMap::new(),
        }
    }

    fn update_book_ticker(&self, ticker: BookTickerData) {
        let symbol = ticker.s.to_uppercase();
        self.book_tickers.insert(symbol, ticker);
    }

    fn update_trade(&self, trade: TradeData) {
        let symbol = trade.s.to_uppercase();
        self.last_trades.insert(symbol, trade);
    }
}

// WebSocket client for Binance API
struct BinanceWebSocketClient {
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    symbols: Vec<String>,
    cache: Arc<MarketDataCache>,
    reconnect_attempts: u8,
    last_update: std::time::Instant,
}

impl BinanceWebSocketClient {
    fn new(symbols: Vec<String>) -> Self {
        Self {
            ws_stream: None,
            symbols,
            cache: Arc::new(MarketDataCache::new()),
            reconnect_attempts: 0,
            last_update: std::time::Instant::now(),
        }
    }

    /// Establishes a connection to the Binance WebSocket API
    async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let stream_query = self.symbols.iter()
            .flat_map(|symbol| {
                let symbol = symbol.to_lowercase();
                vec![format!("{}@bookTicker", symbol), format!("{}@trade", symbol)]
            })
            .collect::<Vec<_>>()
            .join("/");
        let ws_url = format!("{}?streams={}", BINANCE_WS_URL, stream_query);
        let url = Url::parse(&ws_url)?;
        let connect_future = connect_async(url);
        let (ws_stream, _) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connect_future
        ).await??;
        self.ws_stream = Some(ws_stream);
        self.reconnect_attempts = 0;
        Ok(())
    }

    /// Processes incoming WebSocket messages
    async fn process_messages(&mut self) -> Result<(), Box<dyn Error>> {
        if let Some(ws_stream) = &mut self.ws_stream {
            let (write, mut read) = ws_stream.split();
            let write = Arc::new(tokio::sync::Mutex::new(write));

            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        if self.last_update.elapsed() < std::time::Duration::from_millis(100) {
                            continue;
                        }
                        self.last_update = std::time::Instant::now();
                        match serde_json::from_str::<StreamMessage>(&text) {
                            Ok(stream_msg) => {
                                let stream_parts: Vec<&str> = stream_msg.stream.split('@').collect();
                                if stream_parts.len() == 2 {
                                    let stream_type = stream_parts[1];
                                    match stream_type {
                                        "bookTicker" => {
                                            if let Ok(ticker) = serde_json::from_value::<BookTickerData>(stream_msg.data) {
                                                self.cache.update_book_ticker(ticker);
                                            }
                                        }
                                        "trade" => {
                                            if let Ok(trade) = serde_json::from_value::<TradeData>(stream_msg.data) {
                                                self.cache.update_trade(trade);
                                            }
                                        }
                                        _ => {
                                            warn!("Unknown stream type: {}", stream_type);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error parsing stream message: {}", e);
                                error!("Message: {}", text);
                            }
                        }
                    }
                    Ok(Message::Ping(data)) => {
                        let mut write_lock = write.lock().await;
                        write_lock.send(Message::Pong(data)).await?;
                    }
                    Ok(Message::Close(frame)) => {
                        error!("WebSocket closed: {:?}", frame);
                        break;
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// Runs the WebSocket client with reconnection logic
    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            if self.reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                return Err("Max reconnect attempts reached".into());
            }
            match self.connect().await {
                Ok(_) => {
                    info!("Connected to Binance WebSocket");
                    if let Err(e) = self.process_messages().await {
                        error!("Error processing messages: {}", e);
                    }
                    self.reconnect_attempts += 1;
                    error!("Connection lost. Reconnect attempt {} of {}", 
                             self.reconnect_attempts, MAX_RECONNECT_ATTEMPTS);
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_DELAY_MS)).await;
                }
                Err(e) => {
                    error!("Failed to connect: {}", e);
                    self.reconnect_attempts += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_DELAY_MS)).await;
                }
            }
        }
    }

    /// Retrieves the latest book ticker for a symbol
    fn get_book_ticker(&self, symbol: &str) -> Option<BookTickerData> {
        let symbol = symbol.to_uppercase();
        self.cache.book_tickers
            .get(&symbol)
            .map(|r| r.value().clone())
    }

    /// Retrieves the last trade for a symbol
    fn get_last_trade(&self, symbol: &str) -> Option<TradeData> {
        let symbol = symbol.to_uppercase();
        self.cache.last_trades
            .get(&symbol)
            .map(|r| r.value().clone())
    }
}

// Implement proper connection pooling
struct WebSocketManager;

#[async_trait::async_trait]
impl deadpool::managed::Manager for WebSocketManager {
    type Type = WebSocketStream<MaybeTlsStream<TcpStream>>;
    type Error = Box<dyn Error + Send + Sync>;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let url = Url::parse(BINANCE_WS_URL)?;
        let (ws_stream, _) = connect_async(url).await?;
        Ok(ws_stream)
    }

    async fn recycle(&self, _conn: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}

struct ConnectionPool {
    pool: deadpool::managed::Pool<WebSocketManager>,
}

impl ConnectionPool {
    async fn new() -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mgr = WebSocketManager;
        let pool = deadpool::managed::Pool::builder(mgr)
            .max_size(5)
            .build()?;
        Ok(Self { pool })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Define symbols to subscribe to
    let symbols = vec!["btcusdt".to_string(), "ethusdt".to_string()];
    
    // Initialize the WebSocket client
    let mut client = BinanceWebSocketClient::new(symbols);
    
    // Spawn a task to periodically access and display data
    tokio::spawn(async move {
        loop {
            {
                let cache_guard = client.cache.book_tickers.iter();
                for ticker in cache_guard {
                    info!("BTC bid: {}, ask: {}", ticker.b, ticker.a);
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });
    
    // Start the WebSocket client
    client.run().await?;
    
    tracing_subscriber::fmt::init();
    
    Ok(())
}