use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;

use geo::GeoIndex;
use poem::{
    get, handler,
    listener::TcpListener,
    middleware::Tracing,
    web::{
        websocket::{Message, WebSocket},
        Data,
    },
    EndpointExt, Route, Server,
};

use clap::Parser;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct QueryParams {
    latitude: f32,
    longitude: f32,
}

/// Pbf query server
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Cached geo-index for faster load time
    #[arg(short, long, env)]
    cache: Option<String>,

    /// Path to pbf file
    #[arg(short, long, env)]
    pbf: String,
}

mod geo;

#[derive(serde::Serialize)]
struct Response<T> {
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(serde::Serialize)]
struct DataResponse {
    wikipedia: String,
}

#[handler]
async fn ws_handler(data: Data<&Arc<GeoIndex>>, ws: WebSocket) -> impl poem::IntoResponse {
    // Clone the Arc to avoid lifetime issues
    let geo_index = data.0.clone();

    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();

        while let Some(Ok(msg)) = stream.next().await {
            if let Message::Text(text) = msg {
                match serde_json::from_str::<QueryParams>(&text) {
                    Ok(params) => {
                        let response = if let Some(wikipedia) =
                            geo_index.find(params.latitude, params.longitude)
                        {
                            Response {
                                success: true,
                                data: Some(DataResponse { wikipedia }),
                                error: None,
                            }
                        } else {
                            Response {
                                success: false,
                                data: None,
                                error: Some("No address found".to_string()),
                            }
                        };

                        if let Ok(response_text) = serde_json::to_string(&response) {
                            if sink.send(Message::Text(response_text)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let error_response = Response::<DataResponse> {
                            success: false,
                            data: None,
                            error: Some(format!("Invalid query format: {}", e)),
                        };

                        if let Ok(response_text) = serde_json::to_string(&error_response) {
                            let _ = sink.send(Message::Text(response_text)).await;
                        }
                    }
                }
            }
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "poem=debug");
    }
    tracing_subscriber::fmt::init();

    let geo = match args.cache {
        Some(path) => {
            //check if file path exists
            match std::fs::File::open(&path) {
                Ok(file) => {
                    let start = std::time::Instant::now();
                    println!("load index from file");
                    let geo: GeoIndex = bincode::deserialize_from(file).unwrap();
                    println!("Loaded index in {}ms", start.elapsed().as_millis());
                    geo
                }
                Err(_e) => {
                    println!("cannot load index => rebuild");
                    let mut geo = GeoIndex::new();
                    geo.build(&args.pbf);
                    // save geo to file
                    std::fs::write(&path, bincode::serialize(&geo).unwrap())
                        .expect("Unable to write file");
                    geo
                }
            }
        }
        None => {
            let mut geo = GeoIndex::new();
            geo.build(&args.pbf);
            geo
        }
    };

    let app = Route::new()
        .at("/", get(ws_handler))
        .data(Arc::new(geo))
        .with(Tracing);
    Server::new(TcpListener::bind("0.0.0.0:3000"))
        .name("Fast-pbf-server")
        .run(app)
        .await
}
