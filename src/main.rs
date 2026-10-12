use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    process::{Child, Command},
    sync::RwLock,
};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    inner: Arc<RwLock<ManagerState>>,
    token: Arc<str>,
    data_dir: PathBuf,
    server_binary: PathBuf,
    server_cwd: PathBuf,
    saves_dir: PathBuf,
}

#[derive(Serialize)]
struct ServerStatusView {
    process_id: String,
    status: ProcessStatus,
    player_count: usize,
    uptime_seconds: u64,
}

#[derive(Default)]
struct ManagerState {
    processes: HashMap<String, ProcessRecord>,
    children: HashMap<String, Child>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ProcessRecord {
    id: String,
    command: String,
    args: Vec<String>,
    cwd: Option<String>,
    env: HashMap<String, String>,
    auto_restart: bool,
    pid: Option<u32>,
    status: ProcessStatus,
    created_at: u64,
    updated_at: u64,
    #[serde(default)]
    world_id: Option<String>,
    #[serde(default)]
    game: Option<String>,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    run_started_at: Option<u64>,
    #[serde(default)]
    log_path: Option<PathBuf>,
    #[serde(default)]
    run_offset: u64,
    #[serde(default)]
    detached: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ProcessStatus {
    Stopped,
    Running,
    Exited,
    Unknown,
}

#[derive(Deserialize)]
struct CreateProcess {
    id: Option<String>,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    auto_restart: bool,
}

#[derive(Serialize)]
struct ProcessView {
    id: String,
    command: String,
    args: Vec<String>,
    cwd: Option<String>,
    auto_restart: bool,
    pid: Option<u32>,
    status: ProcessStatus,
    created_at: u64,
    updated_at: u64,
    world_id: Option<String>,
    game: Option<String>,
    run_id: Option<String>,
    log_path: Option<String>,
    detached: bool,
}

#[derive(Serialize)]
struct LogView {
    process_id: String,
    content: String,
}

#[derive(Deserialize)]
struct StartServer {
    world_id: String,
    game: String,
    port: u16,
    max_players: u16,
    #[serde(default)]
    no_exit: bool,
}

fn blockheads_server_args(input: &StartServer) -> Vec<String> {
    let mut args = vec![
        "--load".to_string(),
        input.world_id.clone(),
        "--port".to_string(),
        input.port.to_string(),
        "--max_players".to_string(),
        input.max_players.to_string(),
        "--save_delay".to_string(),
        "3".to_string(),
    ];
    if input.no_exit {
        args.push("--no-exit".to_string());
    }
    args
}

#[derive(Deserialize)]
struct LogQuery {
    run_id: Option<String>,
    offset: Option<u64>,
    wait: Option<u64>,
}

#[derive(Serialize)]
struct ServerLogView {
    process_id: String,
    run_id: Option<String>,
    offset: u64,
    content: String,
    status: ProcessStatus,
}

#[derive(Serialize)]
struct ErrorView {
    error: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG").unwrap_or_else(|_| "blockheads_host_manager=info".into()),
        )
        .init();

    let data_dir =
        PathBuf::from(env::var("BLOCKHEADS_MANAGER_DATA").unwrap_or_else(|_| "./data".into()));
    fs::create_dir_all(data_dir.join("logs")).await?;

    let token =
        env::var("BLOCKHEADS_MANAGER_TOKEN").map_err(|_| "BLOCKHEADS_MANAGER_TOKEN must be set")?;
    if token.len() < 32 {
        return Err("BLOCKHEADS_MANAGER_TOKEN must be at least 32 characters".into());
    }

    let state = AppState {
        inner: Arc::new(RwLock::new(ManagerState::default())),
        token: Arc::from(token),
        data_dir,
        server_binary: PathBuf::from(
            env::var("BLOCKHEADS_SERVER_BINARY")
                .unwrap_or_else(|_| "./blockheads_server171".into()),
        ),
        server_cwd: PathBuf::from(env::var("BLOCKHEADS_SERVER_CWD").unwrap_or_else(|_| ".".into())),
        saves_dir: PathBuf::from(env::var("BLOCKHEADS_SAVES_DIR").unwrap_or_else(|_| {
            "/home/novadev/GNUstep/Library/ApplicationSupport/TheBlockheads/saves".into()
        })),
    };
    load_records(&state).await?;
    let monitor_state = state.clone();
    tokio::spawn(async move {
        monitor_processes(monitor_state).await;
    });

    let protected = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/processes", get(list_processes).post(create_process))
        .route(
            "/v1/processes/{id}",
            get(get_process).delete(delete_process),
        )
        .route("/v1/processes/{id}/start", post(start_process))
        .route("/v1/processes/{id}/stop", post(stop_process))
        .route("/v1/processes/{id}/restart", post(restart_process))
        .route("/v1/processes/{id}/logs", get(process_logs))
        .route("/v1/servers/{id}/start", post(start_server))
        .route("/v1/servers/{id}/stop", post(stop_process))
        .route("/v1/servers/{id}/disconnect", post(disconnect_process))
        .route("/v1/servers/{id}/reconnect", post(reconnect_process))
        .route("/v1/servers/{id}/status", get(server_status))
        .route("/v1/servers/{id}/logs", get(server_logs))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate));

    let app = Router::new().merge(protected).with_state(state);
    let bind = env::var("BLOCKHEADS_MANAGER_BIND").unwrap_or_else(|_| "127.0.0.1:4040".into());
    let address: SocketAddr = bind.parse()?;
    info!(%address, "manager listening");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn authenticate(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let supplied = request
        .headers()
        .get("x-manager-token")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
        });

    if supplied != Some(state.token.as_ref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true, "service": "blockheads-host-manager" }))
}

async fn list_processes(
    State(state): State<AppState>,
) -> Result<Json<Vec<ProcessView>>, (StatusCode, Json<ErrorView>)> {
    reconcile(&state).await.map_err(internal_error)?;
    let manager = state.inner.read().await;
    Ok(Json(
        manager.processes.values().map(ProcessView::from).collect(),
    ))
}

async fn get_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    reconcile(&state).await.map_err(internal_error)?;
    let manager = state.inner.read().await;
    manager
        .processes
        .get(&id)
        .map(ProcessView::from)
        .map(Json)
        .ok_or_else(|| not_found("process not found"))
}

async fn create_process(
    State(state): State<AppState>,
    Json(input): Json<CreateProcess>,
) -> Result<(StatusCode, Json<ProcessView>), (StatusCode, Json<ErrorView>)> {
    validate_command(&input)?;
    let id = input.id.unwrap_or_else(|| Uuid::new_v4().to_string());
    validate_id(&id)?;
    let now = now();
    let record = ProcessRecord {
        id: id.clone(),
        command: input.command,
        args: input.args,
        cwd: input.cwd,
        env: input.env,
        auto_restart: input.auto_restart,
        pid: None,
        status: ProcessStatus::Stopped,
        created_at: now,
        updated_at: now,
        world_id: None,
        game: None,
        run_id: None,
        run_started_at: None,
        log_path: None,
        run_offset: 0,
        detached: false,
    };

    let mut manager = state.inner.write().await;
    if manager.processes.contains_key(&id) {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorView {
                error: "process already exists".into(),
            }),
        ));
    }
    manager.processes.insert(id.clone(), record);
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    let view = manager.processes.get(&id).map(ProcessView::from);
    drop(manager);
    view.map(|view| (StatusCode::CREATED, Json(view)))
        .ok_or_else(|| internal_error("failed to create process"))
}

async fn start_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let mut manager = state.inner.write().await;
    let record = manager
        .processes
        .get(&id)
        .cloned()
        .ok_or_else(|| not_found("process not found"))?;
    if record.pid.is_some_and(process_is_alive) {
        return Ok(Json(ProcessView::from(&record)));
    }

    let log_path = state.data_dir.join("logs").join(format!("{id}.log"));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let stderr = log
        .try_clone()
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let mut command = Command::new(&record.command);
    command.args(&record.args);
    if let Some(cwd) = &record.cwd {
        command.current_dir(cwd);
    }
    command.envs(&record.env);
    command
        .stdin(std::process::Stdio::null())
        .stdout(log.into_std().await)
        .stderr(stderr.into_std().await);
    let child = command.spawn().map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: error.to_string(),
            }),
        )
    })?;
    let pid = child.id();
    manager.children.insert(id.clone(), child);
    if let Some(record) = manager.processes.get_mut(&id) {
        record.pid = pid;
        record.status = ProcessStatus::Running;
        record.detached = false;
        record.updated_at = now();
    }
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(Json(ProcessView::from(
        manager.processes.get(&id).expect("record exists"),
    )))
}

async fn start_server(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<StartServer>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    if input.game != "blockheads_1" {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorView {
                error: "Blockheads 2 servers are not available yet".into(),
            }),
        ));
    }
    if !valid_world_id(&input.world_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "world_id must contain only letters, numbers, underscores, or hyphens"
                    .into(),
            }),
        ));
    }
    let world_id = input.world_id.clone();
    if input.port == 0 || input.max_players == 0 || input.max_players > 128 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "port and max_players must be valid positive values".into(),
            }),
        ));
    }

    let mut manager = state.inner.write().await;
    if let Some(record) = manager.processes.get(&id) {
        if record.pid.is_some_and(process_is_alive) {
            if record.world_id.as_deref() != Some(input.world_id.as_str()) {
                return Err((
                    StatusCode::CONFLICT,
                    Json(ErrorView {
                        error: "this process is already running a different world".into(),
                    }),
                ));
            }
            return Ok(Json(ProcessView::from(record)));
        }
    }

    let logs_dir = state.saves_dir.join(&world_id).join("logs");
    fs::create_dir_all(&logs_dir)
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let (log_path, mut log_file) = open_daily_log(&logs_dir).await.map_err(internal_error)?;
    let run_id = Uuid::new_v4().to_string();
    let started_at = Local::now().format("%Y-%m-%d %H:%M:%S %:z");
    log_file
        .write_all(format!("\n=== Starting run {run_id} at {started_at} ===\n").as_bytes())
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    log_file
        .flush()
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let run_offset = log_file
        .metadata()
        .await
        .map_err(|error| internal_error(error.to_string()))?
        .len();
    let stderr = log_file
        .try_clone()
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let args = blockheads_server_args(&input);
    let child = Command::new(&state.server_binary)
        .args(&args)
        .current_dir(&state.server_cwd)
        .stdin(std::process::Stdio::null())
        .stdout(log_file.into_std().await)
        .stderr(stderr.into_std().await)
        .spawn()
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorView {
                    error: format!("unable to start Blockheads server: {error}"),
                }),
            )
        })?;

    let pid = child.id();
    manager.children.insert(id.clone(), child);
    let timestamp = now();
    let record = manager
        .processes
        .entry(id.clone())
        .or_insert_with(|| ProcessRecord {
            id: id.clone(),
            command: String::new(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            auto_restart: false,
            pid: None,
            status: ProcessStatus::Stopped,
            created_at: timestamp,
            updated_at: timestamp,
            world_id: None,
            game: None,
            run_id: None,
            run_started_at: None,
            log_path: None,
            run_offset: 0,
            detached: false,
        });
    record.command = state.server_binary.to_string_lossy().into_owned();
    record.args = args;
    record.cwd = Some(state.server_cwd.to_string_lossy().into_owned());
    record.world_id = Some(input.world_id);
    record.game = Some(input.game);
    record.pid = pid;
    record.status = ProcessStatus::Running;
    record.run_id = Some(run_id);
    record.run_started_at = Some(timestamp);
    record.log_path = Some(log_path);
    record.run_offset = run_offset;
    record.detached = false;
    record.updated_at = timestamp;

    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(Json(ProcessView::from(
        manager.processes.get(&id).expect("record exists"),
    )))
}

async fn stop_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let mut manager = state.inner.write().await;
    let pid = manager
        .processes
        .get(&id)
        .ok_or_else(|| not_found("process not found"))?
        .pid;
    if let Some(pid) = pid {
        if process_is_alive(pid) {
            if let Some(child) = manager.children.get_mut(&id) {
                send_signal(pid, "TERM").map_err(internal_error)?;
                match tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => return Err(internal_error(error.to_string())),
                    Err(_) => child
                        .kill()
                        .await
                        .map_err(|error| internal_error(error.to_string()))?,
                }
            } else {
                terminate(pid).await.map_err(internal_error)?;
            }
        }
    }
    manager.children.remove(&id);
    let record = manager
        .processes
        .get_mut(&id)
        .ok_or_else(|| not_found("process not found"))?;
    record.pid = None;
    record.status = ProcessStatus::Stopped;
    record.detached = false;
    record.updated_at = now();
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(Json(ProcessView::from(
        manager.processes.get(&id).expect("record exists"),
    )))
}

async fn disconnect_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let mut manager = state.inner.write().await;
    let record = manager
        .processes
        .get_mut(&id)
        .ok_or_else(|| not_found("process not found"))?;
    if !record.pid.is_some_and(process_is_alive) {
        return Err(not_found("process is not running"));
    }
    record.detached = true;
    record.updated_at = now();
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(Json(ProcessView::from(
        manager.processes.get(&id).expect("record exists"),
    )))
}

async fn reconnect_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let mut manager = state.inner.write().await;
    let record = manager
        .processes
        .get_mut(&id)
        .ok_or_else(|| not_found("process not found"))?;
    if !record.pid.is_some_and(process_is_alive) {
        record.pid = None;
        record.status = ProcessStatus::Exited;
        record.updated_at = now();
        return Err(not_found("process is not running"));
    }
    record.status = ProcessStatus::Running;
    record.detached = false;
    record.updated_at = now();
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(Json(ProcessView::from(
        manager.processes.get(&id).expect("record exists"),
    )))
}

async fn restart_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ProcessView>, (StatusCode, Json<ErrorView>)> {
    let _ = stop_process(State(state.clone()), Path(id.clone())).await?;
    start_process(State(state), Path(id)).await
}

async fn delete_process(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorView>)> {
    let _ = stop_process(State(state.clone()), Path(id.clone())).await?;
    let mut manager = state.inner.write().await;
    manager.processes.remove(&id);
    persist_records(&state, &manager.processes)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn process_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<LogView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let manager = state.inner.read().await;
    if !manager.processes.contains_key(&id) {
        return Err(not_found("process not found"));
    }
    drop(manager);
    let path = state.data_dir.join("logs").join(format!("{id}.log"));
    let content = match fs::read_to_string(path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(internal_error(error.to_string())),
    };
    let content = content
        .chars()
        .rev()
        .take(64 * 1024)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    Ok(Json(LogView {
        process_id: id,
        content,
    }))
}

async fn server_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<LogQuery>,
) -> Result<Json<ServerLogView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let wait = query.wait.unwrap_or(20).min(25);
    let started = tokio::time::Instant::now();

    loop {
        let (path, run_id, run_offset, status) = {
            let manager = state.inner.read().await;
            let Some(record) = manager.processes.get(&id) else {
                return Ok(Json(ServerLogView {
                    process_id: id,
                    run_id: None,
                    offset: 0,
                    content: String::new(),
                    status: ProcessStatus::Stopped,
                }));
            };
            (
                record.log_path.clone(),
                record.run_id.clone(),
                record.run_offset,
                record.status.clone(),
            )
        };

        let offset = match query.run_id.as_deref() {
            None => run_offset,
            Some(requested_run) if Some(requested_run) != run_id.as_deref() => run_offset,
            Some(_) => query.offset.unwrap_or(run_offset),
        };
        let Some(path) = path else {
            return Ok(Json(ServerLogView {
                process_id: id,
                run_id,
                offset: 0,
                content: String::new(),
                status,
            }));
        };
        let (content, next_offset) = match fs::File::open(path).await {
            Ok(mut file) => {
                let file_length = file
                    .metadata()
                    .await
                    .map_err(|error| internal_error(error.to_string()))?
                    .len();
                let safe_offset = offset.min(file_length);
                file.seek(std::io::SeekFrom::Start(safe_offset))
                    .await
                    .map_err(|error| internal_error(error.to_string()))?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)
                    .await
                    .map_err(|error| internal_error(error.to_string()))?;
                (
                    String::from_utf8_lossy(&bytes).into_owned(),
                    safe_offset + bytes.len() as u64,
                )
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (String::new(), 0),
            Err(error) => return Err(internal_error(error.to_string())),
        };
        if !content.is_empty()
            || status != ProcessStatus::Running
            || wait == 0
            || started.elapsed() >= std::time::Duration::from_secs(wait)
        {
            return Ok(Json(ServerLogView {
                process_id: id,
                run_id,
                offset: next_offset,
                content,
                status,
            }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
}

async fn server_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ServerStatusView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let record = {
        let manager = state.inner.read().await;
        manager.processes.get(&id).cloned()
    };
    let Some(record) = record else {
        return Ok(Json(ServerStatusView {
            process_id: id,
            status: ProcessStatus::Stopped,
            player_count: 0,
            uptime_seconds: 0,
        }));
    };

    let is_running =
        record.status == ProcessStatus::Running && record.pid.is_some_and(process_is_alive);
    let player_count = if is_running {
        let Some(log_path) = record.log_path.as_ref() else {
            return Err(internal_error("running server has no log path"));
        };
        let mut file = fs::File::open(log_path)
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        let file_length = file
            .metadata()
            .await
            .map_err(|error| internal_error(error.to_string()))?
            .len();
        const MAX_PLAYER_LOG_BYTES: u64 = 256 * 1024;
        let log_start = record
            .run_offset
            .max(file_length.saturating_sub(MAX_PLAYER_LOG_BYTES));
        file.seek(std::io::SeekFrom::Start(log_start))
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        count_active_players(&String::from_utf8_lossy(&bytes))
    } else {
        0
    };
    let current_time = now();
    let uptime_seconds = if is_running {
        current_time.saturating_sub(
            record
                .run_started_at
                .unwrap_or(record.updated_at)
                .min(current_time),
        )
    } else {
        0
    };

    Ok(Json(ServerStatusView {
        process_id: id,
        status: if is_running {
            ProcessStatus::Running
        } else {
            ProcessStatus::Stopped
        },
        player_count,
        uptime_seconds,
    }))
}

fn count_active_players(log: &str) -> usize {
    let mut players = HashSet::new();
    for line in log.lines() {
        let Some((connected, raw_name)) = [
            (" - Player Connected ", true),
            (" - Player Disconnected ", false),
        ]
        .iter()
        .find_map(|(marker, connected)| {
            line.to_ascii_lowercase()
                .find(&marker.to_ascii_lowercase())
                .map(|index| (*connected, &line[index + marker.len()..]))
        }) else {
            continue;
        };
        let name = raw_name
            .split('|')
            .next()
            .unwrap_or_default()
            .trim()
            .to_lowercase();
        if name.is_empty() {
            continue;
        }
        if connected {
            players.insert(name);
        } else {
            players.remove(&name);
        }
    }
    players.len()
}

async fn load_records(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    let path = state.data_dir.join("processes.json");
    let contents = match fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let records: HashMap<String, ProcessRecord> = serde_json::from_str(&contents)?;
    let mut manager = state.inner.write().await;
    manager.processes = records;
    for record in manager.processes.values_mut() {
        if let Some(pid) = record.pid {
            if process_is_alive(pid) {
                record.status = ProcessStatus::Running;
                if record.run_started_at.is_none() {
                    record.run_started_at = Some(record.updated_at);
                }
            } else {
                record.pid = None;
                record.status = ProcessStatus::Exited;
            }
        }
    }
    persist_records(state, &manager.processes).await?;
    Ok(())
}

async fn reconcile(state: &AppState) -> Result<(), String> {
    let mut manager = state.inner.write().await;
    let ids: Vec<String> = manager.processes.keys().cloned().collect();
    for id in ids {
        let Some(record) = manager.processes.get_mut(&id) else {
            continue;
        };
        if let Some(pid) = record.pid {
            if !process_is_alive(pid) {
                record.pid = None;
                record.status = ProcessStatus::Exited;
                record.updated_at = now();
                if record.auto_restart {
                    warn!(process_id = %id, "managed process exited; restart is enabled");
                }
            }
        }
    }
    persist_records(state, &manager.processes).await
}

async fn monitor_processes(state: AppState) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        interval.tick().await;
        let restart_ids = {
            let mut manager = state.inner.write().await;
            let mut restart_ids = Vec::new();
            let child_ids: Vec<String> = manager.children.keys().cloned().collect();
            for id in child_ids {
                let exited = match manager.children.get_mut(&id) {
                    Some(child) => match child.try_wait() {
                        Ok(Some(status)) => {
                            info!(process_id = %id, ?status, "managed process exited");
                            true
                        }
                        Ok(None) => false,
                        Err(error) => {
                            error!(process_id = %id, %error, "failed to poll managed process");
                            true
                        }
                    },
                    None => false,
                };
                if exited {
                    manager.children.remove(&id);
                    if let Some(record) = manager.processes.get_mut(&id) {
                        record.pid = None;
                        record.status = ProcessStatus::Exited;
                        record.updated_at = now();
                        if record.auto_restart {
                            restart_ids.push(id.clone());
                        }
                    }
                }
            }
            let orphaned_ids: Vec<String> = manager
                .processes
                .iter()
                .filter_map(|(id, record)| {
                    (record.pid.is_some()
                        && !manager.children.contains_key(id)
                        && !process_is_alive(record.pid.expect("pid exists")))
                    .then_some(id.clone())
                })
                .collect();
            for id in orphaned_ids {
                if let Some(record) = manager.processes.get_mut(&id) {
                    record.pid = None;
                    record.status = ProcessStatus::Exited;
                    record.updated_at = now();
                    if record.auto_restart {
                        restart_ids.push(id);
                    }
                }
            }
            if let Err(error) = persist_records(&state, &manager.processes).await {
                error!(%error, "failed to persist process state");
            }
            restart_ids
        };
        for id in restart_ids {
            if let Err((_, Json(error))) =
                start_process(State(state.clone()), Path(id.clone())).await
            {
                error!(process_id = %id, message = %error.error, "automatic process restart failed");
            }
        }
    }
}

async fn persist_records(
    state: &AppState,
    records: &HashMap<String, ProcessRecord>,
) -> Result<(), String> {
    let path = state.data_dir.join("processes.json");
    let temporary = state.data_dir.join("processes.json.tmp");
    let contents = serde_json::to_vec_pretty(records).map_err(|error| error.to_string())?;
    let mut file = fs::File::create(&temporary)
        .await
        .map_err(|error| error.to_string())?;
    file.write_all(&contents)
        .await
        .map_err(|error| error.to_string())?;
    file.sync_all().await.map_err(|error| error.to_string())?;
    fs::rename(temporary, path)
        .await
        .map_err(|error| error.to_string())
}

async fn open_daily_log(directory: &FsPath) -> Result<(PathBuf, tokio::fs::File), std::io::Error> {
    let date = Local::now().format("%Y-%m-%d");
    let path = directory.join(format!("{date}.log"));
    let file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&path)
        .await?;
    Ok((path, file))
}

fn validate_command(input: &CreateProcess) -> Result<(), (StatusCode, Json<ErrorView>)> {
    if input.command.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "command is required".into(),
            }),
        ));
    }
    if input.command.len() > 4096 || input.args.iter().any(|arg| arg.len() > 4096) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "command or argument is too long".into(),
            }),
        ));
    }
    if let Some(cwd) = &input.cwd {
        if !FsPath::new(cwd).is_absolute() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorView {
                    error: "cwd must be an absolute path".into(),
                }),
            ));
        }
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), (StatusCode, Json<ErrorView>)> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "invalid process id".into(),
            }),
        ));
    }
    Ok(())
}

fn valid_world_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn process_is_alive(pid: u32) -> bool {
    FsPath::new("/proc").join(pid.to_string()).exists()
}

async fn terminate(pid: u32) -> Result<(), String> {
    send_signal(pid, "TERM")?;
    for _ in 0..50 {
        if !process_is_alive(pid) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    send_signal(pid, "KILL")?;
    for _ in 0..20 {
        if !process_is_alive(pid) {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(format!("process {pid} did not stop"))
}

fn send_signal(pid: u32, signal: &str) -> Result<(), String> {
    let status = std::process::Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() || !process_is_alive(pid) {
        Ok(())
    } else {
        Err(format!("failed to send {signal} to process {pid}"))
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn internal_error(error: impl ToString) -> (StatusCode, Json<ErrorView>) {
    error!(message = %error.to_string(), "manager request failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorView {
            error: "internal manager error".into(),
        }),
    )
}

fn not_found(message: &str) -> (StatusCode, Json<ErrorView>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorView {
            error: message.into(),
        }),
    )
}

impl From<&ProcessRecord> for ProcessView {
    fn from(record: &ProcessRecord) -> Self {
        Self {
            id: record.id.clone(),
            command: record.command.clone(),
            args: record.args.clone(),
            cwd: record.cwd.clone(),
            auto_restart: record.auto_restart,
            pid: record.pid,
            status: record.status.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
            world_id: record.world_id.clone(),
            game: record.game.clone(),
            run_id: record.run_id.clone(),
            log_path: record
                .log_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned()),
            detached: record.detached,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_players_from_blockheads_connection_log_lines() {
        let logs = concat!(
            "2026-10-01 08:09:03.998 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Connected CAPI-BARA | 68.6.109.102 | 3a4e9c0c2cdf97571fa3b664f96ac0f2\n",
            "2026-10-10 12:07:56.429 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Connected DEM_KAMPUCHEA | 99.71.236.0 | c30b57bb7f04f5dc11c2147beab0b285\n",
        );
        assert_eq!(count_active_players(logs), 2);

        let logs = concat!(
            "2026-10-01 08:09:03.998 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Connected CAPI-BARA | 68.6.109.102 | 3a4e9c0c2cdf97571fa3b664f96ac0f2\n",
            "2026-10-01 08:09:42.501 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Disconnected CAPI-BARA\n",
        );
        assert_eq!(count_active_players(logs), 0);
    }

    #[test]
    fn blockheads_start_args_include_save_delay_and_plan_gated_no_exit() {
        let base = StartServer {
            world_id: "world123".into(),
            game: "blockheads_1".into(),
            port: 30_123,
            max_players: 8,
            no_exit: false,
        };
        assert_eq!(
            blockheads_server_args(&base),
            [
                "--load",
                "world123",
                "--port",
                "30123",
                "--max_players",
                "8",
                "--save_delay",
                "3",
            ]
        );

        let premium = StartServer {
            no_exit: true,
            ..base
        };
        assert_eq!(
            blockheads_server_args(&premium).last().map(String::as_str),
            Some("--no-exit")
        );
    }

    #[tokio::test]
    async fn starts_on_the_same_local_date_append_to_one_log() {
        let directory = env::temp_dir().join(format!("blockheads-manager-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory)
            .await
            .expect("create test directory");

        let (first_path, mut first_file) = open_daily_log(&directory).await.expect("first log");
        let (second_path, mut second_file) = open_daily_log(&directory).await.expect("second log");
        let prefix = Local::now().format("%Y-%m-%d").to_string();
        let expected_name = format!("{prefix}.log");
        first_file
            .write_all(b"first run\n")
            .await
            .expect("write first run");
        second_file
            .write_all(b"second run\n")
            .await
            .expect("write second run");

        assert_eq!(first_path, second_path);
        assert_eq!(
            first_path.file_name().and_then(|name| name.to_str()),
            Some(expected_name.as_str())
        );

        drop((first_file, second_file));
        let content = fs::read_to_string(&first_path)
            .await
            .expect("read daily log");
        assert_eq!(content, "first run\nsecond run\n");
        fs::remove_dir_all(directory)
            .await
            .expect("remove test directory");
    }
}
