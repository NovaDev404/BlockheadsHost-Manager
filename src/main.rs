use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use chrono::{Local, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::os::unix::{
    ffi::OsStrExt,
    fs::{FileTypeExt, PermissionsExt},
};
use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::CString,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::RwLock,
};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    inner: Arc<RwLock<ManagerState>>,
    deletion_jobs: Arc<RwLock<HashMap<String, ServerDeletionJob>>>,
    token: Arc<str>,
    data_dir: PathBuf,
    server_binary: PathBuf,
    server_cwd: PathBuf,
    saves_dir: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
struct ServerDeletionJob {
    id: String,
    server_id: String,
    world_id: String,
    status: String,
    error: Option<String>,
}

#[derive(Deserialize)]
struct DeleteServer {
    world_id: String,
}

#[derive(Serialize)]
struct ServerDeletionView {
    id: String,
    server_id: String,
    status: String,
    error: Option<String>,
}

#[derive(Serialize)]
struct ServerStatusView {
    process_id: String,
    status: ProcessStatus,
    player_count: usize,
    uptime_seconds: u64,
}

#[derive(Serialize, Clone)]
struct ServerActivity {
    kind: String,
    message: String,
    timestamp: String,
}

#[derive(Default)]
struct ManagerState {
    processes: HashMap<String, ProcessRecord>,
    children: HashMap<String, Child>,
    console_inputs: HashMap<String, fs::File>,
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
    console_fifo: Option<PathBuf>,
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

#[derive(Deserialize)]
struct ServerCommand {
    command: String,
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
struct ServerLogFileView {
    date: String,
    from: Option<String>,
    till: Option<String>,
    size_bytes: u64,
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
        deletion_jobs: Arc::new(RwLock::new(HashMap::new())),
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
    load_deletion_jobs(&state).await?;
    resume_server_deletions(state.clone()).await;
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
        .route(
            "/v1/server-deletions/{id}",
            get(get_server_deletion).post(delete_server),
        )
        .route("/v1/servers/{id}/stop", post(stop_process))
        .route("/v1/servers/{id}/disconnect", post(disconnect_process))
        .route("/v1/servers/{id}/reconnect", post(reconnect_process))
        .route("/v1/servers/{id}/status", get(server_status))
        .route("/v1/servers/{id}/activity", get(server_activity))
        .route("/v1/servers/{id}/command", post(server_command))
        .route("/v1/servers/{id}/logs", get(server_logs))
        .route("/v1/servers/{id}/log-files", get(server_log_files))
        .route("/v1/servers/{id}/log-files/{date}", get(server_log_file))
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
        console_fifo: None,
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

    let deletion_in_progress = state
        .deletion_jobs
        .read()
        .await
        .values()
        .any(|job| job.server_id == id && matches!(job.status.as_str(), "pending" | "running"));
    if deletion_in_progress {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorView {
                error: "this server is being deleted".into(),
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
    let started_at = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let is_restart = manager
        .processes
        .get(&id)
        .is_some_and(|record| record.run_id.is_some());
    log_file
        .write_all(
            format!(
                "\n=== Starting run {run_id} at {started_at} ===\n=== Activity {} {started_at} ===\n",
                if is_restart {
                    "server_restarted"
                } else {
                    "server_started"
                }
            )
            .as_bytes(),
        )
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
    let console_dir = state.data_dir.join("console");
    fs::create_dir_all(&console_dir)
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let console_fifo = console_dir.join(format!("{id}.stdin"));
    ensure_fifo(&console_fifo).map_err(internal_error)?;
    let console_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&console_fifo)
        .map_err(|error| internal_error(error.to_string()))?;
    let console_stdin = console_file
        .try_clone()
        .map_err(|error| internal_error(error.to_string()))?;
    let args = blockheads_server_args(&input);
    let child = Command::new(&state.server_binary)
        .args(&args)
        .current_dir(&state.server_cwd)
        .stdin(std::process::Stdio::from(console_stdin))
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
    manager
        .console_inputs
        .insert(id.clone(), fs::File::from_std(console_file));
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
            console_fifo: None,
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
    record.console_fifo = Some(console_fifo);
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
    let was_running = pid.is_some_and(process_is_alive);
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
    manager.console_inputs.remove(&id);
    let record = manager
        .processes
        .get_mut(&id)
        .ok_or_else(|| not_found("process not found"))?;
    record.pid = None;
    record.status = ProcessStatus::Stopped;
    record.detached = false;
    record.updated_at = now();
    let stopped_log = (was_running && record.game.as_deref() == Some("blockheads_1"))
        .then(|| record.log_path.clone())
        .flatten();
    if let Some(log_path) = stopped_log {
        append_server_activity(&log_path, "server_stopped", now_activity_timestamp())
            .await
            .map_err(internal_error)?;
    }
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

async fn server_log_files(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ServerLogFileView>>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let record = {
        let manager = state.inner.read().await;
        manager.processes.get(&id).cloned()
    };
    let Some(record) = record.filter(|record| record.game.as_deref() == Some("blockheads_1"))
    else {
        return Ok(Json(Vec::new()));
    };
    let Some(log_path) = record.log_path else {
        return Ok(Json(Vec::new()));
    };
    let Some(logs_dir) = log_path.parent() else {
        return Err(internal_error("server log path has no parent directory"));
    };

    let mut entries = fs::read_dir(logs_dir)
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let mut log_files = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| internal_error(error.to_string()))?
    {
        let path = entry.path();
        let Some(date) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if path.extension().is_none_or(|extension| extension != "log")
            || NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err()
        {
            continue;
        }
        let metadata = entry
            .metadata()
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        if !metadata.is_file() {
            continue;
        }
        let (from, till) = read_log_time_range(&path)
            .await
            .map_err(|error| internal_error(error))?;
        log_files.push(ServerLogFileView {
            date: date.to_string(),
            from,
            till,
            size_bytes: metadata.len(),
        });
    }
    log_files.sort_by(|left, right| right.date.cmp(&left.date));
    Ok(Json(log_files))
}

async fn server_log_file(
    State(state): State<AppState>,
    Path((id, date)): Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    if NaiveDate::parse_from_str(&date, "%Y-%m-%d").is_err() {
        return Err(not_found("log file not found"));
    }
    let record = {
        let manager = state.inner.read().await;
        manager.processes.get(&id).cloned()
    };
    let Some(record) = record.filter(|record| record.game.as_deref() == Some("blockheads_1"))
    else {
        return Err(not_found("log file not found"));
    };
    let Some(log_path) = record.log_path else {
        return Err(not_found("log file not found"));
    };
    let Some(logs_dir) = log_path.parent() else {
        return Err(internal_error("server log path has no parent directory"));
    };
    let path = logs_dir.join(format!("{date}.log"));
    let contents = fs::read(path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            not_found("log file not found")
        } else {
            internal_error(error.to_string())
        }
    })?;
    let mut response = Response::new(Body::from(String::from_utf8_lossy(&contents).into_owned()));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    Ok(response)
}

async fn read_log_time_range(path: &FsPath) -> Result<(Option<String>, Option<String>), String> {
    let file = fs::File::open(path)
        .await
        .map_err(|error| error.to_string())?;
    let mut lines = BufReader::new(file);
    let mut from = None;
    let mut till = None;
    let mut line = Vec::new();
    while lines
        .read_until(b'\n', &mut line)
        .await
        .map_err(|error| error.to_string())?
        > 0
    {
        if let Some(timestamp) = parse_log_timestamp(&String::from_utf8_lossy(&line)) {
            from.get_or_insert_with(|| timestamp.clone());
            till = Some(timestamp);
        }
        line.clear();
    }
    Ok((from, till))
}

fn parse_log_timestamp(line: &str) -> Option<String> {
    let fields: Vec<_> = line.split_whitespace().collect();
    fields.windows(2).find_map(|pair| {
        let date = NaiveDate::parse_from_str(pair[0], "%Y-%m-%d").ok()?;
        let time = pair[1].trim_end_matches("===");
        let timestamp = NaiveDateTime::parse_from_str(
            &format!("{} {time}", date.format("%Y-%m-%d")),
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .ok()?;
        Some(timestamp.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
    })
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

async fn server_activity(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ServerActivity>>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let record = {
        let manager = state.inner.read().await;
        manager.processes.get(&id).cloned()
    };
    let Some(record) = record.filter(|record| record.game.as_deref() == Some("blockheads_1"))
    else {
        return Ok(Json(Vec::new()));
    };
    let Some(log_path) = record.log_path else {
        return Ok(Json(Vec::new()));
    };
    let Some(logs_dir) = log_path.parent() else {
        return Err(internal_error("server log path has no parent directory"));
    };
    let mut entries = fs::read_dir(logs_dir)
        .await
        .map_err(|error| internal_error(error.to_string()))?;
    let mut paths = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| internal_error(error.to_string()))?
    {
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "log") {
            paths.push(path);
        }
    }
    paths.sort_by(|left, right| right.file_name().cmp(&left.file_name()));

    const MAX_ACTIVITY_FILES: usize = 7;
    const MAX_ACTIVITY_FILE_BYTES: u64 = 256 * 1024;
    let mut activities = Vec::new();
    for path in paths.into_iter().take(MAX_ACTIVITY_FILES) {
        let mut file = fs::File::open(&path)
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        let length = file
            .metadata()
            .await
            .map_err(|error| internal_error(error.to_string()))?
            .len();
        file.seek(std::io::SeekFrom::Start(
            length.saturating_sub(MAX_ACTIVITY_FILE_BYTES),
        ))
        .await
        .map_err(|error| internal_error(error.to_string()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .await
            .map_err(|error| internal_error(error.to_string()))?;
        activities.extend(parse_server_activities(&String::from_utf8_lossy(&bytes)));
    }
    Ok(Json(latest_server_activities(activities)))
}

async fn delete_server(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<DeleteServer>,
) -> Result<(StatusCode, Json<ServerDeletionView>), (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    if !valid_world_id(&input.world_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "invalid world ID".into(),
            }),
        ));
    }

    let mut jobs = state.deletion_jobs.write().await;
    if let Some(existing) = jobs.values().find(|job| {
        job.server_id == id
            && job.world_id == input.world_id
            && matches!(job.status.as_str(), "pending" | "running" | "completed")
    }) {
        return Ok((
            StatusCode::ACCEPTED,
            Json(ServerDeletionView {
                id: existing.id.clone(),
                server_id: existing.server_id.clone(),
                status: existing.status.clone(),
                error: existing.error.clone(),
            }),
        ));
    }

    let job = ServerDeletionJob {
        id: Uuid::new_v4().to_string(),
        server_id: id,
        world_id: input.world_id,
        status: "pending".into(),
        error: None,
    };
    jobs.insert(job.id.clone(), job.clone());
    persist_deletion_jobs(&state, &jobs)
        .await
        .map_err(internal_error)?;
    drop(jobs);

    let worker_state = state.clone();
    let job_id = job.id.clone();
    tokio::spawn(async move {
        run_server_deletion(worker_state, job_id).await;
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(ServerDeletionView {
            id: job.id,
            server_id: job.server_id,
            status: job.status,
            error: None,
        }),
    ))
}

async fn get_server_deletion(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ServerDeletionView>, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let jobs = state.deletion_jobs.read().await;
    jobs.get(&id)
        .map(|job| {
            Json(ServerDeletionView {
                id: job.id.clone(),
                server_id: job.server_id.clone(),
                status: job.status.clone(),
                error: job.error.clone(),
            })
        })
        .ok_or_else(|| not_found("deletion job not found"))
}

async fn run_server_deletion(state: AppState, job_id: String) {
    let job = {
        let mut jobs = state.deletion_jobs.write().await;
        let Some(job) = jobs.get_mut(&job_id) else {
            return;
        };
        job.status = "running".into();
        job.error = None;
        let job = job.clone();
        if let Err(error) = persist_deletion_jobs(&state, &jobs).await {
            error!(deletion_id = %job_id, %error, "failed to persist server deletion state");
            return;
        }
        job
    };

    let result = async {
        let has_process = state
            .inner
            .read()
            .await
            .processes
            .contains_key(&job.server_id);
        if has_process {
            let _ = stop_process(State(state.clone()), Path(job.server_id.clone()))
                .await
                .map_err(|(_, Json(error))| error.error)?;
        }

        match fs::canonicalize(&state.saves_dir).await {
            Ok(saves_root) => {
                let world_path = saves_root.join(&job.world_id);
                match fs::symlink_metadata(&world_path).await {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(
                            "world directory is a symbolic link; refusing to remove it".into()
                        );
                    }
                    Ok(metadata) if metadata.is_dir() => {
                        force_remove_world_dir(&world_path)
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    Ok(_) => return Err("world path exists but is not a directory".into()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }

        let mut manager = state.inner.write().await;
        manager.processes.remove(&job.server_id);
        manager.children.remove(&job.server_id);
        drop(manager.console_inputs.remove(&job.server_id));
        persist_records(&state, &manager.processes).await?;
        drop(manager);
        let console_fifo = state
            .data_dir
            .join("console")
            .join(format!("{}.stdin", job.server_id));
        match fs::remove_file(console_fifo).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
        Ok::<(), String>(())
    }
    .await;

    if let Err(error) = &result {
        warn!(deletion_id = %job_id, server_id = %job.server_id, %error, "server deletion failed");
    }

    let mut jobs = state.deletion_jobs.write().await;
    if let Some(job) = jobs.get_mut(&job_id) {
        match result {
            Ok(()) => {
                job.status = "completed".into();
                job.error = None;
            }
            Err(error) => {
                job.status = "failed".into();
                job.error = Some(error);
            }
        }
        if let Err(error) = persist_deletion_jobs(&state, &jobs).await {
            error!(deletion_id = %job_id, %error, "failed to persist server deletion result");
        }
    }
}

async fn server_command(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<ServerCommand>,
) -> Result<StatusCode, (StatusCode, Json<ErrorView>)> {
    validate_id(&id)?;
    let command = input.command.trim();
    if command.is_empty()
        || command.len() > 512
        || command.chars().any(|character| character.is_control())
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorView {
                error: "command must be a single non-empty line of at most 512 characters".into(),
            }),
        ));
    }

    let mut manager = state.inner.write().await;
    let record = manager
        .processes
        .get(&id)
        .ok_or_else(|| not_found("server process not found"))?;
    if record.game.as_deref() != Some("blockheads_1") {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorView {
                error: "console commands are only available for Blockheads 1".into(),
            }),
        ));
    }
    let pid = record
        .pid
        .filter(|pid| process_is_alive(*pid))
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                Json(ErrorView {
                    error: "server is not running".into(),
                }),
            )
        })?;

    let console_fifo = record.console_fifo.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            Json(ErrorView {
                error: "restart this server once to enable console commands".into(),
            }),
        )
    })?;
    if !manager.console_inputs.contains_key(&id) {
        if !process_is_alive(pid) {
            return Err(not_found("server is not running"));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&console_fifo)
            .map_err(|error| {
                internal_error(format!("unable to open server console input: {error}"))
            })?;
        manager
            .console_inputs
            .insert(id.clone(), fs::File::from_std(file));
    }
    let mut line = command.as_bytes().to_vec();
    line.push(b'\n');
    let stdin = manager
        .console_inputs
        .get_mut(&id)
        .expect("console input was opened");
    stdin
        .write_all(&line)
        .await
        .map_err(|error| internal_error(format!("unable to send console command: {error}")))?;
    stdin
        .flush()
        .await
        .map_err(|error| internal_error(format!("unable to flush console command: {error}")))?;
    Ok(StatusCode::NO_CONTENT)
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

async fn load_deletion_jobs(state: &AppState) -> Result<(), Box<dyn std::error::Error>> {
    let path = state.data_dir.join("server-deletions.json");
    let contents = match fs::read_to_string(path).await {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut jobs: HashMap<String, ServerDeletionJob> = serde_json::from_str(&contents)?;
    for job in jobs.values_mut() {
        if matches!(job.status.as_str(), "pending" | "running") {
            job.status = "pending".into();
            job.error = None;
        }
    }
    let mut stored_jobs = state.deletion_jobs.write().await;
    *stored_jobs = jobs;
    persist_deletion_jobs(state, &stored_jobs).await?;
    Ok(())
}

async fn resume_server_deletions(state: AppState) {
    let pending: Vec<String> = state
        .deletion_jobs
        .read()
        .await
        .values()
        .filter(|job| job.status == "pending")
        .map(|job| job.id.clone())
        .collect();
    for job_id in pending {
        let worker_state = state.clone();
        tokio::spawn(async move {
            run_server_deletion(worker_state, job_id).await;
        });
    }
}

async fn persist_deletion_jobs(
    state: &AppState,
    jobs: &HashMap<String, ServerDeletionJob>,
) -> Result<(), String> {
    let path = state.data_dir.join("server-deletions.json");
    let temporary = state.data_dir.join("server-deletions.json.tmp");
    let contents = serde_json::to_vec_pretty(jobs).map_err(|error| error.to_string())?;
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
                    let log_path = manager
                        .processes
                        .get(&id)
                        .filter(|record| record.game.as_deref() == Some("blockheads_1"))
                        .and_then(|record| record.log_path.clone());
                    if let Some(record) = manager.processes.get_mut(&id) {
                        record.pid = None;
                        record.status = ProcessStatus::Exited;
                        record.updated_at = now();
                        if record.auto_restart {
                            restart_ids.push(id.clone());
                        }
                    }
                    if let Some(log_path) = log_path {
                        if let Err(error) = append_server_activity(
                            &log_path,
                            "server_stopped",
                            now_activity_timestamp(),
                        )
                        .await
                        {
                            error!(process_id = %id, %error, "failed to log server stop activity");
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
                let log_path = manager
                    .processes
                    .get(&id)
                    .filter(|record| record.game.as_deref() == Some("blockheads_1"))
                    .and_then(|record| record.log_path.clone());
                if let Some(record) = manager.processes.get_mut(&id) {
                    record.pid = None;
                    record.status = ProcessStatus::Exited;
                    record.updated_at = now();
                    if record.auto_restart {
                        restart_ids.push(id.clone());
                    }
                }
                if let Some(log_path) = log_path {
                    if let Err(error) = append_server_activity(
                        &log_path,
                        "server_stopped",
                        now_activity_timestamp(),
                    )
                    .await
                    {
                        error!(process_id = %id, %error, "failed to log server stop activity");
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

async fn force_remove_world_dir(root: &FsPath) -> Result<(), std::io::Error> {
    let mut pending = vec![(root.to_path_buf(), false)];
    while let Some((path, expanded)) = pending.pop() {
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let file_type = metadata.file_type();

        if file_type.is_symlink() || !file_type.is_dir() {
            if !file_type.is_symlink() {
                fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).await?;
            }
            fs::remove_file(path).await?;
        } else if expanded {
            fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).await?;
            fs::remove_dir(path).await?;
        } else {
            fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).await?;
            pending.push((path.clone(), true));
            let mut entries = fs::read_dir(path).await?;
            while let Some(entry) = entries.next_entry().await? {
                pending.push((entry.path(), false));
            }
        }
    }
    Ok(())
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

fn now_activity_timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

async fn append_server_activity(
    log_path: &FsPath,
    kind: &str,
    timestamp: String,
) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .append(true)
        .open(log_path)
        .await
        .map_err(|error| error.to_string())?;
    file.write_all(format!("=== Activity {kind} {timestamp} ===\n").as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    file.flush().await.map_err(|error| error.to_string())
}

fn parse_server_activities(contents: &str) -> Vec<ServerActivity> {
    let mut activities = Vec::new();
    for line in contents.lines() {
        if let Some(activity) = parse_server_activity_line(line) {
            activities.push(activity);
        }
    }
    activities
}

fn latest_server_activities(mut activities: Vec<ServerActivity>) -> Vec<ServerActivity> {
    activities.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
    activities.truncate(5);
    activities
}

fn parse_server_activity_line(line: &str) -> Option<ServerActivity> {
    let trimmed = line.trim();
    if let Some(marker) = trimmed.strip_prefix("=== Activity ") {
        let mut fields = marker.split_whitespace();
        let kind = fields.next()?;
        let date = fields.next()?;
        let time = fields.next()?.trim_end_matches("===");
        if !matches!(
            kind,
            "server_started" | "server_restarted" | "server_stopped"
        ) {
            return None;
        }
        return Some(ServerActivity {
            kind: kind.to_string(),
            message: match kind {
                "server_started" => "Server started",
                "server_restarted" => "Server restarted",
                _ => "Server stopped",
            }
            .to_string(),
            timestamp: format!("{date} {time}"),
        });
    }

    let timestamp = trimmed.get(..23)?;
    for (marker, kind, action) in [
        (" - Player Connected ", "player_join", "joined"),
        (" - Player Disconnected ", "player_leave", "left"),
    ] {
        let Some(start) = trimmed.find(marker) else {
            continue;
        };
        let player_name = trimmed[start + marker.len()..]
            .split(" | ")
            .next()
            .unwrap_or_default()
            .trim();
        if player_name.is_empty() {
            return None;
        }
        return Some(ServerActivity {
            kind: kind.to_string(),
            message: format!("Player {action}: {player_name}"),
            timestamp: timestamp.to_string(),
        });
    }
    None
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

fn ensure_fifo(path: &FsPath) -> Result<(), String> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| "console FIFO path contains a NUL byte".to_string())?;
    let result = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
    if result == 0 {
        return Ok(());
    }

    let error = std::io::Error::last_os_error();
    if error.raw_os_error() != Some(libc::EEXIST) {
        return Err(format!("unable to create console FIFO: {error}"));
    }

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("unable to inspect console FIFO: {error}"))?;
    if !metadata.file_type().is_fifo() {
        return Err("console input path exists but is not a FIFO".into());
    }
    Ok(())
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
    fn console_input_uses_a_private_reusable_fifo() {
        let directory = env::temp_dir().join(format!("blockheads-console-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).expect("create console directory");
        let fifo = directory.join("server.stdin");

        ensure_fifo(&fifo).expect("create console fifo");
        assert!(std::fs::symlink_metadata(&fifo)
            .expect("inspect console fifo")
            .file_type()
            .is_fifo());
        ensure_fifo(&fifo).expect("reuse console fifo");

        std::fs::remove_dir_all(directory).expect("remove console directory");
    }

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
    fn parses_activity_from_blockheads_player_log_lines() {
        let logs = concat!(
            "2026-10-01 08:09:03.998 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Connected CAPI-BARA | 68.6.109.102 | 3a4e9c0c2cdf97571fa3b664f96ac0f2\n",
            "2026-10-01 08:09:42.400 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Client disconnected:3a4e9c0c2cdf97571fa3b664f96ac0f2\n",
            "2026-10-01 08:09:42.501 blockheads_server171[1158115:1158115] SKEEVES ASTEROID - Player Disconnected CAPI-BARA\n",
        );
        let activities = parse_server_activities(logs);

        assert_eq!(activities.len(), 2);
        assert_eq!(activities[0].kind, "player_join");
        assert_eq!(activities[0].message, "Player joined: CAPI-BARA");
        assert_eq!(activities[0].timestamp, "2026-10-01 08:09:03.998");
        assert_eq!(activities[1].kind, "player_leave");
        assert_eq!(activities[1].message, "Player left: CAPI-BARA");
        assert_eq!(activities[1].timestamp, "2026-10-01 08:09:42.501");
    }

    #[test]
    fn parses_server_lifecycle_activity_markers() {
        let activities =
            parse_server_activities("=== Activity server_restarted 2026-10-01 08:09:03.998 ===\n");

        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].kind, "server_restarted");
        assert_eq!(activities[0].message, "Server restarted");
        assert_eq!(activities[0].timestamp, "2026-10-01 08:09:03.998");
    }

    #[test]
    fn parses_timestamps_from_output_and_activity_log_lines() {
        assert_eq!(
            parse_log_timestamp("2026-10-01 08:09:03.998 blockheads_server171[1:1] Server started"),
            Some("2026-10-01 08:09:03.998".into())
        );
        assert_eq!(
            parse_log_timestamp("=== Activity server_stopped 2026-10-01 08:09:42.5 ==="),
            Some("2026-10-01 08:09:42.500".into())
        );
        assert_eq!(parse_log_timestamp("unrecognized log line"), None);
    }

    #[tokio::test]
    async fn force_removes_read_only_world_contents_without_following_symlinks() {
        let directory = env::temp_dir().join(format!("blockheads-delete-{}", Uuid::new_v4()));
        let world = directory.join("world");
        let nested = world.join("nested");
        std::fs::create_dir_all(&nested).expect("create world directory");
        let saved_file = nested.join("world.dat");
        std::fs::write(&saved_file, "world data").expect("write world file");
        let outside_file = directory.join("outside.dat");
        std::fs::write(&outside_file, "keep").expect("write external file");
        std::os::unix::fs::symlink(&outside_file, world.join("external-link"))
            .expect("create external symlink");
        std::fs::set_permissions(&saved_file, std::fs::Permissions::from_mode(0o400))
            .expect("make world file read-only");
        std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o500))
            .expect("make nested directory read-only");
        std::fs::set_permissions(&world, std::fs::Permissions::from_mode(0o500))
            .expect("make world directory read-only");

        force_remove_world_dir(&world)
            .await
            .expect("force-remove world directory");

        assert!(!world.exists());
        assert_eq!(
            std::fs::read_to_string(outside_file).expect("read external file"),
            "keep"
        );
        std::fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn returns_only_the_five_newest_activity_events() {
        let activities = (1..=7)
            .map(|minute| ServerActivity {
                kind: "player_join".into(),
                message: format!("Player {minute} joined"),
                timestamp: format!("2026-10-01 08:{minute:02}:00.000"),
            })
            .collect();

        let latest = latest_server_activities(activities);

        assert_eq!(latest.len(), 5);
        assert_eq!(latest[0].message, "Player 7 joined");
        assert_eq!(latest[4].message, "Player 3 joined");
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
