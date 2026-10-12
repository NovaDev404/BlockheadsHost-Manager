# BlockheadsHost Manager

BlockheadsHost Manager is the long-lived process supervisor for game servers.
Run it separately from the Next.js application so rebuilding or restarting the
web application does not stop managed game-server processes.

## Run locally

```bash
export BLOCKHEADS_MANAGER_TOKEN="$(openssl rand -hex 32)"
export BLOCKHEADS_MANAGER_DATA="$PWD/data"
cargo run
```

The API listens on `127.0.0.1:4040` by default. Change the bind address with
`BLOCKHEADS_MANAGER_BIND`; do not expose this API directly to the public
internet. It can execute arbitrary programs, so keep it on a private network
and protect it with a high-entropy `BLOCKHEADS_MANAGER_TOKEN`.

## Configure the Blockheads server

Put the `blockheads_server171` executable in the manager's working directory
(or set `BLOCKHEADS_SERVER_BINARY` to its path). The manager starts it with an
argument vector, never through a shell:

```text
./blockheads_server171 --load WORLD_ID --port PORT --max_players AMOUNT --save_delay 3
```

For Sapphire and Diamond plans, BlockheadsHost requests `--no-exit` when starting
the world. Amethyst starts omit that flag.

By default the world save and run logs are under:

```text
/home/novadev/GNUstep/Library/ApplicationSupport/TheBlockheads/saves/WORLD_ID/logs/
```

Set `BLOCKHEADS_SAVES_DIR` to override that saves directory. Each start creates
one local-date file (`YYYY-MM-DD.log`) and appends a run marker plus that run's
output. Restarts on the same day stay together in that date's file; the first
start on the next local date creates a new file. Output is written directly
while the game process runs. `BLOCKHEADS_SERVER_CWD` sets the executable's
working directory; it defaults to the manager's working directory.

Blockheads 2 option is reserved but currently returns HTTP 501 until its
server executable is added.

## Configure BlockheadsHost

Set these server-only environment variables in the Next.js application:

```dotenv
BLOCKHEADS_MANAGER_URL=http://127.0.0.1:4040
BLOCKHEADS_MANAGER_TOKEN=same-token-used-by-the-manager
```

Apply [`supabase/world-id-mapping.sql`](../BlockheadsHost/supabase/world-id-mapping.sql)
in the Supabase SQL Editor to add an opaque `world_id` to each existing server.
New server rows receive a UUID automatically. The host app authorizes each
operation against the signed-in user's `servers` row, then proxies it to the
manager; the manager token is never sent to a browser.

## Process lifecycle

Create a durable process definition:

```bash
curl -X POST http://127.0.0.1:4040/v1/processes \
  -H "x-manager-token: $BLOCKHEADS_MANAGER_TOKEN" \
  -H "content-type: application/json" \
  -d '{"id":"sunset","command":"/srv/blockheads/server","args":["--port","25565"],"cwd":"/srv/blockheads/sunset","auto_restart":true}'
```

The game API uses the Supabase server row ID as the manager process ID:

* `POST /v1/servers/{id}/start` with `{ "world_id": "...", "game": "blockheads_1", "port": 1234, "max_players": 8, "no_exit": false }`; set `no_exit` to `true` for Sapphire or Diamond.
* `POST /v1/servers/{id}/stop`
* `POST /v1/servers/{id}/disconnect` to leave the world running and persist it as disconnected
* `POST /v1/servers/{id}/reconnect` to re-check the surviving process and reconnect it
* `GET /v1/servers/{id}/status` for live process uptime and player count, inferred from the current run's connection logs
* `GET /v1/servers/{id}/activity` for the latest server lifecycle and player join/leave events
* `POST /v1/servers/{id}/command` with `{ "command": "..." }` to send a single-line command to a running Blockheads 1 server's stdin FIFO
* `GET /v1/servers/{id}/logs?run_id=...&offset=...&wait=20`

The first logs request omits `run_id` and `offset` to receive the complete
current run. Later requests pass back the returned `run_id` and byte `offset`;
the manager long-polls for new output (up to 25 seconds) and returns only bytes
appended since that offset. A run change resets the offset and returns the new
run's complete output. The authenticated host API is exposed to the dashboard
at `/api/servers/{id}/logs`.
The status endpoint tracks `Player Connected` and `Player Disconnected` log
events and reports uptime since the current process run started. The authenticated
host API exposes it to the dashboard at `/api/servers/{id}/status`.
The activity endpoint reads the latest seven dated world logs and returns up to
20 newest events. It includes starts, restarts, stops, player joins, and player
leaves; `Client disconnected` lines are ignored because they do not identify a
confirmed player leave. The authenticated host API exposes it at
`/api/servers/{id}/activity`.
The private per-process stdin FIFO remains available to a surviving world
process after the manager restarts, so console commands continue to work after
the manager reconnects.

The generic process API remains available. Its output is appended to
`data/logs/{id}.log` and can be retrieved with
`GET /v1/processes/{id}/logs` (limited to the most recent 64 KiB).

Definitions and the last known PID are stored in `data/processes.json`. When
the manager starts again, it reconciles those PIDs and can continue managing
processes that survived the manager restart. With systemd, `KillMode=process`
keeps game processes alive when the manager service restarts. The manager never
shells through a string: commands and arguments are passed to the operating
system as separate arguments.

## systemd

Build a release binary and install a unit similar to:

```ini
[Unit]
Description=BlockheadsHost process manager
After=network.target

[Service]
Type=simple
WorkingDirectory=/srv/blockheadshost-manager
ExecStart=/srv/blockheadshost-manager/target/release/blockheads-host-manager
Environment=BLOCKHEADS_MANAGER_DATA=/srv/blockheadshost-manager/data
Environment=BLOCKHEADS_MANAGER_BIND=127.0.0.1:4040
EnvironmentFile=/etc/blockheadshost-manager.env
KillMode=process
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

The environment file must contain a token of at least 32 characters:

```dotenv
BLOCKHEADS_MANAGER_TOKEN=replace-with-a-random-secret
```
