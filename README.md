# docker-volume-migrate

Scans a Docker host for containers using bind mounts and migrates them to named Docker volumes.

## Install

```bash
pip install docker rich
```

## Usage

```bash
# List all containers with bind mounts (no changes made)
python3 docker_volume_migrate.py --list

# Preview what would happen
python3 docker_volume_migrate.py --dry-run

# Run interactively (prompts before every change)
python3 docker_volume_migrate.py

# Migrate a specific container
python3 docker_volume_migrate.py -c <container-name>

# Connect to a remote Docker host
python3 docker_volume_migrate.py -H tcp://10.0.0.1:2376
```

## Options

| Flag | Description |
|------|-------------|
| `-H`, `--host` | Docker socket or URL (default: local) |
| `-c`, `--container` | Limit to a specific container (repeatable) |
| `-n`, `--dry-run` | Show planned actions, make no changes |
| `-l`, `--list` | List bind mounts only, do not migrate |
| `-y`, `--yes` | Auto-confirm all prompts |
| `--skip-copy` | Create empty volume without copying data |
| `--volume-prefix` | Prefix for auto-generated volume names |
| `--stop-timeout` | Seconds to wait before force-stopping (default: 30) |
| `--log-file` | Write log output to a file |

## Notes

- Containers are stopped during migration and restarted when done.
- Data is copied using `cp -a` (preserves permissions, ownership, and symlinks).
- If a container is managed by Docker Compose, the tool will flag it and show the updated `docker-compose.yml` snippet to apply after migration.
- If migration fails after the container is removed, the tool attempts to restore the original container automatically.
