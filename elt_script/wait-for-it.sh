#!/bin/bash
# wait-for-it.sh: Wait for PostgreSQL to be ready before executing a command.

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

until PGPASSWORD="secret" psql -h "$host" -U "postgres" -p "$port" -c '\q'; do
  >&2 echo "Waiting for PostgreSQL on $host:$port to be ready..."
  sleep 1
done

>&2 echo "PostgreSQL on $host:$port is ready, executing command: $cmd"
exec $cmd
