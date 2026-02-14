#!/usr/bin/env bash
#
# Integration test: starts the example server, runs the Node.js client against it,
# and verifies basic operations work.
#
# Usage: bash tools/integration-test.sh
#
# Prerequisites:
# - cargo build (basic-crud binary available)
# - Node.js 18+ installed
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLIENT="$PROJECT_ROOT/tools/grounddb-client/grounddb-client.js"
SERVER_BIN="$PROJECT_ROOT/target/debug/basic-crud"
PORT=18080
SERVER_URL="http://localhost:$PORT"
SERVER_PID=""

# ── Cleanup ──────────────────────────────────────────────────────────

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo "[test] Stopping server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Build ────────────────────────────────────────────────────────────

echo "[test] Building basic-crud..."
(cd "$PROJECT_ROOT" && cargo build --bin basic-crud)

# ── Start server ─────────────────────────────────────────────────────

echo "[test] Starting server on port $PORT..."
GROUNDDB_PORT=$PORT GROUNDDB_DATA_DIR="$PROJECT_ROOT/examples/basic-crud/data" \
    "$SERVER_BIN" &
SERVER_PID=$!

# Wait for server to be ready
echo "[test] Waiting for server..."
for i in $(seq 1 30); do
    if curl -s "$SERVER_URL/api/status" > /dev/null 2>&1; then
        echo "[test] Server is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "[test] ERROR: Server did not start within 30 seconds."
        exit 1
    fi
    sleep 1
done

# ── Tests ────────────────────────────────────────────────────────────

PASS=0
FAIL=0

run_test() {
    local name="$1"
    shift
    echo -n "[test] $name ... "
    if output=$(node "$CLIENT" --server "$SERVER_URL" "$@" 2>&1); then
        echo "PASS"
        PASS=$((PASS + 1))
    else
        echo "FAIL"
        echo "  Output: $output"
        FAIL=$((FAIL + 1))
    fi
}

run_test "status" status
run_test "list-users" list-users
run_test "get-user alice-chen" get-user alice-chen
run_test "create-user" create-user --name "Test User" --email test@example.com
run_test "list-posts" list-posts
run_test "get-post 2026-02-13-quarterly-review" get-post 2026-02-13-quarterly-review
run_test "create-post" create-post --title "Test Post" --author alice-chen --date 2026-02-13
run_test "update-post" update-post 2026-02-13-test-post --status published
run_test "delete-post" delete-post 2026-02-13-test-post
run_test "feed view" feed
run_test "users-lookup view" users-lookup
run_test "recent view" recent
run_test "comments view" comments --post-id 2026-02-13-quarterly-review

# ── Summary ──────────────────────────────────────────────────────────

echo ""
echo "[test] Results: $PASS passed, $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi

echo "[test] All integration tests passed."
