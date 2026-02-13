#!/usr/bin/env node

// GroundDB CLI Client
// Talks to the GroundDB example HTTP server via REST API.
// Requires Node.js 18+ (uses built-in fetch).

const DEFAULT_SERVER = "http://localhost:8080";

// ---------------------------------------------------------------------------
// Color helpers (no dependencies -- uses ANSI codes when stdout is a TTY)
// ---------------------------------------------------------------------------

const useColor = process.stdout.isTTY ?? false;

const c = {
  reset: useColor ? "\x1b[0m" : "",
  bold: useColor ? "\x1b[1m" : "",
  dim: useColor ? "\x1b[2m" : "",
  red: useColor ? "\x1b[31m" : "",
  green: useColor ? "\x1b[32m" : "",
  yellow: useColor ? "\x1b[33m" : "",
  blue: useColor ? "\x1b[34m" : "",
  cyan: useColor ? "\x1b[36m" : "",
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = argv.slice(2);
  const positional = [];
  const flags = {};

  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--")) {
      const key = args[i].slice(2);
      const next = args[i + 1];
      if (next === undefined || next.startsWith("--")) {
        flags[key] = true;
      } else {
        flags[key] = next;
        i++;
      }
    } else {
      positional.push(args[i]);
    }
  }

  return { positional, flags };
}

function prettyJson(data) {
  if (!useColor) return JSON.stringify(data, null, 2);
  return colorizeJson(data, 0);
}

function colorizeJson(value, indent) {
  const pad = "  ".repeat(indent);
  const padInner = "  ".repeat(indent + 1);

  if (value === null) return `${c.dim}null${c.reset}`;
  if (typeof value === "boolean") return `${c.yellow}${value}${c.reset}`;
  if (typeof value === "number") return `${c.cyan}${value}${c.reset}`;
  if (typeof value === "string") return `${c.green}"${value}"${c.reset}`;

  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    const items = value.map((v) => `${padInner}${colorizeJson(v, indent + 1)}`);
    return `[\n${items.join(",\n")}\n${pad}]`;
  }

  if (typeof value === "object") {
    const keys = Object.keys(value);
    if (keys.length === 0) return "{}";
    const entries = keys.map(
      (k) =>
        `${padInner}${c.blue}"${k}"${c.reset}: ${colorizeJson(value[k], indent + 1)}`
    );
    return `{\n${entries.join(",\n")}\n${pad}}`;
  }

  return String(value);
}

async function request(server, method, path, body) {
  const url = `${server}${path}`;
  const opts = { method, headers: {} };

  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }

  let res;
  try {
    res = await fetch(url, opts);
  } catch (err) {
    console.error(
      `${c.red}Error:${c.reset} Could not connect to ${c.bold}${url}${c.reset}`
    );
    console.error(`${c.dim}${err.message}${c.reset}`);
    process.exit(1);
  }

  let data;
  const text = await res.text();
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }

  if (!res.ok) {
    console.error(
      `${c.red}Error ${res.status}:${c.reset} ${method} ${path}`
    );
    if (typeof data === "object") {
      console.error(prettyJson(data));
    } else if (data) {
      console.error(data);
    }
    process.exit(1);
  }

  return data;
}

function die(msg) {
  console.error(`${c.red}Error:${c.reset} ${msg}`);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

const commands = {
  status: {
    usage: "status",
    description: "Show server status (schema info, collection counts)",
    async run(server) {
      const data = await request(server, "GET", "/api/status");
      console.log(prettyJson(data));
    },
  },

  "list-users": {
    usage: "list-users",
    description: "List all users",
    async run(server) {
      const data = await request(server, "GET", "/api/users");
      console.log(prettyJson(data));
    },
  },

  "get-user": {
    usage: "get-user <id>",
    description: "Get a user by ID",
    async run(server, positional) {
      const id = positional[1];
      if (!id) die("Missing required argument: <id>");
      const data = await request(server, "GET", `/api/users/${encodeURIComponent(id)}`);
      console.log(prettyJson(data));
    },
  },

  "create-user": {
    usage: "create-user --name <name> --email <email> [--role <role>]",
    description: "Create a new user",
    async run(server, _positional, flags) {
      if (!flags.name) die("Missing required flag: --name");
      if (!flags.email) die("Missing required flag: --email");
      const body = { name: flags.name, email: flags.email };
      if (flags.role) body.role = flags.role;
      const data = await request(server, "POST", "/api/users", body);
      console.log(prettyJson(data));
    },
  },

  "list-posts": {
    usage: "list-posts",
    description: "List all posts",
    async run(server) {
      const data = await request(server, "GET", "/api/posts");
      console.log(prettyJson(data));
    },
  },

  "get-post": {
    usage: "get-post <id>",
    description: "Get a post by ID",
    async run(server, positional) {
      const id = positional[1];
      if (!id) die("Missing required argument: <id>");
      const data = await request(server, "GET", `/api/posts/${encodeURIComponent(id)}`);
      console.log(prettyJson(data));
    },
  },

  "create-post": {
    usage: "create-post --title <title> --author <author_id> --date <YYYY-MM-DD> [--content <markdown>] [--tags <tag1,tag2>] [--status <status>]",
    description: "Create a new post",
    async run(server, _positional, flags) {
      if (!flags.title) die("Missing required flag: --title");
      if (!flags.author) die("Missing required flag: --author");
      if (!flags.date) die("Missing required flag: --date");
      const body = {
        title: flags.title,
        author_id: flags.author,
        date: flags.date,
      };
      if (flags.content) body.content = flags.content;
      if (flags.tags) body.tags = flags.tags.split(",").map((t) => t.trim());
      if (flags.status) body.status = flags.status;
      const data = await request(server, "POST", "/api/posts", body);
      console.log(prettyJson(data));
    },
  },

  "update-post": {
    usage: "update-post <id> [--title <title>] [--status <status>] [--tags <tags>]",
    description: "Update an existing post",
    async run(server, positional, flags) {
      const id = positional[1];
      if (!id) die("Missing required argument: <id>");
      const body = {};
      if (flags.title) body.title = flags.title;
      if (flags.status) body.status = flags.status;
      if (flags.tags) body.tags = flags.tags.split(",").map((t) => t.trim());
      if (flags.author) body.author_id = flags.author;
      if (flags.date) body.date = flags.date;
      if (flags.content) body.content = flags.content;
      if (Object.keys(body).length === 0) die("No fields to update. Use --title, --status, --tags, etc.");
      const data = await request(server, "PUT", `/api/posts/${encodeURIComponent(id)}`, body);
      console.log(prettyJson(data));
    },
  },

  "delete-post": {
    usage: "delete-post <id>",
    description: "Delete a post",
    async run(server, positional) {
      const id = positional[1];
      if (!id) die("Missing required argument: <id>");
      const data = await request(server, "DELETE", `/api/posts/${encodeURIComponent(id)}`);
      console.log(prettyJson(data));
    },
  },

  feed: {
    usage: "feed",
    description: "Get the post feed view (published posts with author info)",
    async run(server) {
      const data = await request(server, "GET", "/api/views/post_feed");
      console.log(prettyJson(data));
    },
  },

  "users-lookup": {
    usage: "users-lookup",
    description: "Get the user lookup view",
    async run(server) {
      const data = await request(server, "GET", "/api/views/user_lookup");
      console.log(prettyJson(data));
    },
  },

  recent: {
    usage: "recent",
    description: "Get recent activity view",
    async run(server) {
      const data = await request(server, "GET", "/api/views/recent_activity");
      console.log(prettyJson(data));
    },
  },

  comments: {
    usage: "comments --post-id <post_id>",
    description: "Get comments for a post",
    async run(server, _positional, flags) {
      const postId = flags["post-id"];
      if (!postId) die("Missing required flag: --post-id");
      const data = await request(
        server,
        "GET",
        `/api/views/post_comments?post_id=${encodeURIComponent(postId)}`
      );
      console.log(prettyJson(data));
    },
  },
};

// ---------------------------------------------------------------------------
// Help
// ---------------------------------------------------------------------------

function printHelp() {
  console.log(`${c.bold}grounddb-client${c.reset} - CLI client for GroundDB example server\n`);
  console.log(`${c.bold}Usage:${c.reset} node grounddb-client.js [--server <url>] <command> [args...]\n`);
  console.log(`${c.bold}Options:${c.reset}`);
  console.log(`  --server <url>  Server URL (default: ${DEFAULT_SERVER})\n`);
  console.log(`${c.bold}Commands:${c.reset}`);
  for (const [name, cmd] of Object.entries(commands)) {
    console.log(`  ${c.cyan}${cmd.usage.padEnd(70)}${c.reset} ${c.dim}${cmd.description}${c.reset}`);
  }
  console.log(`\n${c.bold}Examples:${c.reset}`);
  console.log(`  node grounddb-client.js status`);
  console.log(`  node grounddb-client.js list-users`);
  console.log(`  node grounddb-client.js get-user alice-chen`);
  console.log(`  node grounddb-client.js create-user --name "Carol Wu" --email carol@example.com`);
  console.log(`  node grounddb-client.js create-post --title "Hello" --author alice-chen --date 2026-02-13`);
  console.log(`  node grounddb-client.js update-post 2026-02-13-hello --status published`);
  console.log(`  node grounddb-client.js feed`);
  console.log(`  node grounddb-client.js comments --post-id 2026-02-13-quarterly-review`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const { positional, flags } = parseArgs(process.argv);
  const server = flags.server || DEFAULT_SERVER;
  const commandName = positional[0];

  if (!commandName || commandName === "help" || flags.help) {
    printHelp();
    process.exit(0);
  }

  const cmd = commands[commandName];
  if (!cmd) {
    console.error(`${c.red}Unknown command:${c.reset} ${commandName}`);
    console.error(`Run with ${c.bold}help${c.reset} to see available commands.`);
    process.exit(1);
  }

  await cmd.run(server, positional, flags);
}

main();
