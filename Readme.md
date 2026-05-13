# Running the program

run program with

> cargo run -- -c <config_path>

e.g.

> cargo run -- -c config1.toml

this will start the program and listen on tcp port 7001
it will also start a unix socket listening on /tmp/event_listener.sock that expects to receive xes formatted events

# TCP peer connections

then connect to another peer with

> connect <peer_id>

e.g.

> connect 2

the peer has to be running in another temrinal

then send a message

> send <peer_address> <string_message>

e.g.

> send 127.0.0.1:8002 "hello peer"

# Unix socket event listener

pipe a test file into the socket

> socat -u FILE:<path_to_test_file> UNIX-CONNECT:<socket_path>

e.g.

> socat -u FILE:test_single.xes UNIX-CONNECT:/tmp/event_listener1.sock

the test files are in the project root directory
