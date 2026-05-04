# Running the program

run program with

> cargo run <host_id>

e.g.

> cargo run 1

this will start the program and listen on tcp port 7001
it will also start a unix socket listening on /tmp/event_listener.sock that expects to receive xes formatted events

# TCP peer connections

then connect to another peer with

> connect <peer_id>

e.g.

> connect 2

the peer has to be running in another temrinal

then send a message

> send <peer_id> <string message>

e.g.

> send 2 "hello peer"

# Unix socket event listener

pipe a test file into the socket

> socat -u FILE:test_single.xes UNIX-CONNECT:/tmp/myapp.sock

or

> socat -u FILE:test_multi.xes UNIX-CONNECT:/tmp/myapp.sock

the test files are in the project root directory
