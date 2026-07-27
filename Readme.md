# Single Node Process

## Running the Node Process

run program with

> cargo run -- -c <config_path>

test configurations are in the testdata directory but can also be generated using the `gen-config` binary

## CLI

The terminal where the node process runs also has a CLI to manually connect a node or send a String message

### Connect Node

connect to another peer with

> connect <node_address>

e.g.

> connect 127.0.0.1:8002

### Send Message

send a message like so:

> send <peer_address> <string_message>

e.g.

> send 127.0.0.1:8002 "hello peer"

## Unix socket event listener

pipe a test file into the event listener unix socket

> socat -u FILE:<path_to_test_file> UNIX-CONNECT:<socket_path>

e.g.

> socat -u FILE:testdata/xes/test_single.xes UNIX-CONNECT:/tmp/event_listener.sock

test files are in the testdata directory but can also be generated using the `gen-events` binary

# Config Generation

config files for a distributed or centralized system can be generated using the `gen-config` binary

> cargo run --bin gen-config

see also

> cargo run --bin gen-config -- -h

to see which are the defaults and how to change them

# Event Generation

XES event files for testing the system (or even a single node) can be generated using the `gen-events` binary

> cargo run --bin gen-events -- --config-dir <path_to_config_dir>

see also

> cargo run --bin gen-events -- -h

the gen-events binary needs at least `--config-dir` to specify where the configuration file(s) are

each \*.toml file in the `config-dir` is read and then parsed to collect the configured constraints and which event ids they expect

a file `manifest.jsonl` is created along with the events to fix the order in the event stream and also to pass the event file name and the event id to the sending script

a file `expected_results.jsonl` is created that contains the results for which the events were generated and can be used for automatic comparison with `compare_results.py`

a file `cases.jsonl` is created that contains the deviations (constraint violations) for each case that were generated

a file `seed.txt` is created that contains the seed for this event generation run to possibly reproduce the event stream in the future

# Launching and Tearing Down Systems

## Launch

to launch a system (or both) use the script `launch.sh`

it runs a node for each config file in the given `config-dir` and creates a tmux session with a window for each node process

see also

> ./launch.sh -h

## Teardown

to tear down a system (or both) use the script `destroy.sh`

it will stop the tmux session(s) and clean up the sockets by default

see also

> ./destroy.sh -h

# Streaming Events to a System

to send an event stream to a system (or a single node) use the script `send_event_stream.py` (needs at least python 3.11)

e.g.

> python3.11 send_event_stream.py --config-dir <path_to_config_dir> --stream <path_to_manifest.jsonl>

see also

> python3.11 send_event_stream.py -h

the script needs at least the arguments `--config-dir` and `--stream`
it reads the sockets and the configured constraints with their expected event ids from the config files in `config-dir`
it uses the `manifest.jsonl` file to match the expected events from the config files with the event ids of the events

# Looking at Constraint Check results

results can either be inspected by browsing the sqlite databases that a configured result sink is writing to
OR
if no result sink is configured, a node outputs the result in its log file

to automatically compare a results of the decentralized and the centralized system the script `compare_results.py` can be used like so:

> python compare_results.py

see also

> python compare_results.py -h
