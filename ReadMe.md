# Bitcask Server and Database

## Overview

This project implements a simple TCP server using a Bitcask-inspired database for handling key-value store operations. The server supports basic commands such as `SET`, `GET`, and `DELETE`.

## Features

- **SET**: Store a key-value pair in the database.
- **GET**: Retrieve the value associated with a key.
- **DELETE**: Remove a key-value pair from the database.

## Requirements

- Python 3.11 or later
- `bitcaskdb` module (ensure it's accessible within your project or install if available)

## Installation

1. **Clone the repository**:
    ```sh
    git clone https://github.com/yourusername/bitcask-server.git
    cd bitcask-server
    ```

2. **Install dependencies**:
    ```sh
    pip install -r requirements.txt
    ```

3. **Run the server**:
    ```sh
    python main.py
    ```

## Usage

### Server

To start the server, run:
```sh
python main.py
```

### Client
You can use any TCP client to interact with the server. Here is an example using telnet:

1. Connect to the server:
```sh
telnet localhost 9999
```

2. Commands:

- SET:
```sh
SET key value
```

- GET:
```sh
GET key
```

- DELETE:
```sh
DELETE key
```