"""MySQL connection utility with environment variable support."""

import os
import mysql.connector


def get_connection(user: str = "root", password: str = "") -> mysql.connector.MySQLConnection:
    """Create MySQL connection using environment-aware socket path.
    
    Checks MYSQL_UNIX_PORT environment variable first, then falls back to defaults.
    """
    socket_path = os.environ.get("MYSQL_UNIX_PORT")
    
    connect_args = {
        "user": user,
        "password": password,
    }
    
    if socket_path:
        # Use explicit socket path from environment
        connect_args["unix_socket"] = socket_path
        print(f"[connection] Using socket: {socket_path}")
    else:
        # Fall back to localhost (will use default socket)
        connect_args["host"] = "localhost"
        print("[connection] Using host: localhost")
    
    return mysql.connector.connect(**connect_args)

