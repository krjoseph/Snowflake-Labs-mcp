# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import hashlib
import threading
from typing import Any, Dict, Optional, Tuple

from fastmcp.utilities.logging import get_logger
from snowflake.core import Root

logger = get_logger(__name__)


class ConnectionPool:
    """
    Thread-safe connection pool for Snowflake connections.
    
    Pools connections by a hash key based on credentials:
    token-account-user-<role if set>-<warehouse if set>
    """

    def __init__(self):
        self._pool: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._root_cache: Dict[str, Root] = {}

    def _generate_key(
        self,
        token: str,
        account: str,
        user: str,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
    ) -> str:
        """
        Generate a hash key for connection pooling.
        
        Format: token-account-user-<role if set>-<warehouse if set>
        """
        key_parts = [token, account, user]
        if role:
            key_parts.append(role)
        if warehouse:
            key_parts.append(warehouse)
        
        key_string = "-".join(key_parts)
        # Use SHA256 hash to avoid storing tokens in memory
        return hashlib.sha256(key_string.encode()).hexdigest()

    def get_connection(
        self,
        connection_factory,
        token: str,
        account: str,
        user: str,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
    ) -> Tuple[Any, Root]:
        """
        Get or create a connection from the pool.
        
        Parameters
        ----------
        connection_factory : callable
            Function that creates a new connection
        token : str
            OAuth token (PAT)
        account : str
            Snowflake account identifier
        user : str
            Snowflake username
        role : str, optional
            Snowflake role (optional)
        warehouse : str, optional
            Snowflake warehouse (optional)
        
        Returns
        -------
        tuple
            (Connection, Root) tuple
        """
        key = self._generate_key(token, account, user, role, warehouse)
        
        with self._lock:
            # Check if connection exists and is still valid
            if key in self._pool:
                conn = self._pool[key]
                try:
                    # Test if connection is still alive
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                    # Connection is valid, return cached root if available
                    root = self._root_cache.get(key)
                    if root is None:
                        root = Root(conn)
                        self._root_cache[key] = root
                    return conn, root
                except Exception as e:
                    logger.warning(f"Connection {key[:8]}... is invalid, removing from pool: {e}")
                    # Connection is invalid, remove it
                    try:
                        conn.close()
                    except Exception:
                        pass
                    del self._pool[key]
                    if key in self._root_cache:
                        del self._root_cache[key]
            
            # Create new connection
            logger.debug(f"Creating new connection for key {key[:8]}...")
            conn = connection_factory()
            self._pool[key] = conn
            root = Root(conn)
            self._root_cache[key] = root
            return conn, root

    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for key, conn in list(self._pool.items()):
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection {key[:8]}...: {e}")
            self._pool.clear()
            self._root_cache.clear()

    def remove_connection(
        self,
        token: str,
        account: str,
        user: str,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
    ):
        """Remove a specific connection from the pool."""
        key = self._generate_key(token, account, user, role, warehouse)
        with self._lock:
            if key in self._pool:
                try:
                    self._pool[key].close()
                except Exception:
                    pass
                del self._pool[key]
            if key in self._root_cache:
                del self._root_cache[key]
