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
import argparse
import hashlib
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generator, Literal, Optional, Tuple, cast

import yaml
from fastmcp import FastMCP
from fastmcp.utilities.logging import get_logger
from snowflake.connector import DictCursor, connect
from snowflake.core import Root

from mcp_server_snowflake.cortex_services.tools import (
    initialize_cortex_agent_tool,
    initialize_cortex_analyst_tool,
    initialize_cortex_search_tool,
)
from mcp_server_snowflake.environment import (
    get_spcs_container_token,
    is_running_in_spcs_container,
)
from mcp_server_snowflake.object_manager.tools import initialize_object_manager_tools
from mcp_server_snowflake.query_manager.tools import initialize_query_manager_tool
from mcp_server_snowflake.semantic_manager.tools import (
    initialize_semantic_manager_tools,
)
from mcp_server_snowflake.server_utils import (
    initialize_middleware,
    request_connection_params,
)
from mcp_server_snowflake.utils import (
    cleanup_snowflake_service,
    get_login_params,
    load_tools_config_resource,
    unpack_sql_statement_permissions,
    warn_deprecated_params,
)

# Used to quantify Snowflake usage
server_name = "mcp-server-snowflake"
tag_major_version = 1
tag_minor_version = 3
query_tag = {"origin": "sf_sit", "name": "mcp_server"}

logger = get_logger(server_name)


class ConnectionPool:
    """
    Manages a pool of Snowflake connections isolated by connection parameters.
    
    Connections are keyed by a hash of account, user, role, warehouse, and password
    to ensure proper isolation between different credentials.
    """

    def __init__(self):
        self._connections: Dict[str, Any] = {}
        self._roots: Dict[str, Any] = {}
        self._lock = Lock()

    def _get_connection_key(
        self,
        account: Optional[str],
        user: Optional[str],
        role: Optional[str],
        warehouse: Optional[str],
        password: Optional[str],
        token: Optional[str] = None,
    ) -> str:
        """
        Generate a unique key for connection parameters.
        
        Uses a hash of the connection parameters to create a unique identifier
        for each distinct set of credentials.
        """
        key_parts = [
            account or "",
            user or "",
            role or "",
            warehouse or "",
            password or "",
            token or "",  # Include token in key for isolation
        ]
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def get_connection(
        self,
        connection_params: Dict[str, Any],
        service_config_file: str,
        is_spcs_container: bool,
        query_tag_params: Optional[Dict[str, Any]],
    ) -> Tuple[Any, Any]:
        """
        Get or create a connection from the pool based on connection parameters.
        
        Parameters
        ----------
        connection_params : dict
            Connection parameters (account, user, role, warehouse, password, etc.)
        service_config_file : str
            Path to service configuration file
        is_spcs_container : bool
            Whether running in SPCS container
        query_tag_params : dict, optional
            Query tag parameters for the connection
            
        Returns
        -------
        tuple
            A tuple containing (connection, root) objects
        """
        account = connection_params.get("account")
        user = connection_params.get("user")
        role = connection_params.get("role")
        warehouse = connection_params.get("warehouse")
        password = connection_params.get("password")
        token = connection_params.get("token")

        connection_key = self._get_connection_key(
            account, user, role, warehouse, password, token
        )

        with self._lock:
            if connection_key not in self._connections:
                logger.info(
                    f"Creating new connection for account={account}, user={user}, "
                    f"role={role}, warehouse={warehouse}"
                )
                connection = self._create_connection(
                    connection_params,
                    is_spcs_container,
                    query_tag_params,
                )
                root = Root(connection)
                self._connections[connection_key] = connection
                self._roots[connection_key] = root
            else:
                logger.debug(
                    f"Reusing existing connection for account={account}, user={user}"
                )

            return self._connections[connection_key], self._roots[connection_key]

    def _create_connection(
        self,
        connection_params: Dict[str, Any],
        is_spcs_container: bool,
        query_tag_params: Optional[Dict[str, Any]],
    ) -> Any:
        """Create a new Snowflake connection."""
        if is_spcs_container:
            logger.info("Using SPCS container OAuth authentication")
            params = {
                "host": os.getenv("SNOWFLAKE_HOST"),
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "token": get_spcs_container_token(),
                "authenticator": "oauth",
            }
            params = {k: v for k, v in params.items() if v is not None}
        else:
            logger.info("Using external authentication")
            params = connection_params.copy()
            
            # If token is provided, use it as password (programmatic access token)
            # or as OAuth token if authenticator is set to oauth
            if "token" in params and "password" not in params:
                # Use token as password for programmatic access tokens
                params["password"] = params.pop("token")

        # Only fall back to connection_name if we have no params AND connection_name is explicitly provided
        # For HTTP transports, we require connection params via headers or env vars
        if not params:
            connection_name = os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME")
            if connection_name:
                params = {"connection_name": connection_name}
            else:
                # For HTTP transports, require connection params - don't use default connection_name
                raise ValueError(
                    "No connection parameters provided. For HTTP transports, provide connection "
                    "parameters via headers (X-Snowflake-Account, X-Snowflake-User, etc.) or "
                    "environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.)."
                )

        connection = connect(
            **params,
            session_parameters=query_tag_params,
            client_session_keep_alive=True,
            paramstyle="qmark",
        )

        if connection:
            SnowflakeService.send_initial_query(connection)

        return connection

    def cleanup_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for key, connection in self._connections.items():
                try:
                    logger.info(f"Closing connection {key}")
                    connection.close()
                except Exception as e:
                    logger.error(f"Error closing connection {key}: {e}")
            self._connections.clear()
            self._roots.clear()


# Global connection pool instance
_connection_pool = ConnectionPool()


class SnowflakeService:
    """
    Snowflake service configuration and management.

    This class handles the configuration and setup of Snowflake Cortex services
    including search, and analyst. It loads service specifications from a
    YAML configuration file and provides access to service parameters.

    It also handles all Snowflake authentication and connection logic,
    automatically detecting the environment (container vs external) and
    providing appropriate authentication parameters for both database
    connections and REST API calls.

    Parameters
    ----------
    service_config_file : str
        Path to the service configuration YAML file
    transport : str
        Transport for the MCP server
    connection_params : dict
        Connection parameters for Snowflake connector
    endpoint : str, default="/mcp"
        Custom endpoint path for HTTP transports

    Attributes
    ----------
    service_config_file : str
        Path to configuration file
    transport : Literal["stdio", "http", "sse", "streamable-http"]
        Transport for the MCP server
    endpoint : str
        Custom endpoint path for HTTP transports
    search_services : list
        List of configured search service specifications
    analyst_services : list
        List of configured analyst service specifications
    agent_services : list
        List of configured agent service specifications
    sql_statement_allowed : list
        List of allowed SQL statement types
    sql_statement_disallowed : list
        List of disallowed SQL statement types
    connection : snowflake.connector.Connection
        Snowflake connection object
    """

    def __init__(
        self,
        service_config_file: str,
        transport: str,
        connection_params: dict,
        endpoint: str = "/mcp",
    ):
        if service_config_file is None:
            raise ValueError(
                "service_config_file cannot be None. Please provide a path to the service configuration file."
            )

        self.service_config_file = str(Path(service_config_file).expanduser().resolve())
        self.config_path_uri = Path(self.service_config_file).as_uri()
        self.transport = cast(
            Literal["stdio", "http", "sse", "streamable-http"], transport
        )
        self.connection_params = connection_params
        self.endpoint = endpoint
        self.search_services = []
        self.analyst_services = []
        self.agent_services = []
        self.sql_statement_allowed = []
        self.sql_statement_disallowed = []
        self.object_manager = False
        self.query_manager = False
        self.semantic_manager = False
        self.default_session_parameters: Dict[str, Any] = {}
        self.query_tag = query_tag if query_tag is not None else None
        self.tag_major_version = (
            tag_major_version if tag_major_version is not None else None
        )
        self.tag_minor_version = (
            tag_minor_version if tag_minor_version is not None else None
        )

        # Environment detection for authentication
        self._is_spcs_container = is_running_in_spcs_container()

        self.unpack_service_specs()
        # Store default connection params for fallback
        self.default_connection_params = connection_params.copy()
        # For non-HTTP transports, create default connection immediately
        if transport not in ["http", "sse", "streamable-http"]:
            self.connection, self.root = _connection_pool.get_connection(
                connection_params=connection_params,
                service_config_file=self.service_config_file,
                is_spcs_container=self._is_spcs_container,
                query_tag_params=self.get_query_tag_param(),
            )
        else:
            # For HTTP transports, connections will be created per-request based on headers
            self.connection = None
            self.root = None

    def unpack_service_specs(self) -> None:
        """
        Load and parse service specifications from configuration file.

        Reads the YAML configuration file and extracts service specifications
        for all services managed by YAML configuration.
        """
        try:
            with open(self.service_config_file, "r") as file:
                service_config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(
                f"Service configuration file not found: {self.service_config_file}"
            )
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading service config: {e}")
            raise

        try:
            self.search_services = service_config.get("search_services", [])
            self.analyst_services = service_config.get("analyst_services", [])
            self.agent_services = service_config.get(
                "agent_services", []
            )  # Not supported yet
            self.sql_statement_allowed, self.sql_statement_disallowed = (
                unpack_sql_statement_permissions(
                    service_config.get("sql_statement_permissions", [])
                )
            )
            other_services = service_config.get("other_services", {})
            if other_services is not None:
                self.object_manager = other_services.get("object_manager", False)
                self.query_manager = other_services.get("query_manager", False)
                self.semantic_manager = other_services.get("semantic_manager", False)

        except Exception as e:
            logger.error(f"Error extracting service specifications: {e}")
            raise

    def get_api_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for REST API calls.

        Returns
        -------
        Dict[str, str]
            HTTP headers with authentication
        """
        # Get the current connection (may be from pool based on headers)
        connection = self._get_current_connection()
        
        if self._is_spcs_container:
            return {
                "Authorization": f"Bearer {get_spcs_container_token()}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
        else:
            # For external environments, we need to use the connection token
            return {
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                "Authorization": f'Snowflake Token="{connection.rest.token}"',
            }

    def get_api_host(self) -> str:
        """
        Get the API host for REST API calls.

        Returns
        -------
        str
            API host URL
        """
        # Get the current connection (may be from pool based on headers)
        connection = self._get_current_connection()
        
        if self._is_spcs_container:
            return os.getenv(
                "SNOWFLAKE_HOST", self.default_connection_params.get("account", "")
            )
        else:
            return connection.host

    @staticmethod
    def send_initial_query(connection: Any) -> None:
        """
        Send an initial query to the Snowflake service.
        """
        with connection.cursor() as cur:
            cur.execute("SELECT 'MCP Server Snowflake'").fetchone()


    def _get_current_connection(self) -> Any:
        """
        Get the current connection, either from pool (for HTTP transports) or default.
        
        Returns
        -------
        connection
            The current Snowflake connection object
            
        Raises
        ------
        ValueError
            If no connection parameters are available and connection cannot be created
        """
        # Check if we have per-request connection params from headers
        request_params = request_connection_params.get()
        
        if request_params is not None:
            # Merge header params with default params (headers take precedence)
            merged_params = self.default_connection_params.copy()
            merged_params.update(request_params)
            
            # Use connection from pool based on merged params
            connection, root = _connection_pool.get_connection(
                connection_params=merged_params,
                service_config_file=self.service_config_file,
                is_spcs_container=self._is_spcs_container,
                query_tag_params=self.get_query_tag_param(),
            )
            return connection
        elif self.connection is not None:
            # Use default connection (for non-HTTP transports)
            return self.connection
        elif self.transport in ["http", "sse", "streamable-http"]:
            # For HTTP transports, require connection params via headers
            # Don't create a connection without params
            raise ValueError(
                "No connection parameters provided in request headers. "
                "Please provide X-Snowflake-Account, X-Snowflake-User, X-Snowflake-Role, "
                "X-Snowflake-Warehouse, and Authorization Bearer token headers."
            )
        else:
            # For non-HTTP transports, try to create connection from default params
            # This will raise an error if params are missing
            connection, root = _connection_pool.get_connection(
                connection_params=self.default_connection_params,
                service_config_file=self.service_config_file,
                is_spcs_container=self._is_spcs_container,
                query_tag_params=self.get_query_tag_param(),
            )
            self.connection = connection
            self.root = root
            return connection

    @contextmanager
    def get_connection(
        self,
        use_dict_cursor: bool = False,
        session_parameters: Optional[Dict[str, Any]] = None,
    ) -> Generator[Tuple[Any, Any], None, None]:
        """
        Get a Snowflake connection with the specified configuration.

        This context manager ensures proper connection handling and cleanup.
        It automatically detects the environment and uses appropriate authentication.
        For HTTP transports, connections are isolated based on runtime headers.

        Parameters
        ----------
        use_dict_cursor : bool, default=False
            Whether to use DictCursor instead of regular cursor
        session_parameters : dict, optional
            Additional session parameters to add to connection such as query tag

        Yields
        ------
        tuple
            A tuple containing (connection, cursor)

        Examples
        --------
        >>> with service.get_connection(use_dict_cursor=True) as (con, cur):
        ...     cur.execute("SELECT current_version()")
        ...     result = cur.fetchone()
        """

        try:
            connection = self._get_current_connection()

            cursor = (
                connection.cursor(DictCursor)
                if use_dict_cursor
                else connection.cursor()
            )

            try:
                yield connection, cursor
            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Error establishing Snowflake connection: {e}")
            raise

    def get_query_tag_param(
        self,
    ) -> Optional[Dict[str, Any]] | None:
        """
        Get the query tag parameters for the Snowflake service.

        Parameters
        ----------
        query_tag : dict[str, str], optional
            Query tag dictionary
        major_version : int, optional
            Major version of the query tag
        minor_version : int, optional
            Minor version of the query tag
        """
        if self.query_tag is not None:
            query_tag = self.query_tag.copy()
            if (
                self.tag_major_version is not None
                and self.tag_minor_version is not None
            ):
                query_tag["version"] = {
                    "major": self.tag_major_version,
                    "minor": self.tag_minor_version,
                }

            # Set the query tag in default session parameters
            session_parameters = {"QUERY_TAG": json.dumps(query_tag)}

            return session_parameters
        else:
            return None


def get_var(var_name: str, env_var_name: str, args) -> Optional[str]:
    """
    Retrieve variable value from command line arguments or environment variables.

    Checks for a variable value first in command line arguments, then falls back
    to environment variables. This provides flexible configuration options for
    the MCP server.

    Parameters
    ----------
    var_name : str
        The attribute name to check in the command line arguments object
    env_var_name : str
        The environment variable name to check if command line arg is not provided
    args : argparse.Namespace
        Parsed command line arguments object

    Returns
    -------
    Optional[str]
        The variable value if found in either source, None otherwise

    Examples
    --------
    Get account identifier from args or environment:

    >>> args = parser.parse_args(["--account", "myaccount"])
    >>> get_var("account", "SNOWFLAKE_ACCOUNT", args)
    'myaccount'

    >>> os.environ["SNOWFLAKE_ACCOUNT"] = "myaccount"
    >>> args = parser.parse_args([])
    >>> get_var("account", "SNOWFLAKE_ACCOUNT", args)
    'myaccount'
    """

    if getattr(args, var_name):
        return getattr(args, var_name)
    if env_var_name in os.environ:
        return os.environ[env_var_name]
    return None


def parse_arguments():
    """Parse command line arguments once at startup."""
    parser = argparse.ArgumentParser(description="Snowflake MCP Server")

    login_params = get_login_params()

    for value in login_params.values():
        parser.add_argument(
            *value[:-2], required=False, default=value[-2], help=value[-1]
        )

    parser.add_argument(
        "--service-config-file",
        required=False,
        help="Path to service specification file",
    )
    parser.add_argument(
        "--transport",
        required=False,
        choices=["stdio", "http", "sse", "streamable-http"],
        help="Transport for the MCP server",
        default="stdio",
    )
    parser.add_argument(
        "--server-host",  # Avoid using simply host here as it conflicts with the host argument in the Snowflake Python Connector
        required=False,
        help="Host address to bind the server to (default: 0.0.0.0)",
        default="0.0.0.0",
    )
    # These left as simply port and endpoint for backward compatibility with existing deployments
    parser.add_argument(
        "--port",
        required=False,
        type=int,
        help="Port number for the server to listen on (default: 9000)",
        default=9000,
    )
    parser.add_argument(
        "--endpoint",
        required=False,
        help="Endpoint path for the MCP server (default: /mcp)",
        default="/mcp",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        required=False,
        help="Enable verbose/debug logging",
        default=False,
    )

    return parser.parse_args()


def create_lifespan(args):
    """Create a lifespan function with captured arguments."""

    @asynccontextmanager
    async def create_snowflake_service(
        server: FastMCP,
    ) -> AsyncIterator[SnowflakeService]:
        """
        Create main entry point for the Snowflake MCP server package.

        Uses pre-parsed command line arguments to create and configure the Snowflake service.
        """
        connection_params = {
            key: getattr(args, key)
            for key in get_login_params().keys()
            if getattr(args, key) is not None
        }
        service_config_file = get_var(
            "service_config_file", "SERVICE_CONFIG_FILE", args
        )

        endpoint = os.environ.get("SNOWFLAKE_MCP_ENDPOINT", args.endpoint)

        snowflake_service = None
        try:
            snowflake_service = SnowflakeService(
                service_config_file=service_config_file,
                transport=args.transport,
                connection_params=connection_params,
                endpoint=endpoint or args.endpoint,
            )

            # Initialize tools and resources now that we have the service
            logger.info("Initializing tools and resources...")
            initialize_tools(snowflake_service, server)
            initialize_middleware(server, snowflake_service)
            initialize_resources(snowflake_service, server)

            yield snowflake_service
        except Exception as e:
            logger.error(f"Error creating Snowflake service: {e}")
            raise

        finally:
            if snowflake_service is not None:
                cleanup_snowflake_service(snowflake_service)

    return create_snowflake_service


def initialize_resources(snowflake_service: SnowflakeService, server: FastMCP):
    @server.resource(snowflake_service.config_path_uri)
    async def get_tools_config():
        """
        Tools Specification Configuration.

        Provides access to the YAML tools configuration file as JSON.
        """
        tools_config = await load_tools_config_resource(
            snowflake_service.service_config_file
        )
        return json.loads(tools_config)


def initialize_tools(snowflake_service: SnowflakeService, server: FastMCP):
    if snowflake_service is not None:
        # Add tools for object manager
        if snowflake_service.object_manager:
            initialize_object_manager_tools(server, snowflake_service)

        # Add tools for query manager
        if snowflake_service.query_manager:
            initialize_query_manager_tool(server, snowflake_service)

        # Add tools for semantic manager
        if snowflake_service.semantic_manager:
            initialize_semantic_manager_tools(server, snowflake_service)

        # Add tool for agent service
        if snowflake_service.agent_services:
            initialize_cortex_agent_tool(server, snowflake_service)

        # Add tool for search service
        if snowflake_service.search_services:
            initialize_cortex_search_tool(server, snowflake_service)

        if snowflake_service.analyst_services:
            initialize_cortex_analyst_tool(server, snowflake_service)


def main():
    args = parse_arguments()

    # Configure logging level based on verbose flag or environment variable
    if args.verbose or os.getenv("SNOWFLAKE_MCP_VERBOSE", "").lower() in (
        "true",
        "1",
        "yes",
    ):
        import logging

        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("fastmcp").setLevel(logging.DEBUG)
        logging.getLogger(server_name).setLevel(logging.DEBUG)
        logger.debug("Verbose/debug logging enabled")

    warn_deprecated_params()

    # Create server with lifespan that has access to args
    server = FastMCP("Snowflake MCP Server", lifespan=create_lifespan(args))

    # Add HTTP middleware for header extraction if using HTTP transport
    if args.transport and args.transport in ["http", "sse", "streamable-http"]:
        try:
            from starlette.middleware.base import BaseHTTPMiddleware
            from starlette.requests import Request
            
            class HeaderExtractionMiddleware(BaseHTTPMiddleware):
                """HTTP middleware to extract connection parameters from headers."""
                
                async def dispatch(self, request: Request, call_next):
                    """Extract headers and store in context variable."""
                    headers = request.headers
                    
                    # Extract connection parameters from headers
                    # Support both x-snowflake-* and SNOWFLAKE_* header formats
                    account = (
                        headers.get("x-snowflake-account") 
                        or headers.get("X-Snowflake-Account")
                        or headers.get("SNOWFLAKE_ACCOUNT")
                    )
                    user = (
                        headers.get("x-snowflake-user")
                        or headers.get("X-Snowflake-User")
                        or headers.get("SNOWFLAKE_USER")
                    )
                    role = (
                        headers.get("x-snowflake-role")
                        or headers.get("X-Snowflake-Role")
                        or headers.get("SNOWFLAKE_ROLE")
                    )
                    warehouse = (
                        headers.get("x-snowflake-warehouse")
                        or headers.get("X-Snowflake-Warehouse")
                        or headers.get("SNOWFLAKE_WAREHOUSE")
                    )
                    
                    # Extract token from Authorization Bearer header
                    token = None
                    auth_header = headers.get("authorization") or headers.get("Authorization")
                    if auth_header:
                        # Support "Bearer <token>" format
                        if auth_header.startswith("Bearer ") or auth_header.startswith("bearer "):
                            token = auth_header.split(" ", 1)[1] if " " in auth_header else None
                    
                    # Fallback to X-Snowflake-Password header for backward compatibility
                    password = (
                        headers.get("x-snowflake-password")
                        or headers.get("X-Snowflake-Password")
                        or headers.get("SNOWFLAKE_PASSWORD")
                    )
                    
                    # Only set if at least one header is provided
                    if any([account, user, role, warehouse, password, token]):
                        connection_params = {}
                        if account:
                            connection_params["account"] = account
                        if user:
                            connection_params["user"] = user
                        if role:
                            connection_params["role"] = role
                        if warehouse:
                            connection_params["warehouse"] = warehouse
                        if token:
                            # Use token (will be converted to password in _create_connection)
                            connection_params["token"] = token
                        elif password:
                            connection_params["password"] = password
                        
                        # Set in context for this request
                        request_connection_params.set(connection_params)
                    
                    try:
                        response = await call_next(request)
                        return response
                    finally:
                        # Clear the context after request completes
                        request_connection_params.set(None)
            
            # Add HTTP middleware to the FastMCP server's underlying app
            if hasattr(server, "app"):
                server.app.add_middleware(HeaderExtractionMiddleware)
            elif hasattr(server, "_app"):
                server._app.add_middleware(HeaderExtractionMiddleware)
            else:
                logger.warning("Could not access FastMCP app to add HTTP middleware. Header extraction may not work.")
        except ImportError:
            logger.warning("Starlette not available. HTTP header extraction may not work.")
        except Exception as e:
            logger.warning(f"Could not add HTTP middleware: {e}")

    try:
        logger.info("Starting Snowflake MCP Server...")

        if args.transport and args.transport in [
            "http",
            "sse",
            "streamable-http",
        ]:
            host = os.environ.get("SNOWFLAKE_MCP_HOST", args.server_host)
            port = int(os.environ.get("SNOWFLAKE_MCP_PORT", str(args.port)))
            endpoint = os.environ.get("SNOWFLAKE_MCP_ENDPOINT", args.endpoint)
            logger.info(f"Starting server with transport: {args.transport}")
            server.run(transport=args.transport, host=host, port=port, path=endpoint)
        else:
            logger.info(f"Starting server with transport: {args.transport or 'stdio'}")
            server.run(transport=args.transport or "stdio")

    except Exception as e:
        logger.error(f"Error starting MCP server: {e}")
        raise


if __name__ == "__main__":
    main()
