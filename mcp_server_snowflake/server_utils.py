from contextvars import ContextVar
from typing import Any, Dict, Optional

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.utilities.logging import get_logger

from mcp_server_snowflake.object_manager.tools import validate_object_tool
from mcp_server_snowflake.query_manager.tools import validate_sql_type

try:
    from starlette.requests import Request
    STARLETTE_AVAILABLE = True
except ImportError:
    STARLETTE_AVAILABLE = False

logger = get_logger(__name__)

# Context variable to store per-request connection parameters from headers
# Defined here to avoid circular import with server.py
request_connection_params: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
    "request_connection_params", default=None
)


class HeaderConnectionMiddleware(Middleware):
    """Middleware that extracts connection parameters from HTTP headers for streamable-http transport."""

    def __init__(self, transport: str):
        self.transport = transport

    def _extract_headers(self):
        """Extract HTTP headers using FastMCP's dependency injection."""
        headers = {}
        
        # Use FastMCP's dependency injection to get HTTP headers
        # Use include_all=True to get ALL headers including custom X-Snowflake-* headers
        try:
            from fastmcp.server.dependencies import get_http_headers, get_http_request
            headers = get_http_headers(include_all=True) or {}
            
            # Also try to get the raw request object
            try:
                request = get_http_request()
                if request:
                    raw_headers = dict(request.headers)
                    # Merge raw headers (they might have more headers)
                    headers.update(raw_headers)
            except Exception:
                pass
        except (ImportError, Exception):
            pass
        
        # Also try to get from Starlette's request scope as fallback/additional source
        if STARLETTE_AVAILABLE:
            try:
                from contextvars import copy_context
                ctx = copy_context()
                # Look for Request in context
                for var in ctx:
                    if isinstance(var, Request):
                        starlette_headers = dict(var.headers)
                        # Merge Starlette headers (they might have more headers)
                        if starlette_headers:
                            # Starlette headers might have more complete set
                            headers.update(starlette_headers)
                        break
            except Exception:
                pass
        
        return headers

    def _extract_connection_params(self, headers):
        """Extract connection parameters from headers."""
        # Extract connection parameters from headers (case-insensitive)
        # Normalize headers to lowercase for matching
        headers_lower = {k.lower(): v for k, v in headers.items()}
        
        # Extract role and warehouse from headers (optional)
        role = (
            headers_lower.get("x-snowflake-role")
            or headers_lower.get("snowflake-role")
            or headers_lower.get("snowflake_role")
        )
        warehouse = (
            headers_lower.get("x-snowflake-warehouse")
            or headers_lower.get("snowflake-warehouse")
            or headers_lower.get("snowflake_warehouse")
        )
        
        # Extract token from Authorization Bearer header (case-insensitive)
        token = None
        auth_header = headers_lower.get("authorization")
        if auth_header:
            # Support "Bearer <token>" format (case-insensitive)
            auth_header_lower = auth_header.lower()
            if auth_header_lower.startswith("bearer "):
                token = auth_header.split(" ", 1)[1] if " " in auth_header else None
        
        # Fallback to X-Snowflake-Password header for backward compatibility (case-insensitive)
        password = (
            headers_lower.get("x-snowflake-password") 
            or headers_lower.get("snowflake-password")
            or headers_lower.get("snowflake_password")
        )
        
        # Only set if at least one header is provided
        if any([role, warehouse, password, token]):
            connection_params = {}
            if role:
                connection_params["role"] = role
            if warehouse:
                connection_params["warehouse"] = warehouse
            if token:
                # Use token (will be converted to password in _create_connection)
                connection_params["token"] = token
            elif password:
                connection_params["password"] = password
            
            return connection_params
        return None

    async def on_request(self, context: MiddlewareContext, call_next):
        """Extract connection parameters from headers at request level (runs before tool calls)."""
        # Only process headers for HTTP transports
        if self.transport not in ["http", "sse", "streamable-http"]:
            return await call_next(context)

        connection_params = None
        
        try:
            # Try multiple methods to get headers
            headers = self._extract_headers()
            
            # Extract connection parameters from headers
            connection_params = self._extract_connection_params(headers)
            
            if connection_params:
                # Set in context for this request
                request_connection_params.set(connection_params)
        except Exception:
            # If header extraction fails, continue with default connection
            pass

        try:
            return await call_next(context)
        finally:
            # Clear the context after request completes
            if connection_params is not None:
                request_connection_params.set(None)


class CheckQueryType(Middleware):
    """Middleware that checks SQL statement to ensure it is of an approved type."""

    def __init__(self, sql_allow_list: list[str], sql_disallow_list: list[str]):
        self.sql_allow_list = sql_allow_list
        self.sql_disallow_list = sql_disallow_list

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        """Called for all MCP tool calls."""
        tool_name = context.message.name

        # Check SQL statement permissions before running query
        if tool_name.lower() == "run_snowflake_query" and context.message.arguments.get(
            "statement", None
        ):
            statement_type, valid = validate_sql_type(
                context.message.arguments.get("statement", None),
                self.sql_allow_list,
                self.sql_disallow_list,
            )

        elif tool_name.lower().startswith("create") or tool_name.lower().startswith(
            "drop"
        ):
            statement_type, valid = validate_object_tool(
                tool_name, self.sql_allow_list, self.sql_disallow_list
            )

        # Allow other tools to proceed
        else:
            valid = True

        if valid:
            return await call_next(context)
        else:
            raise ToolError(
                f"Statement type of {statement_type} is not allowed. Please review sql statement permissions in configuration file."
            )


def initialize_middleware(server: FastMCP, snowflake_service):
    # Add header extraction middleware first (runs before other middleware)
    server.add_middleware(
        HeaderConnectionMiddleware(transport=snowflake_service.transport)
    )
    # Add query type checking middleware
    server.add_middleware(
        CheckQueryType(
            sql_allow_list=snowflake_service.sql_statement_allowed,
            sql_disallow_list=snowflake_service.sql_statement_disallowed,
        )
    )
