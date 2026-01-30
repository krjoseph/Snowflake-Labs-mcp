from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware, MiddlewareContext

from mcp_server_snowflake.object_manager.tools import validate_object_tool
from mcp_server_snowflake.query_manager.tools import validate_sql_type
from mcp_server_snowflake.server import request_connection_params

try:
    from starlette.requests import Request
    STARLETTE_AVAILABLE = True
except ImportError:
    STARLETTE_AVAILABLE = False


class HeaderConnectionMiddleware(Middleware):
    """Middleware that extracts connection parameters from HTTP headers for streamable-http transport."""

    def __init__(self, transport: str):
        self.transport = transport

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        """Extract connection parameters from headers if using HTTP transport."""
        # Only process headers for HTTP transports
        if self.transport not in ["http", "sse", "streamable-http"]:
            return await call_next(context)

        connection_params = None
        
        try:
            headers = {}
            
            # Try multiple ways to access the HTTP request
            # Method 1: Check if context has request attribute
            if hasattr(context, "request"):
                request = context.request
                if hasattr(request, "headers"):
                    headers = request.headers
            # Method 2: Try to get from Starlette's request scope
            elif STARLETTE_AVAILABLE:
                try:
                    # Try to access request from contextvars (Starlette pattern)
                    from contextvars import copy_context
                    ctx = copy_context()
                    # Look for Request in context
                    for var in ctx:
                        if isinstance(var, Request):
                            headers = var.headers
                            break
                except Exception:
                    pass
            
            # Extract connection parameters from headers
            # Support both x-snowflake-* and snowflake-* header formats
            account = headers.get("x-snowflake-account") or headers.get("snowflake-account") or headers.get("X-Snowflake-Account")
            user = headers.get("x-snowflake-user") or headers.get("snowflake-user") or headers.get("X-Snowflake-User")
            role = headers.get("x-snowflake-role") or headers.get("snowflake-role") or headers.get("X-Snowflake-Role")
            warehouse = headers.get("x-snowflake-warehouse") or headers.get("snowflake-warehouse") or headers.get("X-Snowflake-Warehouse")
            
            # Extract token from Authorization Bearer header
            token = None
            auth_header = headers.get("authorization") or headers.get("Authorization")
            if auth_header:
                # Support "Bearer <token>" format
                if auth_header.startswith("Bearer ") or auth_header.startswith("bearer "):
                    token = auth_header.split(" ", 1)[1] if " " in auth_header else None
            
            # Fallback to X-Snowflake-Password header for backward compatibility
            password = headers.get("x-snowflake-password") or headers.get("snowflake-password") or headers.get("X-Snowflake-Password")
            
            # Also check environment variable names as headers (for compatibility)
            if not account:
                account = headers.get("SNOWFLAKE_ACCOUNT")
            if not user:
                user = headers.get("SNOWFLAKE_USER")
            if not role:
                role = headers.get("SNOWFLAKE_ROLE")
            if not warehouse:
                warehouse = headers.get("SNOWFLAKE_WAREHOUSE")
            if not password and not token:
                password = headers.get("SNOWFLAKE_PASSWORD")
            
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
        except Exception as e:
            # If header extraction fails, continue with default connection
            import logging
            logging.getLogger(__name__).debug(f"Could not extract headers: {e}")

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
