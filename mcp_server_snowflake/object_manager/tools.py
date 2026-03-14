import json
from typing import Annotated, Any, Literal, Optional, Union, get_args

from fastmcp import FastMCP
from pydantic import Field
from snowflake.core import CreateMode, Root

from mcp_server_snowflake.object_manager.objects import (
    SnowflakeComputePool,
    SnowflakeDatabase,
    SnowflakeImageRepository,
    SnowflakeObject,
    SnowflakeRole,
    SnowflakeSchema,
    SnowflakeStage,
    SnowflakeTable,
    SnowflakeUser,
    SnowflakeView,
    SnowflakeWarehouse,
    supported_objects,
)
from mcp_server_snowflake.object_manager.prompts import (
    get_object_mgmt_prompt,
)
from mcp_server_snowflake.utils import get_request_headers_for_tools, SnowflakeException, execute_query


def get_class_name(object_type: Any) -> str:
    return object_type.__class__.__name__.removesuffix("Model")


def create_object(
    snowflake_object: SnowflakeObject,
    root: Root,
    mode: Literal["error_if_exists", "replace", "if_not_exists"] = "error_if_exists",
):
    if mode == "error_if_exists":
        create_mode = CreateMode.error_if_exists
    elif mode == "replace":
        create_mode = CreateMode.or_replace
    elif mode == "if_not_exists":
        create_mode = CreateMode.if_not_exists
    else:
        create_mode = CreateMode.if_not_exists
    core_object = snowflake_object.get_core_object()
    core_path = snowflake_object.get_core_path(root=root)
    try:
        core_path.create(core_object, mode=create_mode)
        return f"Created {get_class_name(core_object)} {core_object.name}."
    except Exception as e:
        raise SnowflakeException(tool="create_object", message=str(e))


def drop_object(snowflake_object: SnowflakeObject, root: Root, if_exists: bool = False):
    core_object = snowflake_object.get_core_object()
    core_path = snowflake_object.get_core_path(root=root)
    try:
        core_path[core_object.name].drop(if_exists=if_exists)
        return f"Dropped {get_class_name(core_object)} {core_object.name}."
    except Exception as e:
        raise SnowflakeException(tool="drop_object", message=str(e))


def create_or_alter_object(snowflake_object: SnowflakeObject, root: Root):
    core_object = snowflake_object.get_core_object()
    core_path = snowflake_object.get_core_path(root=root)
    try:
        # First need to fetch the existing object
        existing_object = core_path[core_object.name].fetch()

        # Then update the existing object with the new properties
        data = snowflake_object.model_dump(exclude_unset=True)
        # Update only non-None values
        for key, value in data.items():
            if value is not None and hasattr(existing_object, key):
                setattr(existing_object, key, value)
        # Then create or alter the object
        core_path[core_object.name].create_or_alter(existing_object)
        return f"Created or altered {get_class_name(core_object)} {core_object.name}."

    except Exception as e:
        raise SnowflakeException(tool="create_or_alter_object", message=str(e))


def describe_object(snowflake_object: SnowflakeObject, root: Root):
    core_object = snowflake_object.get_core_object()
    core_path = snowflake_object.get_core_path(root=root)
    try:
        return core_path[core_object.name].fetch().to_dict()
    except Exception as e:
        raise SnowflakeException(tool="describe_object", message=str(e))


def list_objects(
    snowflake_service,
    object_type: supported_objects,
    database_name: str = None,
    schema_name: str = None,
    like: str = None,
    starts_with: str = None,
    headers: dict = None,
):
    bindvars = []
    if object_type == "image_repository":
        object_name = "image repositories"
    elif object_type == "compute_pool":
        object_name = "compute pools"
    else:
        object_name = f"{object_type}s"

    # Note: SHOW statements do not support variable binding. String formatting is
    # safe here because object_type is restricted to a set of whitelisted values.
    statement = f"SHOW {object_name}"

    if like:
        statement += " LIKE ?"
        bindvars.extend([f"%{like.replace('%', '')}%"])

    if object_type in ["database", "compute_pool", "role", "user"]:
        pass
    elif database_name is None and schema_name is None:
        statement += " IN ACCOUNT"
    elif database_name and schema_name:
        statement += " IN SCHEMA identifier(?)"
        bindvars.extend([f"{database_name}.{schema_name}"])
    elif database_name:
        statement += " IN DATABASE identifier(?)"
        bindvars.extend([database_name])
    elif schema_name:
        statement += " IN SCHEMA identifier(?)"
        bindvars.extend([schema_name])
    else:
        raise SnowflakeException(
            tool="list_objects",
            message="Please specify a database, database + schema, or neither to query the account.",
        )

    if starts_with:
        # sanitizing string manually because bind variables are not supported here
        sanitized_starts_with = starts_with.replace("'", "")
        statement += f" STARTS WITH '{sanitized_starts_with}'"

    try:
        result = execute_query(statement, snowflake_service, bindvars, headers=headers)

        if len(result) > 0:
            return result[0:1000]  # Limit to 1000 results
        else:
            return f"No matching {object_name} found."
    except Exception as e:
        raise SnowflakeException(tool="list_objects", message=str(e))


def _get_object_class(obj_type: supported_objects):
    """Map object type string to Snowflake object model class."""
    if obj_type == "database":
        return SnowflakeDatabase
    elif obj_type == "schema":
        return SnowflakeSchema
    elif obj_type == "table":
        return SnowflakeTable
    elif obj_type == "view":
        return SnowflakeView
    elif obj_type == "warehouse":
        return SnowflakeWarehouse
    elif obj_type == "compute_pool":
        return SnowflakeComputePool
    elif obj_type == "role":
        return SnowflakeRole
    elif obj_type == "stage":
        return SnowflakeStage
    elif obj_type == "user":
        return SnowflakeUser
    elif obj_type == "image_repository":
        return SnowflakeImageRepository
    else:
        raise ValueError(f"Invalid object type: {obj_type}")


def build_target_object_from_scope(
    object_type: supported_objects,
    database_name: str | None,
    schema_name: str | None,
    name: str | None,
) -> dict:
    """Build target_object dict from scope params so callers can pass database_name/schema_name/name instead of target_object."""
    if object_type == "database":
        if not name:
            raise SnowflakeException(
                tool="describe_object",
                message="For object_type 'database' provide target_object or name (database name).",
            )
        return {"name": name}
    if object_type == "schema":
        if not database_name or not name:
            msg = "For object_type 'schema' provide target_object or both database_name and name (schema name)."
            if database_name and not name:
                msg += " To list schemas in a database, use list_objects with object_type 'schema' and database_name."
            raise SnowflakeException(tool="describe_object", message=msg)
        return {"database_name": database_name, "name": name}
    if object_type in ("table", "view"):
        if not database_name or not schema_name or not name:
            raise SnowflakeException(
                tool="describe_object",
                message=f"For object_type '{object_type}' provide target_object or database_name, schema_name, and name.",
            )
        key = "table_name" if object_type == "table" else "view_name"
        return {"database_name": database_name, "schema_name": schema_name, key: name}
    if object_type in ("warehouse", "compute_pool", "role", "user"):
        if not name:
            raise SnowflakeException(
                tool="describe_object",
                message=f"For object_type '{object_type}' provide target_object or name.",
            )
        return {"name": name}
    if object_type == "stage":
        if not database_name or not schema_name or not name:
            raise SnowflakeException(
                tool="describe_object",
                message="For object_type 'stage' provide target_object or database_name, schema_name, and name.",
            )
        return {"database_name": database_name, "schema_name": schema_name, "name": name}
    if object_type == "image_repository":
        if not database_name or not schema_name or not name:
            raise SnowflakeException(
                tool="describe_object",
                message="For object_type 'image_repository' provide target_object or database_name, schema_name, and name.",
            )
        return {"database_name": database_name, "schema_name": schema_name, "name": name}
    raise SnowflakeException(tool="describe_object", message=f"Unknown object_type: {object_type}")


def parse_object(target_object: Any, obj_type: supported_objects):
    """Parse target_object into a Pydantic model.
    If the target_object is a string, parse JSON then into a Pydantic model.
    If the target_object is a dict, convert it to the appropriate Pydantic model.
    If the target_object is already a Pydantic model, return it.
    """
    if isinstance(target_object, str):
        try:
            parsed_data = json.loads(target_object)
        except Exception as e:
            raise e
        target_object = parsed_data
    if isinstance(target_object, dict):
        obj_class = _get_object_class(obj_type)
        return obj_class(**target_object)
    return target_object


def initialize_object_manager_tools(server: FastMCP, snowflake_service):
    supported_objects_list = list(get_args(supported_objects))
    sql_allowed = getattr(snowflake_service, "sql_statement_allowed", [])
    # Only expose write/delete tools when config allows (Create/Drop in sql_statement_permissions)
    allow_create = "create" in sql_allowed
    allow_drop = "drop" in sql_allowed

    def get_root(headers: dict = None):
        """Get Root object, using headers for multi-tenant mode."""
        if snowflake_service.transport == "streamable-http" and headers:
            try:
                _, _, root = snowflake_service.get_connection_from_headers(headers)
                return root
            except Exception:
                pass
        return snowflake_service.root
    object_type_annotation = Annotated[
        supported_objects,
        Field(
            description=f"Type of Snowflake object. One of {', '.join(supported_objects_list)}"
        ),
    ]
    # Extract union members from SnowflakeObject TypeAlias for Pydantic schema generation
    # (TypeAlias doesn't work well with FastMCP/Pydantic v2, but explicit Union does)
    target_object_annotation = Annotated[
        Union[
            str, *get_args(SnowflakeObject)
        ],  # Allow both object and string inputs - Some LLMs still pass as JSON string
        Field(
            description="Always pass properties of target_object as an object, not a string"
        ),
    ]
    optional_target_object_annotation = Annotated[
        Union[str, None, *get_args(SnowflakeObject)],
        Field(
            description="Target object. Optional if database_name/schema_name/name are provided instead.",
            default=None,
        ),
    ]

    if allow_create:
        @server.tool(
            name="create_object",
            description=get_object_mgmt_prompt("create", supported_objects_list),
        )
        def create_object_tool(
            object_type: object_type_annotation,
            target_object: target_object_annotation,
            mode: Literal[
                "error_if_exists", "replace", "if_not_exists"
            ] = "error_if_exists",
        ):
            http_headers = get_request_headers_for_tools()
            target_object = parse_object(target_object, object_type)
            root = get_root(http_headers)
            return create_object(target_object, root, mode)

        @server.tool(
            name="create_or_alter_object",
            description=get_object_mgmt_prompt("create_or_alter", supported_objects_list),
        )
        def create_or_alter_object_tool(
            object_type: object_type_annotation,
            target_object: target_object_annotation,
        ):
            http_headers = get_request_headers_for_tools()
            target_object = parse_object(target_object, object_type)
            root = get_root(http_headers)
            return create_or_alter_object(target_object, root)

    if allow_drop:
        @server.tool(
            name="drop_object",
            description=get_object_mgmt_prompt("drop", supported_objects_list),
        )
        def drop_object_tool(
            object_type: object_type_annotation,
            target_object: target_object_annotation,
            if_exists: bool = False,
        ):
            http_headers = get_request_headers_for_tools()
            target_object = parse_object(target_object, object_type)
            root = get_root(http_headers)
            return drop_object(target_object, root, if_exists)

    @server.tool(
        name="describe_object",
        description=get_object_mgmt_prompt("describe", supported_objects_list),
    )
    def describe_object_tool(
        object_type: object_type_annotation,
        target_object: optional_target_object_annotation = None,
        database_name: Annotated[
            str | None,
            Field(
                description="Database name. Use with schema_name and name (or alone for database object_type) when not passing target_object.",
                default=None,
            ),
        ] = None,
        schema_name: Annotated[
            str | None,
            Field(
                description="Schema name. Use with database_name and name when not passing target_object.",
                default=None,
            ),
        ] = None,
        name: Annotated[
            str | None,
            Field(
                description="Object name (e.g. table name, schema name). Use with database_name (and schema_name for table/view) when not passing target_object.",
                default=None,
            ),
        ] = None,
    ):
        http_headers = get_request_headers_for_tools()
        if target_object is None:
            if database_name is not None or schema_name is not None or name is not None:
                target_object = build_target_object_from_scope(
                    object_type, database_name, schema_name, name
                )
            else:
                raise SnowflakeException(
                    tool="describe_object",
                    message="Provide target_object or database_name/schema_name/name. To list objects (e.g. schemas in a database), use list_objects instead.",
                )
        target_object = parse_object(target_object, object_type)
        root = get_root(http_headers)
        return describe_object(target_object, root)

    @server.tool(
        name="list_objects",
        description=get_object_mgmt_prompt("list", supported_objects_list),
    )
    def list_objects_tool(
        object_type: object_type_annotation,
        database_name: str | None = None,
        schema_name: str | None = None,
        like: Annotated[
            str | None,
            Field(
                description="Filter objects by keyword in name. Uses case-insensitive pattern matching, with support for SQL wildcard characters (% and _).",
                default=None,
            ),
        ] = None,
        starts_with: Annotated[
            str | None,
            Field(
                description="Filter objects by start of name. Case sensitive. Ignored for warehouses.",
                default=None,
            ),
        ] = None,
    ):
        http_headers = get_request_headers_for_tools()
        return list_objects(
            snowflake_service,
            object_type,
            database_name,
            schema_name,
            like,
            starts_with,
            headers=http_headers,
        )


def validate_object_tool(
    function_name: str, sql_allow_list: list[str], sql_disallow_list: list[str]
) -> tuple[str, bool]:
    """
    Validates a function call against a list of allowed and disallowed object types.

    Only consider some object actions for now including create, create_or_alter, and drop.
    """
    if function_name.lower().startswith(
        "create"
    ):  # Will also capture create_or_alter, which is intended
        func_type = "create"
    elif function_name.lower().startswith("drop"):
        func_type = "drop"
    else:
        return ("", True)

    # User has not added any permissions, so we default to disallowing all object actions
    if len(sql_allow_list) == 0 and len(sql_disallow_list) == 0:
        valid = False

    if func_type in sql_allow_list:
        valid = True
    elif func_type in sql_disallow_list:
        valid = False
    else:
        valid = False

    return (func_type, valid)
