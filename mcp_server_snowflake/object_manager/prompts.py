def get_object_mgmt_prompt(action: str, object_types: list[str]):
    base = f"""Generic tool to {action.lower()} a Snowflake object including {", ".join(object_types)}."""
    samples = _get_object_mgmt_sample(action)
    return f"{base}\n\n{samples}" if samples else base


def _get_object_mgmt_sample(action: str) -> str:
    """Return a sample invocation for the LLM to refer to."""
    action_lower = action.lower()
    if action_lower == "create":
        return """Sample: create_object(object_type="database", target_object={"name": "MY_DB"}, mode="if_not_exists")
Sample: create_object(object_type="table", target_object={"database_name": "MY_DB", "schema_name": "PUBLIC", "name": "MY_TABLE", "columns": [{"name": "ID", "datatype": "NUMBER"}]})"""
    if action_lower == "drop":
        return """Sample: drop_object(object_type="table", target_object={"database_name": "MY_DB", "schema_name": "PUBLIC", "name": "MY_TABLE"})"""
    if action_lower == "create_or_alter":
        return """Sample: create_or_alter_object(object_type="warehouse", target_object={"name": "MY_WH", "warehouse_size": "SMALL"})"""
    if action_lower == "describe":
        return """Sample (use target_object): describe_object(object_type="table", target_object={"database_name": "MY_DB", "schema_name": "PUBLIC", "table_name": "MY_TABLE"})
Sample (use name/database_name/schema_name): describe_object(object_type="table", database_name="MY_DB", schema_name="PUBLIC", name="MY_TABLE")
Note: For tables/views you may use "table_name"/"view_name" or "name" in target_object. To list objects (e.g. schemas in a database), use list_objects instead."""
    if action_lower == "list":
        return """Sample: list_objects(object_type="schema", database_name="MY_DB")
Sample: list_objects(object_type="table", database_name="MY_DB", schema_name="PUBLIC")
Sample: list_objects(object_type="database")  # list in account"""
    return ""
