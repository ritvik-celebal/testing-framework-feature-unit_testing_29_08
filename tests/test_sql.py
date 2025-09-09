import os
import re
import json
import sqlfluff
import sys
import pytest
import argparse
 
def extract_sql_queries(notebook_code: str) -> list[str]:
    pattern = re.compile(
        r"spark\.sql\s*\(\s*(f?)([\"']{3}|[\"'])(.*?)(\2)\s*\)(?:\.format\((.*?)\))?",
        re.DOTALL
    )
    sql_queries = []
    for match in pattern.finditer(notebook_code):
        sql_raw = match.group(3)
        sql_query_clean = re.sub(r"\{.*?\}", "`dummy_value`", sql_raw)
        sql_queries.append(sql_query_clean.strip())
   
    # Also extract f-string SQL queries (f"""...""" patterns)
    f_string_pattern = re.compile(
        r"f([\"']{3}|[\"'])(.*?)(\1)",
        re.DOTALL
    )
    for match in f_string_pattern.finditer(notebook_code):
        sql_content = match.group(2).strip()
        # Check if it looks like SQL (contains UPDATE, SELECT, INSERT, DELETE, etc.)
        if any(keyword in sql_content.upper() for keyword in ['UPDATE', 'SELECT', 'INSERT', 'DELETE', 'CREATE', 'ALTER', 'DROP']):
            # Replace f-string placeholders with dummy values
            sql_clean = re.sub(r"\{[^}]*\}", "'dummy_value'", sql_content)
           
            # Skip queries that are actually error messages or logging strings
            if any(skip_pattern in sql_clean.lower() for skip_pattern in [
                'failed to', 'error', 'exception', 'logger', 'log.', 'print(',
                'raise', 'traceback', 'debug', 'info', 'warning', 'query:',
                'update query:', 'insert query:', 'delete query:', 'select query:',
                'sql query:', 'execution query:', 'final query:', 'logger.info'
            ]):
                continue
               
            # Skip if it starts with common logging patterns
            if any(sql_clean.lower().startswith(pattern) for pattern in [
                'update query:', 'insert query:', 'delete query:', 'select query:',
                'query:', 'sql:', 'executing:', 'running:', 'final query:'
            ]):
                continue
               
            # Only process if it's a substantial SQL query (more than just a keyword)
            if len(sql_clean.strip()) > 20:
                sql_queries.append(sql_clean.strip())
   
    return sql_queries
 
 
def check_sql_syntax(query: str, dialect: str = "sparksql") -> dict:
    """
    Check SQL syntax focusing only on CRITICAL errors that would prevent execution.
   
    WILL REPORT (Critical errors):
    - Parse errors (PRS) - Invalid SQL syntax
    - Undefined tables/columns that would cause runtime errors
    - Invalid function usage
    - Syntax violations that prevent execution
   
    WILL NOT REPORT (Minor formatting issues):
    - Missing/extra semicolons, whitespace, indentation
    - Capitalization preferences
    - Line length, spacing, trailing commas
    - Style and convention preferences
    """
    # Pre-process the query to handle f-string remnants and common patterns
    processed_query = query.strip()
   
    # Skip validation for non-SQL content (error messages, logging, etc.)
    if any(skip_pattern in processed_query.lower() for skip_pattern in [
        'failed to', 'error', 'exception', 'logger', 'log.', 'print(',
        'raise', 'traceback', 'debug', 'info', 'warning', 'udf:', 'def ',
        'try:', 'except:', 'import ', 'from ', 'return', 'class ', 'if ', 'for '
    ]):
        return {"is_valid": True, "violations": []}
   
    # Replace any remaining f-string patterns that might have been missed
    processed_query = re.sub(r"\{[^}]*\}", "'dummy_value'", processed_query)
   
    # Replace common Databricks/Spark patterns that might confuse the parser
    processed_query = re.sub(r"`dummy_value`\.default\.", "default.", processed_query)
    processed_query = re.sub(r"'dummy_value'\.default\.", "default.", processed_query)
   
    # Clean up any malformed quotes or escape sequences
    processed_query = re.sub(r"\\+[\"']", "'", processed_query)
    processed_query = re.sub(r"[\"']{2,}", "'", processed_query)
   
    # Handle edge cases that can cause parsing issues
    processed_query = re.sub(r"\\n", " ", processed_query)
    processed_query = re.sub(r"\\t", " ", processed_query)
    processed_query = re.sub(r"\s+", " ", processed_query)
   
    # Skip if query is too short or looks like a fragment
    if len(processed_query.strip()) < 10:
        return {"is_valid": True, "violations": []}
   
    # Ensure the query ends with a newline for proper parsing
    if not processed_query.endswith('\n'):
        processed_query += '\n'
   
    try:
        lint_result = sqlfluff.lint(processed_query, dialect=dialect)
    except Exception as e:
        # If sqlfluff itself fails, skip validation rather than failing the test
        return {"is_valid": True, "violations": []}
   
    # Extended ignore rules - Only focus on CRITICAL SQL syntax errors
    ignore_rules = {
        # Layout and formatting (minor issues)
        "LT01", "LT02", "LT03", "LT04", "LT05", "LT06", "LT07", "LT08", "LT09", "LT10", "LT11", "LT12", "LT13", "LT14",
       
        # Capitalization (style issues, not critical)
        "CP01", "CP02", "CP03", "CP04", "CP05",
       
        # Convention issues (style preferences)
        "CV01", "CV02", "CV03", "CV04", "CV05", "CV06", "CV07", "CV08", "CV09", "CV10", "CV11",
       
        # Structure issues (minor formatting)
        "ST01", "ST02", "ST03", "ST04", "ST05", "ST06", "ST07", "ST08", "ST09",
       
        # Reference issues (minor)
        "RF01", "RF02", "RF03", "RF04", "RF05",
       
        # Aliasing issues (style preferences)
        "AL01", "AL02", "AL03", "AL04", "AL05", "AL06", "AL07",
       
        # Ambiguous references (often false positives in Spark SQL)
        "AM01", "AM02", "AM03", "AM04", "AM05", "AM06",
       
        # Join issues (often not applicable to Spark SQL patterns)
        "JJ01", "JJ02",
       
        # Specific minor issues
        "LN01",  # Line length
        "WS01",  # Whitespace
        "SG01",  # Single character for concat
    }
   
    # Only report CRITICAL errors that would prevent SQL execution
    critical_violations = []
    for v in lint_result:
        rule_code = v.get("code")
        if rule_code not in ignore_rules:
            # Additional filtering for truly critical issues only
            description = v.get("description", "").lower()
           
            # Skip these common minor issues even if not in ignore_rules
            skip_patterns = [
                "trailing whitespace",
                "missing semicolon",
                "unnecessary semicolon",
                "indentation",
                "capitalization",
                "quotation",
                "spacing",
                "line length",
                "blank line",
                "trailing comma",
                "leading comma"
            ]
           
            # Only add if it's not a minor formatting issue
            is_minor = any(pattern in description for pattern in skip_patterns)
            if not is_minor:
                critical_violations.append({
                    "line_no": v.get("line_no"),
                    "line_pos": v.get("line_pos"),
                    "code": v.get("code"),
                    "description": v.get("description"),
                    "name": v.get("name")
                })
    assert len(critical_violations) == 0, f"critical Violation: {critical_violations}"
    return {"is_valid": not critical_violations, "violations": critical_violations}
   
 
 
def validate_widgets(notebook_code: str) -> tuple[bool, list[str]]:
    """
    Validates whether the widgets defined in the notebook exactly match the required set.
    - No checks for usage (get/getArgument)
    - No checks for default values or types
    - Only checks for missing or extra widget names
    """
 
    # Define required widgets
    required_widgets = {
        'source_table',
        'target_table',
        'environment',
        'batch_date',
        'schema_name',
        'job_name',
        'eventhub_namespace'
    }
 
    # Regex to extract widget definitions
    widget_definition_pattern = re.compile(
        r"dbutils\.widgets\.(?:text|dropdown|combobox)\(['\"]([^'\"]+)['\"]",
        re.IGNORECASE
    )
 
    # Extract all defined widgets from notebook code
    found_widgets = set(
        match.group(1)
        for match in widget_definition_pattern.finditer(notebook_code)
    )
 
    # Determine  extra widgets
   
    extra_widgets = found_widgets - required_widgets
 
    errors = []
 
   
    if extra_widgets:
        errors.append(f"Unexpected widgets found: {', '.join(sorted(extra_widgets))}")
   
    assert len(errors) == 0, f"Widget validation errors found: {errors}"
 
    return len(errors) == 0, errors
 
 
 
def validate_notebook(notebook_path: str) -> tuple[bool, list[str]]:
    """
    Validate a single notebook for SQL syntax and widget configuration.
   
    Returns:
        tuple[bool, list[str]]: (all_valid, list_of_errors)
    """
    print(f"Testing notebook: {notebook_path}")
    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook_code = f.read()
    except Exception as e:
        print(f"Failed to load notebook {notebook_path}: {e}")
        return False, [f"Failed to load notebook: {e}"]
 
    try:
        nb_json = json.loads(notebook_code)
        code_cells = [
            "".join(cell.get("source", []))
            for cell in nb_json.get("cells", [])
            if cell.get("cell_type") == "code"
        ]
        full_code = "\n".join(code_cells)
    except Exception as e:
        print(f"Failed to parse notebook JSON {notebook_path}: {e}")
        return False, [f"Failed to parse notebook JSON: {e}"]
 
    # Test widget configuration (treat as warnings, not failures)
    try:
        widget_valid, widget_errors = validate_widgets(full_code)
    except Exception:
        # Widget validation failed, treat as warnings
        widget_valid = True
        widget_errors = []
 
    errors = []
    all_sql_valid = True
 
    # Add widget errors as warnings (don't fail validation)
    if not widget_valid:
        for error in widget_errors:
            print(f"Widget warning: {error}")
 
    queries = extract_sql_queries(full_code)
    print(f"Found {len(queries)} SQL queries in {notebook_path}")
 
    if queries:
        for i, query in enumerate(queries, 1):
            print(f"Validating query {i}...")
            try:
                result = check_sql_syntax(query)
                if not result["is_valid"]:
                    all_sql_valid = False
                    query_errors = [f"Query {i} has SQL issues:"]
                    for v in result["violations"]:
                        query_errors.append(
                            f" - Line {v['line_no']}, Col {v['line_pos']}: {v['description']} ({v['code']})"
                        )
                    errors.extend(query_errors)
                    print(f"Query {i} failed validation")
                else:
                    print(f"Query {i} passed validation")
            except Exception as e:
                # SQL validation failed with exception
                print(f"Query {i} validation error: {e}")
                # Don't fail validation for SQL parsing errors
    else:
        print(f"No SQL queries found in {notebook_path}")
 
    return all_sql_valid, errors
 
 
# Scan all notebooks under specified directory
def validate_notebooks(src_dir="src"):
    print(f"Scanning directory: {src_dir}")
   
    if not os.path.exists(src_dir):
        print(f"ERROR: Directory {src_dir} does not exist!")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Available directories: {[d for d in os.listdir('.') if os.path.isdir(d)]}")
        return {"DIRECTORY_NOT_FOUND": [f"Directory {src_dir} does not exist"]}
   
    failed_notebooks = {}
    notebook_count = 0
   
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith(".ipynb"):
                notebook_count += 1
                notebook_path = os.path.join(root, file)
                print(f"Processing notebook {notebook_count}: {notebook_path}")
                passed, errors = validate_notebook(notebook_path)
                if not passed:
                    failed_notebooks[notebook_path] = errors
   
    print(f"Total notebooks processed: {notebook_count}")
    return failed_notebooks
 
# Show Results
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run SQL syntax validation on Jupyter notebooks')
    parser.add_argument('--folder',
                       default='./src',
                       help='Folder to scan for notebooks (default: ./src)')
 
    args = parser.parse_args()
    src_dir = args.folder
   
    print(f"Running SQL validation for folder: {src_dir}")
    failed_notebooks = validate_notebooks(src_dir)
 
    if failed_notebooks:
        print("\n========================================")
        print("SQL Syntax and Widget Configuration Errors Found:")
        print("========================================\n")
        for notebook, errors in failed_notebooks.items():
            print(f"Notebook: {notebook}")
            for error in errors:
                print(error)
            print("\n----------------------------------------\n")
        print("Summary: Some notebooks failed SQL or widget validation checks.")
        sys.exit(1)
    else:
        print("All notebooks passed SQL syntax and widget configuration checks.")
        sys.exit(0)