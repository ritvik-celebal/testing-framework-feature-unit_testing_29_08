import os
import ast
import sqlparse
import re
import pytest
import json
 
SRC_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/udp_etl_framework'))
 
 
SQL_FILE_EXTENSIONS = ['.sql', '.hql']
PYTHON_FILE_EXTENSIONS = ['.py']
IPYNB_FILE_EXTENSIONS = ['.ipynb']
 
# Regex to find SQL queries in Python files (very basic, can be improved)
SQL_PATTERN = re.compile(r'(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|WITH)\s', re.IGNORECASE)
 
 
def is_python_file(filename):
    return any(filename.endswith(ext) for ext in PYTHON_FILE_EXTENSIONS)
 
def is_ipynb_file(filename):
    return any(filename.endswith(ext) for ext in IPYNB_FILE_EXTENSIONS)
 
def is_sql_file(filename):
    return any(filename.endswith(ext) for ext in SQL_FILE_EXTENSIONS)
 
def walk_source_files():
    for root, _, files in os.walk(SRC_ROOT):
        for file in files:
            yield os.path.join(root, file)
 
 
def check_python_syntax(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        source = f.read()
    try:
        ast.parse(source, filename=filepath)
    except SyntaxError as e:
        pytest.fail(f"Syntax error in {filepath}: {e}")
    # Optionally, check for embedded SQL strings
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Str):
            if SQL_PATTERN.search(node.s):
                try:
                    sqlparse.parse(node.s)
                except Exception as e:
                    pytest.fail(f"SQL parse error in string in {filepath}: {e}")
 
def check_ipynb_syntax(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            nb = json.load(f)
        except Exception as e:
            pytest.fail(f"Invalid JSON in notebook {filepath}: {e}")
    if 'cells' not in nb:
        pytest.fail(f"No 'cells' key in notebook {filepath}")
    for idx, cell in enumerate(nb['cells']):
        if cell.get('cell_type') == 'code':
            source = cell.get('source', [])
            if isinstance(source, list):
                code_lines = source
                code = ''.join(source)
            else:
                code_lines = source.splitlines(keepends=True)
                code = source
            # Skip if all lines are empty or whitespace
            if not code.strip():
                continue
            # Skip if first non-empty line starts with '%'
            for line in code_lines:
                if line.strip():
                    if line.lstrip().startswith('%'):
                        break
                    else:
                        # Only check syntax if not a magic command
                        try:
                            ast.parse(code, filename=f"{filepath} (cell {idx})")
                        except SyntaxError as e:
                            pytest.fail(f"Syntax error in {filepath} cell {idx}: {e}")
                        # Optionally, check for embedded SQL strings in code cell
                        for node in ast.walk(ast.parse(code)):
                            if isinstance(node, ast.Str):
                                if SQL_PATTERN.search(node.s):
                                    try:
                                        sqlparse.parse(node.s)
                                    except Exception as e:
                                        pytest.fail(f"SQL parse error in string in {filepath} cell {idx}: {e}")
                        break
 
def check_sql_file_syntax(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        sql = f.read()
    try:
        sqlparse.parse(sql)
    except Exception as e:
        pytest.fail(f"SQL parse error in {filepath}: {e}")
 
def test_all_source_files_syntax():
    """
    CI/CD test: Ensures all Python (.py), Databricks Notebook (.ipynb), and SQL (.sql, .hql) files in src/udp_etl_framework are free from syntax errors.
    Checks Python syntax (including PySpark and dynamic SQL in strings), parses SQL queries (in .sql files and Python strings), and checks .ipynb code cells.
    This test is designed to be run in a CI/CD pipeline to block PRs with syntax errors in any supported file type.
    """
    print("Running syntax check on all source files...")
    found = False
    for filepath in walk_source_files():
        found = True
        if is_python_file(filepath):
            check_python_syntax(filepath)
        elif is_ipynb_file(filepath):
            check_ipynb_syntax(filepath)
        elif is_sql_file(filepath):
            check_sql_file_syntax(filepath)
    assert found, "No source files found to check!"