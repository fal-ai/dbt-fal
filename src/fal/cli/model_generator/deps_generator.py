import ast
from typing import List
import astor
import re


def generate_dbt_dependencies(module: ast.Module) -> str:
    """
    Search for dbt function uses and return them as found wrapped in Jinja braces.
    We do not modify them to let dbt decide if they make sense.
    """

    function_calls = _find_function_calls(module)

    dbt_ast_calls: List[ast.Call] = []
    dbt_ast_calls.extend(_find_dbt_function_calls(function_calls, "ref"))
    dbt_ast_calls.extend(_find_dbt_function_calls(function_calls, "source"))

    # Convert ast.Calls back to source code
    dbt_function_calls = list(map(astor.to_source, dbt_ast_calls))
    docstring_dbt_functions = _find_docstring_dbt_functions(module)

    lines: List[str] = docstring_dbt_functions + dbt_function_calls

    # Jinja-fy the calls
    return "\n".join(map(lambda s: "{{ " + s.strip() + " }}", lines))


def _find_function_calls(module: ast.Module) -> List[ast.Call]:
    return [node for node in ast.walk(module) if isinstance(node, ast.Call)]


def _find_dbt_function_calls(calls: List[ast.Call], func_name: str) -> List[ast.Call]:
    """
    Analyze all function calls in the file to find the ones that call `func_name` with all literal arguments.
    We ignore a `ref(var)` but accept a `ref('model_name')`
    """
    func_calls = [
        call
        for call in calls
        if isinstance(call.func, ast.Name) and call.func.id == func_name
    ]

    return [
        call
        for call in func_calls
        if all(map(lambda arg: isinstance(arg, ast.Constant), call.args))
    ]


REF_RE = re.compile("ref\\([^)]*\\)")
SOURCE_RE = re.compile("source\\([^)]*\\)")


def _find_docstring_dbt_functions(module: ast.Module) -> List[str]:
    '''
    Simple regex analysis for docstring in top of the file. User can list dependencies one per line, but not multiline.
    Example:

    """
    A Python model with some docstring introduction.

    Dependencies:
    - ref('model')
    - source('some', 'table')
    """
    '''
    docstring = ast.get_docstring(module, True) or ""

    calls = []
    for line in docstring.splitlines():
        calls.extend(REF_RE.findall(line))
        calls.extend(SOURCE_RE.findall(line))

    return calls
