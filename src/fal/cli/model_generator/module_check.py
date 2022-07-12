import ast
from typing import Iterator, List
import astor
import re


def generate_dbt_dependencies(module: ast.Module) -> str:
    """
    Search for dbt function uses and return them as found wrapped in Jinja braces.
    We do not modify them to let dbt decide if they make sense.
    """

    function_calls = _find_function_calls(ast.walk(module))
    ref_calls = _filter_function_calls_by_name(function_calls, "ref")
    source_calls = _filter_function_calls_by_name(function_calls, "source")

    dbt_ast_calls = _filter_constant_calls(ref_calls + source_calls)

    # Convert ast.Calls back to source code
    dbt_function_calls = list(map(astor.to_source, dbt_ast_calls))
    docstring_dbt_functions = _find_docstring_dbt_functions(module)

    lines: List[str] = docstring_dbt_functions + dbt_function_calls

    # Jinja-fy the calls
    return "\n".join(map(lambda s: "{{ " + s.strip() + " }}", lines))


def write_to_model_check(module: ast.Module):
    """
    Make sure a there is a single write_to_model function call on the top level
    """

    all_function_calls = _find_function_calls(ast.walk(module))
    all_wtm_calls = _filter_function_calls_by_name(all_function_calls, "write_to_model")

    assert (
        len(all_wtm_calls) > 0
    ), "There must be at least one write_to_model call in the Python Model"


def _find_function_calls(nodes: Iterator[ast.AST]) -> List[ast.Call]:
    return [node for node in nodes if isinstance(node, ast.Call)]


def _filter_function_calls_by_name(calls: List[ast.Call], func_name: str):
    """
    Analyze all function calls passed to find the ones that call `func_name`.
    """
    return [
        call
        for call in calls
        if isinstance(call.func, ast.Name) and call.func.id == func_name
    ]


def _filter_constant_calls(calls: List[ast.Call]) -> List[ast.Call]:
    """
    Analyze all function calls passed to find the ones with all literal arguments.
    We ignore a `_func(var)` but accept a `_func('model_name')`
    """

    def _is_constant(arg: ast.expr):
        import sys

        if sys.version_info < (3, 8):
            return isinstance(arg, ast.Str)
        else:
            return isinstance(arg, ast.Constant)

    return [call for call in calls if all(map(_is_constant, call.args))]


def _print_node(node: ast.AST):
    """
    For temporary usage during debugging.
    """
    print(
        node,
        *((f, getattr(node, f)) for f in node._fields),
        *((f, getattr(node, f)) for f in node._attributes),
    )


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
