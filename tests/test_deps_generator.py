import ast
from fal.cli.model_generator.deps_generator import generate_dbt_dependencies
from inspect import cleandoc


def test_finding_functions_with_literals():
    program = cleandoc(
        """
        mdl = ref('model_a')
        ref('model_b')
        ref('package', 'model_c')
        src = source('db', 'table_a')
        source('db', 'table_b')
        """
    )
    module = ast.parse(program)
    results = generate_dbt_dependencies(module)

    assert "{{ ref('model_a') }}" in results
    assert "{{ ref('model_b') }}" in results
    assert "{{ ref('package', 'model_c') }}" in results
    assert "{{ source('db', 'table_a') }}" in results
    assert "{{ source('db', 'table_b') }}" in results


def test_ignoring_functions_with_vars_or_exprs():
    program = cleandoc(
        """
        mdl = ref(some_var)
        ref(other)
        ref('a' + 'b')
        src = source('db', table_var)
        source(*['db', 'table_b'])
        """
    )
    module = ast.parse(program)
    results = generate_dbt_dependencies(module)

    assert "{{ ref(some_var) }}" not in results
    assert "{{ ref(other) }}" not in results
    assert "{{ ref('a' + 'b') }}" not in results
    assert "{{ source('db', table_var) }}" not in results
    assert "{{ source(*['db', 'table_b']) }}" not in results


def test_finding_functions_non_top_level():
    program = cleandoc(
        """
        if True:
            mdl = ref('model_a')
        else:
            mdl = ref('model_b')
        def my_funct():
            return ref('package', 'model_c')

        for x in []:
            if y:
                for z in [let for let in (lambda x: x + source('db', 'table_a'))(y)]:
                    source('db', 'table_b')
        """
    )
    module = ast.parse(program)
    results = generate_dbt_dependencies(module)

    assert "{{ ref('model_a') }}" in results
    assert "{{ ref('model_b') }}" in results
    assert "{{ ref('package', 'model_c') }}" in results
    assert "{{ source('db', 'table_a') }}" in results
    assert "{{ source('db', 'table_b') }}" in results


def test_finding_functions_in_docstring():
    program = cleandoc(
        """
        '''
        deps:
        ref('model_a') and source('db', 'table_a')
        '''
        mdl = ref(context.current_model.name)
        """
    )
    module = ast.parse(program)
    results = generate_dbt_dependencies(module)

    assert "{{ ref('model_a') }}" in results
    assert "{{ source('db', 'table_a') }}" in results
