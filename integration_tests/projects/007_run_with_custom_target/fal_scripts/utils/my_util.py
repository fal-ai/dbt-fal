from .model_info import model_info_str


def handle_dbt_test(tests, output, model_name):
    for test in tests:
        output += f"\nRan {test.name} for {test.column}, result: {test.status}"
    f = open(f"temp/{model_name}_test", "w")
    f.write(output)
    f.close()
