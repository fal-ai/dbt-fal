import sys
from fal import FalDbt
from _fal_testing import create_file

from faldbt.project import DbtGenericTest, DbtSingularTest

project_dir = "."
if len(sys.argv) >= 2:
    project_dir = sys.argv[1]
profiles_dir = "~/.dbt"
if len(sys.argv) >= 3:
    profiles_dir = sys.argv[2]

print(f"project_dir={project_dir}, profiles_dir={profiles_dir}")

faldbt = FalDbt(project_dir=project_dir, profiles_dir=profiles_dir)
print(faldbt)
print(faldbt.tests)

for test in faldbt.tests:
    print(test)

for source in faldbt.sources:
    print(source.name, source.table_name, [(t.name, t.status) for t in source.tests])

for model in faldbt.models:
    print(model.name, [(t.name, t.status) for t in model.tests])

output = f"There are {len(faldbt.tests)} tests\n"
output += f"There are {len([t for t in faldbt.tests if isinstance(t, DbtGenericTest)])} generic tests\n"
output += f"There are {len([t for t in faldbt.tests if isinstance(t, DbtSingularTest)])} singular tests\n"

for test in faldbt.tests:
    output += f"test {test.name} {'generic' if isinstance(test, DbtGenericTest) else 'singular'} \n"

create_file(output, "fal_dbt.txt")
