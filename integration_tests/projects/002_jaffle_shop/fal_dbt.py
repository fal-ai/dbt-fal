from fal import FalDbt

faldbt = FalDbt(project_dir=".", profiles_dir="~/.dbt")
print(faldbt)
print(faldbt.tests)

for test in faldbt.tests:
    print(test)

for source in faldbt.sources:
    print(source.name, source.table_name, [(t.name, t.status) for t in source.tests])

for model in faldbt.models:
    print(model.name, [(t.name, t.status) for t in model.tests])
