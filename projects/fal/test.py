from fal import FalDbt

p = FalDbt('/Users/matteo/Projects/fal-ai/fal/projects/fal/integration_tests/projects/000_fal_run', '~/.dbt')
print(p)
print(p.models)
print(p.sources)
