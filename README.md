### Building

```
python -m venv .venv
.  .venv/bin/activate
poetry build
pipx install dist/athena_tools-0.1.0-py3-none-any.whl
athena_query_executor -h
```
