#!/bin/bash
# Local dbt runner script
python -m dbt.cli.main "$@" --profiles-dir .