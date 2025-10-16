{% macro export_to_silver(model_name) %}
{% if target.name == 'dev' %}
{{ log(
    "Exporting " ~ model_name ~ " to silver folder",
    info = true
) }}
COPY (
    SELECT *
    FROM {{ this }}
) TO '../data/silver/{{ model_name }}.parquet' (FORMAT PARQUET);
{% endif %}
{% endmacro %}
