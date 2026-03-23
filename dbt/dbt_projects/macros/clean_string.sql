{% macro clean_string(column_name) %}
    CASE
        WHEN TRIM({{ column_name }}) IN ("","—") THEN NULL
        ELSE TRIM({{ column_name }})
    END
{% endmacro %}