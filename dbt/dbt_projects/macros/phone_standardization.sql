{% macro standardize_phone(column_name) %}
    CASE
        WHEN {{ column_name }} LIKE '(%'
            THEN REGEXP_REPLACE({{ column_name }}, r'[- ]', '')
        ELSE
            CASE
                WHEN REGEXP_REPLACE({{ column_name }}, r'[- ()]','') LIKE '+62%'
                    THEN REGEXP_REPLACE({{ column_name }}, r'[- ()]','')
                WHEN REGEXP_REPLACE({{ column_name }}, r'[- ()]','') LIKE '08%'
                    THEN CONCAT('+62', SUBSTR(REGEXP_REPLACE({{ column_name }}, r'[- ()]',''), 2))
                WHEN REGEXP_REPLACE({{ column_name }}, r'[- ()]','') LIKE '62%'
                    THEN CONCAT('+', REGEXP_REPLACE({{ column_name }}, r'[- ()]',''))
                ELSE REGEXP_REPLACE({{ column_name }}, r'[- ()]','')
        END
    END
{% endmacro %}