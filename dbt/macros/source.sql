{#-
    https://stackoverflow.com/questions/69491178/dbtdata-build-tools-drop-the-default-database-prefix-that-gets-added-to-each
    duckdb does not support external databases (or attaching databases yet).

    maybe this macro belongs in dbt-duckdb, similar to dbt-sqlite
    see sqlite version here: https://github.com/codeforkjeff/dbt-sqlite/blob/main/dbt/adapters/sqlite/relation.py#L21
#}

{% macro source(source_name, table_name, include_database = False) %}

    {% do return(builtins.source(source_name, table_name).include(database = include_database)) %}

{% endmacro %}