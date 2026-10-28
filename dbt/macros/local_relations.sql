{# All sources and models live in the selected file. Omitting the catalog in
   view bodies lets working copies/backups remain usable after a file rename. #}
{% macro ref() %}
  {% set relation = builtins.ref(*varargs, **kwargs) %}
  {% do return(relation.include(database=false)) %}
{% endmacro %}

{% macro source(source_name, table_name) %}
  {% do return(builtins.source(source_name, table_name).include(database=false)) %}
{% endmacro %}
