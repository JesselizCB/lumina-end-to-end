{#- 15-minute slot of the day (0-95) in the project's local time zone. -#}
{% macro franja_id(ts_column) -%}
(
  EXTRACT(HOUR FROM DATETIME({{ ts_column }}, '{{ var("zona_horaria") }}')) * 4
  + DIV(EXTRACT(MINUTE FROM DATETIME({{ ts_column }}, '{{ var("zona_horaria") }}')), 15)
)
{%- endmacro %}

{% macro fecha_local(ts_column) -%}
DATE({{ ts_column }}, '{{ var("zona_horaria") }}')
{%- endmacro %}
