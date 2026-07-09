{# on-run-end hook: persist every node result into ops.run_log so the
   warehouse keeps its own operational history. #}
{% macro log_run_results(results) %}
    {% if execute and results | length > 0 %}
        {% do run_query("create schema if not exists ops") %}
        {% do run_query(
            "create table if not exists ops.run_log ("
            "  invocation_id varchar,"
            "  node_id varchar,"
            "  status varchar,"
            "  rows_affected bigint,"
            "  execution_time_s double,"
            "  completed_at timestamp"
            ")"
        ) %}
        {% set value_rows = [] %}
        {% for result in results %}
            {% set rows_affected = result.adapter_response.get("rows_affected", -1)
                   if result.adapter_response else -1 %}
            {% do value_rows.append(
                "('" ~ invocation_id ~ "', '"
                ~ result.node.unique_id ~ "', '"
                ~ result.status ~ "', "
                ~ rows_affected ~ ", "
                ~ result.execution_time ~ ", current_timestamp)"
            ) %}
        {% endfor %}
        {% do run_query("insert into ops.run_log values " ~ value_rows | join(", ")) %}
    {% endif %}
{% endmacro %}
