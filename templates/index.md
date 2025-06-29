{# 
Paramters: 
list[JinjaBreadcrumb], list[JinjaIndex], str : cur_option
 #}
{% import "_macros.md" as macros -%}
{{ macros.breadcrumbs(breadcrumbs) }}


{{ macros.table_index(indexes, cur_option) }}