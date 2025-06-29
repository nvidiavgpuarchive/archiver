{# 
Paramters: 
list[JinjaBreadcrumb], list[JinjaIndexFile]
 #}
{% import "_macros.md" as macros -%}
{{ macros.breadcrumbs(breadcrumbs) }}


{{ macros.table_filelist(filelists) }}
