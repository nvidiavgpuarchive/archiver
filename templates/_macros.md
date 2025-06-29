{##}
{#{% macro format_bytes(size) -%}#}
{#{% if size < 1024 -%}{{ size }} B{%- elif size < 1024 * 1024 -%}{{ '%.2f' | format(size / 1024) }} KB{%- elif size < 1024 * 1024 * 1024 -%}{{ '%.2f' | format(size / (1024 * 1024)) }} MB{%- else -%}{{ '%.2f' | format(size / (1024 * 1024 * 1024)) }} GB{%- endif %}#}
{#{%- endmacro %}#}

{% macro format_bytes(size) -%}
{{ size if size < 1024 else ('%.2f' | format(size / 1024)) + ' KB' if size < 1024**2 else ('%.2f' | format(size / 1024**2)) + ' MB' if size < 1024**3 else ('%.2f' | format(size / 1024**3)) + ' GB' if size < 1024**4 else ('%.2f' | format(size / 1024**4)) + ' TB' if size < 1024**5 else ('%.2f' | format(size / 1024**5)) + ' PB' if size < 1024**6 else ('%.2f' | format(size / 1024**6)) + ' EB' }}
{%- endmacro %}



{% macro badge(subject,status, color) -%}
    {% set base_url = "https://flat.badgen.net/static/" -%}
    {% set badge_url = (base_url + subject + '/' + status | string + '/' + color )  -%}
    <img src="{{ badge_url }}" />
{%- endmacro %}


{% macro breadcrumbs(jinja_breadcrumbs) %}
{%- for item in jinja_breadcrumbs -%}
    {%- if loop.last -%}
        **{{ item.name }}**
    {%- else -%}
        [{{ item.name }}]({{ item.url }})  > {{ " " }}
    {%- endif -%}
{%- endfor %}
{% endmacro %}


{% macro table_filelist(filelists) -%}
| Description            | Product Version    | Platform                | Platform Version           | Release Date           |             Actions              |
| ---------------------- | :----------------- | :---------------------- | -------------------------- | :--------------------- | :------------------------------: |
{% for d in filelists -%}
| {{ d.entry.meta.description }} | {{ d.entry.meta.version }} | {{ d.entry.meta.platformName }} | {{ d.entry.meta.platformVersion }} | {{ d.entry.meta.releaseDate }} | [View Details]({{ d.url }}) |
{% endfor -%}
{% endmacro -%}


{% macro table_index(indexes, cur_option, show_newest=False) -%}

| {{ cur_option|title }} | Last Updated | {% if show_newest %}  Latest Entry | {% endif %} Count | Filter | 
|---|:-------:|:-------:|:----:|{% if show_newest %}:---:| {% endif %} 
{% for index in indexes-%}
| {{ index.option_value }} | {{ index.newest_entry.entry.meta.releaseDate  }}|  {% if show_newest %}  [View Latest]({{ index.newest_entry.url }}) | {% endif %} {{index.result_cnt }} |  [Apply]({{ index.nextlevel_url }}) |
{% endfor-%}
{% endmacro -%}