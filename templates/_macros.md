{##}
{#{% macro format_bytes(size) -%}#}
{#{% if size < 1024 -%}{{ size }} B{%- elif size < 1024 * 1024 -%}{{ '%.2f' | format(size / 1024) }} KB{%- elif size < 1024 * 1024 * 1024 -%}{{ '%.2f' | format(size / (1024 * 1024)) }} MB{%- else -%}{{ '%.2f' | format(size / (1024 * 1024 * 1024)) }} GB{%- endif %}#}
{#{%- endmacro %}#}

{% macro format_bytes(size) -%}
{{ size if size < 1024 else ('%.2f' | format(size / 1024)) + ' KB' if size < 1024**2 else ('%.2f' | format(size / 1024**2)) + ' MB' if size < 1024**3 else ('%.2f' | format(size / 1024**3)) + ' GB' if size < 1024**4 else ('%.2f' | format(size / 1024**4)) + ' TB' if size < 1024**5 else ('%.2f' | format(size / 1024**5)) + ' PB' if size < 1024**6 else ('%.2f' | format(size / 1024**6)) + ' EB' }}
{%- endmacro %}



{% macro badge(subject,status, color) -%}
    {% set base_url = "https://flat.badgen.net/static/" -%}
    {% set badge_url = (base_url + subject | e + '/' + status | string | e + '/' + color )  -%}
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
{% set columns = [
  {"label": "Description", "key": "description", "always": true},
  {"label": "Product Version", "key": "version"},
  {"label": "Platform", "key": "platformName"},
  {"label": "Platform Version", "key": "platformVersion"},
  {"label": "Release Date", "key": "releaseDate"},
] -%}
{% set visible = [] -%}
{% for col in columns -%}
    {% set ns = namespace(show=col.always|default(false)) -%}
    {% for d in filelists -%}
        {% if d.entry.meta[col.key] -%}
            {% set ns.show = true -%}
        {% endif -%}
    {% endfor -%}
    {% if ns.show -%}
        {% set _ = visible.append(col) -%}
    {% endif -%}
{% endfor -%}
|{% for col in visible %} {{ col.label }} |{% endfor %} Actions |
|{% for col in visible %} --- |{% endfor %} :---: |
{% for d in filelists -%}
|{% for col in visible %} {{ d.entry.meta[col.key] }} |{% endfor %} [View Details]({{ d.url }}) |
{% endfor -%}
{% endmacro -%}


{% macro table_index(indexes, cur_option, show_newest=False) -%}

| {{ cur_option|title }} | Last Updated | {% if show_newest %}  Latest Entry | {% endif %} Count | Browse | 
|---|:-------:|:-------:|:----:|{% if show_newest %}:---:| {% endif %} 
{% for index in indexes-%}
| {{ index.option_value }} | {{ index.newest_entry.entry.meta.releaseDate  }}|  {% if show_newest %}  [View Latest]({{ index.newest_entry.url }}) | {% endif %} {{index.result_cnt }} |  [Browse]({{ index.nextlevel_url }}) |
{% endfor-%}
{% endmacro -%}
