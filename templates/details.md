{# 
Paramters: 
list[JinjaBreadcrumb], JinjaEntry
 #}
{% import "_macros.md" as macros -%}
{{ macros.breadcrumbs(breadcrumbs) }}

### {{ entry.file.filenames[0] if entry.file.filenames else 'No Title' }}

{% if entry.file.filenames|length > 1 -%}
{% for name in entry.file.filenames[1:] -%}
> {{ name }} {{ '  ' }}
{% endfor %}
{% endif -%}


{% set torrent_link = 'https://archive.org/download/' + entry.identifier + '/'+ entry.identifier +  '_archive.torrent' -%}
{% set http_link = 'https://archive.org/compress/' + entry.identifier  -%}
{% set ia_page_link = 'https://archive.org/details/' + entry.identifier -%}

| **File Size** | **Torrent**  | **HTTP Link** | **Internet Archive** |
|:-------------:|:------------:|:-------------:|:--------------------:|
| {{ macros.format_bytes(entry.file.size) }} |  [Download]({{ torrent_link }})       | [Download]({{ http_link }}) | [View Page]({{ ia_page_link }})       |

#### Meta

<table>
{% for key, value in entry.meta.items() -%}
{% if key not in ['description', 'downloadId'] -%}
<tr><td><strong>{{ key|title|replace('_', ' ') }}</strong></td><td>{{ value if value is string else '<code>' + value|string + '</code>' }}</td></tr>
{% endif -%}
{% endfor -%}
</table>

#### Zip Content

**MD5 Checksum**: `{{ entry.file.md5 }}`

{% set pre_content = entry.ia_meta.description.split('<pre><code>')[-1].split('</code></pre>')[0] -%}
```text
{% for line in pre_content.split('\n') if line -%}
{{ line }}
{% endfor -%}
```