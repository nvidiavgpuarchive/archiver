{% import "_macros.md" as macros -%}

<div align="center">
  <img width="500px" alt="logo" src="logo_index.png"/></a>
  <br/>
  <p><i> NVIDIA vGPU Archive is a project inspired by <a href = "https://msdn.itellyou.cn/" >MSDN ITellYou</a> </i></p>

{{ macros.badge("Last Updated", readme_meta.last_updated ,  "green") }} {{ macros.badge("Total Size", macros.format_bytes(readme_meta.total_size),  "red") }} {{ macros.badge("Duplicate Ratio", readme_meta.duplicate_ratio ,  "orange") }}  
{{ macros.badge("Driver Counts", readme_meta.driver_count,  "blue") }} {{ macros.badge("Cloud Gaming Driver Count", readme_meta.gaming_driver_count,  "purple") }} {{ macros.badge("Non-Driver Count", readme_meta.non_driver_count,  "black") }} 

</div>

## About

The **vGPU Archive Index** is an open catalog of NVIDIA vGPU drivers and related files preserved for archival and reference use. Entries include release metadata, checksums, Internet Archive download links, and file listings where available.

The index is generated from the archive database and updated periodically after files are downloaded, uploaded, and verified.

## Web Index

For faster search and filtering, use the web interface:

> [Enter Web Search](https://nvidiavgpuarchive.github.io)

You can also browse the Markdown index below.

## How to Use This Index

- Choose a section and platform from the tables below.
- Open a detail page to view metadata, checksums, filenames, and download links.
- Prefer torrent downloads when possible; Internet Archive provides both torrent and HTTP options.

## Index

### Drivers

{{ macros.table_index(driver_indexes, "Platform Name", True) }}

### Non-Drivers / Misc

{{ macros.table_index(non_driver_indexes, "Platform Name", False) }}

### Cloud Gaming Drivers 

These drivers are indexed from NVIDIA's public [AWS S3 gaming driver listing](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvidia-gaming-driver.html). They provide host and GRID vGaming support for devices including Tesla P40, P4, T10, T4, V100 PCIE 32GB, RTX 8000, RTX 6000.  

{{ macros.table_index(gaming_driver_indexes, "Platform Name", True) }}