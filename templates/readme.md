{% import "_macros.md" as macros -%}

<div align="center">
  <img width="500px" alt="logo" src="logo_index.png"/></a>
  <br/>
  <p><i> NVIDIA vGPU Archive is a project inspired by <a href = "https://msdn.itellyou.cn/" >MSDN ITellYou</a> </i></p>

{{ macros.badge("Last Updated", readme_meta.last_updated ,  "green") }} {{ macros.badge("Total Size", macros.format_bytes(readme_meta.total_size),  "red") }} {{ macros.badge("Driver Counts", readme_meta.driver_count,  "blue") }} {{ macros.badge("Cloud Gaming Driver Count", readme_meta.gaming_driver_count,  "purple") }} {{ macros.badge("Non-Driver Count", readme_meta.non_driver_count,  "black") }} {{ macros.badge("Duplicate Ratio", readme_meta.duplicate_ratio ,  "orange") }}
{# <p><em>🗂️A file list program that supports multiple storages, powered by Gin and SolidJS, fork of AList.</em></p>#}

</div>

## About

The **vGPU Archive Index** is an open catalog of NVIDIA vGPU drivers and related files preserved for archival and reference use. Entries include release metadata, checksums, Internet Archive download links, and file listings where available.

The index is generated from the archive database and updated periodically after files are downloaded, uploaded, and verified.

## Browse

For faster search and filtering, use the web interface:

> [Enter Web Search](https://nvidiavgpuarchive.github.io)

You can also browse the Markdown index below.

## How to Use This Index

- Choose a section and platform from the tables below.
- Open a detail page to view metadata, checksums, filenames, and download links.
- Prefer torrent downloads when possible; Internet Archive provides both torrent and HTTP options.

## Archive Sections

- **Drivers**: NVIDIA vGPU driver packages from the enterprise portal.
- **Cloud Gaming Drivers**: NVIDIA gaming and cloud guest drivers listed from the public AWS S3 bucket.
- **Non-Drivers / Misc**: license servers, catalogs, management tools, documentation, and related files.

## Index

### Drivers

{{ macros.table_index(driver_indexes, "Platform Name", True) }}

### Cloud Gaming Drivers 

These drivers are indexed from NVIDIA's public [AWS S3 gaming driver listing](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvidia-gaming-driver.html). They are listed separately because their metadata shape differs from enterprise vGPU portal releases.

{{ macros.table_index(gaming_driver_indexes, "Platform Name", True) }}

### Non-Drivers / Misc

{{ macros.table_index(non_driver_indexes, "Platform Name", False) }}
