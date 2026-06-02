{% import "_macros.md" as macros -%}

<div align="center">
  <img width="500px" alt="logo" src="logo_index.png"/></a>
  <br/>
  <p><i> NVIDIA vGPU Archive is a project inspired by <a href = "https://msdn.itellyou.cn/" >MSDN ITellYou</a> </i></p>

{{ macros.badge("Last Updated", readme_meta.last_updated ,  "green") }} {{ macros.badge("Total Size", macros.format_bytes(readme_meta.total_size),  "red") }} {{ macros.badge("Driver Counts", readme_meta.driver_count,  "blue") }} {{ macros.badge("Non-Driver Count", readme_meta.non_driver_count,  "black") }} {{ macros.badge("Duplicate Ratio", readme_meta.duplicate_ratio ,  "orange") }}
{# <p><em>🗂️A file list program that supports multiple storages, powered by Gin and SolidJS, fork of AList.</em></p>#}

</div>

> [!NOTE]
> Currently, we only have this GitHub repository and a related Internet Archive account. We do not plan to open any other social media accounts, such as Discord or Telegram, as we welcome open and upfront discussions.
>
> If you have found an issue with drivers or wish to request new drivers, kindly raise an issue.


The **vGPU Archive Index** is an open catalog of NVIDIA vGPU drivers. All drivers in the index are available for download, either directly over HTTP or through torrents. The goal is to make older and newer drivers easily accessible for anyone who needs them.  

The index is updated periodically via an automated script, all drivers are uploaded as is and complete with checksums.   


### Repository Overview

- **Index**: See below. You may use the index to narrow down and locate what you are looking for in <5 clicks.
- **Search**: Type `/` to search in the whole repo, title, filename or md5.
- **dump.json**: A JSON file that fully describes this repo so you can access the information programmatically.
- **Driver Catalogue**: Located in the "Non-Drivers / Misc" section, provides devices id and driver compatibility information.

Downloading via **torrents** is highly recommended. Internet archive seeds the content automatically, so you'll get the normal http bandwidth + p2p bandwidth.

## Index

### Drivers

{{ macros.table_index(driver_indexes, "Platform Name", True) }}

### Non-Drivers / Misc

{{ macros.table_index(non_driver_indexes, "Platform Name", False) }}

### Cloud Gaming Drivers 

> These drivers are dumped from [AWS S3 buckets](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvidia-gaming-driver.html), they include support for some non-publcially available GPUs like Tesla T10.
