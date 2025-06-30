{% import "_macros.md" as macros -%}
<div align="center">
  <img width="500px" alt="logo" src="logo_index.png"/></a>
  <br/>
  <p><i> NVIDIA vGPU Archive is a project inspired by <a href = "https://msdn.itellyou.cn/" >MSDN ITellYou</a> </i></p>

  {{ macros.badge("Last Updated", readme_meta.last_updated ,  "green") }} {{ macros.badge("Total Size", macros.format_bytes(readme_meta.total_size),  "red") }} {{ macros.badge("Driver Counts", readme_meta.driver_count,  "blue") }} {{ macros.badge("Non-Driver Count", readme_meta.non_driver_count,  "black") }} {{ macros.badge("Duplicate Ratio", readme_meta.duplicate_ratio ,  "orange") }}
{#  <p><em>🗂️A file list program that supports multiple storages, powered by Gin and SolidJS, fork of AList.</em></p>#}
</div>

> [!NOTE]
> Currently, we only have this GitHub repository and a related Internet Archive account. We do not plan to open any other social media accounts, such as Discord or Telegram, as we welcome open and upfront discussions.  
>
> If you have found an issue with drivers or wish to request new drivers, kindly raise an issue.

## Description 

The **vGPU Archive Index** serves as a programmatically generated catalog of NVIDIA vGPU drivers uploaded to the Internet Archive. The project focuses on preserving older drivers and devices by indexing these files and ensuring they are accessible for future use.

- **Automated Archiving**: Built on Python scripts, the repository uses [Archiver](https://github.com/nvidiavgpuarchive/archiver) for automating the scraping and uploading of drivers, simplifying the process of maintaining the index.
- **Preservation First**: The priority is to archive as many drivers as possible; maintaining an up-to-date index, while desirable, is a secondary focus.
- **Dynamic Updates**: Due to the nature of this project, the project might break from time to time.

> Why not just download the driver from [NVIDIA's Driver Search](https://www.nvidia.com/en-us/drivers/)?

The NVIDIA driver search interface is quite limited—it primarily focuses on consumer hardware and only lists a subset of the available drivers. Additionally, some drivers, such as GRID drivers for Windows with WDDM mode, are not readily available from NVIDIA's site. These drivers provide specific functionality critical for certain use cases, making this archive essential for preservation.

### Integrity and Verification
All drivers uploaded to the Internet Archive are checked rigorously for integrity:
- **Checksum Matching**: Each file’s checksum is validated before and after the upload.
- **Compressed Files**: The integrity of compressed files is verified for zip files.

If you encounter a broken archive, raise an issue and we'll fix it asap.

### Repository Resources
- **dump.json**: A JSON file that fully describes this repository.
- **Driver Catalogue**: Located in the "Non-Drivers / Misc" section, provides devices id and driver compatibility information.
- **Index**: The repository maintains structured indexes for both drivers and non-drivers, aiding navigation.
- **Search**: You can search directly within the repository if you have specific files in mind.

Downloading via **torrents** is highly recommended:
- The torrent files are seeded by the Internet Archive (via HTTP sources), which ensures high download speeds on top of IA bandwidth.
- Using torrents ensures that the archived content remains available even after potential takedown requests.

## Index

### Drivers

{{ macros.table_index(driver_indexes, "Platform Name", True) }}

### Non-Drivers / Misc

{{ macros.table_index(non_driver_indexes, "Platform Name", False) }}

