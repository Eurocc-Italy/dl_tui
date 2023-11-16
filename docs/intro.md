# DTaaS_TUI User Guide

Version: [`0.2`](changelog)

This is the user guide for the Digital Twin as a Service Text User Interface (`DTaaS_TUI`) Python library. The TUI is used to query the data lake and run processing scripts on the files matching the query.

The service is composed of: 

  - a Cloud-based infrastructure with a VM running a MongoDB database instance. This database contains the metadata for all the files in the data lake / digital twin, including the path to the actual corresponding file in the HPC parallel filesystem.
  - a HPC cluster which contains the actual files, stored in dual parallel filesystem/S3 mode. The HPC part runs the processing script sent by the user on the files matching the query.

The library consists of two executables, `dtaas_tui_client` and `dtaas_tui_server`. 

The `client` version is intended to be run on the machine with direct access to the files of the data lake and runs the actual querying and processing.

The `server` version is intended to be run on a cloud machine with access to an HPC infrastructure running Slurm. Its purpose is to parse the user input (query and processing script) and launch a Slurm job on HPC which calls the client version.

::::{grid} 1 1 2 2
:class-container: text-center
:gutter: 3

:::{grid-item-card}
:link: getting-started
:link-type: doc
:class-header: bg-light

Getting started 💡
^^^

A quick tour covering the library installation and some basic configuration.
:::

:::{grid-item-card}
:link: user-guide
:link-type: doc
:class-header: bg-light

User guide 📑
^^^

A general "hands-on" description of the features available in the library.
:::

:::{grid-item-card}
:link: api
:link-type: doc
:class-header: bg-light

API reference 🔎
^^^

A detailed collection of all the objects and features implemented in the library
:::

:::{grid-item-card}
:link: contributor-guide
:link-type: doc
:class-header: bg-light

Contributor's guide 🖥️
^^^

A short guide for all the developers that want to contribute to the libray
:::