Urban Data Integration - Data Provider
======================================

This Java library is part of the **Urban Data Integeration** project. It provides functionality to download and transform (open urban) data sets from different data provider.


Data Provider - Socrata
-----------------------

For Socrata the library uses the [Discovery API](https://socratadiscovery.docs.apiary.io/). There are three tools included in the repository:


###List Socrata Domain

The `SocrataDomains.jar` JAR file lists the names (and number of available resources) for each domain available from the Socrata API.

``` 
java -jar SocrataDomains.jar
  {<output-file>} : Optional output file. If omitted output os printed to standard output
```


###Download Catalog for Resource Type

The `SocrataCatalog.jar` JAR file downloads partes of the Socrata catalog into a local Json file. Download is limited to resources of a specified type. Optional, download can further be limited to resources from a given domain.

```
java -jar SocrataCatalog.jar
  <catalog-file>      : Name of the output file (e.g., catalog.json)
  <resource-type> [   : Download all resources for a given type from the Socrata taxonomy.
    api |
    calendar |
    chart |
    datalens |
    dataset |
    federated_href |
    file |
    filter |
    form |
    href |
    link |
    map |
    measure |
    story |
    visualization
  ]
  {<domain>}          : Optional. Limit download to resources from given domain.
```


###Download Datasets

There are various ways to download the actual datasets from the Socrata API. One way is to (1) get the value of the permalink element in the catalog entry for a resource, (2) extract the base Url and the dataset identifier (e.g., for [https://data.ny.gov/d/kwxv-fwze](https://data.ny.gov/d/kwxv-fwze) the base Url is https://data.ny.gov/ and the resource identifier is kwxv-fwze), and (3) generate a download Url as *baseUrl*/api/views/*identifier*/*downloadFormat* (e.g., [https://data.ny.gov/api/views/kwxv-fwze/rows.json?accessType=DOWNLOAD](https://data.ny.gov/api/views/kwxv-fwze/rows.json?accessType=DOWNLOAD)). By using `rows.json`, `rows.csv`, or `rows.tsv` the data can be doanloaded in different formats.

The data format that is used by the Socrata GUI for publishing the data appears to be a bit more structured compared to the download data (i.e., in some cases the Json objects are nested objects instead of arrays of tuple values). Access to this format, however, is a bit more complicated. The JAR file 'SocrataDatasets.jar' is intended to allow downloading datasets in the *advanced Json format*.

```
java -jar SocrataDatasets.jar
  <catalog-file>     : Socarata catalog file
  <domain>           : Name of the Socrata domain for which datasets are downloaded
  <threads>          : Number of parallel threads to use for file download
  <overwrite>        : Overwrite existing files [true | false]
  <dataset-file>     : Output file that will contain identifier and name of downloaded datasets
  <output-directory> : Output directory for downloaded Json files
```
