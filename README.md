Urban Data Integration - Data Provider
======================================

This Java library is part of the **Urban Data Integration** project. It provides functionality to download open urban data sets from the  [Socrata Discovery API](https://socratadiscovery.docs.apiary.io/).


Install
-------

Compile the sources and create a runnable jar file using maven. Then copy the jar file as `Socrata.jar` into a local folder on your machine.

```
mvn clean install
cp target/Socrata-jar-with-dependencies.jar ~/lib/Socrata.jar
```

Download Datasets
-----------------

The easiest way to download all datasets for a Socrata domain is as follows. First, create an empty folder (e.g. `~/data). When you run `socrata download --domain=domain-name` the tab-delimited tsv files for the specified domain will be downloaded in a folder that is named after the current date.

```
cd ~/data
java -jar ~/lib/Socrata.jar download --domain=data.vermont.gov
```
