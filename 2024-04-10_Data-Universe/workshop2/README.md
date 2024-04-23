# Workshop 2: Understanding Modern Table Formats


## Logistics

Wednesday, April 10, 2024 | 3:00pm - 4:30pm ET | Room 404


## Artifacts

[Presentation](./Workshop2-Iceberg-Preso.pdf) - just a few slides

[Lab Guide](./Workshop2-Iceberg-Labs.pdf) - step/by/step instructions

[SQL.txt](./Workshop2-Iceberg-SQL.txt) - copy/n/paste into SQL editor


## Description

Table formats provide an abstraction layer which helps to interact with the files in the data lake by defining schema, columns and datatypes. Limitations to Apache Hive, the first generation table format, drove the formation of modern open-source table formats like Apache Iceberg, Delta Lake, and Hudi. These table formats store the metadata on the data lake alongside the actual data, and provide ACID transactions for data transformation statements thereby enabling a full-featured data lakehouse. Utilizing modern table formats in your architectural design allows for data warehouse-like functionality on the data lake, aids in increased performance and scalability, and also incorporates new versioning features such as time-travel and rollbacks. In addition to providing a base knowledge of the leading modern table formats, the workshop provides a hands-on exercise utilizing Apache Iceberg within your data lakehouse architecture to see firsthand the benefits that modern table formats offer. 
