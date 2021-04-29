# `int_service_supply`

Data in this directory is consumed from the `int_service_supply` schema. The data is published by AWS DMS (data migration service); a service that's responsible for continuous data replication from service-supply towards Redshift.

The DBT models are configured in such a way as late-binding views as data models in service-supply change frequently (e.g. column drop, data type change).