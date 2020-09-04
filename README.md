# Rocky
Rocky is a key/value storage service based on facebook [RocksDb](https://github.com/facebook/rocksdb/wiki) with support for multiple databases and record expiration.


## Rocky REST API 
### Open db

You can open a new db with a ```POST``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}```  

```curl -X POST localhost:8080/database_1```

A successful request is indicated by a ```200 OK``` HTTP status code.  
Each database is created with the same [configuration](#Configuration).

### Close db
You can close an existing db with a ```DELETE``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}```  

```curl -X DELETE localhost:8080/database_1```

A successful request is indicated by a ```200 OK``` HTTP status code.

### Check if db exists
You can check if a database exists/already open with a ```GET``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}```  

```curl -v localhost:8080/database_1```

A successful request is indicated by a ```200 OK``` HTTP status code for an existing database and ```204 No Content``` HTTP status code for a non-existing database.

### Store record
You can write data with a ```POST``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}/{key}```  

```curl -d 'payload can be anything' localhost:8080/database_1/record_1```  

A successful request is indicated by a ```200 OK``` HTTP status code.

#### TTL support
Rocky support time to live per record provided in milliseconds.  
You can add ```ttl``` on record by providing a custom header

```curl -d 'I\'ll expire soon' -H 'ttl: 5000' localhost:8080/database_1/expiring_record_1```

### Read record
You can read data with a ```GET``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}/{key}```  

```curl -v localhost:8080/database_1/record_1```

The response contains data associated with the database and and key provided in path:

From the previous example this is our response  
```payload can be anything⏎ ```

Data is always return with content type header  ```content-type: application/octet-stream```  
A successful request is indicated by a ```200 OK``` HTTP status code.  

### Delete record
You can delete data with a ```DELETE``` request on ```SERVICE_URL:SERVICE_PORT/{db_name}/{key}```  

```curl -X DELETE localhost:8080/database_1/record_1```

A successful request is indicated by a ```200 OK``` HTTP status code.  

### Metrics
Service metrics in [prometheus format](https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/exposition_formats.md) are available for scraping under ```SERVICE_URL:SERVICE_PORT/metrics```  

## Configuration

When running service external configuration and log path should be provided or Rocky will use defaults.  
```--log_path```  path where log files should be written  
```--config_path``` path where service should look for external database and service configuration. Rocky will look for 
db_config.toml and service_config.toml files under this path if not found will create config files with defaults.
 
For database performance tuning check the official [RocksDb tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)  
For service performance tuning check example service_config.toml and yes - ```workers``` is the only config parameter that matters, default is number of logical CPUs  
Example configuration is provided under ```project_root/config```

### TODO
 - [ ] move IT to separate module
 - [ ] impl From for errors 
 - [ ] support for more rocksDb options in config (bloom, block cache..)
 - [ ] db iterator for on startup init
 - [ ] channel for expire
 - [ ] test compaction
 - [ ] code coverage
 - [ ] docker 
 - [ ] range scan 
 - [ ] performance
 

## Licence
Rocky is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)


