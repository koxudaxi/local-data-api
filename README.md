# local-data-api - Local Data API for AWS Aurora Serverless
[![Build Status](https://travis-ci.org/koxudaxi/local-data-api.svg?branch=master)](https://travis-ci.org/koxudaxi/local-data-api)

local-data-api support test for Data API (AWS Aura Serverless)


## What's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html


## How does local-data-api work?
local-data-api is "proxy server" to real databases.

The API converts RESTful request to SQL statements.


## Example: local-data-api with Python's aws-sdk client(boto3) 
1. start local-data-api containers
```bash
$ docker-compose up -d
```

2. change a endpoint to local-data-api in your code. 
```bash
$ ipython
```
```python
In [1]: client = boto3.client('rds-data', endpoint_url='http://127.0.0.1:18080', aws_access_key_id='aaa',  aws_secret_access_key='bbb') 
```

3. execute a sql statement
```python
In [2]: client.execute_statement(resourceArn='dummy', secretArn='dummy', sql='select * from users', database='test')
```

4. local-data-api return the result from a MySQL Server.
```python
Out[3]: {'ResponseMetadata': {'HTTPStatusCode': 200,
 'HTTPHeaders': {'date': 'Sun, 09 Jun 2019 18:35:22 GMT',
 'server': 'uvicorn',
 'content-length': '492',
 'content-type': 'application/json'},
 'RetryAttempts': 0},
 'numberOfRecordsUpdated': 0,
 'records': [[{'longValue': 1}, {'stringValue': 'ichiro'}, {'longValue': 17}],
  [{'longValue': 2}, {'stringValue': 'ken'}, {'longValue': 20}],
  [{'longValue': 3}, {'stringValue': 'lisa'}, {'isNull': True}],}
```

## Features
### Implemented
- `BeginTransaction`  - core  
- `CommitTransaction` - core 
- `ExecuteStatement` - core 
- `RollbackTransaction` - core


### Not Implemented
- `BatchExecuteStatement`
- `ExecuteSql`


## This project is an experimental phase.
