# local-data-api - Local Data API for AWS Aurora Serverless Data API
[![Build Status](https://travis-ci.org/koxudaxi/local-data-api.svg?branch=master)](https://travis-ci.org/koxudaxi/local-data-api)
[![](https://images.microbadger.com/badges/version/koxudaxi/local-data-api.svg)](https://microbadger.com/images/koxudaxi/local-data-api "Get your own version badge on microbadger.com")
[![codecov](https://codecov.io/gh/koxudaxi/local-data-api/branch/master/graph/badge.svg)](https://codecov.io/gh/koxudaxi/local-data-api)

If you want to run tests on your local machine and CI then then, local-data-api can run in your local machine with MySQL Server.

## What's AWS Aurora Serverless's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html

## How does local-data-api work?
local-data-api is "proxy server" to real databases.

The API converts RESTful request to SQL statements.

## How to use this image
You set your MYSQL Server configs as environments.
```bash
docker run --rm -it --name my-data-api -p 8080:80  -e MYSQL_HOST=<YOUR_MYSQL_HOST> -e MYSQL_PORT=<YOUR_MYSQL_PORT> -e MYSQL_USER=<YOUR_MYSQL_USER> -e MYSQL_PASSWORD=<YOUR_MYSQL_PASS>  -e RESOURCE_ARN=arn:aws:rds:us-east-1:123456789012:cluster:dummy -e SECRET_ARN=arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy  koxudaxi/local-data-api
```

In this case, you give local-data-api URL to aws client (like aws-cli).
```bash
$ aws --endpoint-url http://127.0.0.1:8080 rds-data execute-statement --resource-arn "arn:aws:rds:us-east-1:123456789012:cluster:dummy" --sql "show databases"  --secret-arn "arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy" --database 'test'
```
## docker-compose
docker-compose.yml
```yaml
version: '3.1'

services:
  local-data-api:
    image: koxudaxi/local-data-api
    restart: always
    environment:
      MYSQL_HOST: db
      MYSQL_PORT: 3306
      MYSQL_USER: root
      MYSQL_PASSWORD: example
      RESOURCE_ARN: 'arn:aws:rds:us-east-1:123456789012:cluster:dummy'
      SECRET_ARN: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'
    ports:
      - "8080:80"
  db:
    image: mysql:5.6
    command: --default-authentication-plugin=mysql_native_password
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: example
      MYSQL_DATABASE: test
    ports:
        - "3306:3306"
```

### docker-compose with Python's aws-sdk client(boto3) 
1. start local-data-api containers
```bash
$ docker-compose up -d
```

2. change a endpoint to local-data-api in your code. 
```bash
$ ipython
```
```python
In [1]: import boto3; client = boto3.client('rds-data', endpoint_url='http://127.0.0.1:8080', aws_access_key_id='aaa',  aws_secret_access_key='bbb') 
```

3. execute a sql statement
```python
In [2]: client.execute_statement(resourceArn='arn:aws:rds:us-east-1:123456789012:cluster:dummy', secretArn='arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy', sql='show databases', database='test')
```

4. local-data-api return the result from a MySQL Server.
```python
Out[2]: {'ResponseMetadata': {'HTTPStatusCode': 200,
 'HTTPHeaders': {'date': 'Sun, 09 Jun 2019 18:35:22 GMT',
 'server': 'uvicorn',
 'content-length': '492',
 'content-type': 'application/json'},
 'RetryAttempts': 0},
 'numberOfRecordsUpdated': 0,
 'records': [[{'stringValue': 'information_schema'}],
  [{'stringValue': 'mysql'}],
  [{'stringValue': 'performance_schema'}],
  [{'stringValue': 'sys'}],
  [{'stringValue': 'test'}]]}
```

If a table has some records, then the local-data-api can run `select`
```python
In [3]: client.execute_statement(resourceArn='arn:aws:rds:us-east-1:123456789012:cluster:dummy', secretArn='arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy', sql='select * from users', database='test')
```
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
- `BatchExecuteStatement` - core

### Not Implemented

- `ExecuteSql(Deprecated API)`

## Related projects
### py-data-api

DataAPI client for Python

https://github.com/koxudaxi/py-data-api

## Docker Image 

[https://hub.docker.com/r/koxudaxi/local-data-api](https://hub.docker.com/r/koxudaxi/local-data-api)

## Source Code

[https://github.com/koxudaxi/local-data-api](https://github.com/koxudaxi/local-data-api)

## Documentation

[https://koxudaxi.github.io/local-data-api](https://koxudaxi.github.io/local-data-api)

## License

local-data-api is released under the MIT License. http://www.opensource.org/licenses/mit-license
