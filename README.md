# local-data-api - Local Data API for AWS Aurora Serverless Data API
[![Build Status](https://travis-ci.org/koxudaxi/local-data-api.svg?branch=master)](https://travis-ci.org/koxudaxi/local-data-api)

local-data-api can run in your local machine with MySQL Server.

dockerhub: [local-data-api](https://hub.docker.com/r/koxudaxi/local-data-api)

## What's AWS Aurora Sreverlss's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html

## This project is an experimental phase.

## How does local-data-api work?
local-data-api is "proxy server" to real databases.

The API converts RESTful request to SQL statements.

## How to use this image
you set your MYSQL Server configs as environments.

```bash
docker run --name my-data-api -p 8080:80  -e MYSQL_HOST=127.0.0.1 -e MYSQL_PORT=3306 -e MYSQL_USER=root -e MYSQL_PASSWORD=example -e RESOURCE_ARN=dummy -e SECRET_ARN=dummy  koxudaxi/local-data-api
```
In this caese, you give local-data-api url to aws client (like aws-cli).

```bash
$ aws --endpoint-url http://127.0.0.1:8080 rds-data execute-statement --resource-arn "dummy" --sql "show databases"  --secret-arn "dummy" --database 'test'
```

### Example: docker-compose with Python's aws-sdk client(boto3) 
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
      RESOURCE_ARN: dummy
      SECRET_ARN: dummy
    ports:
      - "8080:80"
  db:
    image: mysql
    command: --default-authentication-plugin=mysql_native_password
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: example
      MYSQL_DATABASE: test
    ports:
        - "3306:3306"
```

1. start local-data-api containers
```bash
$ docker-compose up -d
```

2. change a endpoint to local-data-api in your code. 
```bash
$ ipython
```
```python
In [1]: client = boto3.client('rds-data', endpoint_url='http://127.0.0.1:8080', aws_access_key_id='aaa',  aws_secret_access_key='bbb') 
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

## Related projects
### py-data-api

DataAPI client for Python

https://github.com/koxudaxi/py-data-api
