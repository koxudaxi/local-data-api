# local-data-api - Local Data API for AWS Aurora Serverless Data API
[![CI](https://github.com/koxudaxi/local-data-api/actions/workflows/ci.yml/badge.svg)](https://github.com/koxudaxi/local-data-api/actions/workflows/ci.yml)
[![](https://images.microbadger.com/badges/version/koxudaxi/local-data-api.svg)](https://hub.docker.com/r/koxudaxi/local-data-api)
[![](https://badgen.net/docker/pulls/koxudaxi/local-data-api)](https://hub.docker.com/r/koxudaxi/local-data-api)
[![codecov](https://codecov.io/gh/koxudaxi/local-data-api/branch/master/graph/badge.svg)](https://codecov.io/gh/koxudaxi/local-data-api)
![license](https://img.shields.io/github/license/koxudaxi/local-data-api.svg)

If you want to run tests on your local machine and CI then, local-data-api can run in your local machine with MySQL and PostgreSQL Servers.

## What's AWS Aurora Serverless's Data API?
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.html

## How does local-data-api work?
local-data-api is "proxy server" to real databases.

The API converts RESTful request to SQL statements.

## Support Database Types
- MySQL
- PostgreSQL

## How to insert array data into PostgreSQL
DataAPI have not support inserting array data with `SqlParameter` yet.
But, @ormu5 give us this workaround to insert array data.
```sql
insert into cfg.attributes(id, type, attributes)
values(:id, :type, cast(:attributes as text[]));
```
where the value for attributes parameter is a string properly formatted as Postgres array, e.g., `'{"Volume","Material"}'`.

Thanks to @ormu5.



## Version 0.6.0
### local-data-api has been re-written in Kotlin
#### Motivation
This project was written in Python at the start.

But, We need a JDBC driver that needs Java to reproduce real Data API behavior.

I have re-written local-data-api in Kotlin. The design is the same as the old version.

Also, I wrote unittest and made coverage 100%. We welcome your feedback and error reports.


Related issue: [#70](https://github.com/koxudaxi/local-data-api/issues/70)


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
### MySQL
docker-compose-mysql.yml
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

#### If you use PostgreSQL, then you should run this line to check databases. 
```python
In [2]: client.execute_statement(resourceArn='arn:aws:rds:us-east-1:123456789012:cluster:dummy', secretArn='arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy', sql='SELECT datname FROM pg_database', database='test')
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


### PostgreSQL
Now, local-data-api supports PostgreSQL

docker-compose-postgres.yml
```yaml
version: '3.1'

services:
  local-data-api:
    image: koxudaxi/local-data-api
    restart: always
    environment:
      ENGINE: PostgreSQLJDBC
      POSTGRES_HOST: db
      POSTGRES_PORT: 5432
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: example
      RESOURCE_ARN: 'arn:aws:rds:us-east-1:123456789012:cluster:dummy'
      SECRET_ARN: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy'
    ports:
      - "8080:80"
  db:
    image: postgres:10.7-alpine
    restart: always
    environment:
      POSTGRES_PASSWORD: example
      POSTGRES_DB: test
    ports:
        - "5432:5432"

```


## Contribute
We are waiting for your contributions to `local-data-api`.

### How to contribute
```bash
## 1. Clone your fork repository
$ git clone git@github.com:<your username>/local-data-api.git
$ cd local-data-api

## 2. Open the project with IDE/Editor
The project path is `./kotlin/local-data-api`

## 3. Run on your local machine
### Shell on Linux or MacOS 
$ cd ./kotlin/local-data-api
$ ./gradlew run

### Command Prompt on Windows
$ cd ./kotlin/local-data-api
$ gradlew.bat run

## 4. Create new branch and rewrite code.
$ git checkout -b new-branch

## 5. Run unittest
$ cd ./kotlin/local-data-api
$ ./gradlew test jacocoTestReport

## 6. Run integration-test
$ ./scripts/integration-test.sh

## 7. Commit and Push...
```

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
