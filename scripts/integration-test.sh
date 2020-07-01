#!/usr/bin/env bash
set -ex

#
# Basic E2E testing with the AWS CLI
# Tests against PostgreSQL and MySQL
#
# More complex tests should be written as unit tests
#

export RDS_DATA_API_CLIENT_RESOURCE_ARN=arn:aws:rds:us-east-1:123456789012:cluster:dummy
export RDS_DATA_API_CLIENT_SECRETARN=arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy

function down {
    db=$1 # arg 1 is the db type, 'pg' or 'mysql' or no value

    if [ "$db" = "mysql" ]
    then
        docker-compose -f ./docker-compose-mysql.yml -f ./docker-compose.override-ci.yml down -v
    fi

    if [ "$db" = "pg" ]
    then
        docker-compose -f ./docker-compose-postgres.yml -f ./docker-compose.override-ci.yml down -v
    fi
}

function test {
    db=$1 # arg 1 is the db type, 'pg' or 'mysql'

    # select text
    aws rds-data execute-statement \
    --endpoint-url 'http://localhost:8080' \
    --database 'test' \
    --resource-arn $RDS_DATA_API_CLIENT_RESOURCE_ARN \
    --secret-arn $RDS_DATA_API_CLIENT_SECRETARN \
    --sql $'SELECT \'hello\' AS value' \
    | jq -e '.records[0][0].stringValue == "hello"'

    # select number
    aws rds-data execute-statement \
    --endpoint-url 'http://localhost:8080' \
    --database 'test' \
    --resource-arn $RDS_DATA_API_CLIENT_RESOURCE_ARN \
    --secret-arn $RDS_DATA_API_CLIENT_SECRETARN \
    --sql 'SELECT 1 AS value' \
    | jq -e '.records[0][0].longValue == 1'

    # select boolean (pg)
    if [ "$db" = "pg" ]
    then
        aws rds-data execute-statement \
        --endpoint-url 'http://localhost:8080' \
        --database 'test' \
        --resource-arn $RDS_DATA_API_CLIENT_RESOURCE_ARN \
        --secret-arn $RDS_DATA_API_CLIENT_SECRETARN \
        --sql 'SELECT 1 > 0 AS value' \
        | jq -e '.records[0][0].booleanValue == true'
    fi

    # select boolean (mysql)
    # note that bools in MySQL are just numbers
    if [ "$db" = "mysql" ]
    then
        aws rds-data execute-statement \
        --endpoint-url 'http://localhost:8080' \
        --database 'test' \
        --resource-arn $RDS_DATA_API_CLIENT_RESOURCE_ARN \
        --secret-arn $RDS_DATA_API_CLIENT_SECRETARN \
        --sql 'SELECT 1 > 0 AS value' \
        | jq -e '.records[0][0].longValue == 1'
    fi

    aws rds-data execute-statement \
    --endpoint-url 'http://localhost:8080' \
    --database 'test' \
    --resource-arn $RDS_DATA_API_CLIENT_RESOURCE_ARN \
    --secret-arn $RDS_DATA_API_CLIENT_SECRETARN \
    --sql 'SELECT NOW() AS value' \
    | jq -e '.records[0][0].stringValue | length >= 19' # eg "2020-07-01 20:21:35.738998" or "2020-07-01 19:45:27"

    # TODO list, JSON, enum
}

# cleanup all
down

#
# MySQL E2E
#

docker-compose -f ./docker-compose-mysql.yml -f ./docker-compose.override-ci.yml up --build --force-recreate -d
test mysql # run tests declared above
echo ' ✅ MySQL E2E completed successfully'
down mysql

#
# PostgreSQL E2E
#

docker-compose -f ./docker-compose-postgres.yml -f ./docker-compose.override-ci.yml up --build --force-recreate -d
test pg # run tests declared above
echo ' ✅ PostgreSQL E2E completed successfully'
down pg
