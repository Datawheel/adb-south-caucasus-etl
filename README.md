# ADB South Caucasus ETL

This repo includes the pipelines made to create the different tables needed in clickhouse for ADB.

Here, the idea is to use the OEC module with/without a token in order to connect with the OEC and extract data from there.

A template for the .env file:

´´´
# clickhouse-local
export DB_USER="default";
export DB_PW_VM="";
export DB_URL_VM="host.docker.internal";
export DB_NAME="default";

# python path
export PYTHONPATH="/adb-south-caucasus-etl";

# clickhouse-database
export CLICKHOUSE_USERNAME="deploy";
export CLICKHOUSE_PASSWORD="";
export CLICKHOUSE_URL="";
export CLICKHOUSE_DATABASE="default";

# OEC Token
export OEC_TOKEN='';
´´´

