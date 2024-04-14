# OpenSearch Java Client Demo in Kotlin

Working samples written in Kotlin that make requests to a self-hosted instance of OpenSearch or Amazon OpenSearch/Serverless using the OpenSearch Java Client.

## Running

### Local OpenSearch in Docker

Start a local instance of OpenSearch with Docker.

```
docker pull opensearchproject/opensearch:latest
docker run -d -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:latest
```

The following environment variables are required.

```
export USERNAME=admin
export PASSWORD=admin

export ENDPOINT=https://localhost:9200
```

### Amazon OpenSearch 

Create an OpenSearch domain in (AWS) which support IAM based AuthN/AuthZ or an instance of OpenSearch Serverless.

The following environment variables are required.

```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=
export AWS_REGION=us-west-2

export SERVICE=es # use "aoss" for OpenSearch Serverless
export ENDPOINT=https://....us-west-2.es.amazonaws.com
```

### Run the Sample

```
mvn install
mvn compile exec:java \
  -Dexec.mainClass="Example" \
  -Dlog4j.configurationFile=target/log4j2.xml \
  -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.SimpleLog \
  -Dorg.apache.commons.logging.simplelog.log.org.apache.http.wire=INFO
```

The [code](src/main/kotlin/Example.kt) will show the server version, create an index, add a document, search for it, output the result, then cleanup.

```
2022-12-26 15:55:02 [Example.main()] INFO  - opensearch: 2.3.0
2022-12-26 15:55:04 [Example.main()] INFO  - "Moneyball 2" by Bennett Miller (2011)
```

## License 

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
