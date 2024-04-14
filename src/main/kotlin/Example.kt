/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import java.io.IOException
import java.util.Objects
import org.apache.hc.client5.http.auth.AuthScope
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier
import org.apache.hc.core5.ssl.SSLContextBuilder
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch._types.OpenSearchException
import org.opensearch.client.opensearch._types.query_dsl.Query
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders
import org.opensearch.client.opensearch.core.IndexRequest
import org.opensearch.client.opensearch.core.IndexResponse
import org.opensearch.client.opensearch.core.InfoResponse
import org.opensearch.client.opensearch.core.SearchResponse
import org.opensearch.client.opensearch.core.UpdateRequest
import org.opensearch.client.opensearch.core.UpdateResponse
import org.opensearch.client.opensearch.indices.CreateIndexRequest
import org.opensearch.client.opensearch.indices.DeleteIndexRequest
import org.opensearch.client.opensearch.indices.IndexSettings
import org.opensearch.client.opensearch.indices.PutIndicesSettingsRequest
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
import org.opensearch.client.transport.OpenSearchTransport
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.regions.Region

object Example {
    @JvmStatic
    fun main(args: Array<String>) {
        var transport: OpenSearchTransport? = null
        var httpClient: SdkHttpClient? = null // TODO: transport.close() should be closing this
        
        try {
            val logger: Logger = LoggerFactory.getLogger(Example::class.java)
            val endpoint: String = System.getenv().getOrDefault("ENDPOINT", "https://localhost:9200")
            var service: String? = System.getenv().getOrDefault("SERVICE", null)

            if (service == null) { // self-hosted, localhost
                logger.info("Connecting to ${endpoint} (self-hosted) ...")
                val host = org.apache.hc.core5.http.HttpHost.create(endpoint)
                transport = ApacheHttpClient5TransportBuilder.builder(host)
                        .setMapper(JacksonJsonpMapper())
                        .setHttpClientConfigCallback({ httpClientBuilder ->

                            // BASIC auth, username/password
                            val username: String? = System.getenv().getOrDefault("USERNAME", null)
                            val password: String? = System.getenv().getOrDefault("PASSWORD", null)
                            if (username != null && password != null) {
                                val credentialsProvider = BasicCredentialsProvider();
                                credentialsProvider.setCredentials(
                                    AuthScope(host),
                                    UsernamePasswordCredentials(username, password.toCharArray()));
                                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                            }

                            // localhost, disable TLS
                            if (endpoint.startsWith("https://localhost:")) {
                                val sslContext = SSLContextBuilder
                                    .create()
                                    .loadTrustMaterial(null, {_, _ -> true })
                                    .build();
                                val tlsStrategy = ClientTlsStrategyBuilder.create()
                                    .setSslContext(sslContext)
                                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                    .build();
                                val connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                                    .setTlsStrategy(tlsStrategy)
                                    .build();
                                httpClientBuilder.setConnectionManager(connectionManager);
                            }

                            httpClientBuilder
                        })
                        .build()

            } else { // AWS
                val region: String = System.getenv().getOrDefault("AWS_REGION", "us-east-1")
                logger.info("Connecting to ${endpoint} (${region}) ...")
                httpClient = ApacheHttpClient.builder().build() // TODO: move below when transport.close() closes
                transport = AwsSdk2Transport(
                    httpClient,
                    org.apache.http.HttpHost.create(endpoint).getHostName(),
                    service,
                    Region.of(region),
                    AwsSdk2TransportOptions.builder().build())
            }

            val client: OpenSearchClient = OpenSearchClient(transport)

            // TODO: remove when Serverless supports GET /
            if (! service.equals("aoss")) {
                val info: InfoResponse = client.info()
                logger.info(info.version().distribution() + ": " + info.version().number())
            }

            // create the index
            val index: String = "movies"
            val createIndexRequest: CreateIndexRequest = CreateIndexRequest.Builder().index(index).build()

            try {
                client.indices().create(createIndexRequest)

                // add settings to the index
                val indexSettings: IndexSettings = IndexSettings.Builder().build()
                val putSettingsRequest: PutIndicesSettingsRequest = PutIndicesSettingsRequest.Builder()
                        .index(index)
                        .settings(indexSettings)
                        .build()
                client.indices().putSettings(putSettingsRequest)
            } catch (ex: OpenSearchException) {
                val errorType: String = Objects.requireNonNull(ex.response().error().type())
                if (! errorType.equals("resource_already_exists_exception") && ! errorType.equals("action_request_validation_exception") && ! errorType.equals("index_create_block_exception")) {
                    throw ex
                }
            }

            // index data
            class Movie() {
                var Director: String? = null
                var Title: String? = null
                var Year: Int? = null

                init {
                    
                }

                constructor(director: String, title: String, year: Int) : this() {
                    this.Director = director
                    this.Title = title
                    this.Year = year
                }

                override fun toString(): String {
                    return "\"${this.Title}\" by ${this.Director} (${this.Year})"
                }
            }

            val movie: Movie = Movie("Bennett Miller", "Moneyball", 2011)
            val indexRequest: IndexRequest<Movie> = IndexRequest.Builder<Movie>()
                    .index(index)
                    .id("1")
                    .document(movie)
                    .build()
            val indexResponse: IndexResponse = client.index(indexRequest)
            logger.info(String.format("Document %s.", indexResponse.result().toString().lowercase()))

            // update data
            val movieUpdate: Movie = Movie("Bennett Miller", "Moneyball 2", 2011)
            val updateRequest: UpdateRequest<Movie, Movie> = UpdateRequest.Builder<Movie, Movie>()
                .id("1")
                .index(index)
                .doc(movieUpdate)
                .build()
            val updateResponse: UpdateResponse<Movie> = client.update(updateRequest, Movie::class.java)
            logger.info(String.format("Document %s.", updateResponse.result().toString().lowercase()))

            // wait for the document to index
            Thread.sleep(3000)

            // search for the document
            val searchResponse: SearchResponse<Movie> = client.search({ s -> s.index(index) }, Movie::class.java)
            for (i in 0..searchResponse.hits().hits().size - 1) {
                logger.info(searchResponse.hits().hits().get(i).source().toString())
            }

            // delete the document
            client.delete({ b -> b.index(index).id("1") })

            // delete the index
            val deleteRequest: DeleteIndexRequest = DeleteIndexRequest.Builder().index(index).build()
            client.indices().delete(deleteRequest)
        } finally {
            if (httpClient != null) {
                httpClient.close()
            }
            if (transport != null) {
                transport.close()
            }
        }
    }
}
