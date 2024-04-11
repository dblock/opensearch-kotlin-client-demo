import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor
import org.apache.http.HttpRequestInterceptor
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType.APPLICATION_JSON
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.opensearch.client.json.JsonpMapper
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch._types.SearchType.DfsQueryThenFetch
import org.opensearch.client.opensearch._types.query_dsl.MultiMatchQuery
import org.opensearch.client.opensearch._types.query_dsl.Query
import org.opensearch.client.opensearch._types.query_dsl.TextQueryType.BoolPrefix
import org.opensearch.client.opensearch.core.MsearchRequest
import org.opensearch.client.opensearch.core.msearch.MultisearchBody
import org.opensearch.client.opensearch.core.msearch.MultisearchHeader
import org.opensearch.client.opensearch.core.msearch.RequestItem
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider
import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region.US_EAST_1
import software.amazon.awssdk.utils.IoUtils.toUtf8String
import java.io.ByteArrayOutputStream

private const val INDEX1 = "index1"
private const val FIELD11 = "field11"

private const val INDEX2 = "index2"
private const val FIELD21 = "field21"
private const val FIELD22 = "field22"

private const val INDEX3 = "index3"
private const val FIELD31 = "field31"

private const val MY_INPUT = "AA"

private const val HOST = "cluster.host.name.com"
private const val URI = "https://$HOST/_msearch"

private const val ES = "es"
private const val PROFILE_NAME = "abcd"

class MyMultiSearchTest2 {

    private val msearchRequest = msearchRequest(listOf(INDEX1, INDEX2, INDEX3), MY_INPUT)

    private val httpRequestInterceptor: HttpRequestInterceptor = AwsRequestSigningApacheInterceptor(
        ES,
        Aws4Signer.create(),
        ProfileCredentialsProvider.builder().profileName(PROFILE_NAME).build(),
        US_EAST_1
    )

    private val httpClient: CloseableHttpClient = HttpClients.custom()
        .addInterceptorLast(httpRequestInterceptor)
        .build()

    private val objectMapper = ObjectMapper()
    private val jsonFactory: JsonFactory by lazy { objectMapper.factory }
    private val jsonpMapper: JsonpMapper by lazy { JacksonJsonpMapper(objectMapper, jsonFactory) }

    private val transportOptions = AwsSdk2TransportOptions.builder()
        .setMapper(jsonpMapper)
        .setResponseCompression(true)
        .build()

    private val sdkHttpClient = ApacheHttpClient.builder()
        .maxConnections(50)
        .tcpKeepAlive(true)
        .expectContinueEnabled(true)
        .build()

    private val awsSdk2Transport = AwsSdk2Transport(
        sdkHttpClient,
        HOST,
        US_EAST_1,
        transportOptions
    )
    private val openSearchClient = OpenSearchClient(
        awsSdk2Transport,
        transportOptions
    )

    private fun toByteArray(data: Any): ByteArray {
        val baos = ByteArrayOutputStream()
        val generator = jsonpMapper.jsonProvider().createGenerator(baos)
        jsonpMapper.serialize(data, generator)
        generator.close()
        return baos.toByteArray()
    }

    private fun msearchRequest(
        indices: List<String>,
        input: String
    ): MsearchRequest {
        val requestItems = mutableListOf<RequestItem>()
        for (index in indices) {
            val mmq = MultiMatchQuery.Builder()
                .query(input)
                .type(BoolPrefix)
                .fields(lookupSearchFieldsByIndexName(index))
                .build()
            val query = Query.Builder().multiMatch(mmq).build()
            val header = MultisearchHeader.Builder().index(index).build()
            val body = MultisearchBody.Builder().trackScores(true).query(query).build()
            requestItems.add(RequestItem.Builder().header(header).body(body).build())
        }
        val mSearchRequest = MsearchRequest.Builder()
            .index(indices)
            .searchType(DfsQueryThenFetch)
            .searches(requestItems)
            .build()
        return mSearchRequest
    }

    private fun lookupSearchFieldsByIndexName(index: String): List<String> {
        if (index == INDEX1) {
            return listOf(FIELD11)
        }
        if (index == INDEX2) {
            return listOf(FIELD21, FIELD22)
        }
        if (index == INDEX3) {
            return listOf(FIELD31)
        }
        return emptyList()
    }

    @Test
    fun testOpenSearchClient() {
        val response = openSearchClient.msearch(msearchRequest, ObjectNode::class.java)
        assertNotNull(response)
        assertEquals(3, response.responses().size)
    }

    @Test
    fun testHttpClient1() {
        val request = HttpPost(URI)
        request.entity = ByteArrayEntity(toByteArray(msearchRequest), APPLICATION_JSON)
        val response = httpClient.execute(request)
        val content = response.entity.content
        println(toUtf8String(content))
        content.close()
        assertEquals(200, response.statusLine.statusCode)
        // {"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"The msearch request must be terminated by a newline [\n]"}],"type":"illegal_argument_exception","reason":"The msearch request must be terminated by a newline [\n]"},"status":400}
    }

    @Test
    fun testHttpClient2() {
        val query = StringBuilder()
        query.append("{\"index\": \"$INDEX1\", \"search_type\": \"dfs_query_then_fetch\"}")
        query.append("\n")
        query.append("{\"query\":{\"multi_match\":{\"query\":\"$MY_INPUT\",\"type\":\"bool_prefix\",\"fields\":[\"$FIELD11\"],\"operator\":\"and\"}}}")
        query.append("\n")
        query.append("{\"index\": \"$INDEX2\", \"search_type\": \"dfs_query_then_fetch\"}")
        query.append("\n")
        query.append("{\"query\":{\"multi_match\":{\"query\":\"$MY_INPUT\",\"type\":\"bool_prefix\",\"fields\":[\"$FIELD21\",\"$FIELD22\"],\"operator\":\"and\"}}}")
        query.append("\n")
        query.append("{\"index\": \"$INDEX3\", \"search_type\": \"dfs_query_then_fetch\"}")
        query.append("\n")
        query.append("{\"query\":{\"multi_match\":{\"query\":\"$MY_INPUT\",\"type\":\"bool_prefix\",\"fields\":[\"$FIELD31\"],\"operator\":\"and\"}}}")
        query.append("\n")
        println(query.toString())
        val request = HttpPost(URI)
        request.entity = ByteArrayEntity(toByteArray(query.toString()), APPLICATION_JSON)
        val response = httpClient.execute(request)
        val content = response.entity.content
        println(toUtf8String(content))
        content.close()
        assertEquals(200, response.statusLine.statusCode)
        // {"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"The msearch request must be terminated by a newline [\n]"}],"type":"illegal_argument_exception","reason":"The msearch request must be terminated by a newline [\n]"},"status":400}
    }
}
