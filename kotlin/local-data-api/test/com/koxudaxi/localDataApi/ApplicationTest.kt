package com.koxudaxi.localDataApi

import io.ktor.http.*
import kotlin.test.*
import io.ktor.server.testing.*
import io.mockk.*
import io.mockk.mockk
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import org.junit.After
import org.junit.Before
import java.sql.Connection
import java.sql.DriverManager

class ApplicationTest {
    private val dummyResourceArn = "arn:aws:rds:us-east-1:123456789012:cluster:test"
    private val dummySecretArn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
    private val dummyTransactionId =
        "oxBOsIDToCxlKBUQcmGtySBNU+GDBR=rVHqopvbILoUsBT/fDmIPVIKxMiluReISVLfjwywRbAQqxfPIttZDEHo/kxpezCC/rN+blsnDUaL+lhlVaiTEyybaQx/JJZqQHa+WjvaBsnFFrti=vAwpOKYWXguJVSBz=PWMRfGnsfWqhJgPx=cGNDqi"
    private val username = "username"
    private val password = "password"

    @Before
    fun setUp() {
        val jdbcName = "h2:./test;MODE=MySQL"
        ResourceManager.INSTANCE.setResource(jdbcName, dummyResourceArn, null, null, emptyMap())
        SecretManager.INSTANCE.setSecret(username, password, dummySecretArn)
        ConnectionManager.INSTANCE.createConnection("jdbc:${jdbcName}", username, password, null, null, emptyMap())
            .createStatement()
            .execute("DROP ALL OBJECTS")
    }

    @After
    fun tearDown() {
        unmockkAll()
    }

    @Test
    fun testExecuteSql() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/ExecuteSql").apply {
                assertEquals(HttpStatusCode.NotFound, response.status())
                assertEquals("{\"message\":\"NotImplemented\",\"code\":\"NotFoundException\"}", response.content)
            }
        }
    }

    @Test
    fun testBeginTransaction() {
        withTestApplication({ module(testing = true) }) {
            mockkObject(ConnectionManager)
            val connectionManager = spyk<ConnectionManager>()
            val connection = mockk<Connection>(relaxed = true)
            mockkStatic(DriverManager::class)
            every { DriverManager.getConnection(any(), any(), any()) } returns connection
            every { ConnectionManager.INSTANCE } returns connectionManager
            every { connectionManager.createTransactionId() } returns dummyTransactionId

            handleRequest(HttpMethod.Post, "/BeginTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BeginTransactionRequest(dummyResourceArn, dummySecretArn)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals("{\"transactionId\":\"${dummyTransactionId}\"}", response.content)
                verify { connectionManager.createTransactionId() }
            }
        }
    }

    @Test
    fun testCommitTransaction() {
        withTestApplication({ module(testing = true) }) {
            mockkObject(ConnectionManager)
            val connectionManager = spyk<ConnectionManager>()
            every { ConnectionManager.INSTANCE } returns connectionManager
            val connection = mockk<Connection>(relaxed = true)
            every { connectionManager.getConnection(dummyTransactionId) } returns connection


            handleRequest(HttpMethod.Post, "/CommitTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(CommitTransactionRequest(dummyResourceArn,
                    dummySecretArn,
                    dummyTransactionId)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals("{\"transactionStatus\":\"Transaction Committed\"}", response.content)
                verify { connection.commit() }
            }
        }
    }

    @Test
    fun testRollbackTransaction() {
        withTestApplication({ module(testing = true) }) {
            mockkObject(ConnectionManager)
            val connectionManager = spyk<ConnectionManager>()
            every { ConnectionManager.INSTANCE } returns connectionManager
            val connection = mockk<Connection>(relaxed = true)
            every { connectionManager.getConnection(dummyTransactionId) } returns connection

            handleRequest(HttpMethod.Post, "/RollbackTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(CommitTransactionRequest(dummyResourceArn,
                    dummySecretArn,
                    dummyTransactionId)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals("{\"transactionStatus\":\"Rollback Complete\"}", response.content)
                verify { connection.rollback() }
            }
        }
    }

    @Test
    fun testExecuteSelectLong() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn, "select 1")))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":" +
                            "[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null," +
                            "\"longValue\":1,\"stringValue\":null}]],\"columnMetadata\":null}",
                    response.content)
            }
        }
    }

    @Test
    fun testExecuteSelectDouble() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn,
                    dummySecretArn,
                    "select cast(1.0 as DOUBLE)")))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":" +
                            "[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":1.0,\"isNull\":null," +
                            "\"longValue\":null,\"stringValue\":null}]],\"columnMetadata\":null}",
                    response.content)
            }
        }
    }

    @Test
    fun testExecuteSelectVARCHAR() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select 'hello'")))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"hello\"}]],\"columnMetadata\":null}",
                    response.content)
            }
        }
    }

    @Test
    fun testExecuteSelectBINARY() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select cast('hello' as BINARY)")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":\"aGVsbG8=\",\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":null}]],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteSelectBOOL() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select 1 > 0 as value")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":true,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":null}]],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteSelectTIMESTAMP() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "SELECT CAST('2021-03-10 22:41:04.968123' AS TIMESTAMP) AS value")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"2021-03-10 22:41:04.968\"}]],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "SELECT CAST('2021-03-10 22:41:04' AS TIMESTAMP) AS value")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"2021-03-10 22:41:04\"}]],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteSelectNull() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select null")))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":true,\"longValue\":null,\"stringValue\":null}]],\"columnMetadata\":null}",
                    response.content)
            }
        }
    }

    @Test
    fun testExecuteInsertParameters() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "CREATE TABLE TEST(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(10), age INT)"
                )))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":[],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO TEST (name, age) VALUES ('cat', 1)")))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":1,\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO test (name, age) VALUES (:name, :age)", parameters = listOf(
                        SqlParameter("name", Field(stringValue = "dog")),
                        SqlParameter("age", Field(longValue = 3)),
                    ))))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":1,\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null}],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteInsertParametersWithTransaction() {
        withTestApplication({ module(testing = true) }) {

            val beginTransaction = handleRequest(HttpMethod.Post, "/BeginTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BeginTransactionRequest(dummyResourceArn, dummySecretArn)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertNotNull(response.content)
            }

            val transactionId = Json.decodeFromString(
                BeginTransactionResponse.serializer(),
                beginTransaction.response.content!!
            ).transactionId

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "CREATE TABLE TEST(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(10), age INT)",
                    transactionId = transactionId
                )))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":[],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[],\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO TEST (name, age) VALUES ('cat', 1)", transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":1,\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO test (name, age) VALUES (:name, :age)", parameters = listOf(
                        SqlParameter("name", Field(stringValue = "dog")),
                        SqlParameter("age", Field(longValue = 3)),
                    ), transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":1,\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null}],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true, transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/CommitTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(CommitTransactionRequest(dummyResourceArn,
                    dummySecretArn,
                    transactionId)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals("{\"transactionStatus\":\"Transaction Committed\"}", response.content)
            }

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteInvalidQuery() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select )")))
            }.apply {
                assertEquals(HttpStatusCode.BadRequest, response.status())
                assertEquals(
                    "{\"message\":\"Database error code: 42001. Message: [42001-200]\",\"code\":\"BadRequestException\"}",
                    response.content)
            }
        }
    }

    @Test
    fun testExecuteBatch() {
        withTestApplication({ module(testing = true) }) {
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "CREATE TABLE TEST(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(10), age INT)"
                )))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":[],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/BatchExecute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BatchExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST")))
            }.apply {
                assertEquals(
                    "{\"updateResults\":[]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/BatchExecute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BatchExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO test (name, age) VALUES (:name, :age)", parameterSets = listOf(
                        listOf(
                            SqlParameter("name", Field(stringValue = "cat")),
                            SqlParameter("age", Field(longValue = 1)),
                        ),
                        listOf(
                            SqlParameter("name", Field(stringValue = "dog")),
                            SqlParameter("age", Field(longValue = 3)),
                        ),
                    ))))
            }.apply {
                assertEquals(
                    "{\"updateResults\":[{\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}]},{\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null}]}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }

    @Test
    fun testExecuteBatchWithTransactionId() {
        withTestApplication({ module(testing = true) }) {
            val beginTransaction = handleRequest(HttpMethod.Post, "/BeginTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BeginTransactionRequest(dummyResourceArn, dummySecretArn)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertNotNull(response.content)
            }

            val transactionId = Json.decodeFromString(
                BeginTransactionResponse.serializer(),
                beginTransaction.response.content!!
            ).transactionId

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "CREATE TABLE TEST(id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(10), age INT)",
                    transactionId = transactionId
                )))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":[],\"records\":null,\"columnMetadata\":null}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/BatchExecute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BatchExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST",
                    transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"updateResults\":[]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/BatchExecute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(BatchExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "INSERT INTO test (name, age) VALUES (:name, :age)", parameterSets = listOf(
                        listOf(
                            SqlParameter("name", Field(stringValue = "cat")),
                            SqlParameter("age", Field(longValue = 1)),
                        ),
                        listOf(
                            SqlParameter("name", Field(stringValue = "dog")),
                            SqlParameter("age", Field(longValue = 3)),
                        ),
                    ),
                    transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"updateResults\":[{\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}]},{\"generatedFields\":[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null}]}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true, transactionId = transactionId)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }

            handleRequest(HttpMethod.Post, "/CommitTransaction") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(CommitTransactionRequest(dummyResourceArn,
                    dummySecretArn,
                    transactionId)))
            }.apply {
                assertEquals(HttpStatusCode.OK, response.status())
                assertEquals("{\"transactionStatus\":\"Transaction Committed\"}", response.content)
            }

            handleRequest(HttpMethod.Post, "/Execute") {
                addHeader(HttpHeaders.ContentType, "*/*")
                setBody(Json.encodeToString(ExecuteStatementRequest(dummyResourceArn, dummySecretArn,
                    "select * from TEST", includeResultMetadata = true)))
            }.apply {
                assertEquals(
                    "{\"numberOfRecordsUpdated\":0,\"generatedFields\":null,\"records\":[[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"cat\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":1,\"stringValue\":null}],[{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":2,\"stringValue\":null},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":null,\"stringValue\":\"dog\"},{\"blobValue\":null,\"booleanValue\":null,\"doubleValue\":null,\"isNull\":null,\"longValue\":3,\"stringValue\":null}]],\"columnMetadata\":[{\"arrayBaseColumnType\":0,\"isAutoIncrement\":true,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"ID\",\"name\":\"ID\",\"nullable\":0,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"NAME\",\"name\":\"NAME\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":12,\"typeName\":\"VARCHAR\"},{\"arrayBaseColumnType\":0,\"isAutoIncrement\":false,\"isCaseSensitive\":true,\"isCurrency\":false,\"isSigned\":true,\"label\":\"AGE\",\"name\":\"AGE\",\"nullable\":1,\"precision\":10,\"scale\":0,\"schemaName\":\"PUBLIC\",\"tableName\":\"TEST\",\"type\":4,\"typeName\":\"INTEGER\"}]}",
                    response.content)
                assertEquals(HttpStatusCode.OK, response.status())
            }
        }
    }
}

