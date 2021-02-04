package com.is_gr8.ksqldb.testing

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.ksql.parser.DefaultKsqlParser
import io.confluent.ksql.test.model.*
import io.confluent.ksql.test.tools.*
import io.confluent.ksql.util.KsqlException
import io.confluent.ksql.util.PersistentQueryMetadata
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.fail
import java.io.File
import java.io.IOException
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asStream

class KsqlTestFactory {

    private val objectMapper: ObjectMapper = TestJsonMapper.INSTANCE.get()


    fun findKsqlTestCases(
        pathname: String,
        ksqlExtension: String = "ksql",
        inputFileName: String = "input.json",
        outputfileName: String = "output.json"
    ): Stream<DynamicTest> {
        return File(pathname).walk()
            .filter { file: File -> file.isDirectory && file.listFiles()?.any { it.extension == ksqlExtension } == true}
            .map { testCaseFolder: File ->
                val contents = testCaseFolder.listFiles()!!
                val ksqlFile = contents.first { it.extension == ksqlExtension }
                val inputFile = contents.first { it.name == inputFileName }
                val outputFile = contents.first { it.name == outputfileName }
                createTestFromTriple(ksqlFile, inputFile, outputFile)
            }.asStream()
    }

    private fun createTestFromTriple(ksqlFile: File, inputFile: File, outputFile: File): DynamicTest {
        val statements = getKsqlStatements(ksqlFile)
        val inputRecordNodes: InputRecordsNode? = readInputRecords(inputFile)
        val outRecordNodes: OutputRecordsNode = readOutputRecordNodes(outputFile)

        val testCaseNode = TestCaseNode(
            "KSQL_Test",
            Optional.empty(),
            null,
            inputRecordNodes?.inputRecords,
            outRecordNodes.outputRecords,
            emptyList(),
            statements,
            null,
            null as ExpectedExceptionNode?,
            null as PostConditionsNode?,
            true
        )
        val testCase = TestCaseBuilder.buildTests(testCaseNode, ksqlFile.toPath())[0]
        return DynamicTest.dynamicTest(ksqlFile.path) { executeTestCase(testCase) }
    }

    private fun readOutputRecordNodes(outputFile: File): OutputRecordsNode {
        return try {
            objectMapper.readValue(
                outputFile,
                OutputRecordsNode::class.java
            )
        } catch (var8: Exception) {
            throw RuntimeException("File name: " + outputFile + " Message: " + var8.message)
        }
    }

    private fun readInputRecords(inputFile: File): InputRecordsNode? {
        return try {
            if (inputFile.exists()) objectMapper.readValue(
                inputFile,
                InputRecordsNode::class.java
            ) else null
        } catch (e: Exception) {
            throw RuntimeException(String.format("File name: %s Message: %s", inputFile, e.message))
        }
    }

    private fun executeTestCase(testCase: TestCase) {
        val testExecutor = TestExecutor.create()
        try {
            val testExecutionListener = object : TestExecutionListener {
                override fun acceptQuery(query: PersistentQueryMetadata?) {
                    // a printed topology might be helpful when tests fail
                    print(query?.topologyDescription)
                }
            }
            testExecutor.buildAndExecuteQuery(testCase, testExecutionListener)
        } catch (e: AssertionError) {
            fail(e.message)
        } catch (e: Exception) {
            fail(e.message)
        } finally {
            testExecutor.close()
        }
    }

    private fun getKsqlStatements(queryFile: File): List<String> {
        return try {
            val sqlStatements = queryFile.readText()
            DefaultKsqlParser().let { parser ->
                parser.parse(sqlStatements).map { it.statementText }
            }
        } catch (e: IOException) {
            throw KsqlException("Could not read the query file: $queryFile. Details: ${e.message}", e)
        }
    }
}
