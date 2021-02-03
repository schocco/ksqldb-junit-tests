package com.is_gr8.ksqldb.testing

import io.confluent.ksql.test.tools.TestExecutor
import org.junit.Ignore
import org.junit.jupiter.api.*
import java.util.stream.Stream

class KsqlTest {

    @TestFactory
    fun testBarPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar")
    }

    @TestFactory
    fun testFooPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/foo")
    }

    companion object {
        private val ksqlTestFactory = KsqlTestFactory()
    }

}



