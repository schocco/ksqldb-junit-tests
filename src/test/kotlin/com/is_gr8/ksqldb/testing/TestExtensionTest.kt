package com.is_gr8.ksqldb.testing

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.extension.ExtendWith
import java.util.stream.Stream

@ExtendWith(KsqlParameterResolverExtension::class)
class TestExtensionTest(private val ksqlTestFactory: KsqlTestFactory) {

    @TestFactory
    internal fun testBarPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar")
    }

}
