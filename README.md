# KSQL testing with JUnit 5
This is an example to show how JUnit 5 can be used as an alternative to the ksql-test-runner.
Internally it still uses the same classes that the ksql-test-runner uses.

See https://docs.ksqldb.io/en/latest/how-to-guides/test-an-app/

## Why would you do this?

- better reporting in CI systems which parse junit results
- faster than the testing tool when running multiple tests
- easy to extend with custom checks, e.g. to ensure naming patterns, max number of joins, etc
