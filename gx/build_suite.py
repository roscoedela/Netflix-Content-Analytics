import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration

context = gx.get_context()

suite = context.add_or_update_expectation_suite("mart_cvi_suite")

suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_table_row_count_to_be_between",
    kwargs={"min_value": 80000, "max_value": 100000}
))

for col in ["MOVIE_ID", "MOVIE_TITLE", "CONTENT_VALUE_INDEX", "CONTENT_TIER",
            "QUALITY_SCORE", "ENGAGEMENT_SCORE", "RELEASE_YEAR"]:
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": col}
    ))

suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={"column": "CONTENT_VALUE_INDEX"}
))
suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "CONTENT_VALUE_INDEX", "min_value": 0.0, "max_value": 100.0}
))

for col in ["QUALITY_SCORE", "ENGAGEMENT_SCORE"]:
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": col}
    ))
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": col, "min_value": 0.0, "max_value": 1.0}
    ))

suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "CONTENT_TIER",
        "value_set": ["Full Score", "No Efficiency",
                      "Quality Engagement Only", "Limited"]
    }
))

suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={"column": "MOVIE_ID"}
))

suite.add_expectation(ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={"column": "MOVIE_TITLE"}
))

context.save_expectation_suite(suite)
print(f"Saved suite with {len(suite.expectations)} expectations")