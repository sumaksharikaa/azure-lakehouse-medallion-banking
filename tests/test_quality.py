from lakehouse import quality


def _errors(df):
    return {r["id"]: sorted(r[quality.DQ_ERRORS]) for r in df.collect()}


def test_value_rules_and_null_handling(spark):
    df = spark.createDataFrame(
        [
            (1, "a@b.com", "USD", 10.0),
            (2, "bad-email", "XXX", -1.0),
            (3, None, None, None),
            (4, "  ", "USD", 5.0),
        ],
        "id int, email string, currency string, amount double",
    )
    rules = [
        {"name": "email_nn", "type": "not_null", "column": "email"},
        {"name": "email_fmt", "type": "regex", "column": "email", "pattern": r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"},
        {"name": "ccy", "type": "accepted_values", "column": "currency", "values": ["USD"]},
        {"name": "amt", "type": "range", "column": "amount", "min": 0.01, "max": 1000},
    ]
    errs = _errors(quality.apply_rules(df, rules))
    assert errs[1] == []
    assert errs[2] == ["amt", "ccy", "email_fmt"]
    # Nulls are reported once by not_null, not by every value rule
    assert errs[3] == ["email_nn"]
    # Whitespace-only counts as missing
    assert "email_nn" in errs[4]


def test_expression_rule_treats_null_result_as_failure(spark):
    df = spark.createDataFrame(
        [(1, "CREDIT_CARD", 500.0), (2, "CHECKING", 500.0), (3, "CHECKING", None), (4, None, 1.0)],
        "id int, account_type string, credit_limit double",
    )
    rule = {
        "name": "limit_on_cards",
        "type": "expression",
        "expr": "credit_limit IS NULL OR account_type = 'CREDIT_CARD'",
    }
    errs = _errors(quality.apply_rules(df, [rule]))
    assert errs == {1: [], 2: ["limit_on_cards"], 3: [], 4: ["limit_on_cards"]}


def test_foreign_key_rule(spark):
    txns = spark.createDataFrame(
        [("t1", "A1"), ("t2", "A9"), ("t3", None)], "txn_id string, account_id string"
    )
    accounts = spark.createDataFrame([("A1",), ("A1",), ("A2",)], "account_id string")
    rule = {
        "name": "acct_exists",
        "type": "foreign_key",
        "column": "account_id",
        "reference": "accounts",
        "reference_column": "account_id",
    }
    out = quality.apply_rules(txns, [rule], {"accounts": accounts})
    errs = {r["txn_id"]: r[quality.DQ_ERRORS] for r in out.collect()}
    assert errs == {"t1": [], "t2": ["acct_exists"], "t3": []}
    assert out.count() == 3  # duplicate reference keys must not fan out rows
    assert not [c for c in out.columns if c.startswith("__fk")]


def test_split_and_summary(spark):
    df = spark.createDataFrame([(1, None), (2, "x"), (3, None)], "id int, v string")
    checked = quality.apply_rules(df, [{"name": "v_nn", "type": "not_null", "column": "v"}])
    valid, invalid = quality.split_valid_invalid(checked)
    assert [r.id for r in valid.collect()] == [2]
    assert quality.DQ_ERRORS not in valid.columns
    assert "_quarantined_at" in invalid.columns
    assert quality.failure_summary(invalid) == {"v_nn": 2}
