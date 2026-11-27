# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange

SQLContext = pytest.importorskip('pypaimon_rust.datafusion').SQLContext

pytestmark = pytest.mark.native_plan


WAREHOUSE = os.environ.get("PAIMON_TEST_WAREHOUSE", "/tmp/paimon-warehouse")


class SQLContextTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Create the test table once before all tests in this class."""
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        ctx.sql("DROP TABLE IF EXISTS sql_test_table")
        ctx.sql("CREATE TABLE sql_test_table (id INT, name STRING)")
        ctx.sql("INSERT INTO sql_test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

    @classmethod
    def tearDownClass(cls):
        """Clean up the test table after all tests."""
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        ctx.sql("DROP TABLE IF EXISTS sql_test_table")

    def _create_sql_context(self):
        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
        return ctx

    def test_sql_returns_table(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        table = pa.Table.from_batches(batches)
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(table.column("name").to_pylist(), ["alice", "bob", "carol"])

    def test_sql_to_pandas(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table ORDER BY id")
        table = pa.Table.from_batches(batches)
        df = table.to_pandas()
        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df.columns), ["id", "name"])

    def test_sql_with_filter(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 1 ORDER BY id")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("id").to_pylist(), [2, 3])

    def test_sql_with_empty_result(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT id, name FROM sql_test_table WHERE id > 4 ORDER BY id")
        self.assertEqual(len(batches), 0)

    def test_sql_with_aggregation(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT count(*) AS cnt FROM sql_test_table")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.column("cnt").to_pylist(), [3])

    def test_sql_two_part_reference(self):
        ctx = self._create_sql_context()
        batches = ctx.sql("SELECT count(*) AS cnt FROM default.sql_test_table")
        table = pa.Table.from_batches(batches)
        self.assertEqual(table.column("cnt").to_pylist(), [3])


@pytest.fixture
def catalog_sql(tmp_path):
    options = {"warehouse": str(tmp_path)}
    catalog = CatalogFactory.create(options)
    catalog.create_database("db", False)

    def sql(query):
        ctx = SQLContext()
        ctx.register_catalog("paimon", options)
        return [row for batch in ctx.sql(query) for row in batch.to_pylist()]

    return catalog, sql


def test_rename_ordering_column_preserves_group_deletes(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (
               id BIGINT, ts BIGINT, channel STRING, ts2 BIGINT, label STRING, op STRING, PRIMARY KEY (id)
           ) WITH ('bucket'='1', 'merge-engine'='partial-update', 'rowkind.field'='op',
                   'fields.ts.sequence-group'='channel', 'fields.ts.sequence-group.sort-order'='ascending',
                   'fields.ts2.sequence-group'='label', 'partial-update.remove-record-on-sequence-group'='ts,ts2')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 'organic', 100, 'active', '+I')")
    catalog.alter_table("db.events", [SchemaChange.rename_column("ts", "event_time")])
    options = catalog.get_table("db.events").table_schema.options
    assert options["partial-update.remove-record-on-sequence-group"] == "event_time,ts2"

    query = "SELECT event_time, channel, label FROM paimon.db.events"
    original = [{"event_time": 100, "channel": "organic", "label": "active"}]
    assert sql(query) == original
    sql("INSERT INTO paimon.db.events VALUES (1, 80, 'older', NULL, NULL, '+I')")
    assert sql(query) == original
    sql("INSERT INTO paimon.db.events VALUES (1, 90, NULL, NULL, NULL, '-D')")
    assert sql(query) == original
    sql("INSERT INTO paimon.db.events VALUES (1, 101, NULL, NULL, NULL, '-D')")
    assert sql(query) == []


def test_rename_protected_column_preserves_aggregation(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id BIGINT, ts BIGINT, amount BIGINT, op STRING, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='amount',
                 'fields.amount.aggregate-function'='sum', 'fields.amount.ignore-retract'='true',
                 'rowkind.field'='op')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 7, '+I')")
    catalog.alter_table("db.events", [SchemaChange.rename_column("amount", "total")])
    options = catalog.get_table("db.events").table_schema.options
    assert options["fields.total.aggregate-function"] == "sum"
    assert "fields.amount.aggregate-function" not in options

    query = "SELECT total FROM paimon.db.events"
    assert sql(query) == [{"total": 7}]
    sql("INSERT INTO paimon.db.events VALUES (1, 101, 5, '+I')")
    assert sql(query) == [{"total": 12}]
    sql("INSERT INTO paimon.db.events VALUES (1, 102, 2, '-D')")
    assert sql(query) == [{"total": 12}]


def test_rename_rejects_target_retract_option(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, amount INT, op STRING, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'rowkind.field'='op',
                 'fields.ts.sequence-group'='amount', 'fields.amount.aggregate-function'='sum',
                 'fields.total.ignore-retract'='true')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 7, '+I')")
    sql("INSERT INTO paimon.db.events VALUES (1, 101, 2, '-D')")
    rename = SchemaChange.rename_column("amount", "total")
    with pytest.raises(RuntimeError, match=r"fields\.total\.ignore-retract"):
        catalog.alter_table("db.events", [rename])
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT amount FROM paimon.db.events") == [{"amount": 5}]

    catalog.alter_table("db.events", [SchemaChange.remove_option("fields.total.ignore-retract"), rename])
    assert sql("SELECT total FROM paimon.db.events") == [{"total": 5}]


def test_rename_protected_column_preserves_listagg_options(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id BIGINT, ts BIGINT, tags STRING, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='tags',
                 'fields.tags.aggregate-function'='listagg', 'fields.tags.list-agg-delimiter'='|',
                 'fields.tags.distinct'='true')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 'a')")
    catalog.alter_table("db.events", [SchemaChange.rename_column("tags", "labels")])

    query = "SELECT labels FROM paimon.db.events"
    assert sql(query) == [{"labels": "a"}]
    sql("INSERT INTO paimon.db.events VALUES (1, 101, 'a|b')")
    assert sql(query) == [{"labels": "a|b"}]


@pytest.mark.parametrize("function,operand", [("sum", 1), ("product", 2)])
def test_rename_preserves_integer_overflow_check(catalog_sql, function, operand):
    catalog, sql = catalog_sql
    sql(f"""CREATE TABLE paimon.db.events (id INT, ts INT, amount INT, PRIMARY KEY (id))
            WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='amount',
                  'fields.amount.aggregate-function'='{function}',
                  'fields.amount.{function}.fail-on-overflow'='true')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 2147483647)")
    sql(f"INSERT INTO paimon.db.events VALUES (1, 101, {operand})")
    with pytest.raises(ValueError, match="overflowed"):
        sql("SELECT amount FROM paimon.db.events")
    catalog.alter_table("db.events", [SchemaChange.rename_column("amount", "total")])
    with pytest.raises(ValueError, match="overflowed"):
        sql("SELECT total FROM paimon.db.events")


def test_rename_preserves_merge_map_time_field(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, attrs MAP(STRING, STRUCT<stamp STRING, payload STRING>),
               PRIMARY KEY (id)) WITH ('bucket'='1', 'merge-engine'='partial-update',
               'fields.ts.sequence-group'='attrs', 'fields.attrs.aggregate-function'='merge_map_with_keytime',
               'fields.attrs.ts-field'='stamp', 'fields.attrs.overwrite-on-equal-ts'='true')""")
    for ts, stamp, payload in [(100, "9", "a"), (101, "1", "z")]:
        sql(f"""INSERT INTO paimon.db.events VALUES
                (1, {ts}, MAP(['x'], [named_struct('stamp', '{stamp}', 'payload', '{payload}')]))""")
    expected = [{"attrs": [("x", {"stamp": "9", "payload": "a"})]}]
    assert sql("SELECT attrs FROM paimon.db.events") == expected
    catalog.alter_table("db.events", [SchemaChange.rename_column("attrs", "labels")])
    assert sql("SELECT labels AS attrs FROM paimon.db.events") == expected
    options = catalog.get_table("db.events").table_schema.options
    for suffix, value in [("ts-field", "stamp"), ("overwrite-on-equal-ts", "true")]:
        assert options[f"fields.labels.{suffix}"] == value
        assert f"fields.attrs.{suffix}" not in options


@pytest.mark.parametrize("modifiers, second_key, expected_value", [
    ({}, "1", 4),
    ({"nested-sequence-field": "v"}, "1", 9),
    ({"nested-key-null-strategy": "ignore"}, "CAST(NULL AS BIGINT)", 9),
    ({"count-limit": "1"}, "2", 9),
], ids=["nested-key", "nested-sequence-field", "nested-key-null-strategy", "count-limit"])
def test_rename_protected_column_preserves_nested_aggregation(catalog_sql, modifiers, second_key, expected_value):
    catalog, sql = catalog_sql
    settings = {"aggregate-function": "nested_update", "nested-key": "k", **modifiers}
    field_options = ", ".join(f"'fields.items.{key}'='{value}'" for key, value in settings.items())
    sql(f"""CREATE TABLE paimon.db.events (
               id BIGINT, ts BIGINT, items STRUCT<k BIGINT, v BIGINT>[], PRIMARY KEY (id)
           ) WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='items',
                   {field_options})""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, [named_struct('k', 1, 'v', 9)])")
    sql(f"INSERT INTO paimon.db.events VALUES (1, 101, [named_struct('k', {second_key}, 'v', 4)])")
    expected = [{"items": [{"k": 1, "v": expected_value}]}]
    assert sql("SELECT items FROM paimon.db.events") == expected

    catalog.alter_table("db.events", [SchemaChange.rename_column("items", "labels")])

    assert sql("SELECT labels AS items FROM paimon.db.events") == expected
    options = catalog.get_table("db.events").table_schema.options
    for key, value in settings.items():
        assert options[f"fields.labels.{key}"] == value
        assert f"fields.items.{key}" not in options


def test_deduplicate_rename_with_unused_sequence_group(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id BIGINT, ts BIGINT, amount BIGINT, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='deduplicate', 'fields.ts.sequence-group'='amount')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 7)")

    catalog.alter_table("db.events", [SchemaChange.rename_column("amount", "amount,usd")])

    assert sql('SELECT "amount,usd" FROM paimon.db.events') == [{"amount,usd": 7}]


@pytest.mark.parametrize("groups, resolve_conflict", [
    ("'fields.ts.sequence-group'='a', 'fields.ts2.sequence-group'='b'",
     SchemaChange.remove_option("fields.ts2.sequence-group")),
    ("'fields.ts.sequence-group'='a,b'", SchemaChange.set_option("fields.ts.sequence-group", "b")),
], ids=["across-groups", "within-group"])
def test_rename_checks_final_protected_column_ownership(catalog_sql, groups, resolve_conflict):
    catalog, sql = catalog_sql
    sql(f"""CREATE TABLE paimon.db.events (id BIGINT, ts BIGINT, ts2 BIGINT, a BIGINT, b BIGINT, PRIMARY KEY (id))
            WITH ('bucket'='1', 'merge-engine'='partial-update', {groups}, 'fields.b.aggregate-function'='sum')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 100, 7, 1)")
    sql("INSERT INTO paimon.db.events VALUES (1, 101, 101, 5, 2)")
    changes = [SchemaChange.drop_column("b"), SchemaChange.rename_column("a", "b")]

    with pytest.raises(RuntimeError, match="Field 'b' is protected"):
        catalog.alter_table("db.events", changes)
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT a, b FROM paimon.db.events") == [{"a": 5, "b": 3}]

    catalog.alter_table("db.events", changes + [resolve_conflict])
    assert sql("SELECT b FROM paimon.db.events") == [{"b": 5}]


def test_rename_checks_final_ordering_column_ownership(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (
               id BIGINT, ts BIGINT, ts2 BIGINT, seq BIGINT, a BIGINT, b BIGINT, PRIMARY KEY (id)
           ) WITH ('bucket'='1', 'merge-engine'='partial-update',
                   'fields.ts.sequence-group'='a', 'fields.ts2,seq.sequence-group'='b')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 200, 1, 7, 9)")
    changes = [SchemaChange.drop_column("ts2"), SchemaChange.rename_column("ts", "ts2")]

    with pytest.raises(RuntimeError, match="Ordering field 'ts2'") as error:
        catalog.alter_table("db.events", changes)
    assert "fields.ts2.sequence-group" in str(error.value)
    assert "fields.ts2,seq.sequence-group" in str(error.value)
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT ts, ts2, a, b FROM paimon.db.events") == [{"ts": 100, "ts2": 200, "a": 7, "b": 9}]

    catalog.alter_table("db.events", changes + [SchemaChange.remove_option("fields.ts2,seq.sequence-group")])
    assert sql("SELECT ts2, a, b FROM paimon.db.events") == [{"ts2": 100, "a": 7, "b": 9}]


@pytest.mark.parametrize("direction", [None, "ascending"], ids=["remove-sort-order", "set-sort-order"])
def test_renamed_group_cannot_inherit_dropped_sort_order(catalog_sql, direction):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, x INT, y INT, v INT, w INT, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.x.sequence-group'='v',
                 'fields.y.sequence-group'='w', 'fields.y.sequence-group.sort-order'='descending')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 10, 7, 4)")
    sql("INSERT INTO paimon.db.events VALUES (1, 80, 20, 9, 5)")
    changes = [SchemaChange.drop_column("y"), SchemaChange.remove_option("fields.y.sequence-group"),
               SchemaChange.rename_column("x", "y")]
    order = "fields.y.sequence-group.sort-order"
    with pytest.raises(RuntimeError, match=r"fields\.y\.sequence-group\.sort-order"):
        catalog.alter_table("db.events", changes)
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT x, v FROM paimon.db.events") == [{"x": 100, "v": 7}]

    resolve = SchemaChange.remove_option(order) if direction is None else SchemaChange.set_option(order, direction)
    catalog.alter_table("db.events", changes + [resolve])
    assert catalog.get_table("db.events").table_schema.options.get(order) == direction
    assert sql("SELECT y, v FROM paimon.db.events") == [{"y": 100, "v": 7}]


@pytest.mark.parametrize("delete_fields", ["y", "x,y"])
def test_rename_cannot_collapse_composite_ordering_fields(catalog_sql, delete_fields):
    catalog, sql = catalog_sql
    sql(f"""CREATE TABLE paimon.db.events (id INT, x INT, y INT, a INT, op STRING, PRIMARY KEY (id))
            WITH ('bucket'='1', 'merge-engine'='partial-update', 'rowkind.field'='op',
                  'fields.x,y.sequence-group'='a',
                  'partial-update.remove-record-on-sequence-group'='{delete_fields}')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 10, 7, '+I')")
    sql("INSERT INTO paimon.db.events VALUES (1, 80, 20, 9, '-D')")
    with pytest.raises(RuntimeError, match="dropped field"):
        catalog.alter_table("db.events", [SchemaChange.drop_column("x"), SchemaChange.rename_column("y", "x")])
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT id, a FROM paimon.db.events") == [{"id": 1, "a": 7}]


def test_rename_cannot_transfer_protection_through_reused_name(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, a INT, b INT, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='b')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 1, 10)")
    sql("INSERT INTO paimon.db.events VALUES (1, 80, 2, 20)")
    changes = [SchemaChange.drop_column("b"), SchemaChange.rename_column("a", "b"),
               SchemaChange.rename_column("b", "c")]
    with pytest.raises(RuntimeError, match=r"fields\.ts\.sequence-group"):
        catalog.alter_table("db.events", changes)
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT a, b FROM paimon.db.events") == [{"a": 2, "b": 10}]

    catalog.alter_table("db.events", changes + [SchemaChange.remove_option("fields.ts.sequence-group")])
    assert sql("SELECT c FROM paimon.db.events") == [{"c": 2}]


def test_reused_column_name_cannot_inherit_group_deletes(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, ts2 INT, seq INT, a INT, b INT, op STRING,
               PRIMARY KEY (id)) WITH ('bucket'='1', 'merge-engine'='partial-update', 'rowkind.field'='op',
               'fields.ts.sequence-group'='a', 'fields.ts2,seq.sequence-group'='b',
               'partial-update.remove-record-on-sequence-group'='ts2')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 200, 1, 7, 9, '+I')")
    sql("INSERT INTO paimon.db.events VALUES (1, 101, NULL, NULL, NULL, NULL, '-D')")
    query = "SELECT id, a, b FROM paimon.db.events"
    assert sql(query) == [{"id": 1, "a": None, "b": 9}]

    with pytest.raises(RuntimeError, match="partial-update.remove-record-on-sequence-group"):
        catalog.alter_table("db.events", [SchemaChange.drop_column("ts2"),
                                          SchemaChange.rename_column("ts", "ts2"),
                                          SchemaChange.remove_option("fields.ts2,seq.sequence-group")])
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql(query) == [{"id": 1, "a": None, "b": 9}]


def test_rename_protected_column_to_dropped_ordering_field(catalog_sql):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, a INT, b INT, v INT, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='a',
                 'fields.a.aggregate-function'='sum', 'fields.b.sequence-group'='v')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 7, 50, 11)")
    sql("INSERT INTO paimon.db.events VALUES (1, 101, 5, 60, 12)")
    changes = [SchemaChange.drop_column("b"), SchemaChange.rename_column("a", "b")]
    with pytest.raises(RuntimeError, match="fields.b.sequence-group"):
        catalog.alter_table("db.events", changes)
    assert catalog.get_table("db.events").table_schema.id == 0
    assert sql("SELECT a, v FROM paimon.db.events") == [{"a": 12, "v": 12}]

    catalog.alter_table("db.events", changes + [SchemaChange.remove_option("fields.b.sequence-group")])
    assert sql("SELECT b, v FROM paimon.db.events") == [{"b": 12, "v": 12}]


def test_ambiguous_sequence_group_rename_preserves_group_binding(catalog_sql):
    catalog, sql = catalog_sql

    sql("""CREATE TABLE paimon.db.events (
               id BIGINT, ts BIGINT, " ts " BIGINT, channel STRING, " channel " STRING, PRIMARY KEY (id)
           ) WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields. ts .sequence-group'=' channel ')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 100, 10, 'organic', 'first')")

    for name in (" channel ", "channel", " ts ", "ts"):
        with pytest.raises(RuntimeError, match="ambiguous"):
            catalog.alter_table("db.events", [SchemaChange.rename_column(name, "aux")])
    for literal in (" channel ", " ts "):
        with pytest.raises(RuntimeError, match="dropped field"):
            catalog.alter_table("db.events", [SchemaChange.drop_column(literal),
                                              SchemaChange.rename_column(literal.strip(), "aux")])
    assert catalog.get_table("db.events").table_schema.id == 0

    sql("INSERT INTO paimon.db.events VALUES (1, 80, 20, 'older', 'later')")
    rows = sql("SELECT * FROM paimon.db.events")
    assert rows == [{"id": 1, "ts": 100, " ts ": 20, "channel": "organic", " channel ": "later"}]


@pytest.mark.parametrize("set_sum, expected", [(False, 3), (True, 5)], ids=["default", "explicit-sum"])
def test_rename_readded_column_uses_its_own_aggregation(catalog_sql, set_sum, expected):
    catalog, sql = catalog_sql
    sql("""CREATE TABLE paimon.db.events (id INT, ts INT, b INT, PRIMARY KEY (id))
           WITH ('bucket'='1', 'merge-engine'='partial-update', 'fields.ts.sequence-group'='b',
                 'fields.b.aggregate-function'='sum')""")
    sql("INSERT INTO paimon.db.events VALUES (1, 10, 9)")

    changes = [SchemaChange.drop_column("b"), SchemaChange.add_column("b", AtomicType("INT"))]
    if set_sum:
        changes.append(SchemaChange.set_option("fields.b.aggregate-function", "sum"))
    changes.extend([SchemaChange.rename_column("b", "c"), SchemaChange.set_option("fields.ts.sequence-group", "c")])
    catalog.alter_table("db.events", changes)

    query = "SELECT c FROM paimon.db.events"
    assert sql(query) == [{"c": None}]
    sql("INSERT INTO paimon.db.events VALUES (1, 11, 2)")
    sql("INSERT INTO paimon.db.events VALUES (1, 12, 3)")
    assert sql(query) == [{"c": expected}]
    options = catalog.get_table("db.events").table_schema.options
    assert "fields.b.aggregate-function" not in options
    assert options.get("fields.c.aggregate-function") == ("sum" if set_sum else None)


if __name__ == "__main__":
    unittest.main()
