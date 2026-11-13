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

import pickle
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.read.fallback_split import FallbackSplit


@pytest.mark.native_plan
@pytest.mark.skipif(not native_runtime_available(), reason="pypaimon-rust not installed")
def test_native_fallback_partition_precedence_and_transport(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('test', False)
    options = {key: 'false' for key in (
        'read.native.enabled', 'write.native.enabled',
        'scan.native-plan.enabled', 'commit.native.enabled')}
    options['file.format'] = 'parquet'
    schema = pa.schema([('id', pa.int64()), ('pt', pa.string())])
    catalog.create_table('test.t', Schema.from_pyarrow_schema(
        schema, partition_keys=['pt'], options=options), False)
    table = catalog.get_table('test.t')
    table.branch_manager().create_branch('backup')
    for target, rows in (
            (table, [{'id': 1, 'pt': 'shared'}]),
            (table.copy({'branch': 'backup'}),
             [{'id': 2, 'pt': 'backup'}, {'id': 3, 'pt': 'shared'}])):
        builder = target.new_batch_write_builder()
        write, commit = builder.new_write(), builder.new_commit()
        try:
            write.write_arrow(pa.Table.from_pylist(rows, schema=schema))
            commit.commit(write.prepare_commit())
        finally:
            write.close()
            commit.close()

    native = table.copy({'scan.fallback-branch': 'backup',
                         'read.native.enabled': 'true',
                         'scan.native-plan.enabled': 'true'})
    with patch('pypaimon.read.read_dispatch._python_scan_execution',
               side_effect=AssertionError('Python planning must not be used')), \
            patch('pypaimon.read.read_dispatch.native_read_fallback',
                  side_effect=AssertionError('Python reading must not be used')):
        builder = native.new_read_builder().with_projection(['id'])
        splits = builder.new_scan().plan().splits()
        assert any(isinstance(split, FallbackSplit) and split.is_fallback for split in splits)
        for transported in (False, True):
            if transported:
                # Remove the cached Rust handle to exercise both wire bridges.
                for split in splits:
                    if hasattr(split, '_native_split'):
                        del split._native_split
                splits = pickle.loads(pickle.dumps(splits))
            result = builder.new_read().to_arrow(splits)
            assert sorted(result.column('id').to_pylist()) == [1, 2]
        filtered = native.new_read_builder().with_projection(['id'])
        filtered.with_filter(filtered.new_predicate_builder().equal('id', 3))
        # File statistics may conservatively retain splits; the residual must
        # still return no rows rather than revive backup's shadowed id=3.
        splits = filtered.new_scan().plan().splits()
        assert filtered.new_read().to_arrow(splits).num_rows == 0
