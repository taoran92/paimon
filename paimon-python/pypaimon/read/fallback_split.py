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

from pypaimon.read.split import Split


class FallbackSplit(Split):
    """A split whose files use the configured fallback branch's schema history."""

    def __init__(self, split: Split, is_fallback: bool):
        self.split = split
        self.is_fallback = is_fallback

    @property
    def row_count(self):
        return self.split.row_count

    @property
    def files(self):
        return self.split.files

    @property
    def partition(self):
        return self.split.partition

    @property
    def bucket(self):
        return self.split.bucket

    def merged_row_count(self):
        return self.split.merged_row_count()

    def __getattr__(self, name):
        if name.startswith('_') or name in ('split', 'is_fallback'):
            raise AttributeError(name)
        return getattr(self.split, name)
