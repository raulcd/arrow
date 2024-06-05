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

import weakref

import pytest

try:
    import numpy as np
except ImportError:
    pass

import pyarrow as pa
from pyarrow.lib import StringBuilder, StringViewBuilder


def test_weakref():
    sbuilder = StringBuilder()
    wr = weakref.ref(sbuilder)
    assert wr() is not None
    del sbuilder
    assert wr() is None


@pytest.mark.numpy
def test_string_builder_append_with_nan():
    sbuilder = StringBuilder()
    sbuilder.append(b"a byte string")
    sbuilder.append("a string")
    sbuilder.append(np.nan)
    sbuilder.append(None)
    assert len(sbuilder) == 4
    assert sbuilder.null_count == 2
    arr = sbuilder.finish()
    assert len(sbuilder) == 0
    assert isinstance(arr, pa.Array)
    assert arr.null_count == 2
    assert arr.type == 'str'
    expected = ["a byte string", "a string", None, None]
    assert arr.to_pylist() == expected


def test_string_builder_append():
    sbuilder = StringBuilder()
    sbuilder.append(b"a byte string")
    sbuilder.append("a string")
    sbuilder.append(None)
    assert len(sbuilder) == 3
    assert sbuilder.null_count == 1
    arr = sbuilder.finish()
    assert len(sbuilder) == 0
    assert isinstance(arr, pa.Array)
    assert arr.null_count == 1
    assert arr.type == 'str'
    expected = ["a byte string", "a string", None]
    assert arr.to_pylist() == expected


@pytest.mark.numpy
def test_string_builder_append_values_with_nan():
    sbuilder = StringBuilder()
    sbuilder.append_values([np.nan, None, "text", None, "other text"])
    assert sbuilder.null_count == 3
    arr = sbuilder.finish()
    assert arr.null_count == 3
    expected = [None, None, "text", None, "other text"]
    assert arr.to_pylist() == expected


def test_string_builder_append_values():
    sbuilder = StringBuilder()
    sbuilder.append_values([None, "text", None, "other text"])
    assert sbuilder.null_count == 2
    arr = sbuilder.finish()
    assert arr.null_count == 2
    expected = [None, "text", None, "other text"]
    assert arr.to_pylist() == expected


@pytest.mark.numpy
def test_string_builder_append_after_finish_with_nan():
    sbuilder = StringBuilder()
    sbuilder.append_values([np.nan, None, "text", None, "other text"])
    arr = sbuilder.finish()
    sbuilder.append("No effect")
    expected = [None, None, "text", None, "other text"]
    assert arr.to_pylist() == expected


def test_string_builder_append_after_finish():
    sbuilder = StringBuilder()
    sbuilder.append_values([None, "text", None, "other text"])
    arr = sbuilder.finish()
    sbuilder.append("No effect")
    expected = [None, "text", None, "other text"]
    assert arr.to_pylist() == expected


@pytest.mark.numpy
def test_string_view_builder_with_nan():
    builder = StringViewBuilder()
    builder.append(b"a byte string")
    builder.append("a string")
    builder.append("a longer not-inlined string")
    builder.append(np.nan)
    builder.append_values([None, "text"])
    assert len(builder) == 6
    assert builder.null_count == 2
    arr = builder.finish()
    assert isinstance(arr, pa.Array)
    assert arr.null_count == 2
    assert arr.type == 'string_view'
    expected = [
        "a byte string", "a string", "a longer not-inlined string", None, None, "text"
    ]
    assert arr.to_pylist() == expected


def test_string_view_builder():
    builder = StringViewBuilder()
    builder.append(b"a byte string")
    builder.append("a string")
    builder.append("a longer not-inlined string")
    builder.append_values([None, "text"])
    assert len(builder) == 5
    assert builder.null_count == 1
    arr = builder.finish()
    assert isinstance(arr, pa.Array)
    assert arr.null_count == 1
    assert arr.type == 'string_view'
    expected = [
        "a byte string", "a string", "a longer not-inlined string", None, "text"
    ]
    assert arr.to_pylist() == expected
