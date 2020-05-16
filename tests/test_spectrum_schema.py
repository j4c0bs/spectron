# -*- coding: utf-8 -*-

import json
import pytest

from spectron.spectrum_schema import from_dict


def load_sql(path):
    return path.read_text().strip()


def load_json(path):
    return json.loads(path.read_text())


@pytest.mark.parametrize("filename", ["test_1_line", "nested_reserved"])
def test__from_dict__defaults(filename, datadir):

    input_path = datadir / f"{filename}.json"
    result_path = datadir / f"{filename}.sql"

    d = load_json(input_path)
    assert from_dict(d).strip() == result_path.read_text().strip()
