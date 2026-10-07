"""pytest 共享夹具。"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app.storage import Artifact, Fragment, Store


@pytest.fixture()
def store(tmp_path):
    return Store(str(tmp_path / "data"))


@pytest.fixture()
def sample_artifacts():
    return [
        Artifact("全景", [Fragment("卫星过境-"), Fragment("多光谱扫描"),
                          Fragment("·晴空")]),
        Artifact("局部", [Fragment("·晴空"), Fragment("多光谱扫描"),
                          Fragment("雷达回波")]),
        Artifact("附注", [Fragment("卫星过境-"), Fragment("雷达回波")]),
    ]
