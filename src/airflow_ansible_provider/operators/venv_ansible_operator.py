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
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple
from airflow.lineage import prepare_lineage
from airflow.models.variable import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils import hashlib_wrapper
from airflow.utils.context import Context

from airflow_ansible_provider.operators.ansible_operator import AnsibleOperator


class VirtualAnsibleOperator(PythonVirtualenvOperator, AnsibleOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _install_galaxy_packages(self):  # type: ignore
        pass

    def _calculate_cache_hash(self) -> Tuple[str, str]:
        """Helper to generate the hash of the cache folder to use.

        The following factors are used as input for the hash:
        - (sorted) list of requirements
        - pip install options
        - flag of system site packages
        - python version
        - Variable to override the hash with a cache key
        - Index URLs

        Returns a hash and the data dict which is the base for the hash as text.
        """
        hash_dict = {
            "requirements_list": self._requirements_list(),
            "pip_install_options": self.pip_install_options,
            "index_urls": self.index_urls,
            "cache_key": str(Variable.get("PythonVirtualenvOperator.cache_key", "")),
            "python_version": self.python_version,
            "system_site_packages": self.system_site_packages,
            "galaxy_collections": self.galaxy_collections,
        }
        hash_text = json.dumps(hash_dict, sort_keys=True)
        hash_object = hashlib_wrapper.md5(hash_text.encode())
        requirements_hash = hash_object.hexdigest()
        return requirements_hash[:8], hash_text

    @prepare_lineage
    def pre_execute(self, context: Context):
        super().pre_execute(context)
        if self.venv_cache_path:
            self._env_dir = self._ensure_venv_cache_exists(Path(self.venv_cache_path))
        else:
            self._tmp_dir = TemporaryDirectory(prefix="venv-")
            self._env_dir = Path(self._tmp_dir.name)
            self._prepare_venv(self._env_dir)
        self._bin_path = self._env_dir / "bin"
        # result = self._execute_python_callable_in_subprocess(python_path)
        # return result
        super()._install_galaxy_packages(
            galaxy_bin=f"{self._bin_path}/ansible-galaxy",
            HOME=self._env_dir,
        )

    def on_kill(self):
        if self._tmp_dir:
            self._tmp_dir.cleanup()
        return super().on_kill()
