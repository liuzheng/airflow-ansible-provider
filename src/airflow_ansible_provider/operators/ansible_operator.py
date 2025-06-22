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
from __future__ import annotations

import base64
import datetime
import json
import os
import sys
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Collection, Iterable, Mapping, Sequence, Tuple, Union

import airflow.models.xcom_arg
import ansible_runner
import boto3
from airflow.exceptions import AirflowException
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models import Connection, Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils import hashlib_wrapper
from airflow.utils.context import Context
from airflow.utils.process_utils import execute_in_subprocess_with_kwargs
from ansible_runner.interface import init_runner
from ansible_runner.runner_config import ExecutionMode
from botocore.config import Config

from airflow_ansible_provider.hooks.ansible import AnsibleHook

ALL_KEYS = {}
ANSIBLE_PRIVATE_DATA_DIR = "/tmp/ansible_runner" or os.environ.get(
    "ANSIBLE_PRIVATE_DATA_DIR"
)

ANSIBLE_EVENT_STATUS = {
    "playbook_on_start": "running",
    "playbook_on_task_start": "running",
    "runner_on_ok": "successful",
    "runner_on_skipped": "skipped",
    "runner_on_failed": "failed",
    "runner_on_unreachable": "unreachable",
    "on_any": "unknown",
}


def ansible_run(**kwargs):
    # fix: because when use binary, the execution_mode will be set RAW, which whill not append the playbook into the command, see also ansible_runner.runner_config.RunnerConfig.generate_ansible_command
    r = init_runner(**kwargs)
    r.config.command.append(kwargs.get("playbook"))
    r.config.execution_mode = ExecutionMode.ANSIBLE_PLAYBOOK
    r.run()
    return r


class AnsibleOperator(PythonVirtualenvOperator):
    """
    Run an Ansible Runner task in the foreground and return a Runner object when complete.

    :param str playbook: The playbook (as a path relative to ``private_data_dir/project``) that will be invoked by runner when executing Ansible.
    :param str playbook_yaml: The playbook
    :param dict or list roles_path: Directory or list of directories to assign to ANSIBLE_ROLES_PATH
    :param str or dict or list inventory: Overrides the inventory directory/file (supplied at ``private_data_dir/inventory``) with
        a specific host or list of hosts. This can take the form of:

            - Path to the inventory file in the ``private_data_dir``
            - Native python dict supporting the YAML/json inventory structure
            - A text INI formatted string
            - A list of inventory sources, or an empty list to disable passing inventory

    :param int forks: Control Ansible parallel concurrency
    :param str artifact_dir: The path to the directory where artifacts should live, this defaults to 'artifacts' under the private data dir
    :param str project_dir: The path to the directory where the project is located, default will use the setting in conn_id
    :param int ansible_timeout: The timeout value in seconds that will be passed to either ``pexpect`` of ``subprocess`` invocation
                    (based on ``runner_mode`` selected) while executing command. It the timeout is triggered it will force cancel the
                    execution.
    :param dict extravars: Extra variables to be passed to Ansible at runtime using ``-e``. Extra vars will also be
                read from ``env/extravars`` in ``private_data_dir``.

    :param str ansible_conn_id: The ansible connection
    :param list kms_keys: The list of KMS keys to be used to decrypt the ansible extra vars
    :param str path: The path to run the playbook under project directory
    :param str git_repo_conn_id: The connection ID for the playbook git repo
    :param str s3_conn_id: The connection ID for the S3 bucket to save the artifacts
    :param dict git_extra: Extra arguments to pass to the git clone command, e.g. {"branch": "prod"} {"tag": "v1.0.0"} {"commit_id": "123456"}
    :param list tags: List of tags to run
    :param list skip_tags: List of tags to skip
    :param bool get_ci_events: Get CI events
    """

    operator_fields: Sequence[str] = (
        "playbook",
        "playbook_yaml",
        "inventory",
        "roles_path",
        "extravars",
        "tags",
        "skip_tags",
        "artifact_dir",
        "project_dir",
        # "git_extra",
        "path",
        "get_ci_events",
        "forks",
        "ansible_timeout",
        "ansible_vars",
    )
    template_fields_renderers = {
        "conn_id": "ansible_default",
        "path": "",
        "inventory": None,
        "artifact_dir": None,
        "project_dir": None,
        "roles_path": None,
        "extravars": None,
        "tags": None,
        "skip_tags": None,
        "get_ci_events": False,
        "forks": 10,
        "ansible_timeout": None,
        # "git_extra": None,
        "galaxy_collections": None,
    }
    ui_color = "#FFEFEB"
    ui_fgcolor = "#FF0000"

    def __init__(
        self,
        *,
        playbook: str = "",
        playbook_yaml: str = "",
        git_repo_conn_id: str = "ansible_default",
        s3_conn_id: str = "",
        path: str = "",
        inventory: Union[dict, str, list, None] = None,
        artifact_dir: str | None = None,
        project_dir: str | None = None,
        roles_path: Union[dict, list] = None,
        extravars: Union[dict, None] = None,
        tags: Union[list, None] = None,
        skip_tags: Union[list, None] = None,
        get_ci_events: bool = False,
        forks: int = 10,
        ansible_timeout: Union[int, None] = None,
        git_extra: Union[dict, None] = None,
        ansible_vars: dict = None,
        ansible_envvars: dict = None,
        become_user: str = None,
        become_method: str = None,
        become_password: str = None,
        become_exe: str = None,
        become_flags: str = None,
        requirements: None | Iterable[str] | str = None,
        venv_cache_path: None | os.PathLike[str] = None,
        galaxy_collections: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            requirements=requirements,
            system_site_packages=True,  # todo: 当前有些问题，只能true
            venv_cache_path=venv_cache_path,
            pip_install_options=[
                "--ignore-installed",
                "--force-reinstall",
                "--disable-pip-version-check",
                "--no-color",
            ],
            **kwargs,
        )
        self.playbook = playbook
        self.playbook_yaml = playbook_yaml
        self.path = path
        self.inventory = inventory
        self.s3_conn_id = s3_conn_id
        self.roles_path = roles_path
        self.extravars = extravars or {}
        self.tags = tags
        self.skip_tags = skip_tags
        self.get_ci_events = get_ci_events
        self.forks = forks
        self.ansible_timeout = ansible_timeout
        self.git_extra = git_extra
        self.ansible_vars = ansible_vars
        self.ansible_envvars = ansible_envvars or {}
        self.become_user = become_user
        self.become_method = become_method
        self.become_password = become_password
        self.become_exe = become_exe
        self.become_flags = become_flags

        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.galaxy_collections = galaxy_collections

        self.ci_events = {}
        self.last_event = {}
        self._runner_ident = None
        self._context = None
        self._tmp_dir = None
        self._env_dir = None
        self._bin_path = None
        self._collections_paths = []
        self.log.debug("playbook: %s", self.playbook)
        self.log.debug("playbook type: %s", type(self.playbook))

        self._ansible_hook = AnsibleHook(conn_id=git_repo_conn_id)
        self.extravars["ansible_user"] = self._ansible_hook.username
        self.extravars["ansible_port"] = self._ansible_hook.port
        self.extravars["ansible_connection"] = "ssh"
        self.project_dir = project_dir or self._ansible_hook.ansible_playbook_directory
        self.artifact_dir = (
            artifact_dir or self._ansible_hook.ansible_artifact_directory
        )
        if self.playbook_yaml:
            self._tmp_playbook = TemporaryDirectory(prefix="temp-playbook-")

    def event_handler(self, data):
        """event handler"""
        if self.get_ci_events and data.get("event_data", {}).get("host"):
            self.ci_events[data["event_data"]["host"]] = data
        self.last_event = data
        self.log.info("event: %s", self.last_event)
        if not self._runner_ident and data.get("runner_ident"):
            # 执行过程中先获取到 runner_ident，便于日志即时观察输出
            self._context["ti"].xcom_push(
                key="runner_id", value=data.get("runner_ident")
            )
            self._runner_ident = data.get("runner_ident")

    def _calculate_cache_hash(self) -> Tuple[str, str]:
        """
        Calculate a cache hash based on the cache key and galaxy collections.
        This method generates a hash value and its corresponding hash text
        by combining the cache key retrieved from an Airflow Variable and
        the galaxy collections associated with the operator. The hash is
        computed using an MD5 algorithm.
        Returns:
            Tuple[str, str]: A tuple containing:
                - The first 8 characters of the MD5 hash (requirements_hash).
                - The JSON string representation of the hash dictionary (hash_text).
        """

        hash_dict = {
            "cache_key": str(Variable.get("AnsibleOperator.cache_key", "")),
            "galaxy_collections": self.galaxy_collections,
        }
        hash_text = json.dumps(hash_dict, sort_keys=True)
        hash_object = hashlib_wrapper.md5(hash_text.encode())
        requirements_hash = hash_object.hexdigest()
        return requirements_hash[:8], hash_text

    def _install_galaxy_packages(self):
        if self.venv_cache_path:
            self._env_dir = self._ensure_venv_cache_exists(Path(self.venv_cache_path))
        else:
            self._tmp_dir = TemporaryDirectory(prefix="venv-")
            self._env_dir = Path(self._tmp_dir.name)
            self._prepare_venv(self._env_dir)
        self._bin_path = self._env_dir / "bin"
        if self.galaxy_collections:
            ansible_galaxy_binary = self._bin_path / "ansible-galaxy"
            if not (
                ansible_galaxy_binary.exists()
                and ansible_galaxy_binary.is_file()
                and os.access(ansible_galaxy_binary, os.X_OK)
            ):
                ansible_galaxy_binary = "/home/airflow/.local/bin/ansible-galaxy"
            for galaxy_pkg in self.galaxy_collections or []:
                execute_in_subprocess_with_kwargs(
                    cmd=[
                        str(ansible_galaxy_binary),
                        "collection",
                        "install",
                        f"{galaxy_pkg}",
                        "--collections-path",
                        str(self._env_dir / ".ansible" / "collections"),
                    ],
                    env=(
                        {
                            "HTTPS_PROXY": Variable.get("ANSIBLE_GALAXY_PROXY", ""),
                            "PYTHONPATH": ":".join(sys.path),
                            "HOME": self._env_dir,
                        }
                        if self._env_dir
                        else None
                    ),
                )
            self._collections_paths.append(
                str(self._env_dir / ".ansible" / "collections")
            )

    @prepare_lineage
    def pre_execute(self, context: Context):
        if isinstance(self.ansible_vars, airflow.models.xcom_arg.PlainXComArg):
            self.ansible_vars = self.ansible_vars.resolve(context)
        if self.ansible_vars:
            for k in self.operator_fields:
                if k not in self.op_kwargs and k in self.ansible_vars:
                    setattr(self, k, self.ansible_vars.get(k))
        for attr in self.operator_fields:
            value = getattr(self, attr)
            if isinstance(value, airflow.models.xcom_arg.PlainXComArg):
                setattr(self, attr, value.resolve(context))

        # for t in self.kms_keys or []:
        #     pwdKey, pwdValue = get_secret(token=t)
        #     if pwdKey and pwdKey not in self.extravars:
        #         self.extravars[pwdKey] = pwdValue
        self.log.info(
            "project_dir: %s, project path: %s, playbook: %s",
            self.project_dir,
            self.path,
            self.playbook,
        )
        if self._tmp_playbook:
            self.project_dir = self._tmp_playbook.name
            self.playbook = os.path.join(self.project_dir, "playbook.yml")
            with open(self.playbook, "w", encoding="utf-8") as f:
                playbook_data = base64.b64decode(self.playbook_yaml).decode("utf-8")
                f.write(playbook_data)
        else:
            self.log.info(
                "project_dir: %s, project path: %s, playbook: %s",
                self.project_dir,
                self.path,
                self.playbook,
            )
            if self.project_dir == "":
                self.log.critical("project_dir is empty")
                raise AirflowException("project_dir is empty")
            if not os.path.exists(self.project_dir):
                self.log.critical("project_dir is not exist")
                raise AirflowException("project_dir is not exist")
            if not os.path.exists(self.artifact_dir):
                os.makedirs(self.artifact_dir)

        # 处理 ansible inventory数据
        if isinstance(
            self.inventory, dict
        ):  # todo: 暂时仅兼容dict类型的inventory,自定义的inventory不支持 ansible_ssh_common_args
            for group_name, group_data in self.inventory.items():
                if not isinstance(group_data, dict):
                    continue
                if group_name == "_meta":
                    # 处理meta类变量
                    for host_key, host_vars in group_data.get("hostvars", {}).items():
                        # 默认不允许用户传递"ansible_ssh_common_args"参数
                        if "ansible_ssh_common_args" in host_vars:
                            del self.inventory[group_name]["hostvars"][host_key][
                                "ansible_ssh_common_args"
                            ]
                        # 仅当主机变量中存在特殊 idc 时配置ansible_ssh_common_args参数
                        if "idc" in host_vars and Variable.get(
                            "SSH_COMMON_ARGS-" + host_vars["idc"], default_var=None
                        ):
                            self.inventory[group_name]["hostvars"][host_key][
                                "ansible_ssh_common_args"
                            ] = Variable.get(
                                "SSH_COMMON_ARGS-" + host_vars["idc"], default_var=None
                            )
                    continue
                # 处理主机配置
                if "hosts" in group_data:
                    for host_key, host_vars in group_data.get("hosts", {}).items():
                        # 默认不允许用户传递"ansible_ssh_common_args"参数
                        if "ansible_ssh_common_args" in host_vars:
                            del self.inventory[group_name]["hosts"][host_key][
                                "ansible_ssh_common_args"
                            ]
                        # 仅当主机变量中存在特殊 idc 时配置ansible_ssh_common_args参数
                        if "idc" in host_vars and Variable.get(
                            "SSH_COMMON_ARGS-" + host_vars["idc"], default_var=None
                        ):
                            self.inventory[group_name]["hosts"][host_key][
                                "ansible_ssh_common_args"
                            ] = Variable.get(
                                "SSH_COMMON_ARGS-" + host_vars["idc"], default_var=None
                            )
                # 处理组变量配置
                if "vars" in group_data:
                    # 默认不允许用户传递"ansible_ssh_common_args"参数
                    if "ansible_ssh_common_args" in group_data.get("vars", {}):
                        del self.inventory[group_name]["vars"][
                            "ansible_ssh_common_args"
                        ]
                    # 仅当主机变量中存在特殊 idc 时配置ansible_ssh_common_args参数
                    if "idc" in group_data["vars"] and Variable.get(
                        "SSH_COMMON_ARGS-" + group_data["vars"]["idc"], default_var=None
                    ):
                        self.inventory[group_name]["vars"][
                            "ansible_ssh_common_args"
                        ] = Variable.get(
                            "SSH_COMMON_ARGS-" + group_data["vars"]["idc"],
                            default_var=None,
                        )
            # inventory全局变量
            self.inventory["all"] = {
                "vars": Variable.get("ANSIBLE_DEFAULT_VARS", default_var={}),
            }
            if self.become_user is not None:
                self.inventory["all"]["vars"]["ansible_become"] = True
                self.inventory["all"]["vars"]["ansible_become_user"] = self.become_user
                if self.become_method is not None:
                    self.inventory["all"]["vars"][
                        "ansible_become_method"
                    ] = self.become_method
                if self.become_password is not None:
                    self.inventory["all"]["vars"][
                        "ansible_become_password"
                    ] = self.become_password
                if self.become_exe is not None:
                    self.inventory["all"]["vars"][
                        "ansible_become_exe"
                    ] = self.become_exe
                if self.become_flags is not None:
                    self.inventory["all"]["vars"][
                        "ansible_become_flags"
                    ] = self.become_flags
        # tip: this will default inventory was a str for path, cannot pass it as ini
        if isinstance(self.inventory, str):
            self.inventory = os.path.join(self.project_dir, self.path, self.inventory)
        # 处理 galaxy_collections
        if self.galaxy_collections is not None:
            self._install_galaxy_packages()

    def execute(self, context: Context):
        self._context = context
        self.log.info(
            "playbook: %s, roles_path: %s, project_dir: %s, inventory: %s, project_dir: %s, extravars: %s, tags: %s, "
            "skip_tags: %s",
            self.playbook,
            self.roles_path,
            self.project_dir,
            self.inventory,
            self.project_dir,
            self.extravars,
            self.tags,
            self.skip_tags,
        )
        ansible_binary = "/home/airflow/.local/bin/ansible-playbook"
        if self._bin_path is not None:
            ansible_binary = self._bin_path / "ansible-playbook"
            if not (
                ansible_binary.exists()
                and ansible_binary.is_file()
                and os.access(ansible_binary, os.X_OK)
            ):
                ansible_binary = "/home/airflow/.local/bin/ansible-playbook"
        # r = ansible_run(
        r = ansible_runner.run(
            binary=ansible_binary,
            cmdline=self.playbook,  # fix: ansible_runner.run ExecutionMode.RAW for binary is set
            envvars={"ANSIBLE_COLLECTIONS_PATH": ":".join(self._collections_paths)},
            ssh_key=self._ansible_hook.pkey,
            passwords=[self._ansible_hook.password],
            quiet=True,
            roles_path=self.roles_path,
            tags=",".join(self.tags) if self.tags else None,
            skip_tags=",".join(self.skip_tags) if self.skip_tags else None,
            artifact_dir=self.artifact_dir,
            project_dir=os.path.join(self.project_dir, self.path),
            playbook=self.playbook,
            extravars=self.extravars,
            forks=self.forks,
            timeout=self.ansible_timeout,
            inventory=self.inventory,
            event_handler=self.event_handler,
            # status_handler=my_status_handler, # Disable printing to prevent sensitive information leakage, also unnecessary
            # artifacts_handler=my_artifacts_handler, # No need to print
            # cancel_callback=my_cancel_callback,
            # finished_callback=finish_callback,  # No need to print
        )
        self.log.info(
            "status: %s, artifact_dir: %s, command: %s, inventory: %s, playbook: %s, private_data_dir: %s, "
            "project_dir: %s, ci_events: %s",
            r.status,
            r.config.artifact_dir,
            r.config.command,
            r.config.inventory,
            r.config.playbook,
            r.config.private_data_dir,
            r.config.project_dir,
            self.ci_events,
        )
        context["ansible_return"] = {
            "canceled": r.canceled,
            "directory_isolation_cleanup": r.directory_isolation_cleanup,
            "directory_isolation_path": r.directory_isolation_path,
            "errored": r.errored,
            "last_stdout_update": r.last_stdout_update,
            "process_isolation": r.process_isolation,
            "process_isolation_path_actual": r.process_isolation_path_actual,
            "rc": r.rc,
            "remove_partials": r.remove_partials,
            "runner_mode": r.runner_mode,
            "stats": r.stats,
            "status": r.status,
            "timed_out": r.timed_out,
            # config
            "artifact_dir": r.config.artifact_dir,
            "command": r.config.command,
            "cwd": r.config.cwd,
            "fact_cache": r.config.fact_cache,
            "fact_cache_type": r.config.fact_cache_type,
            "ident": r.config.ident,
            "inventory": r.config.inventory,
            "playbook": r.config.playbook,
            "private_data_dir": r.config.private_data_dir,
            "project_dir": r.config.project_dir,
            # event
            "last_event": self.last_event,
            "ci_events": self.ci_events,
        }
        try:
            self.save_on_s3(context)
            self.log.info("Saved on s3: %s", context.get("s3_path_url"))
        except Exception as e:
            self.log.warning("Failed to save on s3, Error: %s", e)
        return context["ansible_return"]

    def save_on_s3(self, context):
        # make sure zip dir exists
        zip_dir = os.path.join(
            self.artifact_dir,
            datetime.datetime.now().strftime("%Y-%m-%d"),
            context["run_id"],
        )
        if not os.path.exists(zip_dir):
            os.makedirs(zip_dir)
        # Zip the artifact_path
        params_file_content = context["dag_run"].conf
        params_file_path = os.path.join(
            self.artifact_dir, f"{context['ansible_return']['ident']}", "params.json"
        )
        ansible_return_path = os.path.join(
            self.artifact_dir,
            f"{context['ansible_return']['ident']}",
            "ansible_return.json",
        )
        # 将参数写入文件
        with open(params_file_path, "w", encoding="utf-8") as f:
            json.dump(params_file_content, f, indent=4)
        with open(ansible_return_path, "w", encoding="utf-8") as f:
            json.dump(context["ansible_return"], f, indent=4)

        ansible_inventory_file = context["ansible_return"]["inventory"]
        ansible_stdout_file = os.path.join(
            self.artifact_dir, f"{context['ansible_return']['ident']}", "stdout"
        )
        ansible_stderr_file = os.path.join(
            self.artifact_dir, f"{context['ansible_return']['ident']}", "stderr"
        )
        ansible_rc_file = os.path.join(
            self.artifact_dir, f"{context['ansible_return']['ident']}", "rc"
        )
        ansible_status_file = os.path.join(
            self.artifact_dir, f"{context['ansible_return']['ident']}", "status"
        )
        # ansible_command_file = os.path.join(
        #     self.artifact_dir, f"{context['ansible_return']['ident']}", "command"
        # ) # 由于其存在敏感信息以及无必要长期存储的需求，暂时不存储

        zip_file = os.path.join(
            zip_dir, f"ansible-{context['ansible_return']['ident']}.zip"
        )

        with zipfile.ZipFile(zip_file, "w") as z:
            z.write(params_file_path, arcname=os.path.basename(params_file_path))
            z.write(ansible_return_path, arcname=os.path.basename(ansible_return_path))
            z.write(
                ansible_inventory_file, arcname=os.path.basename(ansible_inventory_file)
            )
            z.write(ansible_stdout_file, arcname=os.path.basename(ansible_stdout_file))
            z.write(ansible_stderr_file, arcname=os.path.basename(ansible_stderr_file))
            z.write(ansible_rc_file, arcname=os.path.basename(ansible_rc_file))
            z.write(ansible_status_file, arcname=os.path.basename(ansible_status_file))
            # z.write(ansible_command_file, arcname=os.path.basename(ansible_command_file))

        self.log.info("Zipped artifact path: %s", zip_file)
        # Upload the zip file to s3
        if self.s3_conn_id is None or self.s3_conn_id == "":
            raise AirflowException("s3_conn_id is not set, skip saving on s3")
        c = Connection.get_connection_from_secrets(conn_id=self.s3_conn_id)
        extra = json.loads(c.extra)
        s3_url = extra.get("url")
        s3 = boto3.client(
            "s3",
            aws_access_key_id=c.login,
            aws_secret_access_key=c.password,
            endpoint_url=c.host,
            config=Config(
                s3={"addressing_style": extra.get("addressing_style", "path")}
            ),  # idc: path, oss or aws: virtual
            verify=False,
        )
        zip_key = os.path.relpath(zip_file, self.artifact_dir)
        with open(zip_file, "rb") as f:
            s3.upload_fileobj(f, extra.get("bucket_name"), zip_key)
        context["s3_path_url"] = f"{s3_url}/{zip_key}"
        context["ti"].xcom_push(key="s3_path_url", value=context["s3_path_url"])
        self.log.info("Uploaded artifact to s3: %s", context["s3_path_url"])

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        self.log.debug("post_execute context: %s", context)
        # Discuss whether to compress the results and transfer them to storage
        return

    def on_kill(self) -> None:
        """
        Override this method to clean up subprocesses when a task instance gets killed.

        Any use of the threading, subprocess or multiprocessing module within an
        operator needs to be cleaned up, or it will leave ghost processes behind.
        """
        if self._tmp_playbook:
            self._tmp_playbook.cleanup()
