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

import json
import os
from typing import Any, Collection, Mapping, Sequence, Union

import airflow.models.xcom_arg
from ansible_runner.interface import init_runner
from ansible_runner.runner_config import ExecutionMode
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models.baseoperator import BaseOperator
from airflow.utils.process_utils import execute_in_subprocess_with_kwargs
from airflow.utils.context import Context
from airflow.exceptions import AirflowException

# from airflow_ansible_provider.utils.sync_git_repo import sync_repo
# from airflow_ansible_provider.utils.kms import get_secret
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
ANSIBLE_DEFAULT_VARS = {}


def ansible_run(**kwargs):
    # fix: because when use binary, the execution_mode will be set RAW, which whill not append the playbook into the command, see also ansible_runner.runner_config.RunnerConfig.generate_ansible_command
    r = init_runner(**kwargs)
    r.config.command.append(kwargs.get("playbook"))
    r.config.execution_mode = ExecutionMode.ANSIBLE_PLAYBOOK
    r.run()
    return r


class AnsibleOperator(BaseOperator):
    """
    Run an Ansible Runner task in the foreground and return a Runner object when complete.

    :param str playbook: The playbook (as a path relative to ``private_data_dir/project``) that will be invoked by runner when executing Ansible.
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
    :param int timeout: The timeout value in seconds that will be passed to either ``pexpect`` of ``subprocess`` invocation
                    (based on ``runner_mode`` selected) while executing command. It the timeout is triggered it will force cancel the
                    execution.
    :param dict extravars: Extra variables to be passed to Ansible at runtime using ``-e``. Extra vars will also be
                read from ``env/extravars`` in ``private_data_dir``.

    :param str ansible_conn_id: The ansible connection
    :param list kms_keys: The list of KMS keys to be used to decrypt the ansible extra vars
    :param str path: The path to run the playbook under project directory
    :param str conn_id: The connection ID for the playbook git repo
    :param dict git_extra: Extra arguments to pass to the git clone command, e.g. {"branch": "prod"} {"tag": "v1.0.0"} {"commit_id": "123456"}
    :param list tags: List of tags to run
    :param list skip_tags: List of tags to skip
    :param bool get_ci_events: Get CI events
    """

    operator_fields: Sequence[str] = (
        "playbook",
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
        # "kms_keys": None,
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
        conn_id: str = "ansible_default",
        # kms_keys: Union[list, None] = None,
        path: str = "",
        inventory: Union[dict, str, list, None] = None,
        # conn_id: str = ANSIBLE_PLYBOOK_PROJECT,
        artifact_dir: str | None = None,
        project_dir: str | None = None,
        roles_path: Union[dict, list] = None,
        extravars: Union[dict, None] = None,
        tags: Union[list, None] = None,
        skip_tags: Union[list, None] = None,
        get_ci_events: bool = False,
        forks: int = 10,
        ansible_timeout: Union[int, None] = None,
        # git_extra: Union[dict, None] = None,
        ansible_vars: dict = None,
        ansible_envvars: dict = None,
        galaxy_collections: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.playbook = playbook
        # self.kms_keys = kms_keys
        self.path = path
        self.inventory = inventory
        # self.conn_id = conn_id
        self.roles_path = roles_path
        self.extravars = extravars or {}
        self.tags = tags
        self.skip_tags = skip_tags
        self.get_ci_events = get_ci_events
        self.forks = forks
        self.ansible_timeout = ansible_timeout
        # self.git_extra = git_extra
        self.ansible_vars = ansible_vars
        self.ansible_envvars = ansible_envvars or {}
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.galaxy_collections = galaxy_collections

        self.ci_events = {}
        self.last_event = {}
        self.log.debug("playbook: %s", self.playbook)
        self.log.debug("playbook type: %s", type(self.playbook))

        self._ansible_hook = AnsibleHook(conn_id=conn_id)
        self.extravars["ansible_user"] = self._ansible_hook.username
        self.extravars["ansible_port"] = self._ansible_hook.port
        self.extravars["ansible_connection"] = "ssh"
        self.project_dir = project_dir or self._ansible_hook.ansible_playbook_directory
        self.artifact_dir = (
            artifact_dir or self._ansible_hook.ansible_artifact_directory
        )
        self._tmp_dir = None
        self._env_dir = None
        self._bin_path = None

        # todo: add the timeouts

    def event_handler(self, data):
        """event handler"""
        if self.get_ci_events and data.get("event_data", {}).get("host"):
            self.ci_events[data["event_data"]["host"]] = data
        self.last_event = data
        self.log.info("event: %s", self.last_event)

    # def get_key(self, kms_key: None) -> Optional[dict]:
    #     """get ssh key"""
    #     global ALL_KEYS  # pylint: disable=global-variable-not-assigned
    #     if kms_key in ALL_KEYS:
    #         return ALL_KEYS[kms_key]
    #     if kms_key is None:
    #         return None
    #     _, pwdValue = get_secret(token=kms_key)
    #     if pwdValue is None:
    #         return None
    #     try:
    #         ALL_KEYS[kms_key] = base64.b64decode(pwdValue).decode("utf-8")
    #         return ALL_KEYS[kms_key]
    #     except Exception:  # pylint: disable=broad-except
    #         return None

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
        for k, v in ANSIBLE_DEFAULT_VARS.items():
            if k not in self.extravars:
                self.extravars[k] = v
        # self.log.debug("conn_id: %s", self.conn_id)
        # self.log.debug("git_extra: %s", self.git_extra)
        # self.project_dir = sync_repo(conn_id=self.conn_id, extra=self.git_extra)
        self.log.info(
            "project_dir: %s, project path: %s, playbook: %s",
            self.project_dir,
            self.path,
            self.playbook,
        )
        # if self.project_dir == "":
        #     self.log.critical("project_dir is empty")
        #     raise AirflowException("project_dir is empty")
        # if not os.path.exists(self.project_dir):
        #     self.log.critical("project_dir is not exist")
        #     raise AirflowException("project_dir is not exist")
        if not os.path.exists(self.artifact_dir):
            os.makedirs(self.artifact_dir)

        # tip: this will default inventory was a str for path, cannot pass it as ini
        if isinstance(self.inventory, str):
            self.inventory = os.path.join(
                self.project_dir, self.path, self.inventory)
        self._install_galaxy_packages()

    def _install_galaxy_packages(
        self, galaxy_bin: str = "ansible-galaxy", HOME: str = None
    ):
        for galaxy_pkg in self.galaxy_collections or []:
            execute_in_subprocess_with_kwargs(
                cmd=[
                    galaxy_bin,
                    "collection",
                    "install",
                    f"{galaxy_pkg}",
                ],
                env={"HOME": HOME} if HOME else None,
            )

    def execute(self, context: Context):
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
        binary = "ansible-playbook"
        if self._bin_path is not None:
            binary = f"{self._bin_path}/ansible-playbook"
        r = ansible_run(
            binary=binary,
            cmdline=self.playbook,  # fix: ansible_runner.run ExecutionMode.RAW for binary is set
            envvars=self.ansible_envvars,
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
        context["ti"].xcom_push(key="runner_id", value=r.config.ident)
        if r.status == "successful":
            return context["ansible_return"]
        raise AirflowException(f"Ansible run playbook failed: {r.status}")

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        self.log.debug("post_execute context: %s", context)
        # Discuss whether to compress the results and transfer them to storage
        return
        artifact_path = os.path.join(self.artifact_dir, result["ident"])
        artifact_result_file = os.path.join(artifact_path, "result.txt")
        with open(artifact_result_file, "w", encoding="utf-8") as f:
            f.write(json.dumps(result, indent=4))
        # Zip the artifact_path
        zip_file = os.path.join(
            self.artifact_dir,
            context["start_date"].strftime("%Y-%m-%d"),
            f"{context['run_id']}.zip",
        )
        os.system(f"zip -r {zip_file} {artifact_path}")
        self.log.info("Zipped artifact path: %s", zip_file)
        # todo: upload to some storage

    def on_kill(self) -> None:
        """
        Override this method to clean up subprocesses when a task instance gets killed.

        Any use of the threading, subprocess or multiprocessing module within an
        operator needs to be cleaned up, or it will leave ghost processes behind.
        """
