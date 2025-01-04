#
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
"""Hook for SSH connections."""

from __future__ import annotations

import os

# from base64 import decodebytes
import warnings
from collections.abc import Sequence
from functools import cached_property
from io import StringIO
from typing import Any

import paramiko
from paramiko.config import SSH_PORT
from sshtunnel import SSHTunnelForwarder
from tenacity import Retrying, stop_after_attempt, wait_fixed, wait_random

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.platform import getuser
from airflow.utils.types import NOTSET, ArgNotSet
from airflow.utils.log.secrets_masker import mask_secret

TIMEOUT_DEFAULT = 10
CMD_TIMEOUT = 10
CONNECTION_TIMEOUT = 10
KEEPALIVE_INTERVAL = 30
BANNER_TIMEOUT = 30.0
AUTH_TIMEOUT = 120

ANSIBLE_ARTIFACT_DIR = "/tmp/ansible/"
ANSIBLE_PLYBOOK_DIR = "/opt/airflow/ansible_playbook/"


class AnsibleHook(BaseHook):
    """
    This hook also lets you create ssh tunnel and serve as basis for SFTP file transfer.

    :param conn_id: :ref:`ssh connection id<howto/connection:ssh>` from airflow
        Connections from where all the required parameters can be fetched like
        username, password or key_file, though priority is given to the
        params passed during init.
    :param remote_host: remote host to connect
    :param username: username to connect to the remote_host
    :param password: password of the username to connect to the remote_host
    :param key_file: path to key file to use to connect to the remote_host
    :param port: port of remote host to connect (Default is paramiko SSH_PORT)
    :param conn_timeout: timeout (in seconds) for the attempt to connect to the remote_host.
        The default is 10 seconds. If provided, it will replace the `conn_timeout` which was
        predefined in the connection of `ssh_conn_id`.
    :param cmd_timeout: timeout (in seconds) for executing the command. The default is 10 seconds.
        Nullable, `None` means no timeout. If provided, it will replace the `cmd_timeout`
        which was predefined in the connection of `ssh_conn_id`.
    :param keepalive_interval: send a keepalive packet to remote host every
        keepalive_interval seconds
    :param banner_timeout: timeout to wait for banner from the server in seconds
    :param disabled_algorithms: dictionary mapping algorithm type to an
        iterable of algorithm identifiers, which will be disabled for the
        lifetime of the transport
    :param ciphers: list of ciphers to use in order of preference
    :param auth_timeout: timeout (in seconds) for the attempt to authenticate with the remote_host
    """

    # List of classes to try loading private keys as, ordered (roughly) by most common to least common
    _pkey_loaders: Sequence[type[paramiko.PKey]] = (
        paramiko.RSAKey,
        paramiko.ECDSAKey,
        paramiko.Ed25519Key,
        paramiko.DSSKey,
    )

    _host_key_mappings = {
        "rsa": paramiko.RSAKey,
        "dss": paramiko.DSSKey,
        "ecdsa": paramiko.ECDSAKey,
        "ed25519": paramiko.Ed25519Key,
    }

    conn_name_attr = "ansible_conn_id"
    default_conn_name = "ansible_default"
    conn_type = "ansible"
    hook_name = "Ansible"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for SSH connection."""
        return {
            "hidden_fields": [
                "schema",
                "port",
                "extra",
            ],
            "relabeling": {
                "login": "Username",
            },
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import (
            BS3TextFieldWidget,
            BS3PasswordFieldWidget,
            BS3TextAreaFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import StringField, IntegerField
        from wtforms.validators import NumberRange

        return {
            "ansible_playbook_directory": StringField(
                name=lazy_gettext("Ansible Playbook Directory"),
                widget=BS3TextFieldWidget(),
            ),
            "ansible_artifact_directory": StringField(
                name=lazy_gettext("Ansible Artifact Directory"),
                widget=BS3TextFieldWidget(),
            ),
            "port": IntegerField(
                name=lazy_gettext("Port"),
                default=SSH_PORT,
                description=lazy_gettext("The SSH port to connect to"),
                validators=[NumberRange(min=1, max=65535)],
            ),
            "private_key": StringField(
                name=lazy_gettext("Private Key Data"),
                widget=BS3TextAreaFieldWidget(),
            ),
            "private_key_passphrase": StringField(
                name=lazy_gettext("Private Key Passphrase"),
                widget=BS3PasswordFieldWidget(),
            ),
            "conn_timeout": IntegerField(
                name=lazy_gettext("Connection Timeout (s)"),
                validators=[NumberRange(min=0, max=3600)],
                default=TIMEOUT_DEFAULT,
            ),
            "cmd_timeout": IntegerField(
                name=lazy_gettext("Command Timeout (s)"),
                validators=[NumberRange(min=0, max=3600)],
                default=CMD_TIMEOUT,
            ),
            "keepalive_interval": IntegerField(
                name=lazy_gettext("Keepalive Interval (s)"),
                validators=[NumberRange(min=0, max=3600)],
                default=KEEPALIVE_INTERVAL,
            ),
            "banner_timeout": IntegerField(
                name=lazy_gettext("Banner Timeout (s)"),
                validators=[NumberRange(min=0, max=3600)],
                default=BANNER_TIMEOUT,
            ),
            "auth_timeout": IntegerField(
                name=lazy_gettext("Authentication Timeout (s)"),
                validators=[NumberRange(min=0, max=3600)],
                default=AUTH_TIMEOUT,
            ),
            "host_proxy_cmd": StringField(
                name=lazy_gettext("Host Proxy Command"),
                widget=BS3TextFieldWidget(),
            ),
            "ciphers": StringField(
                name=lazy_gettext("Ciphers"),
                widget=BS3TextFieldWidget(),
            ),
            "disabled_algorithms": StringField(
                name=lazy_gettext("Disabled Algorithms"),
                widget=BS3TextAreaFieldWidget(),
            ),
        }

    def __init__(
        self,
        conn_id: str = "ansible_default",
        remote_host: str = "",
        username: str | None = None,
        password: str | None = None,
        private_key: str | None = None,
        private_key_passphrase: str | None = None,
        port: int = SSH_PORT,
        conn_timeout: int = CONNECTION_TIMEOUT,
        cmd_timeout: int = CMD_TIMEOUT,
        keepalive_interval: int = KEEPALIVE_INTERVAL,
        banner_timeout: float = BANNER_TIMEOUT,
        disabled_algorithms: dict | None = None,
        ciphers: list[str] | None = None,
        auth_timeout: int = AUTH_TIMEOUT,
        host_proxy_cmd: str | None = None,
        ansible_playbook_directory: str | None = None,
        ansible_artifact_directory: str | None = None,
    ) -> None:
        super().__init__()
        self.ssh_conn_id = conn_id
        self.remote_host = remote_host
        self.username = username
        self.password = password
        self.private_key = private_key
        self.private_key_passphrase = private_key_passphrase
        self.pkey = None
        self.port = port
        self.conn_timeout = conn_timeout
        self.cmd_timeout = cmd_timeout
        self.keepalive_interval = keepalive_interval
        self.banner_timeout = banner_timeout
        self.disabled_algorithms = disabled_algorithms
        self.ciphers = ciphers
        self.host_proxy_cmd = host_proxy_cmd
        self.auth_timeout = auth_timeout
        self.ansible_playbook_directory = (
            ansible_playbook_directory or ANSIBLE_PLYBOOK_DIR
        )
        self.ansible_artifact_directory = (
            ansible_artifact_directory or ANSIBLE_ARTIFACT_DIR
        )

        # Default values, overridable from Connection
        self.compress = True
        self.no_host_key_check = True
        self.allow_host_key_change = False
        self.host_key = None
        self.look_for_keys = True

        # Placeholder for future cached connection
        self.client: paramiko.SSHClient | None = None

        # Use connection to override defaults
        if self.ssh_conn_id is not None:
            conn = self.get_connection(self.ssh_conn_id)
            if conn.login is not None:
                self.username = conn.login
            if conn.password is not None:
                self.password = conn.password
            if conn.host is not None:
                self.remote_host = conn.host

            if conn.extra is not None:
                extra_options = conn.extra_dejson
                for field in (
                    "port",
                    "private_key",
                    "private_key_passphrase",
                    "conn_timeout",
                    "cmd_timeout",
                    "keepalive_interval",
                    "banner_timeout",
                    "auth_timeout",
                    "host_proxy_cmd",
                    "ansible_playbook_directory",
                    "ansible_artifact_directory",
                ):
                    if getattr(self, field) is not None:
                        setattr(self, field, extra_options.get(field))
                if self.private_key:
                    self.pkey = self._pkey_from_private_key(
                        self.private_key, passphrase=self.private_key_passphrase
                    )

                # host_key = extra_options.get("host_key")
                # no_host_key_check = extra_options.get("no_host_key_check")

                # if no_host_key_check is not None:
                #     no_host_key_check = str(no_host_key_check).lower() == "true"
                #     if host_key is not None and no_host_key_check:
                #         raise ValueError("Must check host key when provided")

                #     self.no_host_key_check = no_host_key_check

                # if (
                #     "allow_host_key_change" in extra_options
                #     and str(extra_options["allow_host_key_change"]).lower() == "true"
                # ):
                #     self.allow_host_key_change = True

                # if (
                #     "look_for_keys" in extra_options
                #     and str(extra_options["look_for_keys"]).lower() == "false"
                # ):
                #     self.look_for_keys = False

                if "disabled_algorithms" in extra_options:
                    self.disabled_algorithms = extra_options.get("disabled_algorithms")

                if "ciphers" in extra_options:
                    self.ciphers = extra_options.get("ciphers")
                # todo: host key and other ssh arguments setting
                # if host_key is not None:
                #     if host_key.startswith("ssh-"):
                #         key_type, host_key = host_key.split(None)[:2]
                #         key_constructor = self._host_key_mappings[key_type[4:]]
                #     else:
                #         key_constructor = paramiko.RSAKey
                #     decoded_host_key = decodebytes(host_key.encode("utf-8"))
                #     self.host_key = key_constructor(data=decoded_host_key)
                #     self.no_host_key_check = False

        if not self.remote_host:
            warnings.warn(
                "remote_host is not provided. This is required for SSH Hook to run a test connect."
            )

        # Auto detecting username values from system
        if not self.username:
            self.log.debug(
                "username to ssh to host: %s is not specified for connection id"
                " %s. Using system's default provided by getpass.getuser()",
                self.remote_host,
                self.ssh_conn_id,
            )
            self.username = getuser()

        # user_ssh_config_filename = os.path.expanduser("~/.ssh/config")
        # if os.path.isfile(user_ssh_config_filename):
        #     ssh_conf = paramiko.SSHConfig()
        #     with open(user_ssh_config_filename, encoding="utf-8") as config_fd:
        #         ssh_conf.parse(config_fd)
        #     host_info = ssh_conf.lookup(self.remote_host)
        #     if host_info and host_info.get("proxycommand") and not self.host_proxy_cmd:
        #         self.host_proxy_cmd = host_info["proxycommand"]

        if not (self.password or self.pkey):
            raise AirflowException("password or private_key is not set")

    # def get_parameter_value(
    #     self, parameter: str, default: str | ArgNotSet = NOTSET
    # ) -> str:
    #     """
    #     Return the provided Parameter or an optional default; if it is encrypted, then decrypt and mask.

    #     .. seealso::
    #         - :external+boto3:py:meth:`SSM.Client.get_parameter`

    #     :param parameter: The SSM Parameter name to return the value for.
    #     :param default: Optional default value to return if none is found.
    #     """
    #     try:
    #         param = self.conn.get_parameter(Name=parameter, WithDecryption=True)[
    #             "Parameter"
    #         ]
    #         value = param["Value"]
    #         if param["Type"] == "SecureString":
    #             mask_secret(value)
    #         return value
    #     except self.conn.exceptions.ParameterNotFound:
    #         if isinstance(default, ArgNotSet):
    #             raise
    #         return default

    @cached_property
    def host_proxy(self) -> paramiko.ProxyCommand | None:
        cmd = self.host_proxy_cmd
        return paramiko.ProxyCommand(cmd) if cmd else None

    def get_conn(self) -> paramiko.SSHClient:
        """Establish an SSH connection to the remote host."""
        if not self.remote_host:
            warnings.warn(
                "remote_host is not provided. We can not provide the ssh client."
            )
            return None
        if self.client:
            transport = self.client.get_transport()
            if transport and transport.is_active():
                # Return the existing connection
                return self.client

        self.log.debug("Creating SSH client for conn_id: %s", self.ssh_conn_id)
        client = paramiko.SSHClient()

        if self.allow_host_key_change:
            self.log.warning(
                "Remote Identification Change is not verified. "
                "This won't protect against Man-In-The-Middle attacks"
            )
            # to avoid BadHostKeyException, skip loading host keys
            client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy)
        else:
            client.load_system_host_keys()

        if self.no_host_key_check:
            self.log.warning(
                "No Host Key Verification. This won't protect against Man-In-The-Middle attacks"
            )
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # nosec B507
            # to avoid BadHostKeyException, skip loading and saving host keys
            known_hosts = os.path.expanduser("~/.ssh/known_hosts")
            if not self.allow_host_key_change and os.path.isfile(known_hosts):
                client.load_host_keys(known_hosts)

        elif self.host_key is not None:
            # Get host key from connection extra if it not set or None then we fallback to system host keys
            client_host_keys = client.get_host_keys()
            if self.port == SSH_PORT:
                client_host_keys.add(
                    self.remote_host, self.host_key.get_name(), self.host_key
                )
            else:
                client_host_keys.add(
                    f"[{self.remote_host}]:{self.port}",
                    self.host_key.get_name(),
                    self.host_key,
                )

        connect_kwargs: dict[str, Any] = {
            "hostname": self.remote_host,
            "username": self.username,
            "timeout": self.conn_timeout,
            "compress": self.compress,
            "port": self.port,
            "sock": self.host_proxy,
            "look_for_keys": self.look_for_keys,
            "banner_timeout": self.banner_timeout,
            "auth_timeout": self.auth_timeout,
        }

        if self.password:
            password = self.password.strip()
            connect_kwargs.update(password=password)

        if self.pkey:
            connect_kwargs.update(pkey=self.pkey)

        if self.disabled_algorithms:
            connect_kwargs.update(disabled_algorithms=self.disabled_algorithms)

        def log_before_sleep(retry_state):
            return self.log.info(
                "Failed to connect. Sleeping before retry attempt %d",
                retry_state.attempt_number,
            )

        for attempt in Retrying(
            reraise=True,
            wait=wait_fixed(3) + wait_random(0, 2),
            stop=stop_after_attempt(3),
            before_sleep=log_before_sleep,
        ):
            with attempt:
                client.connect(**connect_kwargs)

        if self.keepalive_interval:
            # MyPy check ignored because "paramiko" isn't well-typed. The `client.get_transport()` returns
            # type "Transport | None" and item "None" has no attribute "set_keepalive".
            client.get_transport().set_keepalive(self.keepalive_interval)  # type: ignore[union-attr]

        if self.ciphers:
            # MyPy check ignored because "paramiko" isn't well-typed. The `client.get_transport()` returns
            # type "Transport | None" and item "None" has no method `get_security_options`".
            client.get_transport().get_security_options().ciphers = self.ciphers  # type: ignore[union-attr]

        self.client = client
        return client

    def get_tunnel(
        self,
        remote_port: int,
        remote_host: str = "localhost",
        local_port: int | None = None,
    ) -> SSHTunnelForwarder:
        """
        Create a tunnel between two hosts.

        This is conceptually similar to ``ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>``.

        :param remote_port: The remote port to create a tunnel to
        :param remote_host: The remote host to create a tunnel to (default localhost)
        :param local_port:  The local port to attach the tunnel to

        :return: sshtunnel.SSHTunnelForwarder object
        """
        if local_port:
            local_bind_address: tuple[str, int] | tuple[str] = ("localhost", local_port)
        else:
            local_bind_address = ("localhost",)

        tunnel_kwargs = {
            "ssh_port": self.port,
            "ssh_username": self.username,
            "ssh_pkey": self.pkey,
            "ssh_proxy": self.host_proxy,
            "local_bind_address": local_bind_address,
            "remote_bind_address": (remote_host, remote_port),
            "logger": self.log,
        }

        if self.password:
            password = self.password.strip()
            tunnel_kwargs.update(
                ssh_password=password,
            )
        else:
            tunnel_kwargs.update(
                host_pkey_directories=None,
            )

        client = SSHTunnelForwarder(self.remote_host, **tunnel_kwargs)

        return client

    def _pkey_from_private_key(
        self, private_key: str, passphrase: str | None = None
    ) -> paramiko.PKey:
        """
        Create an appropriate Paramiko key for a given private key.

        :param private_key: string containing private key
        :return: ``paramiko.PKey`` appropriate for given key
        :raises AirflowException: if key cannot be read
        """
        if len(private_key.splitlines()) < 2:
            raise AirflowException(
                "Key must have BEGIN and END header/footer on separate lines."
            )

        for pkey_class in self._pkey_loaders:
            try:
                key = pkey_class.from_private_key(
                    StringIO(private_key), password=passphrase
                )
                # Test it actually works. If Paramiko loads an openssh generated key, sometimes it will
                # happily load it as the wrong type, only to fail when actually used.
                key.sign_ssh_data(b"")
                return key
            except (paramiko.ssh_exception.SSHException, ValueError):
                continue
        raise AirflowException(
            "Private key provided cannot be read by paramiko."
            "Ensure key provided is valid for one of the following"
            "key formats: RSA, DSS, ECDSA, or Ed25519"
        )

    def test_connection(self) -> tuple[bool, str]:
        """Test the ssh connection by execute remote bash commands."""
        try:
            with self.get_conn() as conn:
                conn.exec_command("pwd")
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
