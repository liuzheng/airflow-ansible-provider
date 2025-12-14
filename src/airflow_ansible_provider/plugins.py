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

# 导入版本检测
from airflow_ansible_provider import IS_AIRFLOW_3_PLUS
from airflow.plugins_manager import AirflowPlugin


class AirflowAnsiblePlugin(AirflowPlugin):
    name = "AirflowAnsiblePlugin"

    def __init__(self):
        super().__init__()
        # 确保插件在不同版本中都能正确识别
        if IS_AIRFLOW_3_PLUS:
            # Airflow 3.x 特定配置
            pass

    # 添加插件描述信息
    @property
    def description(self):
        return "Ansible Provider Plugin for Apache Airflow"

    # 确保在插件页面显示版本信息
    @property
    def version(self):
        from airflow_ansible_provider import VERSION

        return VERSION

    # A list of references to inject into the macros namespace
    macros = []

    # A list of dictionaries containing FastAPI app objects and some metadata
    fastapi_apps = []

    # A list of dictionaries containing FastAPI middleware factory objects and some metadata
    fastapi_root_middlewares = []

    # A list of dictionaries containing external views and some metadata
    external_views = []

    # A list of dictionaries containing react apps and some metadata
    # Note: React apps are only supported in Airflow 3.1 and later
    react_apps = []

    # A callback to perform actions when Airflow starts and the plugin is loaded.
    # NOTE: Ensure your plugin has *args, and **kwargs in the method definition
    #   to protect against extra parameters injected into the on_load(...)
    #   function in future changes
    def on_load(self, *args, **kwargs):
        # ... perform Plugin boot actions
        pass

    # A list of global operator extra links that can redirect users to
    # external systems. These extra links will be available on the
    # task page in the form of buttons.
    #
    # Note: the global operator extra link can be overridden at each
    # operator level.
    global_operator_extra_links = []

    # A list of operator extra links to override or add operator links
    # to existing Airflow Operators.
    # These extra links will be available on the task page in form of
    # buttons.
    operator_extra_links = []

    # A list of timetable classes to register so they can be used in Dags.
    timetables = []

    # A list of Listeners that plugin provides. Listeners can register to
    # listen to particular events that happen in Airflow, like
    # TaskInstance state changes. Listeners are python modules.
    listeners = []
