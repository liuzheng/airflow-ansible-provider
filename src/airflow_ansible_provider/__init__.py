#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from packaging import version

VERSION = "0.6.0"
VERSIONs = ["0.3.0", "0.4.0", "0.4.1", "0.4.2", "0.5.0", VERSION]

# Airflow 版本检测
try:

    from airflow import __version__
    from packaging.version import Version

    AIRFLOW_VERSION = Version(__version__)
    IS_AIRFLOW_3_PLUS = (
        AIRFLOW_VERSION.major,
        AIRFLOW_VERSION.minor,
        AIRFLOW_VERSION.micro,
    ) >= (
        3,
        0,
        0,
    )
except ImportError:
    # 如果无法导入 airflow，默认为 False
    IS_AIRFLOW_3_PLUS = False
    AIRFLOW_VERSION = None


def get_provider_info():
    """
    Get provider info for Airflow provider registry
    """
    provider_info = {
        "package-name": "airflow-ansible-provider",
        "name": "Airflow Ansible Provider",
        "description": "Run Ansible Playbook as Airflow Task",
        "connection-types": [
            {
                "hook-class-name": "airflow_ansible_provider.hooks.ansible.AnsibleHook",
                "connection-type": "ansible",
            },
        ],
        "extra-links": [],
        "hook-class-names": [
            "airflow_ansible_provider.hooks.ansible.AnsibleHook",
        ],
        "operator-class-names": [
            "airflow_ansible_provider.operators.ansible_operator.AnsibleOperator",
        ],
        "task-decorators": [],
        "transfers": [],
        "sensors": [],
        "versions": VERSIONs,
    }

    # 为 Airflow 3.x 添加额外的插件信息
    if IS_AIRFLOW_3_PLUS:
        provider_info["plugins"] = [
            {
                "name": "AirflowAnsiblePlugin",
                "plugin-class": "airflow_ansible_provider.plugins.AirflowAnsiblePlugin",
            }
        ]

    return provider_info
