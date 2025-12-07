#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from packaging import version

VERSION = "0.5.0"
VERSIONs = ["0.3.0", "0.4.0", "0.4.1", "0.4.2", VERSION]

# Airflow 版本检测
try:
    import airflow

    AIRFLOW_VERSION = version.parse(airflow.__version__)
    IS_AIRFLOW_3_PLUS = AIRFLOW_VERSION >= version.parse("3.0.0")
except ImportError:
    # 如果无法导入 airflow，默认为 False
    IS_AIRFLOW_3_PLUS = False
    AIRFLOW_VERSION = None


def get_provider_info():
    """
    Get provider info
    """
    return {
        "package-name": "airflow-ansible-provider",
        "name": "Airflow Ansible Provider",
        "description": "Run Ansible Playbook as Airflow Task",
        "connection-types": [
            {
                "hook-class-name": "airflow_ansible_provider.hooks.ansible.AnsibleHook",
                "connection-type": "ansible",
            },
            # {
            #     "hook-class-name": "airflow_ansible_provider.hooks.GitHook",
            #     "connection-type": "git",
            # },
        ],
        "versions": VERSIONs,
    }
