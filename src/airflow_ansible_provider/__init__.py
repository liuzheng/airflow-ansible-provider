#!/usr/bin/env python3
# -*- coding: utf-8 -*-

VERSION = "0.5.0"
VERSIONs = ["0.3.0", "0.4.0", "0.4.1", "0.4.2", VERSION]


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
