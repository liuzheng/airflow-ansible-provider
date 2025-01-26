#!/usr/bin/env python3
# -*- coding: utf-8 -*-

VERSION = "0.3.0"
VERSIONs = [VERSION]


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
