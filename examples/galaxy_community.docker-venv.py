#!/bin/env python3
"""
ansible linux ping workflow example
"""
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow_ansible_provider.decorators import ansible_venv_task


@task(task_id="gen_inventory")
def gen_inventory():
    context = get_current_context()
    param = context["dag_run"].conf
    return {
        "default": {
            "hosts": {
                "test": {
                    "ansible_host": param["ip"],
                    "ansible_ssh_host": param["ip"],
                }
            },
        }
    }


@ansible_venv_task(
    task_id="docker_pull",
    playbook="docker.pull.yml",
    get_ci_events=True,
    requirements=["ansible", "ansible-runner"],
    galaxy_collections=["community.docker"],
    venv_cache_path="/tmp/venv_cache",
)
def docker_pull(inventory):  # pylint: disable=unused-argument
    """Collect ansible run results"""
    return get_current_context().get("ansible_return", {})


@dag(
    dag_id="docker_pull_venv",
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
    tags=["ansible_venv", "Linux", "docker_pull"],
    params={
        "ip": Param(
            default="",
            type="string",
            description="server ip",
            title="Server IP",
        ),
    },
)
def main():
    """linux ping workflow"""
    inventory = gen_inventory()
    docker_pull(inventory=inventory)


main()
