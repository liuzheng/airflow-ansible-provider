from __future__ import annotations
import warnings
from typing import Any, Collection, Mapping, Sequence, Callable
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.decorators.base import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)

from airflow_ansible_provider.operators.venv_ansible_operator import (
    VirtualAnsibleOperator,
)


class AnsibleVenvDecoratedOperator(DecoratedOperator, VirtualAnsibleOperator):
    """Ansible Decorated Operator"""

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *VirtualAnsibleOperator.template_fields,
    )
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **VirtualAnsibleOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.ansible_venv"

    def __init__(
        self,
        python_callable: Callable,
        op_kwargs: Mapping[str, Any],
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )
        for k in ("ansible_vars",):  # Unsupported parameters in the decorator
            if k in kwargs:
                kwargs.pop(k)
        for k in VirtualAnsibleOperator.operator_fields:
            # When k is not in kwargs, pass op_kwargs to VirtualAnsibleOperator
            if (
                k
                not in (
                    "conn_id",
                    "pool",
                    "queue",
                )  # Unsupported parameters in the decorator
                and k not in kwargs
                and k in op_kwargs
            ):
                kwargs[k] = op_kwargs.get(k)
        super().__init__(
            kwargs_to_upstream={
                "python_callable": python_callable,
            },
            python_callable=python_callable,
            op_kwargs=op_kwargs,
            **kwargs,
        )
        self.log.debug("AnsibleDecoratedOperator kwargs: %s", kwargs)

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        self.log.debug("AnsibleDecoratedOperator.execute op_kwargs: %s", self.op_kwargs)
        self.log.debug("AnsibleDecoratedOperator.execute op_args: %s", self.op_args)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        self.log.debug("AnsibleDecoratedOperator.execute kwargs: %s", kwargs)
        super().execute(context)
        return self.python_callable(*self.op_args, **kwargs)


def ansible_venv_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Ansible task"""
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=AnsibleVenvDecoratedOperator,
        **kwargs,
    )
