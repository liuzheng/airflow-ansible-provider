#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Airflow version compatibility module.
Handle imports and API differences between Airflow 2.x and 3.x.
"""

from airflow_ansible_provider import IS_AIRFLOW_3_PLUS


def get_operator_class():
    """获取正确版本的 PythonVirtualenvOperator"""
    if IS_AIRFLOW_3_PLUS:
        try:
            from airflow.operators.python import PythonVirtualenvOperator

            return PythonVirtualenvOperator
        except ImportError:
            pass

    # 降级到 2.x
    from airflow.operators.python_operator import PythonVirtualenvOperator

    return PythonVirtualenvOperator


def get_base_hook_class():
    """获取正确版本的 BaseHook"""
    if IS_AIRFLOW_3_PLUS:
        try:
            from airflow.hooks.base import BaseHook

            return BaseHook
        except ImportError:
            pass

    # 降级到 2.x
    from airflow.hooks.base_hook import BaseHook

    return BaseHook


def get_context_type():
    """获取正确版本的 Context 类型"""
    if IS_AIRFLOW_3_PLUS:
        try:
            from airflow.utils.context import Context

            return Context
        except ImportError:
            pass

    # 降级到 2.x
    from airflow.utils.context import Context

    return Context


def get_prepare_lineage_decorator():
    """获取 prepare_lineage 装饰器，如果不存在则返回空装饰器"""
    try:
        from airflow.lineage.decorators import prepare_lineage

        return prepare_lineage
    except ImportError:
        # 如果没有 prepare_lineage，创建一个空的装饰器
        def prepare_lineage(func):
            return func

        return prepare_lineage


# 预定义的兼容性类和函数
PythonVirtualenvOperator = get_operator_class()
BaseHook = get_base_hook_class()
Context = get_context_type()
prepare_lineage = get_prepare_lineage_decorator()
