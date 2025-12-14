FROM apache/airflow:3.1.4-python3.14
COPY dist/ /tmp/dist/ 
RUN pip install /tmp/dist/airflow_ansible_provider-${PKG_VERSION}-py3-none-any.whl
