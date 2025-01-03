# airflow-ansible-provider
An Airflow Ansible provider

## Getting started
Please note that this file is no substitute for reading and understanding the Airflow documentation. This file is only intended to provide a quick start for the SAS providers. Unless an issue relates specifically to the SAS providers, the Airflow documentation should be consulted.

### Install Airflow
Follow instructions at https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html to install Airflow.
If you just want to evaluate the SAS providers, then the simplest path would be to install via PYPI and run Airflow on the local machine in a virtual environment. 

### Install the Ansible provider

If you want to build the package from these sources, install the build module using `pip install build` and then run `python -m build` from the root of the repository which will create a wheel file in the dist subdirectory. 

#### Installing in a local virtual environment
The Ansible provider is available as a package published in PyPI. To install it, switch to the Python environment where Airflow is installed, and run the following command:

`pip install airflow-ansible-provider`

If you would like to install the provider from a package you built locally, run:

`pip install dist/airflow_ansible_provider_xxxxx.whl`

#### Installing in a container
There are a few ways to provide the package:
- Environment variable: ```_PIP_ADDITIONAL_REQUIREMENTS``` Set this variable to the command line that will be passed to ```pip install```
- Create a dockerfile that adds the pip install command to the base image and edit the docker-compose file to use "build" (there is a comment in the docker compose file where you can change it)

### Running a DAG with a Ansible provider
See example files in the src/airflow_ansible_provider/example_dags directory. These dags can be modified and 
placed in your Airflow dags directory. 

Mac note: If you are running Airflow standalone on a Mac, there is a known issue regarding how process forking works.
This causes issues with the urllib which is used by the operator. To get around it set NO_PROXY=* in your environment
prior to running Airflow in standalone mode.
Eg:
`export NO_PROXY="*"`

## Contributing
We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for
details on how to submit contributions to this project.

## License
This project is licensed under the [Apache 2.0 License](LICENSE).
