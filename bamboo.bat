REM set PYTHON365=%bamboo_capability_system_builder_command_Python3_6_5%
REM set PYTHON36=%bamboo_capability_system_builder_command_Python3_6%
set PYTHON39=%bamboo_capability_system_builder_command_Python3_9%
set PyPI=https://artifactory.itcm.oneadr.net/api/pypi/pypi-remote/simple
set SESSION_PYTHON=%1
set SESSION=%2

REM install dependencies
%PYTHON39% -m pip install --user --upgrade pip -i %PyPI%
%PYTHON39% -m pip install --user -c requirements/py39/requirements-dev.txt pygit2 -i %PyPI%
%PYTHON39% -m pip install --user -c requirements/py39/requirements-dev.txt nox -i %PyPI%
%PYTHON39% -m pip install --user -c requirements/py39/requirements-dev.txt requests -i %PyPI%

REM execute CI session with nox
%PYTHON39% -m nox -p %SESSION_PYTHON% -s %SESSION%