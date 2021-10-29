"""Test automation with nox."""
from pathlib import Path
import re
from typing import Set

import nox
from nox.sessions import Session
from pygit2 import Repository
import requests

HERE = Path(__file__).parent

PROD_REQ = "requirements/{version}/requirements.txt"
DEV_REQ = "requirements/{version}/requirements-dev.txt"

PyPI = "https://artifactory.itcm.oneadr.net/api/pypi/pypi-remote/simple"

ARTIFACTORY_URL = (
    "https://artifactory.itcm.oneadr.net/api/pypi/QuantitativeResearch-python"
)

locations = "src", "tests", "noxfile.py"

nox.options.sessions = "lint", "mypy", "tests", "valid_version"

python_versions = ["3.9"]

version_folders = {
    "3.9": "39",
}

dependencies = {
    "easygui": ["0.98.2"],
    "keyring": ["21.8.0", "22.4.0", "23.1.0"],
    "pandas": ["1.1.5", "1.3.2"],
    "numpy": ["1.19.5", "1.21.2"],
    "requests": ["2.26.0"],
    "pyaml": ["20.4.0", "21.8.3"],
}


@nox.session(python="3.9")
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    session.install(
        "-r",
        PROD_REQ.format(version=f"py{version_folders[str(session.python)]}"),
        "--index-url",
        PyPI,
    )
    _install_constrained(session, "black")
    session.run("black", *args)


@nox.session(python="3.9")
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    session.install(
        "-r",
        PROD_REQ.format(version=f"py{version_folders[str(session.python)]}"),
        "--index-url",
        PyPI,
    )
    _install_constrained(
        session,
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-import-order",
        "darglint",
    )
    session.run("flake8", *args)


@nox.session(python=python_versions)
def mypy(
    session: Session,
) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    session.install(
        "-r",
        PROD_REQ.format(
            version=f"py{version_folders[str(session.python)]}",
        ),
        "--index-url",
        PyPI,
    )
    _install_nordea_analytics(session)
    _install_constrained(session, "mypy", "types-PyYAML")
    session.run("mypy", *args)


@nox.session(name="tests")
@nox.parametrize(
    "python,easygui,keyring,pandas,numpy,requests_,pyaml",
    [
        (python, easygui, keyring, pandas, numpy, requests_, pyaml)
        for python in python_versions
        for easygui in dependencies["easygui"]
        for keyring in dependencies["keyring"]
        for pandas in dependencies["pandas"]
        for numpy in dependencies["numpy"]
        for requests_ in dependencies["requests"]
        for pyaml in dependencies["pyaml"]
        if (python, numpy) != ("3.6", "1.21.2") and (python, pandas) != ("3.6", "1.3.2")
    ],
)
def pytest(
    session: Session,
    easygui: str,
    keyring: str,
    pandas: str,
    numpy: str,
    requests_: str,
    pyaml: str,
) -> None:
    """Run the test suite."""
    args = session.posargs or ["-m", "not e2e", "--cov", "--xdoctest"]
    session.install(
        "-r",
        PROD_REQ.format(version=f"py{version_folders[str(session.python)]}"),
        "--index-url",
        PyPI,
    )
    _install_nordea_analytics(session)
    session.install("-i", PyPI, f"easygui=={easygui}")
    session.install("-i", PyPI, f"keyring=={keyring}")
    session.install("-i", PyPI, f"pandas=={pandas}")
    session.install("-i", PyPI, f"numpy=={numpy}")
    session.install("-i", PyPI, f"requests=={requests_}")
    session.install("-i", PyPI, f"pyaml=={pyaml}")
    _install_constrained(
        session,
        "coverage[toml]",
        "nox",
        "pygit2",
        "pytest",
        "pytest-cov",
        "pytest-mock",
        "xdoctest",
    )
    session.run("pytest", *args)


@nox.session(python="3.9")
def valid_version(session: Session) -> None:
    """Check that the version makes sense.

    Runs on master only.

    Args:
        session: Nox session.
    """
    if not _runs_on_master():
        session.skip(f"{session.name} runs only on master branch")

    _validate_version(session)


@nox.session(python="3.9")
def publish(session: Session) -> None:
    """Package and publish to artifactory.

    Runs on master only.

    Args:
        session: Nox session.
    """
    if not _runs_on_master():
        session.skip(f"{session.name} runs only on master branch")

    _validate_version(session)
    session.install("--index-url", PyPI, "build", "wheel", "twine")
    session.run("pyproject-build", "--no-isolation")
    session.run(
        "python",
        "-m",
        "twine",
        "upload",
        "--repository-url",
        ARTIFACTORY_URL,
        "-u",
        "qtoolkit-bamboo",
        "-p",
        "Zaq12wsx",
        "dist\\*",
    )


def _validate_version(session: Session) -> None:
    """Check that the version makes sense."""
    version = _get_version()
    existing_versions = _get_versions_from_artifactory()
    allowed_versions = _generate_allowed_versions(existing_versions)

    if version not in allowed_versions:
        allowed_versions_msg = "\n".join(allowed_versions)
        error_msg = (
            f"Version {version} is not allowed, "
            f"use one of the following: \n{allowed_versions_msg}\n"
            f"Consult the README.md on how to bump the version "
            f"upon making a change"
        )
        session.error(error_msg)


def _install_nordea_analytics(session: Session) -> None:
    """Build and install the current version of nordea_analytics without requirements.

    This is a replacement to `session.install('.')` due to it being super slow.

    Args:
        session: Nox session.
    """
    session.install("--index-url", PyPI, "build")
    session.run("pyproject-build", "--no-isolation", "--wheel")
    wheel = HERE / "dist" / f"nordea_analytics-{_get_version()}-py3-none-any.whl"
    if not wheel.exists():
        session.error(f"No wheel found at: {wheel}")
    session.install("--no-deps", str(wheel))


def _install_constrained(
    session: Session, *packages: str, index_url: str = PyPI, constraint: str = DEV_REQ
) -> None:
    """Install packages into a session using the provided constraint file."""
    session.install(
        *packages,
        "-c",
        constraint.format(version=f"py{version_folders[str(session.python)]}"),
        "--index-url",
        index_url,
    )


def _bump_major(version: str) -> str:
    """Bump major version.

    Args:
        version: Library version.

    Returns:
        None

    Examples:
        >>> _bump_major("0.1.0")
        '1.0.0'

        >>> _bump_major("1.1.1")
        '2.0.0'
    """
    major, _, _ = map(int, version.split("."))
    return f"{major + 1}.0.0"


def _bump_minor(version: str) -> str:
    """Bump patch version.

    Args:
        version: Library version.

    Returns:
        str: Version bumped by minor version number.

    Examples:
        >>> _bump_minor("0.1.0")
        '0.2.0'

        >>> _bump_minor("1.1.1")
        '1.2.0'
    """
    major, minor, _ = map(int, version.split("."))
    return f"{major}.{minor + 1}.0"


def _bump_patch(version: str) -> str:
    """Bump patch version.

    Args:
        version: Library version.

    Returns:
        str: Version bumped by patch version number.

    Examples:
        >>> _bump_patch("0.1.0")
        '0.1.1'

        >>> _bump_patch("1.1.1")
        '1.1.2'
    """
    major, minor, patch = map(int, version.split("."))
    return f"{major}.{minor}.{patch + 1}"


def _generate_allowed_versions(existing_versions: Set[str]) -> Set[str]:
    """Generate allowed semantic versions increments from list of semantic versions.

    Args:
        existing_versions: Set of versions existing (on artifactory).

    Returns:
        Set[str]: Set of allowed versions.

    Examples:
        >>> _generate_allowed_versions(set())
        {'0.1.0'}

        >>> _generate_allowed_versions({"0.1.0"}) - {"1.0.0", "0.2.0", "0.1.1"}
        set()

        >>> _generate_allowed_versions({"0.1.0", "0.2.0", "1.0.0", "1.1.0", "1.1.1"})-\
            {'2.0.0', '1.2.0', '0.3.0', '1.1.2', '1.0.1', '0.2.1', '0.1.1'}
        set()
    """
    if not existing_versions:
        return {"0.1.0"}

    allowed_versions = set()
    for version in existing_versions:
        bumped_major = _bump_major(version)
        bumped_minor = _bump_minor(version)
        bumped_patch = _bump_patch(version)

        if bumped_major not in existing_versions:
            allowed_versions.add(bumped_major)

        if bumped_minor not in existing_versions:
            allowed_versions.add(bumped_minor)

        if bumped_patch not in existing_versions:
            allowed_versions.add(bumped_patch)

    return allowed_versions


def _get_versions_from_artifactory() -> Set[str]:
    url = (
        "https://artifactory.itcm.oneadr.net/api/storage/QuantitativeResearch-python/"
        "nordea-analytics?properties%5C%5B%3Dx%5C%5D"
    )
    response = requests.get(url)
    data = response.json()
    return set([child["uri"].lstrip("/") for child in data["children"]])


def _get_version() -> str:
    """Get the version from the package top-level `__init__.py`."""
    path = HERE / "src" / "nordea_analytics" / "__init__.py"
    content = path.read_text()
    version = re.search(r'__version__ = "(\d\.\d\.\d)"', content)
    if version:
        return version.groups()[0]
    else:
        raise RuntimeError(f"Cannot determine version from {path}")


def _runs_on_master() -> bool:
    """Check if the branch is master."""
    repo = Repository(str(HERE))
    return repo.head.name == "refs/heads/master"
