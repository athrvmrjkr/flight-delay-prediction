from pathlib import Path
from typing import List

from setuptools import find_packages, setup

HYPHEN_E_DOT = "-e ."
ROOT = Path(__file__).parent


def get_requirements(file_path: str) -> List[str]:
    """Read requirements.txt and return a clean list of packages.

    Skips blank lines, comments and the editable-install marker (-e .),
    which is only there so `pip install -r requirements.txt` also installs
    this project as a package.
    """
    requirements = []
    with open(file_path, encoding="utf-8") as file_obj:
        for line in file_obj:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line == HYPHEN_E_DOT:
                continue
            requirements.append(line)
    return requirements


def get_long_description() -> str:
    readme = ROOT / "README.md"
    return readme.read_text(encoding="utf-8") if readme.exists() else ""


setup(
    name="flightdelay",
    version="0.1.0",
    author="Atharva Mirajkar",
    author_email="athrvmirajkar@gmail.com",
    description="End-to-end ML system that predicts US flight arrival delays",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.10",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=get_requirements("requirements.txt"),
)