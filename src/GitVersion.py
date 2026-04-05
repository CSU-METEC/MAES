import subprocess
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent.parent


def readMaesVersion() -> str:
    """Read the MAES version string from src/__init__.py.

    Reads the __version__ assignment directly from the file so this module
    does not depend on the package being installed.
    """
    initPath = Path(__file__).parent / "__init__.py"
    try:
        for line in initPath.read_text().splitlines():
            if line.startswith("__version__"):
                ret = line.split("=")[1].strip().strip('"\'')
                return ret
    except Exception:
        pass
    ret = "unknown"
    return ret


def runGitCommand(args: list[str]) -> str:
    """Run a git command in the MAES repo root and return stripped stdout.

    Returns 'unknown' if git is not installed, the directory is not a git
    repository, or the command fails for any other reason.
    """
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            ret = result.stdout.strip()
            return ret
    except Exception:
        pass
    ret = "unknown"
    return ret


MAES_VERSION: str = readMaesVersion()
GIT_DESCRIBE: str = runGitCommand(["describe", "--tags", "--always", "--dirty"])
GIT_BRANCH: str = runGitCommand(["rev-parse", "--abbrev-ref", "HEAD"])
GIT_COMMIT: str = runGitCommand(["rev-parse", "HEAD"])


def main() -> None:
    """Print MAES version and git metadata to stdout."""
    print(f"maesVersion: {MAES_VERSION}")
    print(f"gitDescribe:  {GIT_DESCRIBE}")
    print(f"gitBranch:    {GIT_BRANCH}")
    print(f"gitCommit:    {GIT_COMMIT}")


if __name__ == "__main__":
    main()
