from invoke import task


@task
def test(ctx):
    """Run tests."""
    ctx.run("pytest", pty=True)


@task
def coverage(ctx):
    """Run test coverage."""
    ctx.run("coverage run -m pytest && coverage report --fail-under=78", pty=True)


@task
def lint(ctx):
    """Run pre-commit linting."""
    ctx.run("pre-commit run --all-files", pty=True)
