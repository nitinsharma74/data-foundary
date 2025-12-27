# databricks_project_bundle

This bundle is a clean scaffold with no jobs, pipelines, or notebooks yet.

* `src/`: Python source code for this project (currently empty).
* `resources/`: Resource configurations (currently empty).
* `tests/`: Unit tests for the shared Python code (currently empty).
* `fixtures/`: Fixtures for data sets (currently empty).


## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

*  Make sure you have the UV package manager installed.
   It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
*  Run `uv sync --dev` to install the project's dependencies.


# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

2. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    The bundle currently has no resources defined.

3. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```
   This uses the same configuration in production mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

4. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
