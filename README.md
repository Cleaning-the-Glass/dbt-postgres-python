# dbt-postgres-python: do more with dbt

dbt-postgres-python is the easiest way to run Python with your [dbt](https://www.getdbt.com/) project.

# Introduction - 📖 [README](./projects/adapter)

dbt-postgres-python is only supporting the adapter originally developed by
[dbt-fal](https://github.com/fal-ai/dbt-fal) going forward (i.e. CLI will be dropped) and only for Postgres.

## Python Adapter

With the Python adapter, you can:

- Enable a developer-friendly Python environment for Postgres.
- Use Python libraries such as [`sklearn`](https://scikit-learn.org/) or [`prophet`](https://facebook.github.io/prophet/) to build more complex `dbt` models including ML models.

# Why are continuing to maintain this?

My work team has been using `dbt-fal` and have found it very useful. The [FAL](https://github.com/fal-ai) team in
April, 2024 chose to stop maintaining `dbt-fal` -- thank you very much for starting this effort. I've decided to pick it
up to try to keep it current with DBT itself, but only for the functionality my team needs.

---

# CTG Fork Changelog

This fork ([Cleaning-the-Glass/dbt-postgres-python](https://github.com/Cleaning-the-Glass/dbt-postgres-python)) contains additional modifications for use in CTG projects.

## Changes from upstream

### 2025-01-08: Remove `fal` PyPI package dependency

**Problem**: The upstream `fal` package pins `cloudpickle==3.0.0`, which conflicts with other dependencies in downstream projects.

**Solution**: Removed the `fal` PyPI package dependency entirely since we don't use fal serverless/cloud execution features.

**Changes made**:
- Removed `fal` from `pyproject.toml` dependencies
- Added `isolate` as a direct dependency (was previously a transitive dep of `fal`)
- Simplified `fal_experimental/adapter.py`, `fal_experimental/teleport.py`, and `fal_experimental/utils/environments.py` to only support local execution
- Non-local environment kinds now raise `NotImplementedError`

**Impact**: 
- ✅ Core adapter functionality (`type: fal` + `db_profile`) works as before
- ✅ Local Python model execution works as before
- ❌ fal serverless/cloud execution is no longer supported (was never used by CTG)
- ✅ No more `cloudpickle==3.0.0` constraint in downstream projects
