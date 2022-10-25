from __future__ import annotations

import os
import pickle
from datetime import timedelta

import jupytext  # type: ignore
import nbformat
import pandas as pd
from dagster import asset
from github import Github, InputFileContent
from nbconvert.preprocessors import ExecutePreprocessor  # type: ignore

ACCESS_TOKEN = os.environ["GITHUB_TOKEN"]


@asset
def github_stargazers():
    return list(
        Github(ACCESS_TOKEN)
        .get_repo("dagster-io/dagster")
        .get_stargazers_with_dates()
    )


@asset
def github_stargazers_by_week(github_stargazers):
    df = pd.DataFrame(
        [
            {
                "users": stargazer.user.login,
                "week": stargazer.starred_at.date()
                + timedelta(days=6 - stargazer.starred_at.weekday()),
            }
            for stargazer in github_stargazers
        ]
    )
    return df.groupby("week").count().sort_values(by="week")


@asset
def github_stars_notebook(github_stargazers_by_week):
    markdown = f"""
# Github Stars

```python
import pickle

github_stargazers_by_week = pickle.loads({pickle.dumps(github_stargazers_by_week)})
```

## Github Stars by Week, last 52 weeks

```python
github_stargazers_by_week.tail(52).reset_index().plot.bar(x="week", y="users")
```
    """
    nb = jupytext.reads(markdown, "md")
    ExecutePreprocessor().preprocess(nb)
    return nbformat.writes(nb)


@asset
def github_stars_noteboook_gist(context, github_stars_notebook):
    gist = (
        Github(ACCESS_TOKEN)
        .get_user()
        .create_gist(
            public=False,
            files={
                "github_stars.ipynb": InputFileContent(github_stars_notebook)
            },
        )
    )
    context.log.info("Notebook created at %s", gist.html_url)
    return gist.html_url
