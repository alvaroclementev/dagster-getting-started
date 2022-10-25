from __future__ import annotations

import os
from datetime import timedelta

import pandas as pd
from dagster import asset
from github import Github

ACCESS_TOKEN = os.environ["GITHUB_TOKEN"]


@asset
def github_stargazers():
    return list(
        Github(ACCESS_TOKEN).get_repo("dagster-io/dagster").get_stargazers_with_dates()
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
