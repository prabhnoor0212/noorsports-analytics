from __future__ import annotations

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def utc_dt(y, m, d, hh=0, mm=0, ss=0) -> datetime:
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


def main(out_dir: Path, seed: int = 42) -> None:
    random.seed(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Small but realistic
    n_users = 250
    n_content = 120
    n_days = 21
    n_sessions = 1200
    max_events_per_session = 20

    end = utc_dt(2025, 12, 31)
    start = end - timedelta(days=n_days)

    countries = ["CA", "US", "GB", "IN", "AU"]
    channels = ["organic", "paid_search", "social", "referral", "email"]
    platforms = ["iOS", "Android", "Web"]
    cities = [
        ("CA", "ON", "Toronto"),
        ("CA", "BC", "Vancouver"),
        ("US", "NY", "New York"),
        ("US", "CA", "San Francisco"),
        ("GB", "ENG", "London"),
        ("IN", "DL", "Delhi"),
        ("AU", "NSW", "Sydney"),
    ]

    # --- categories (sport/league/team) ---
    sports = {
        "Basketball": {"NBA": ["Raptors", "Lakers", "Celtics", "Warriors"]},
        "Hockey": {"NHL": ["Leafs", "Canadiens", "Oilers", "Canucks"]},
        "Soccer": {"EPL": ["Arsenal", "Chelsea", "Liverpool", "City"]},
    }

    categories = []
    category_id = 1
    sport_ids = {}
    league_ids = {}

    # sport nodes
    for sport_name in sports.keys():
        sport_ids[sport_name] = category_id
        categories.append(
            dict(
                category_id=category_id,
                category_name=sport_name,
                category_level="sport",
                parent_category_id="",
                category_desc=f"{sport_name} root node",
            )
        )
        category_id += 1

    # league + team nodes
    for sport_name, leagues in sports.items():
        for league_name, teams in leagues.items():
            league_ids[(sport_name, league_name)] = category_id
            categories.append(
                dict(
                    category_id=category_id,
                    category_name=league_name,
                    category_level="league",
                    parent_category_id=sport_ids[sport_name],
                    category_desc=f"{league_name} league under {sport_name}",
                )
            )
            league_cat_id = category_id
            category_id += 1

            for team in teams:
                categories.append(
                    dict(
                        category_id=category_id,
                        category_name=team,
                        category_level="team",
                        parent_category_id=league_cat_id,
                        category_desc=f"{team} team under {league_name}",
                    )
                )
                category_id += 1

    df_categories = pd.DataFrame(categories)

    # --- users ---
    users = []
    for user_id in range(1, n_users + 1):
        signup_ts = start - timedelta(days=random.randint(1, 180), hours=random.randint(0, 23))
        ctry = random.choice(countries)
        users.append(
            dict(
                user_id=user_id,
                signup_ts=signup_ts.isoformat(),
                country=ctry,
                marketing_channel=random.choice(channels),
                fav_sport=random.choice(list(sports.keys())),
            )
        )
    df_users = pd.DataFrame(users)

    # --- content ---
    content_types = ["news", "video", "game_event", "home_page"]
    content = []
    for content_id in range(1, n_content + 1):
        publish_ts = start + timedelta(days=random.randint(0, n_days - 1), hours=random.randint(0, 23))
        ctype = random.choices(content_types, weights=[0.35, 0.40, 0.20, 0.05])[0]
        title = f"{ctype.upper()} #{content_id}: {random.choice(['Highlights','Recap','Preview','Analysis'])}"
        content.append(
            dict(
                content_id=content_id,
                content_title=title,
                publish_ts=publish_ts.isoformat(),
                content_type=ctype,
                author_id=random.randint(1, 30),
                content_url=f"/content/{content_id}",
            )
        )
    df_content = pd.DataFrame(content)

    # --- content_category bridge (content can reference multiple categories) ---
    bridge_rows = []
    relationship_types = ["primary", "mentioned", "tagged"]
    sources = ["editorial", "nlp", "user"]
    all_team_ids = df_categories[df_categories["category_level"] == "team"]["category_id"].tolist()
    all_league_ids = df_categories[df_categories["category_level"] == "league"]["category_id"].tolist()
    all_sport_ids = df_categories[df_categories["category_level"] == "sport"]["category_id"].tolist()

    for cid in df_content["content_id"].tolist():
        # each content references 1-3 categories, often teams
        k = random.choices([1, 2, 3], weights=[0.55, 0.35, 0.10])[0]
        chosen = set()
        for _ in range(k):
            pool = random.choices(
                ["team", "league", "sport"], weights=[0.70, 0.20, 0.10]
            )[0]
            if pool == "team":
                chosen.add(random.choice(all_team_ids))
            elif pool == "league":
                chosen.add(random.choice(all_league_ids))
            else:
                chosen.add(random.choice(all_sport_ids))

        for cat_id in chosen:
            bridge_rows.append(
                dict(
                    content_id=cid,
                    category_id=cat_id,
                    relationship_type=random.choice(relationship_types),
                    source=random.choice(sources),
                    confidence_score=round(random.uniform(0.6, 0.99), 3),
                    attribution_weight=round(random.uniform(0.2, 1.0), 3),
                )
            )
    df_content_category = pd.DataFrame(bridge_rows)

    # --- sessions ---
    sessions = []
    for i in range(1, n_sessions + 1):
        user_id = random.randint(1, n_users)
        start_ts = start + timedelta(days=random.randint(0, n_days - 1), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        platform = random.choice(platforms)
        ctry, state, city = random.choice(cities)
        sessions.append(
            dict(
                session_id=f"ses_{i}_{uuid.uuid4().hex[:8]}",
                user_id=user_id,
                session_start_ts=start_ts.isoformat(),
                platform_type=platform,
                country=ctry,
                state_province=state,
                city=city,
            )
        )
    df_sessions = pd.DataFrame(sessions)

    # --- user_content_events (event level; later we aggregate to user×session×content fact) ---
    event_types = ["open", "scroll", "watch", "complete"]
    events = []
    event_id = 1
    for row in df_sessions.itertuples(index=False):
        # each session touches 1-4 content items
        content_touched = random.sample(df_content["content_id"].tolist(), k=random.randint(1, 4))
        for content_id in content_touched:
            n_events = random.randint(2, max_events_per_session)
            base_ts = datetime.fromisoformat(row.session_start_ts)
            for _ in range(n_events):
                et = random.choices(event_types, weights=[0.10, 0.55, 0.30, 0.05])[0]
                ts = base_ts + timedelta(seconds=random.randint(0, 1800))
                time_spent = random.randint(1, 60) if et in ["scroll", "watch"] else 0
                scroll_count = random.randint(0, 6) if et == "scroll" else 0
                completion_pct = random.randint(0, 100) if et in ["watch", "complete"] else 0
                completed_flag = 1 if et == "complete" or completion_pct >= 95 else 0

                events.append(
                    dict(
                        event_id=event_id,
                        user_id=row.user_id,
                        session_id=row.session_id,
                        content_id=content_id,
                        event_ts=ts.isoformat(),
                        event_type=et,
                        time_spent_seconds=time_spent,
                        scroll_count=scroll_count,
                        completion_pct=completion_pct,
                        completed_flag=completed_flag,
                        live_game_flag=1 if random.random() < 0.15 else 0,
                        major_tournament_flag=1 if random.random() < 0.10 else 0,
                    )
                )
                event_id += 1

    df_events = pd.DataFrame(events)

    # --- write CSVs ---
    df_users.to_csv(out_dir / "users.csv", index=False)
    df_content.to_csv(out_dir / "content.csv", index=False)
    df_sessions.to_csv(out_dir / "sessions.csv", index=False)
    df_events.to_csv(out_dir / "user_content_events.csv", index=False)
    df_categories.to_csv(out_dir / "categories.csv", index=False)
    df_content_category.to_csv(out_dir / "content_category.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    main(Path(args.out), seed=args.seed)
