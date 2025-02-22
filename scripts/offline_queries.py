#!/usr/bin/env python3
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter

def load_data_from_json(json_file):
    """
    Load the JSON records from the given file and return a list of dicts.

    Each record in the returned list will have:
      - user_id (str)
      - track_name (str)
      - artist_name (str)
      - listened_at (datetime)
    """
    data = []
    with open(json_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                user_id = record['user_name']
                track_name = record['track_metadata']['track_name']
                artist_name = record['track_metadata']['artist_name']
                epoch_time = record['listened_at']
                # Convert epoch to datetime (UTC)
                ts = datetime.utcfromtimestamp(epoch_time)
                data.append({
                    'user_id': user_id,
                    'track_name': track_name,
                    'artist_name': artist_name,
                    'listened_at': ts
                })
            except Exception as e:
                print(f"Warning: Skipping line {line_num} due to error: {e}", file=sys.stderr)
    return data

def get_top_10_users_by_listens(data):
    """
    1) Who are the top 10 users by number of songs listened to?
    Returns a list of (user_id, count) sorted desc by count, limited to 10.
    """
    user_counts = Counter(d['user_id'] for d in data)
    return user_counts.most_common(10)

def get_num_users_listened_on_date(data, target_date):
    """
    2) How many distinct users listened on a specific date (e.g., March 1, 2019)?

    target_date is a datetime.date object for the desired day.
    """
    users_on_date = set()
    for rec in data:
        if rec['listened_at'].date() == target_date:
            users_on_date.add(rec['user_id'])
    return len(users_on_date)

def get_first_song_each_user(data):
    """
    3) For every user, what was the first song they listened to (chronologically)?

    Returns a dict mapping user_id -> (track_name, artist_name, listened_at)
    """
    # Sort all records by user_id, then by listened_at
    data_sorted = sorted(data, key=lambda x: (x['user_id'], x['listened_at']))
    first_song_map = {}
    for rec in data_sorted:
        user_id = rec['user_id']
        if user_id not in first_song_map:
            first_song_map[user_id] = (
                rec['track_name'],
                rec['artist_name'],
                rec['listened_at']
            )
    return first_song_map

def get_top_3_days_per_user(data):
    """
    4) For each user, find the top 3 days (by total listens) and how many listens they had.

    Returns a list of tuples: (user_id, listen_date, num_listens),
    sorted by user_id (ascending) and num_listens (descending) similar to the SQL:
      ORDER BY user_id, num_listens DESC.
    """
    # Aggregate counts per user per day
    user_day_counts = defaultdict(lambda: Counter())
    for rec in data:
        user_id = rec['user_id']
        day_only = rec['listened_at'].date()
        user_day_counts[user_id][day_only] += 1

    # Create a list of results: one row per user/day
    results = []
    for user_id in sorted(user_day_counts.keys()):
        day_counts = user_day_counts[user_id]
        # Get top 3 days (most_common returns sorted descending by count)
        top_3 = day_counts.most_common(3)
        for day, count in top_3:
            results.append((user_id, day, count))
    # Now, sort the complete results by user_id and then by num_listens descending
    results.sort(key=lambda x: (x[0], -x[2]))
    return results

def get_daily_active_users_7day(data):
    """
    5) We define a user to be 'active' on day X if they listened to at least one song in [X-6, X].
       For each day, compute:
         - the absolute number of active users
         - the percentage of active users among all users
       Return a list of dicts with keys: date, number_active_users, percentage_active_users
    """
    # Collect a set of all users
    all_users = set(rec['user_id'] for rec in data)
    total_users = len(all_users)

    # Group listens by date
    date_user_map = defaultdict(set)  # date -> set of users who listened on that date
    for rec in data:
        day_only = rec['listened_at'].date()
        date_user_map[day_only].add(rec['user_id'])

    # Build a sorted list of all dates present in the data
    unique_dates = sorted(date_user_map.keys())
    if not unique_dates:
        return []

    # Create a continuous date range from the minimum to maximum date in the data
    all_dates = []
    current_date = unique_dates[0]
    last_date = unique_dates[-1]
    while current_date <= last_date:
        all_dates.append(current_date)
        current_date += timedelta(days=1)

    # For each date in that range, gather active users = union of users from day-6 to day
    results = []
    for d in all_dates:
        start_window = d - timedelta(days=6)
        # Collect users from start_window to d
        active_users_window = set()
        check_date = start_window
        while check_date <= d:
            if check_date in date_user_map:
                active_users_window |= date_user_map[check_date]
            check_date += timedelta(days=1)

        number_active_users = len(active_users_window)
        percentage_active_users = (number_active_users / total_users * 100.0) if total_users > 0 else 0.0

        results.append({
            'date': d,
            'number_active_users': number_active_users,
            'percentage_active_users': round(percentage_active_users, 2)
        })

    return results

def main(json_file):
    # 1) Load data
    data = load_data_from_json(json_file)

    # 2) Query 1: Top 10 users by count
    top_10 = get_top_10_users_by_listens(data)
    print("1) Top 10 Users by Listen Count:")
    for user_id, cnt in top_10:
        print(f"   {user_id}: {cnt} listens")
    print()

    # 3) Query 2: Number of users who listened on 1 March 2019
    target_date = datetime(2019, 3, 1).date()
    users_on_march_1 = get_num_users_listened_on_date(data, target_date)
    print(f"2) Number of users who listened on {target_date}: {users_on_march_1}")
    print()

    # 4) Query 3: The first song each user listened to
    first_song_map = get_first_song_each_user(data)
    print("3) First Song Each User Listened To:")
    for user_id, (track_name, artist_name, listened_dt) in sorted(first_song_map.items()):
        print(f"   {user_id} -> '{track_name}' by {artist_name} at {listened_dt}")
    print()

    # 5) Query 4: Top 3 days for each user, sorted by user_id and num_listens desc
    top_3_days = get_top_3_days_per_user(data)
    print("4) Top 3 Days with Most Listens per User:")
    for user_id, day, count in top_3_days:
        print(f"   {user_id}: {day} -> {count} listens")
    print()

    # 6) Query 5: Daily Active Users (7-day rolling window)
    daily_active = get_daily_active_users_7day(data)
    print("5) Daily Active Users (7-day rolling window):")
    for row in daily_active:
        print(f"   {row['date']} -> {row['number_active_users']} active users "
              f"({row['percentage_active_users']}% of total)")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python offline_queries.py [path_to_json_file]")
        sys.exit(1)

    json_file_path = sys.argv[1]
    main(json_file_path)
