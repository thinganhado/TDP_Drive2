from database import Database
from pyquadkey2 import quadkey
from itertools import pairwise
from tqdm import tqdm

from process.trajectories.support_func import haversine_distance,  tile_to_str, get_quad_int_range, jaccard_similarity
import pandas as pd


def load_matching_links(traj_id: int, time_diff: int = 10):
    """
    Find all path that has the same trajectory as the given traj_id
    This is done through looking at all the matching quadkey
    Args:
        traj_id:

    Returns:

    """
    time_diff_seconds = time_diff * 60

    db = Database()
    sql = """
    select     q.link_id
    ,          q.quadkey
    ,          l.traj_id
    from       link_qk q
    inner join link l on l.link_id = q.link_id
    inner join (
        select     qk.quadkey
        ,          lk.ts_ini
        from       link_qk qk
        inner join link lk on lk.link_id = qk.link_id
        where      lk.traj_id = ?
    ) x on x.quadkey = q.quadkey
    where l.ts_ini - x.ts_ini < ? and l.ts_ini - x.ts_ini > -? ;
    """

    traj_df = db.query_df(sql, [traj_id, time_diff_seconds, time_diff_seconds])
    return traj_df


def load_match_trajectories(traj_id: int, top: int = 0.05, time_diff: int = 10):
    """
    Load all the trajectories that are similar to the given trajectory and filter the top percentage
    The similarity is calculated using Jaccard Similarity the more points that contains the same quadkey the more
    similar the trajectories are.
    :param traj_id:
    :param top:
    :param time_diff:
    :return:
    """
    df = load_matching_links(traj_id, time_diff)
    trajectories = df["traj_id"].unique()
    original_set = set(df["quadkey"].unique())
    df = df[df["traj_id"] != traj_id]
    traj_df = pd.DataFrame(trajectories, columns=["traj_id"])
    traj_df["similarity"] = traj_df["traj_id"].apply(
        lambda x: jaccard_similarity(original_set, set(df[df["traj_id"] == x]["quadkey"].values)))

    traj_df["percent_rank"] = traj_df["similarity"].rank(pct=True)
    filtered_df = traj_df[traj_df["percent_rank"] > (1.0 - top)]
    return filtered_df["traj_id"].values


def adjacent_quadkeys(loc, quad_level=19, adjacent_level=1):
    """
    Get the adjacent quadkeys of the given location base on the quad_level.
    The function convert loc to quadkey in specific level and then find the adjacent quadkeys
    Args:
        loc: Tuple of latitude and longitude
        quad_level: The level of the quadkey
        adjacent_level: The level of the adjacent quadkeys
    """
    quad = quadkey.from_geo(loc, quad_level)
    adjacent_quads = quad.nearby(adjacent_level)

    return adjacent_quads


def quad_cross_trajectories(location, timestamp, level=18, time_diff: int = 10, base_level=20):
    """
    Convert the current location to quadkey of specified level and find all trajectories that cross the quadkey and the adjacent quadkeys
    Different from the search_adjacent_quadkeys and return different result
taking
    """
    current_quad = str(quadkey.from_geo(location, level))
    time_diff_seconds = time_diff * 60
    shift = 64 - 2 * base_level

    min_quadint, max_quadint = get_quad_int_range(current_quad)
    min_quadint, max_quadint = min_quadint >> shift, max_quadint >> shift
    min_ts, max_ts = timestamp - time_diff_seconds, timestamp + time_diff_seconds
    db = VedDb()
    sql = """
        select l.traj_id
        , qk.quadkey
        , l.ts_ini
        from link_qk qk
            inner join link  l
                on l.link_id = qk.link_id
        where quadkey >= ? and quadkey < ? and ts_ini >= ? and ts_ini <= ?
    """

    df = db.query_df(sql, [min_quadint, max_quadint, min_ts, max_ts])
    return df


def trajectories_cross_df(df_traj, location, time_start):
    """
    Find the nearest link to the location and time_start. The function will calculate the distance between the origin and the link
    Deduplicate the result by only returning the link with the minimum distance
    :param df_traj:
    :param location:
    :param time_start:
    :return:
    """
    df_traj["distance"] = df_traj.apply(lambda x: haversine_distance(location, (x["ini_lat"], x["ini_lon"])), axis=1)
    df_traj["time_diff"] = df_traj.apply(lambda x: abs(x["ts_ini"] - time_start), axis=1)
    min_distance_indices = df_traj.groupby('traj_id')['distance'].idxmin()
    nearest_links = df_traj.loc[min_distance_indices, ['traj_id', 'link_id', 'distance', 'time_diff', 'ts_ini']]
    return nearest_links


def trajectories_cross_quad(quad: str, timestamp, time_diff: int = 10, base_level=20):
    """
    Search all the trajectories that cross the quadkey.

    """
    time_diff_seconds = time_diff * 60
    shift = 64 - 2 * base_level
    min_quadint, max_quadint = get_quad_int_range(quad)
    min_quadint, max_quadint = min_quadint >> shift, max_quadint >> shift
    min_ts, max_ts = timestamp - time_diff_seconds, timestamp + time_diff_seconds
    db = VedDb()

    sql = """
    select l.traj_id
    , l.link_id
    , qk.quadkey
    , l.ts_ini
    , l.ts_end
    , ini.latitude as ini_lat
    , ini.longitude as ini_lon

    from link_qk qk
        inner join link  l
            on l.link_id = qk.link_id
        inner join custom_signal ini
            on l.signal_ini = ini.signal_id
    where quadkey >= ? and quadkey < ? and ts_ini >= ? and ts_ini <= ?
        """

    df = db.query_df(sql, [min_quadint, max_quadint, min_ts, max_ts])
    return df


def search_adjacent_quadkeys(location, timestamp, level=18, time_diff: int = 10, base_level=20):
    """
    Convert the location to quadkey and find all the trajectories that cross the quadkey and the adjacent quadkeys.
    This function have the same functionality as quad_cross_trajectories but with different input and output.
    :param location:
    :param timestamp:
    :param level:
    :param time_diff:
    :param base_level:
    :return:
    """
    search_quads = adjacent_quadkeys(location, level)
    df = pd.DataFrame()
    for quad in search_quads:
        df1 = trajectories_cross_quad(quad, timestamp, time_diff, base_level)
        df = pd.concat([df, df1])

    return df.reset_index(drop=True)


def score_algorithm(df):
    # Will have start_distance, end_distance, start_time_diff, end_time_diff
    res = (df["start_distance"] + df["end_distance"]
           # + df["start_time_diff"] + df["end_time_diff"]
           )
    return res


def search_potential_similar_trajectories(start_lat, start_long, end_lat, end_long, start_time, end_time, level=18,
                                          time_diff=10):
    """
    Search for potential similar trajectories by the given start and end location and time. The function will find all the trajectories that closely match with the
    given trajectory. The function will return the dataframe with the score and rank of the trajectories.
    :param start_lat:
    :param start_long:
    :param end_lat:
    :param end_long:
    :param start_time:
    :param end_time:
    :param level:
    :param time_diff:
    :return:
    """
    df_start = search_adjacent_quadkeys((start_lat, start_long), start_time, level, time_diff)
    df_traj_start = trajectories_cross_df(df_start, (start_lat, start_long), start_time)
    df_traj_start = df_traj_start.add_prefix("start_")

    df_end = search_adjacent_quadkeys((end_lat, end_long), end_time, level, time_diff)
    df_traj_end = trajectories_cross_df(df_end, (end_lat, end_long), end_time)
    df_traj_end = df_traj_end.add_prefix("end_")

    df_final = pd.merge(df_traj_start, df_traj_end, left_on="start_traj_id", right_on="end_traj_id", how="inner")
    df_final = df_final[df_final["start_ts_ini"] < df_final["end_ts_ini"]]
    # df_final["score"] = df_final.apply(score)
    # df_final["rank"] = df_final.rank("score", ascending=True)

    return df_final


def search_potential_similar_trajectories_by_traj(traj_id: int, level=18, time_diff: int = 10,
                                                  score=score_algorithm):
    """
    Search potential similar trajectories by the given trajectory id. The function will find all the trajectories that closely match with the
    given trajectory. The function will return the dataframe with the score and rank of the trajectories.
    Similarity is calculated by the score function

    :param traj_id:
    :param level:
    :param time_diff:
    :param score:
    :return:
    """
    db = VedDb()

    sql = """
        select ini.latitude as ini_lat
        , ini.longitude as ini_lon
        , end.latitude as end_lat
        , end.longitude as end_lon
        , (strftime('%H', ini.time_stamp) * 3600) +(strftime('%M', ini.time_stamp) * 60) + strftime('%S', ini.time_stamp) AS start_time
        , (strftime('%H', end.time_stamp) * 3600) +(strftime('%M', end.time_stamp) * 60) + strftime('%S', end.time_stamp) AS end_time

        from trajectory t
            inner join custom_signal ini on t.ts_ini = ini.time_stamp
                and t.vehicle_id = ini.vehicle_id
                and t.trip_id = ini.trip_id
            inner join custom_signal end on t.ts_end = end.time_stamp
                and t.vehicle_id = end.vehicle_id
                and t.trip_id = end.trip_id
        where t.traj_id = ?

    """

    start_lat, start_long, end_lat, end_long, start_time, end_time = db.query(sql, [traj_id])[0]

    df_final = search_potential_similar_trajectories(start_lat, start_long, end_lat, end_long, start_time, end_time,
                                                     level, time_diff)
    df_final = df_final[df_final["start_traj_id"] != traj_id]
    df_final["score"] = df_final.apply(score, axis=1)
    df_final["rank"] = df_final["score"].rank(ascending=True)
    return df_final

