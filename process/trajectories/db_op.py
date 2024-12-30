from database import Database
from process.trajectories.gmapfunction import get_directions_detail, convert_to_next_weekday_time, encode_polyline
from itertools import pairwise
from tqdm import tqdm
from process.trajectories.support_func import get_qk_line
from pyquadkey2 import quadkey


def insert_single_signal(signal):
    db = Database()
    db.connect()
    sql  = """
    INSERT INTO custom_signal (
    user_id,
    time_stamp,
    latitude,
    longitude
    )
VALUES (
    %s, %s, %s, %s
);
    """
    db.execute_query(sql, signal)

    db.close_connection()

def insert_custom_signal(signals: list):
    db = Database()
    db.connect()
    sql  = """
    INSERT INTO custom_signal (
    user_id,
    time_stamp,
    latitude,
    longitude
    )
    VALUES (%s, %s, %s, %s);
    """
    db.execute_many(sql, signals)



def insert_signal_data_point(user_id, start_location, end_location, start_time):
    # Get the directions detail for the given start and end location
    start_time = convert_to_next_weekday_time(start_time.hour, start_time.minute)
    directions_detail, overall_polyline, estimated_end_time = get_directions_detail(start_location, end_location, start_time)

    # Insert the directions detail into the database
    insert_custom_signal([(user_id, point[1], point[0][0], point[0][1]) for point in directions_detail])

    return user_id, start_location, end_location, start_time, estimated_end_time, "".join(overall_polyline)

def get_signal_input_data(user_id):
    db = Database()
    db.connect()
    sql = """
        select user_id, latitude, longitude, time_stamp
        from custom_signal
        where user_id = %s
    """
    res = db.fetch_all(sql, [user_id])
    db.close_connection()
    return res

def add_trajectory_by_id(user_id: int):
    print("Populate trajectory by id {}".format(user_id))

    # sql = """
    # insert into trajectory (vehicle_id, trip_id)
    #     select distinct vehicle_id, trip_id from custom_signal;
    # """

    sql = """
    insert into trajectory (user_id, ts_ini, ts_end)
    select    tt.user_id
    ,         ( select min(s1.time_stamp)
                from custom_signal s1
                where s1.user_id = tt.user_id
                ) as ts_ini
    ,         ( select max(s2.time_stamp)
                from custom_signal s2
                where s2.user_id = tt.user_id
                ) as ts_end
    from (select distinct user_id from custom_signal) tt
    where tt.user_id = %s;"""

    db = Database()
    db.connect()
    db.execute_query(sql, [user_id])
    db.close_connection()

def load_trajectories_by_id(user_id:int):
    print("Load trajectories")

    sql = """
    select traj_id
    ,      user_id
    from   trajectory
    where user_id = %s;
    """
    db = Database()
    db.connect()
    res = db.fetch_all(sql, [user_id])
    db.close_connection()
    return res


def load_trajectory_points(traj_id:int):
    # Take a look at this to see it is necessary to order by daynum and ts instead of signal_id
    sql = """
    select   max(signal_id) as max_signal_id
    ,        latitude
    ,        longitude
    ,        min(time_stamp) as first_time_stamp
    from     custom_signal cs
        join trajectory t on cs.user_id = t.user_id
    where    t.traj_id = %s
    group by latitude, longitude
    order by max_signal_id;

    """
    db = Database()
    db.connect()
    res = db.fetch_all(sql, [traj_id])
    db.close_connection()
    return res

def insert_link(traj_id, signal_ini, signal_end, ts_ini, ts_end, quadkey = None):
    sql = """
    INSERT INTO link (traj_id, signal_ini, signal_end, ts_ini, ts_end, quadkey)
    VALUES (%s, %s, %s, %s, %s, %s)
    RETURNING link_id;
    """
    db = Database()
    db.connect()
    # db.execute_query(sql, [traj_id, signal_ini, signal_end, ts_ini, ts_end])

    res = db.fetch_one(sql, [traj_id, signal_ini, signal_end, ts_ini, ts_end, quadkey])
    db.connection.commit()
    db.close_connection()

    return res[0]

def insert_link_bulk(link_list):
    sql = """
        INSERT INTO link (traj_id, signal_ini, signal_end, ts_ini, ts_end, quadkey)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    db = Database()
    db.connect()

    db.execute_many(sql, link_list)


def insert_link_quadkeys(link_quadkey_density_list):
    sql = """
    insert into link_qk 
        (link_id, quadkey, density) 
    values 
        (%s, %s, %s)
    """
    db = Database()
    db.connect()
    db.execute_many(sql, link_quadkey_density_list)

    db.close_connection()

def populate_link_by_id(user_id:int):
    print("Populate links")

    shift = 64 - 2 * 20

    trajectories = load_trajectories_by_id(user_id)

    for traj_id, vehicle_id in tqdm(trajectories):
        points = load_trajectory_points(user_id)

        if len(points) > 1:
            for p0, p1 in pairwise(points):
                signal_ini = p0[0]
                signal_end = p1[0]
                ts_ini = p0[3]
                ts_end = p1[3]
                link_id = insert_link(traj_id, signal_ini, signal_end, ts_ini, ts_end)

                loc0 = (p0[1], p0[2])
                loc1 = (p1[1], p1[2])
                line = get_qk_line(loc0, loc1, 20)

                params = [(link_id, pt[0].to_quadint() >> shift, pt[1]) for pt in line]
                insert_link_quadkeys(params)


def populate_link_by_id_test(user_id: int):
    shift = 64 - 2 * 20

    trajectories = load_trajectories_by_id(user_id)
    for traj_id, vehicle_id in trajectories:
        points = load_trajectory_points(traj_id)

        link_list = []
        if len(points) > 1:
            for p0, p1 in pairwise(points):
                signal_ini = p0[0]
                signal_end = p1[0]
                ts_ini = p0[3]
                ts_end = p1[3]
                quad = quadkey.from_geo((p0[1], p0[2]), 20).to_quadint() >> shift
                link_list.append((traj_id, signal_ini, signal_end, ts_ini, ts_end, quad))

            insert_link_bulk(link_list)


def calculate_trajectory_by_id(user_id: int):

    user_id, start, end, start_time = get_user_info(user_id)
    insert_signal_data_point(user_id, start, end, start_time)
    add_trajectory_by_id(user_id)
    populate_link_by_id_test(user_id)
    # modify_link_table_timestamp_by_id(vehicle_id, trip_id)


def get_user_info(user_id):
    db = Database()
    db.connect()
    sql = """
    select user_id, home_latitude, home_longitude, work_latitude, work_longitude, departure_time
    from users
    where user_id = %s
    """
    user_id, home_lat, home_lon, work_lat, work_lon, departure_time = db.fetch_one(sql, [user_id])

    db.close_connection()
    return user_id, (home_lat, home_lon), (work_lat, work_lon), departure_time

def get_user_polyline(user_id: int) -> str:
    db = Database()
    db.connect()
    sql = """
    select  latitude, longitude
    from custom_signal
    where user_id = %s
    order by signal_id
    """
    res = db.fetch_all(sql, [user_id])
    db.close_connection()

    poly = encode_polyline(res)
    return poly