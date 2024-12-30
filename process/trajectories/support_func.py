from pyquadkey2 import quadkey
import numpy as np
import numba
from numba import jit
import math


def jaccard_similarity(set0, set1):
    return len(set0 & set1) / len(set0 | set1)

def get_qk_line(loc0, loc1, level):
    qk0 = quadkey.from_geo((loc0[0], loc0[1]), level)
    qk1 = quadkey.from_geo((loc1[0], loc1[1]), level)

    ((tx0, ty0), _) = qk0.to_tile()
    ((tx1, ty1), _) = qk1.to_tile()

    line = smooth_line(tx0, ty0, tx1, ty1)
    return [(quadkey.from_str(tile_to_str(int(p[0]), int(p[1]), int(level))), p[2]) for p in line if p[2] > 0.0]


def get_qk_line_test(loc0, loc1, level):
    qk0 = quadkey.from_geo((loc0[0], loc0[1]), level)
    qk1 = quadkey.from_geo((loc1[0], loc1[1]), level)

    ((tx0, ty0), _) = qk0.to_tile()
    ((tx1, ty1), _) = qk1.to_tile()

    line = smooth_line(tx0, ty0, tx1, ty1)
    return [(quadkey.from_str(tile_to_str(int(p[0]), int(p[1]), int(level))), p[2]) for p in line if p[2] > 0.0]

def decimal_part(x):
    return x - int(x)


def smooth_line(x0: int, y0: int, x1: int, y1: int):
    steep = (abs(y1 - y0) > abs(x1 - x0))

    if steep:
        x0, y0 = y0, x0
        x1, y1 = y1, x1

    if x0 > x1:
        x0, x1 = x1, x0
        y0, y1 = y1, y0

    dx = x1 - x0
    dy = y1 - y0
    gradient = 1.0 if dx == 0.0 else dy / dx

    xpx11 = x0
    xpx12 = x1
    intersect_y = y0

    line = np.zeros((2 * (xpx12 - xpx11 + 1), 3))
    i = 0

    if steep:
        for x in range(xpx11, xpx12 + 1):
            i_y = int(intersect_y)
            f_y = decimal_part(intersect_y)
            r_y = 1.0 - f_y

            intersect_y += gradient

            line[i, 0] = i_y
            line[i, 1] = x
            line[i, 2] = r_y
            i += 1

            line[i, 0] = i_y + 1
            line[i, 1] = x
            line[i, 2] = f_y
            i += 1
    else:
        for x in range(xpx11, xpx12 + 1):
            i_y = int(intersect_y)
            f_y = decimal_part(intersect_y)
            r_y = 1.0 - f_y

            intersect_y += gradient

            line[i, 0] = x
            line[i, 1] = i_y
            line[i, 2] = r_y
            i += 1

            line[i, 0] = x
            line[i, 1] = i_y + 1
            line[i, 2] = f_y
            i += 1
    return line


def haversine_distance(coord1, coord2):
    """
    Calculate the great-circle distance between two points on the Earth. Convert the distance to metric units.
    """
    R = 6378137.0  # Earth radius in kilometers
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

@jit(nopython=True)
def tile_to_str(x, y, level):
    """
    Converts tile coordinates to a quadkey
    Code adapted from https://docs.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
    :param x: Tile x coordinate
    :param y: Tile y coordinate
    :param level: Detail leve;
    :return: QuadKey
    """
    q = ""
    for i in range(level, 0, -1):
        mask = 1 << (i - 1)

        c = 0
        if (x & mask) != 0:
            c += 1
        if (y & mask) != 0:
            c += 2
        q = q + str(c)
    return q

@jit(nopython=True)
def tile_to_qk(x, y, level):
    """
    Converts tile coordinates to a quadkey
    Code adapted from https://docs.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
    :param x: Tile x coordinate
    :param y: Tile y coordinate
    :param level: Detail leve;
    :return: QuadKey
    """
    q = numba.types.uint64(0)
    for i in range(level, 0, -1):
        mask = 1 << (i - 1)

        q = q << 2
        if (x & mask) != 0:
            q += 1
        if (y & mask) != 0:
            q += 2
    return q


def next_quadkey_same_level(quadkey_str):
    quadkey_list = list(map(int, quadkey_str))
    length = len(quadkey_list)

    # Increment the last digit
    quadkey_list[-1] += 1

    # Handle carry over
    for i in range(length - 1, -1, -1):
        if quadkey_list[i] == 4:
            quadkey_list[i] = 0
            if i > 0:
                quadkey_list[i - 1] += 1
        else:
            break

    next_quadkey = ''.join(map(str, quadkey_list))
    return next_quadkey

def get_quad_int_range(quadkey_str):
    next_quad = next_quadkey_same_level(quadkey_str)

    min_quad = quadkey.from_str(quadkey_str).to_quadint()
    max_quad = quadkey.from_str(next_quad).to_quadint()

    return min_quad, max_quad


def convert_radius_to_quad_level(radius):
    level_1_side = 20037508.342789244
    factor = level_1_side/radius
    output_level = math.log2(factor)/2
    output_level = min(output_level, 20)
    return output_level

