#! /usr/bin/env python3

import argparse
import json
import networkx as nx
import psycopg2
from psycopg2.extras import RealDictCursor

from util.get_pool import get_connection
from xranking import get_victory_graph, get_votes


def main(args):
    with open(args.credentials) as f:
        connection_details = json.load(f)
    conn = get_connection(connection_details)
    filters = {
        "education": args.education,
        "location": args.location,
        "gender": args.gender,
        "age_min": args.age_min,
        "age_max": args.age_max,
        "first_n": args.first_n,
        "ignore_dogs": args.ignore_dogs
    }

    ret = get_number_of_intransitive_users(conn, filters)
    print(ret)
    return ret


def get_number_of_intransitive_users(conn, filters):
    with conn.cursor() as cursor:
        cursor.execute(
            """SELECT voters.id
               FROM voters;""")
        voters = [row[0] for row in cursor]
    count_intransitive = 0
    voters_with_no_votes = 0
    for voter in voters:
        filters["voter"] = voter
        if not get_votes(conn, filters):
            voters_with_no_votes += 1
            continue
        cycles = nx.simple_cycles(get_victory_graph(conn, filters))
        try:
            cycles.__next__() # If there exists a cycle this will not throw an error
            count_intransitive += 1
        except:
            pass
    return count_intransitive, len(voters), voters_with_no_votes

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", required=True,
                        help="Path to the credentials for the postgres instance the data is stored in.")
    parser.add_argument("--output_format", default="python_array", choices=["python_array", "columns"])
    parser.add_argument("--flatten_ties", action="store_true")
    parser.add_argument("--remove_dogs")
    parser.add_argument("--education")
    parser.add_argument("--location")
    parser.add_argument("--gender")
    parser.add_argument("--age_min", type=int)
    parser.add_argument("--age_max", type=int)
    parser.add_argument("--first_n", type=int)
    parser.add_argument("--ignore_dogs", nargs="*")
    args = parser.parse_args()
    main(args)
