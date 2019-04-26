#! /usr/bin/env python3
"""
Computes a ranking for dogs, then writes the ranking to stdout in the specified format.
"""
# TODO filter by vote time
# filter by vote number, as in the number of votes a user has cast (first 20, only votes after the first 20, etc)


import argparse
import json
import random

import networkx
from psycopg2.extras import RealDictCursor

from util.elo import compute_elo
from util.get_pool import get_connection


def main(credentials, ranking_method, output_format, filters):
    with open(credentials) as f:
        connection_details = json.load(f)

    conn = get_connection(connection_details)

    rank_func = rank_function(ranking_method, filters)
    rank = rank_func(conn)

    # if flatten_ties:
    #     rank = [x for row in rank for x in row]

    print(format_rank(rank, output_format))


def filter_statement(conn, filters):
    with conn.cursor() as cursor:
        def mogrify(string, args):
            return bytes.decode(cursor.mogrify(string, args))

        f = ""
        if filters["education"]:
            f += mogrify("AND voters.education = %s", (filters["education"],))
        if filters["location"]:
            f += mogrify("AND voters.location = %s", (filters["location"],))
        if filters["gender"]:
            f += mogrify("AND voters.gender_identity = %s", (filters["gender"],))
        if filters["age_min"]:
            f += mogrify("AND voters.age >= %s", (filters["age_min"],))
        if filters["age_max"]:
            f += mogrify("AND voters.age <= %s", (filters["age_max"],))
        if filters["voter"]:
            f += mogrify("AND voters.id = %s", (int(filters["voter"]),))
        return f

def get_votes(conn, filters):
    """Returns a list of each vote, as an array of dictionaries of shape {"dog1":…, "dog2":…, "result":…} """

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            f"""
            SELECT dog1_id as dog1, dog2_id as dog2, result, submission_time, voter_id
            FROM votes LEFT JOIN voters ON (votes.voter_id = voters.id) WHERE 1 = 1
             AND votes.submission_time >= make_date(2019, 4, 3)
             AND votes.submission_time <= make_date(2019, 4, 22) 
             {filter_statement(conn, filters)}
             ORDER BY random();""")

        results = cursor.fetchall()

        if filters["first_n"]:
            n = filters["first_n"]
            voter_votes = {v["voter_id"]: [] for v in results}

            for result in results:
                voter_votes[result["voter_id"]].append(result)

            for v in voter_votes:
                voter_votes[v] = sorted(voter_votes[v], key=lambda x: x["submission_time"])[0:n]
            results = [v for vote_list in voter_votes.values() for v in vote_list]

        if filters["ignore_dogs"]:
            to_ignore = [int(dog_id_str) for dog_id_str in filters["ignore_dogs"]]
            results = [r for r in results if r["dog1"] not in to_ignore and r["dog2"] not in to_ignore]

    return results


def rank_function(ranking_method, filters):
    """Given a ranking method, returns a function that, given a connection to a database, will compute that
    ranking.

    :param ranking_method: one of "ranked_pairs", "copeland", "elo", "minimax", "win_ratio"
    :return: a ranking over the data in the database, with ties represented as nested arrays
    """

    def ranked_pairs(conn):
        matchups = get_victory_graph(conn, filters)

        g = networkx.DiGraph()
        g.add_nodes_from(matchups.nodes)

        edges = [(u, v, matchups.get_edge_data(u, v)) for (u, v) in matchups.edges]
        edges.sort(key=lambda x: x[2]["margin"], reverse=True)
        for u, v, data in edges:
            g.add_edge(u, v, **data)
            try:
                networkx.find_cycle(g)
                g.remove_edge(u, v)
            except:
                pass

        assert networkx.is_directed_acyclic_graph(g)
        return list(networkx.topological_sort(g))

    def copeland(conn):
        g = get_victory_graph(conn, filters)
        return sorted([(n, g.out_degree(n) - g.in_degree(n)) for n in g.nodes],
                      key=lambda x: x[1], reverse=True)

    def elo(conn):
        votes = get_votes(conn, filters)
        random.shuffle(votes)
        return compute_elo([(v["dog1"], v["dog2"], v["result"]) for v in votes])

    def minimax(conn):
        g = get_matchup_graph(conn, filters)

        # lowest_loss_rate = float("inf")
        # winner = None
        #
        # for n in g.nodes:
        #     greatest_margin = max(g.get_edge_data(u, v)["margin"] for u, v in g.in_edges(n))
        #     if greatest_margin < lowest_loss_rate:
        #         lowest_loss_rate = greatest_margin
        #         winner = n
        return sorted(g.nodes, key=lambda n: max(g.get_edge_data(u, v)["margin"] for u, v in g.in_edges(n)))

    def win_ratio(conn):
        matchups = get_matchups(conn, filters)

        wins_and_ties_vs_losses = {}
        for dog in matchups:
            wins_and_ties_vs_losses[dog] = [0, 0]
            for matchup in matchups[dog]:
                wins = matchups[dog][matchup]["wins"]
                losses = matchups[dog][matchup]["losses"]
                wins_and_ties_vs_losses[dog][0] += wins
                wins_and_ties_vs_losses[dog][1] += losses

        ratios = [(dog, (x[0] / ((x[0] + x[1]) or float("inf")))) for dog, x in wins_and_ties_vs_losses.items()]
        return sorted(ratios, key=lambda x: x[1], reverse=True)

    def win_tie_ratio(conn):
        matchups = get_matchups(conn, filters)

        wins_and_ties_vs_losses = {}
        for dog in matchups:
            wins_and_ties_vs_losses[dog] = [0, 0]
            for matchup in matchups[dog]:
                wins = matchups[dog][matchup]["wins"]
                ties = matchups[dog][matchup]["ties"]
                losses = matchups[dog][matchup]["losses"]
                wins_and_ties_vs_losses[dog][0] += wins + ties
                wins_and_ties_vs_losses[dog][1] += losses

        ratios = [(dog, (x[0] / ((x[0] + x[1]) or float("inf")))) for dog, x in wins_and_ties_vs_losses.items()]
        return sorted(ratios, key=lambda x: x[1], reverse=True)

    methods = {
        "ranked_pairs": ranked_pairs,
        "copeland": copeland,
        "elo": elo,
        "minimax": minimax,
        "win_ratio": win_ratio,
        "win_tie_ratio": win_tie_ratio,
    }

    return methods[ranking_method]


def get_matchups(conn, filters):
    results = get_votes(conn, filters)
    dogs = {result["dog1"] for result in results} | {result["dog2"] for result in results}
    matchups = {}
    for dog in dogs:
        matchups[dog] = {}
    for dog1 in dogs:
        for dog2 in dogs:
            if dog1 == dog2:
                continue
            matchups[dog1][dog2] = {"wins": 0, "losses": 0, "ties": 0}
            matchups[dog2][dog1] = {"wins": 0, "losses": 0, "ties": 0}

    for vote in results:
        dog1 = vote["dog1"]
        dog2 = vote["dog2"]
        result = vote["result"]
        if result == "win":
            matchups[dog1][dog2]["wins"] += 1
            matchups[dog2][dog1]["losses"] += 1
        if result == "loss":
            matchups[dog1][dog2]["losses"] += 1
            matchups[dog2][dog1]["wins"] += 1
        if result == "tie":
            matchups[dog1][dog2]["ties"] += 1
            matchups[dog2][dog1]["ties"] += 1

    return matchups


def get_matchup_graph(conn, filters):
    matchups = get_matchups(conn, filters)
    ids = matchups.keys()

    g = networkx.DiGraph()
    g.add_nodes_from(ids)
    for dog1 in matchups:
        for dog2 in matchups[dog1]:
            dog1to2 = matchups[dog1][dog2]
            total_votes_dog1to2 = sum(dog1to2.values())
            if total_votes_dog1to2 != 0:
                g.add_edge(dog1, dog2,
                           wins=dog1to2["wins"],
                           losses=dog1to2["losses"],
                           ties=dog1to2["ties"],
                           margin=dog1to2["wins"] / total_votes_dog1to2)

            dog2to1 = matchups[dog2][dog1]
            total_votes_dog2to1 = sum(dog2to1.values())
            if total_votes_dog2to1 != 0:
                g.add_edge(dog2, dog1,
                           wins=dog2to1["wins"],
                           losses=dog2to1["losses"],
                           ties=dog2to1["ties"],
                           margin=dog2to1["wins"] / total_votes_dog2to1)

    return g


def get_victory_graph(conn, filters):
    """every out-edge is a loss to another candidate, every in-edge is a win"""
    matchups = get_matchup_graph(conn, filters)
    edges_to_remove = []
    for u, v in matchups.edges:
        margin_u_to_v = matchups.get_edge_data(u, v)["margin"]
        margin_v_to_u = matchups.get_edge_data(v, u)["margin"]

        # u->v has higher win margin than v->u, keep lower margin
        if margin_u_to_v <= margin_v_to_u:
            edges_to_remove.append((u, v))

    matchups.remove_edges_from(edges_to_remove)
    return matchups


def format_rank(rank, fmt):
    if fmt == "python_array":
        return repr(rank)
    elif fmt == "columns":
        return "\n".join(",".join(str(y) for y in x) for x in rank)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", required=True,
                        help="Path to the credentials for the postgres instance the data is stored in.")
    parser.add_argument("--method", required=True,
                        choices=["ranked_pairs", "copeland", "elo", "minimax", "win_ratio", "win_tie_ratio"])
    parser.add_argument("--output_format", default="python_array", choices=["python_array", "columns"])
    # parser.add_argument("--flatten_ties", action="store_true")
    parser.add_argument("--remove_dogs")
    parser.add_argument("--education")
    parser.add_argument("--location")
    parser.add_argument("--gender")
    parser.add_argument("--age_min", type=int)
    parser.add_argument("--age_max", type=int)
    parser.add_argument("--voter", type=int)
    parser.add_argument("--first_n", type=int)
    parser.add_argument("--ignore_dogs", nargs="*")
    args = parser.parse_args()

    filters = {
        "education": args.education,
        "location": args.location,
        "gender": args.gender,
        "age_min": args.age_min,
        "age_max": args.age_max,
        "first_n": args.first_n,
        "ignore_dogs": args.ignore_dogs,
        "voter": args.voter
    }

    main(args.credentials, args.method, args.output_format, filters)
