import argparse

import networkx
import psycopg2
from psycopg2.extras import RealDictCursor
import networkx as nx


#####################################################################################################################
# A note before you go any further: this is bad code.
# I wrote this in ~2 hours so that we would have something to present at the CSSH research compendium.
# TODO refactor, create general-purpose tools for analyzing/visualizing results, don't duplicate code from main.py
#####################################################################################################################

def get_matchups():
    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM dogs;""")
        ids = [x[0] for x in cursor]

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            WITH wins AS (
                SELECT
                  dog1,
                  dog2,
                  COUNT(*) AS win_count
                FROM
                  (SELECT
                     dog1_id AS dog1,
                     dog2_id AS dog2
                   FROM votes
                   WHERE result = 'win'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS')
                   UNION ALL
                   SELECT
                     dog2_id AS dog1,
                     dog1_id AS dog2
                   FROM votes
                   WHERE result = 'loss'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS') 
                   ) AS win
                GROUP BY dog1, dog2
            ), losses AS (
                SELECT
                  dog1,
                  dog2,
                  COUNT(*) AS loss_count
                FROM
                  (SELECT
                     dog1_id AS dog1,
                     dog2_id AS dog2
                   FROM votes
                   WHERE result = 'loss'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS')
                   UNION ALL
                   SELECT
                     dog2_id AS dog1,
                     dog1_id AS dog2
                   FROM votes
                   WHERE result = 'win'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS')
                   ) AS loss
                GROUP BY dog1, dog2
            ), ties AS (
                SELECT
                  dog1,
                  dog2,
                  COUNT(*) AS tie_count
                FROM
                  (SELECT
                     dog1_id AS dog1,
                     dog2_id AS dog2
                   FROM votes
                   WHERE result = 'tie'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS')
                   UNION ALL
                   SELECT
                     dog2_id AS dog1,
                     dog1_id AS dog2
                   FROM votes
                   WHERE result = 'tie'
                   -- AND submission_time < TO_TIMESTAMP('2019-04-9 23:30:20','YYYY-MM-DD HH24:MI:SS')
                   ) AS tie
                GROUP BY dog1, dog2
            )
            SELECT
              dog1,
              dog2,
              SUM(win_count :: FLOAT - loss_count :: FLOAT) / SUM(loss_count + win_count + tie_count) AS margin
            FROM losses
              LEFT OUTER JOIN ties USING (dog1, dog2)
              LEFT OUTER JOIN wins USING (dog1, dog2)
            GROUP BY dog1, dog2
            ORDER BY margin DESC;
            """)
        results = cursor.fetchall()

    mat = [[0 for _ in range(len(ids))] for _ in range(len(ids))]
    for result in results:
        dog1 = result["dog1"]
        dog2 = result["dog2"]
        margin = result["margin"]
        mat[ids.index(dog1)][ids.index(dog2)] = margin

    return ids, mat
def main(conn):

    def ranked_pairs_ordering(dog_ids, matchups):
        """ Implements https://en.wikipedia.org/wiki/Ranked_pairs

        :param dog_ids: List of N IDs of contestants
        :param matchups: An NxN matrix of win magnitude where matchups[i][j]
                         is the i'ths dogs win ratio against the j'th.
        :return: a copy of dog_ids ordered by ranked pairs outcome
        """
        if len(dog_ids) == 0:
            return dog_ids
        assert len(dog_ids) == len(matchups) == len(matchups[0])

        g = networkx.DiGraph()
        g.add_nodes_from(dog_ids)

        edges = []
        for i in range(len(matchups)):
            for j in range(len(matchups[0])):
                margin = matchups[i][j]
                if margin is not None and matchups[i][j] > matchups[j][i]:
                    edges.append((dog_ids[i], dog_ids[j], margin))

        for edge in edges:
            g.add_edge(edge[0], edge[1])

        return g

        # now, we know that g is a DAG, so we can use networkx's topological sort.
        # ordering = networkx.topological_sort(g)

    ids, mat = get_matchups()
    g = ranked_pairs_ordering(ids, mat)

    from graphviz import Digraph

    conn = psycopg2.connect(**{
        'host': "35.245.169.120",
        'user': "postgres",
        'password': "malawi-riflemen-dungeon",
        'dbname': "postgres"
    })

    ids, mat = get_matchups()
    g = ranked_pairs_ordering(ids, mat)

    # Copeland's algorithm
    # rankings = [(n, g.out_degree(n) - g.in_degree(n)) for n in g.nodes]
    # print(sorted(rankings, key=lambda x: x[1], reverse=True))


    from graphviz import Digraph

    cycles = list(nx.simple_cycles(g))
    d = Digraph(engine="circo", graph_attr={"size": "20,20!", "overlap": "false", "mindist": "2.0", "minlen": "1"})

    for idx, cycle in enumerate(cycles):
        for node in cycle:
            d.node(str(node), shape="square")

    for cycle, color in zip(cycles,
                            ['black', 'grey', 'red', 'green', 'blue', 'purple', 'pink', 'orange', 'brown'] * 2):
        for v, u in zip(cycle, cycle[-1:] + cycle[:-1]):
            d.edge(str(v), str(u), color=color)

    d.view()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dbname", required=True)
    args = parser.parse_args()
    conn = psycopg2.connect(host=args.host, user=args.user, password=args.password, dbname=args.dbname)
    main(conn)
