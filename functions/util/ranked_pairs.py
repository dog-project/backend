import networkx

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
    g.add_nodes_from(range(len(matchups)))

    edges = []
    for i in range(len(matchups)):
        for j in range(len(matchups[0])):
            margin = matchups[i][j]
            if margin is not None:
                edges.append((i, j, margin))

    edges.sort(key=lambda x: x[2], reverse=True)

    for edge in edges:
        g.add_edge(edge[0], edge[1])

        # find cycle throws an error if there is no cycle, in which case we want the edge
        try:
            networkx.find_cycle(g)
            g.remove_edge(edge[0], edge[1])
        except:
            pass

    # now, we know that g is a DAG, so we can use networkx's topological sort.
    ordering = networkx.topological_sort(g)
    return [dog_ids[x] for x in ordering]
