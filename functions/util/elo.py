def compute_elo(results):
    """
    :param results: a list of (id1, id2, outcome) tuples, where outcome is one of "win", "loss", or "tie"
    :return: an array of tuples, like [[1, 2134], [2, 1987], ... [last_id, lowest_elo]]
    """
    # k represents a scaling factor that affects how quickly scores change
    # some dispute, see https://en.wikipedia.org/wiki/Elo_rating_system#Most_accurate_K-factor
    # we choose a low k-factor, because a photo's cuteness is unlikely to change over time
    k = 10

    elo = {}
    for id1, id2, outcome in results:
        # Get the current rankings, or 1200 if they haven't been seen yet
        rank1 = elo.get(id1, 1200)
        rank2 = elo.get(id2, 1200)

        # This implements a ratio such that dogs with a difference in elo of 400
        # have an expected outcome of 10:1 win:loss.
        expected_outcome = 1 / (1 + 10 ** ((rank2 - rank1) / 400))

        outcome1 = None
        outcome2 = None
        if outcome == "win":
            outcome1, outcome2 = (1, 0)
        if outcome == "loss":
            outcome1, outcome2 = (0, 1)
        if outcome == "tie":
            outcome1, outcome2 = (0.5, 0.5)

        elo[id1] = rank1 + k * (outcome1 - expected_outcome)
        elo[id2] = rank2 + k * (outcome2 - expected_outcome)

    return sorted([[id, elo_score] for id, elo_score in elo.items()], key=lambda r: r[1], reverse=True)
