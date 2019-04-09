import random

import pytest
from hypothesis import strategies as st
from hypothesis import given

from util.ranked_pairs import ranked_pairs_ordering


@st.composite
def ids_and_matrix(draw):
    n = draw(st.integers(min_value=2, max_value=5))

    lst = draw(st.lists(st.integers(), min_size=n, max_size=n, unique=True))
    mat = draw(st.lists(st.lists(st.floats(-1, 1), min_size=n, max_size=n),
                        min_size=n, max_size=n))

    for i in range(len(mat)):
        for j in range(i):
            top = mat[i][j]
            bot = mat[j][i]

            # ensure that for any i,j: m[i][j] + m[j][i] <= 1
            if top + bot > 1:
                if random.choice([True, False]):
                    mat[i][j] = random.random() * (1.0 - bot)
                else:
                    mat[j][i] = random.random() * (1.0 - top)

    return lst, mat


@given(ids_and_matrix())
def test_highest_magnitude_matchup_ordering_preserved(ids_and_matrix):
    ids, matrix = ids_and_matrix

    best_is = []
    best_js = []
    best_magnitude = -1

    for i in range(len(matrix)):
        for j in range(len(matrix[0])):
            if matrix[i][j] > best_magnitude:
                best_magnitude = matrix[i][j]
                best_is = [i]
                best_js = [j]
            elif matrix[i][j] == best_magnitude:
                best_is.append(i)
                best_js.append(j)

    ranks = ranked_pairs_ordering(ids, matrix)
    assert any([ranks.index(ids[dog_i]) <= ranks.index(ids[dog_j])
                for dog_i, dog_j in zip(best_is, best_js)])


@given(ids_and_matrix())
def test_output_ids_same_as_input(ids_and_matrix):
    ids, matrix = ids_and_matrix

    assert set(ids) == set(ranked_pairs_ordering(ids, matrix))


def test_correct_ranked_pairs():
    ids = [x for x in range(7)]
    #             0     1     2     3     4     5     6
    matrix = [[0.00, 0.00, 0.00, 0.00, 0.00, 0.94, 0.00],  # 0
              [0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70],  # 1
              [0.99, 0.00, 0.00, 0.00, 0.91, 0.00, 0.00],  # 2
              [0.89, 0.00, 0.90, 0.00, 1.00, 0.00, 0.97],  # 3
              [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.96],  # 4
              [0.00, 0.00, 0.93, 0.00, 0.00, 0.00, 0.00],  # 5
              [0.95, 0.00, 0.00, 0.92, 0.00, 0.98, 0.00]]  # 6

    true_ranks = [1,3,2,4,6,0,5]

    assert ranked_pairs_ordering(ids, matrix) == true_ranks
    assert ranked_pairs_ordering(ids, list(zip(*matrix))) == list(reversed(true_ranks))

