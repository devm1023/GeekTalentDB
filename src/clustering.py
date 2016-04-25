__all__ = ['find_clusters']

from numpy.linalg import norm

def find_clusters(nclusters, vectors, labels=None, distance=None, merge=None):
    if distance is None:
        distance = lambda x, y: norm(x-y)
    if merge is None:
        merge = lambda x, y: 0.5*(x + y)
    if labels is None:
        groups = [set([i]) for i in range(len(vectors))]
    else:
        groups = [set([l]) for l in labels]
    vectors = vectors[:]
    dm = []
    for i in range(len(vectors)):
        dm.append([])
        for j in range(i):
            dm[-1].append(distance(vectors[i], vectors[j]))
    while len(vectors) > nclusters:
        nvectors = len(vectors)
        imin, jmin = min(((i, j) for i in range(nvectors) for j in range(i)),
                         key=lambda ij: dm[ij[0]][ij[1]])
        groups[jmin].update(groups[imin])
        del groups[imin]
        vectors[jmin] = merge(vectors[imin], vectors[jmin])
        del vectors[imin]
        del dm[imin]
        for i in range(imin, nvectors-1):
            del dm[i][imin]
        for j in range(jmin):
            dm[jmin][j] = distance(vectors[jmin], vectors[j])
        for j in range(jmin+1, nvectors-1):
            dm[j][jmin] = distance(vectors[jmin], vectors[j])

    return groups, vectors


if __name__ == '__main__':
    vectors = [-1.1, -1.0, 0.0, 1.1, 1.3]
    labels  = ['a',  'b',  'c', 'd', 'e']
    groups, merged_vectors = find_clusters(2, vectors, labels=labels)
    print(groups)
    print(merged_vectors)
    
