import metis

graph = []
with open('com-amazon.ungraph.txt') as fin:
    for cnt, line in enumerate(fin):
        splits = line.strip().split('\t')
        v1 = int(splits[0])
        v2 = int(splits[1])

        graph.append((v1,v2))
        graph.append((v2,v1))


G = metis.adjlist_to_metis(graph)
(edgecuts, parts) = metis.part_graph(G, 7)
print(len(parts))
print(parts[:20])

# print((edgecuts, parts))


'''
 def bfs_partition(self):
        import metis

        graph = []
        with open('com-amazon.ungraph.txt') as fin:
            for cnt, line in enumerate(fin):
                splits = line.strip().split('\t')
                v1 = int(splits[0])
                v2 = int(splits[1])

                graph.append((v1,v2))
                graph.append((v2,v1))


        G = metis.adjlist_to_metis(graph)
        (edgecuts, parts) = metis.part_graph(G, 7)
        for i, p in enumerate(parts):
            self.partition[str(i)] = p

'''