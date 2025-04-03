source("./load_libs.R")

d <- readRDS("../data/processed_data_1033.rds")

# 1. adjacency matrix based on distance:

d$edges %>% head()
head(d$nodes)
head(d$df)
# add the segment length [distance between points] as weight.
E(d$graph)$weight <- as.numeric(d$edges$segment_length)
# compute the new adjacency matrix with weight
d$WAdj <- (as_adjacency_matrix(d$graph, attr = "weight", sparse = T))
# look  at them
d$WAdj[1:10,1:10]
d$Adj[1:10,1:10]



# 2. centrality measures
d$nodes$deg_unweighted <- degree(d$graph)
d$nodes$deg_weighted <- strength(d$graph)
d$nodes$betweenes <- betweenness(d$graph)
d$nodes$closeness <- closeness(d$graph)
d$nodes$eigcentrality <- eigen_centrality(d$graph)$vector
d$nodes$eccentricity <- eccentricity(d$graph)  
d$nodes$harmonic <- harmonic_centrality(d$graph)

# 3. i