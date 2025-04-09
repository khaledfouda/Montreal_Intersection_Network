source("./load_libs.R")
# read both datasets and prepare them

# 1. map of montreal

# link to download this data:
# https://donnees.montreal.ca/en/dataset/geobase
montreal <- st_read("../data/geobase_city_of_montreal.json")
#skimr::skim(montreal)

# 2. data provided by Aurelie. discard all but relevant variables
dat <- read.csv("../data/data_final.csv",sep = ';') #%>%
  #dplyr::select(x, y, pi, fi, acc)
# select all variables
skimr::skim(dat)
#--------------------------------------
# Step 2: Make both dataset have matching longitude+latitude (geometry in sf objects)
# type of json data
st_geometry_type(montreal,F)
st_crs(montreal$geometry)
# convert dat into sf
dat.sf <- st_as_sf(
  dat,
  coords = c("x","y"),
  crs = 2950 # the original one in the data based on trial&error
)
montreal %<>% st_transform(crs = 2950) # to match each other dataset
#---------------------------------------
# testing/checking the dat, geometry is correct and 
#   we didn't make a mistake above
mapview(dat.sf)
#mapview(montreal)
#-------------------------------------
# step 3 works on montreal dataset only.
# step 3.a: working on Montreal only. Extract all intersections as nodes.
# extract start/end locations of streets from montreal.
# geometry has many points (x,y) so we pick the min and max as start/end
montreal %<>%
  mutate(start_pt = lwgeom::st_startpoint(geometry),
         end_pt = lwgeom::st_endpoint(geometry))
# create an sf object with only those points, keep only distinct rows
# and add an id column
# note that node.sf is basically the network nodes (the montreal. Aurelie's, is
# "supposed to be" a small subset of it)
nodes.sf <- st_sf(
  geometry = c(
    st_geometry(montreal$start_pt),
    st_geometry(montreal$end_pt)),
  crs =  2950 #st_crs(montreal)
) %>%
  distinct() %>%
  mutate(node.id = row_number())
head(nodes.sf)
#------------------------------------------------------------
# step 3.b: since we later need that each start/end point to have a unique id, [which is
# what we did above], we now put this new id column back into montreal df.
# note that there will be an id for the start point and id for the end point.
montreal %<>%
  left_join(
    nodes.sf %>% rename(geom_node = geometry) %>%
      as.data.frame() %>% rename(node_id_start = node.id),
    by = c("start_pt" = "geom_node")
  ) %>% 
  left_join(
    nodes.sf %>% rename(geom_node = geometry) %>%
      as.data.frame() %>% rename(node_id_end = node.id),
    by = c("end_pt" = "geom_node")
  ) 
head(montreal)
# since the data is divided into road segment, the length is computed by the list
# of geometry points. Note to Xinyi, this could be use as weight. there are other
# distance functions too than st_length(e.g., st_area, st_perimeter). Check the help page.
montreal$segment_length <- st_length(montreal$geometry)
#---------------------------------------------------------------------------
# step 3.c: edges are street segments defined by the starting point (by its id),
# the end point and length of that segment as we defined it above.
edges <- montreal %>%
  dplyr::select(node_id_start, node_id_end, segment_length) %>%
  st_set_geometry(NULL)
# confirm that we didn't do a mistake above
all(edges$node_id_start == edges$node_id_end)
# define the grand graph. our graph should be only a subset of it.
g <- graph_from_data_frame(
  d = edges,
  directed = FALSE,
  vertices = nodes.sf %>%
    st_set_geometry(NULL)  # If you want node attributes
)
# weight is the length
#E(g)$weight <- edges$segment_length
# check the degree distribution 
deg <- degree(g)
print("Grand Graph degree distribution:")
table(deg)
#------------------------------------------------------------------------
# step 4:  Take the subset of graph G that we're interested in.
# associate each intersection in the dataset with its closest point in montreal.
dat.sf$node_id <- st_nearest_feature(dat.sf, nodes.sf)

# edges.s will contain the subset of edges that we want.

edges.s <- montreal %>%
  dplyr::select(node_id_start, node_id_end, segment_length, geometry) %>%
  filter(node_id_start %in% dat.sf$node_id & 
           node_id_end %in% dat.sf$node_id) #%>% 
  # distinct(node_id_start, node_id_end, .keep_all=T) 
head(edges.s)
dim(edges.s)
#----- [Temporarily until i fix the code]
# keep only nodes with known edges (1863 nodes reduced to 1033 nodes)
edges.s$node_id_end %>% unique() %>% length()
dat.sf %<>%
  filter(node_id %in% unique(c(edges.s$node_id_start,
                               edges.s$node_id_end))) %>%
  distinct()
dim(dat.sf)

# first get rid of geometry
edges.s %<>% select(-geometry) %>% st_set_geometry(NULL)
edges.final <- edges.s
# finally our list of nodes is
nodes.s <-
  dat.sf %>%
  select(geometry, node_id) %>%
  distinct()
dim(nodes.s)
#----------------------------------------------
# verification step
# we now have nodes.s and edges.final for our small network
# we hope that all nodes have at least one edges and on average 2.
# 1. verify that all our start-end edges are within the list of nodes
all.edge.nodes <- unique(c(edges.final$node_id_start,
                           edges.final$node_id_end))
print("The following nodes have no edges:")
nodes.s %>%
  filter(! (node_id %in% all.edge.nodes) )
print("The following edges are between at least one non-existent node")
edges.final %>%
  filter( !(node_id_start %in% nodes.s$node_id) |
            !(node_id_end %in% nodes.s$node_id) )

#------------------------------------------------
# we now build the small network
nodes.s %<>% arrange(node_id)
dat.sf %<>% arrange(node_id)
edges.final %<>% arrange(node_id_start, node_id_end)

gsmall <- graph_from_data_frame(
  d = edges.final,
  directed = FALSE,
  vertices = nodes.s %>%
    st_set_geometry(NULL)  # If you want node attributes
)

sum(is.loop(gsmall)) 

# weight is the length
#E(g)$weight <- edges.final$segment_length
# check the degree distribution 
deg <- degree(gsmall)
print("Grand Graph degree distribution:")
table(deg, useNA = "ifany") 
#--------------------------------------------------
# we create centrality measures on the large connected graph and then later
# we take a subset of these measures to our small dataset.
nodes.sf$deg_unweighted <- degree(g)
nodes.sf$betweenes_unweighted <- betweenness(g)
nodes.sf$closeness_unweighted <- closeness(g)
nodes.sf$eigcentrality_unweighted <- eigen_centrality(g)$vector
nodes.sf$eccentricity_unweighted <- eccentricity(g)  
nodes.sf$harmonic_unweighted <- harmonic_centrality(g)
#-------------
E(g)$weight <- as.numeric(edges$segment_length)
#------------
nodes.sf$deg_weighted <- strength(g)
nodes.sf$betweenes <- betweenness(g)
nodes.sf$closeness <- closeness(g)
nodes.sf$eigcentrality <- eigen_centrality(g)$vector
nodes.sf$eccentricity <- eccentricity(g)  
nodes.sf$harmonic <- harmonic_centrality(g)

head(nodes.sf)
saveRDS(nodes.sf, "../data/all_nodes_with_centrality.rds")
#---------------------------------------------------------
# add centrality measures to our subset
nodes.sf %>%
  filter(node.id %in% nodes.s$node_id) ->
  nodes.s

#---------------------------------------------------
# save data to disk:
#' what we need is: 
#' edges.final: <nodei,nodej,length_between> pairs
#' nodes.s: spatial dataframe <node_id, geometry point>
#' dat.sf: original dataframe with all variable + node_id and geometry point
#' gsmall: undirected, unweighted graph for edges and nodes
#' Adj: The adjacency matrix as a sparse object
#' note, i realized that intersection 4522 has 2 observations. i'll remove one.
Adj_unweighted =  as_adjacency_matrix(gsmall, sparse = TRUE)
E(gsmall)$weight <- as.numeric(edges.final$segment_length)
Adj_weighted =  as_adjacency_matrix(gsmall, attr = "weight", sparse = TRUE)
dat.sf %<>%
  distinct(node_id,.keep_all=T) 
data_to_keep <- list(edges=edges.final,
                     nodes= nodes.s,
                     df= dat.sf,
                     graph = gsmall,
                     adj_unweighted = Adj_unweighted,
                     adj_weighted = Adj_weighted
                     )
saveRDS(data_to_keep, "../data/processed_data_1033.rds")
write_csv(as.data.frame(nodes.s), "../data/nodes_1033.csv")
write_csv(as.data.frame(dat.sf), "../data/data_1033.csv")
write_csv(as.data.frame(edges.final), "../data/edges_1033.csv")
saveRDS(montreal, "../data/montreal_processed.rds")
#----------------------------------------------------
# some visualizations
#plot(gsmall, vertex.size = 5, vertex.label = NA)
#tkplot(gsmall)
#plot(gsmall, layout = st_coordinates(nodes.s),
#vertex.size = 3, vertex.label = NA)
#--------------------------------------------------
# main visualization
montreal %>%
  right_join(edges.final, by=c("node_id_start","node_id_end"),
             multiple="all", relationship="many-to-many") ->
  edges.to.print

 
ggplot() +
  geom_sf(data = montreal, color = "grey90", size = 0.3) +
  geom_sf(data = select(edges.to.print,geometry), color = "blue", size = 1.2) +
  geom_sf(data = nodes.s, color = "red", size = .3) +
  theme_minimal() +
  theme_void()
#------------------------------------------------------------------------
montreal %>%
  right_join(edges, by=c("node_id_start","node_id_end"),
             multiple="all", relationship="many-to-many") ->
  edges.to.print2


ggplot() +
  geom_sf(data = montreal, color = "grey90", size = 0.3) +
  geom_sf(data = select(edges.to.print2,geometry), color = "green", alpha=0.2, size = 1.2) +
  geom_sf(data = nodes.s, color = "red", size = .3) +
  geom_sf(data = select(edges.to.print,geometry), color = "blue", size = 1.2) +
  theme_minimal() +
  theme_void()

#-------------------------------------------------