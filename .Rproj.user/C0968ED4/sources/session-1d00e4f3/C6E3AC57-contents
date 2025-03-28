library(sf) # spatial library
library(tidyverse)
library(magrittr) # extra for tidyverse R (%<>% pipe)
library(igraph) # for visualization
library(mapview) # visualization too
require(lwgeom) # to use the function st_startpoint
require(skimr) # to skim dataframes (summary)
library(parallel)
# read both datasets and prepare them

# 1. map of montreal
montreal <- st_read("./data/geobase_city_of_montreal.json")
#skimr::skim(montreal)

# 2. data provided by Aurelie. discard all but relevant variables
dat <- read.csv("./data/data_final.csv",sep = ';') %>%
  dplyr::select(x, y, pi, fi, acc)

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
mapview(montreal)
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
E(g)$weight <- edges$segment_length
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
           node_id_end %in% dat.sf$node_id) %>%
  distinct(node_id_start, node_id_end, .keep_all=T)
#----- checking area
edges.s$node_id_start %>% unique() %>% length()



#----- end of checking area
problematic <- edges.s %>% filter(!(node_id_end %in% dat.sf$node_id))
# the following part of the code didn't work so ignore it. an alternative method is used
# now the problem is that not all node_id_end are in our small dataset.
# however, we shoulding also ignore them since we could construct a walk such that
# the two nodes are eventuall connected start1 -> end1 = start2 -> end2 = our target
# get a list of these nodes:
dim(problematic)
# the following loop does this but unfortunately, no nodes are found.
for(i in seq_len(nrow(problematic))){
  
  edges %>%
    filter(problematic[i,]$node_id_end == node_id_start) -> t 
  target = -1
  for(j in seq_len(nrow(t))){
    if(t[j,]$node_id_end %in% dat.sf$node_id){
      target = t[j,]$node_id_end
      break
    }
    if(target!=-1) print(target)
  }
}
#-----------ignore the loop above----------------------------------------------
# alternatively, since about 40% of our network is not connected
# we will -temporarily, pick the nearest end notes for those nodes:
problematic$node_id_end2 = NA
# the following loop is in parallel because it's very slow.
# if you wish not to use it, run the commented loop after it, but it's really slow.
run_the_loop = FALSE # since it's slow, i saved it. this will just read the saved one.
if(run_the_loop){
  numCores <- min(detectCores() - 1, 12)
  cl <- makeCluster(numCores)
  clusterEvalQ(cl, { library(sf); library(dplyr) })
  clusterExport(cl, varlist = c("problematic", "dat.sf"), envir = environment())
  result <- parLapply(cl, seq_len(nrow(problematic)), function(i) {
    problem_i <- problematic[i, ]
    nearest_feature_id <- 
    st_nearest_feature(problem_i, filter(dat.sf, node_id != problem_i$node_id_start))
    dat.sf[nearest_feature_id, ]$node_id
  })
  stopCluster(cl)
  problematic$node_id_end2 <- unlist(result)
  # the results are saved here. 
  saveRDS(problematic, "./saved_objects/problematic.RDS")
}else
  problematic <- readRDS("./saved_objects/problematic.RDS")


# this loop is equivalent to the parallel loop above 
# for(i in seq_len(nrow(problematic))){
#   nearest_feature_id <- 
#     st_nearest_feature(problematic[i,],filter(dat.sf, geometry!=problematic[i,]$geometry) )
#   problematic$node_id_end2[i] <- dat.sf[nearest_feature_id,]$node_id
# }



# now  put them back in edges
# first get rid of geometry
edges.s %<>% select(-geometry) %>% st_set_geometry(NULL)
problematic %<>% select(-geometry) %>% st_set_geometry(NULL)
edges.s %>%
  left_join(
    problematic, by = c('node_id_start', 'node_id_end'),
  ) %>% 
  mutate(node_id_end = ifelse( is.na(node_id_end2),
                               node_id_end,
                               node_id_end2),
         segment_length.x = ifelse( is.na(node_id_end2),
                                    segment_length.x,
                                    segment_length.y)) %>% 
  select(-node_id_end2, -segment_length.y) %>%
  rename(segment_length = segment_length.x) ->
  edges.final
# finally our list of nodes is
nodes.s <-
  dat.sf %>%
  select(geometry, node_id) %>%
  distinct()
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
gsmall <- graph_from_data_frame(
  d = edges.final,
  directed = FALSE,
  vertices = nodes.s %>%
    st_set_geometry(NULL)  # If you want node attributes
)

sum(is.loop(gsmall)) 

# weight is the length
E(g)$weight <- edges.final$segment_length
# check the degree distribution 
deg <- degree(gsmall)
print("Grand Graph degree distribution:")
table(deg, useNA = "ifany") 

plot(gsmall, vertex.size = 5, vertex.label = NA)
tkplot(gsmall)

edges %>%
  filter(node_id_start %in% dat.sf$node_id) -> subs

subs %>% filter(!node_id_end %in% dat.sf$node_id)

## DONE!
coords <- st_coordinates(nodes.s)
plot(gsmall, layout = coords, vertex.size = 3, vertex.label = NA)
#--------------------------------------------------
# visualize
montreal %>%
  right_join(edges.final, by=c("node_id_start","node_id_end"),
             multiple="all", relationship="many-to-many") ->
  montreal_sub_sf

nodes_join <- nodes.s %>%
  mutate(x = st_coordinates(.)[,1],
         y = st_coordinates(.)[,2]) %>%
  select(node_id, x, y) %>%
  st_set_geometry(NULL)


edges_sf <- edges.final %>%
  left_join(nodes_join, by = c("node_id_start" = "node_id")) %>%
  rename(x1 = x, y1 = y) %>%
  left_join(nodes_join, by = c("node_id_end" = "node_id")) %>%
  rename(x2 = x, y2 = y) %>%
  rowwise() %>%
  mutate(geometry = st_sfc(st_linestring(matrix(c(x1, y1, x2, y2), ncol = 2, byrow = TRUE)), crs = st_crs(nodes.s))) %>%
  ungroup() %>%
  st_as_sf()
 
ggplot() +
  # Street segments
  geom_sf(data = montreal, color = "grey90", size = 0.3) +
  
  geom_sf(data = edges.s, color = "blue", size=.2, alpha=.1) +
  # Unique nodes from your network
  # (If you want to see them, e.g., as blue points)
  # geom_sf(data = nodes.sf, color = "blue", size = 0.6, alpha = 0.6) +
  # geom_sf(data = select(montreal_sub_sf,geometry), color = "blue", size = .2) +
  # Your 1864 intersections
  geom_sf(data = dat.sf, color = "red", size = .4) +
  
  # A simple theme
  theme_minimal() +
  theme_void()

