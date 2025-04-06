source("./load_libs.R")

d <- readRDS("../data/processed_data_1033.rds")
montreal <- readRDS("../data/montreal_processed.rds")





sum(!(d$edges$node_id_start %in% d$nodes$node.id))


# 3. i


ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = betweenes), 
          size = 1) +
  scale_color_viridis_c(option = "magma") +  # or whichever color scale
  labs(color = "Betweenness",
       title = "Intersections colored by Betweenness Centrality") +
  theme_minimal()


ggplot() +
  geom_sf(data = montreal, color = "grey90", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(size  = deg_weighted,  # bigger circles if total road length is large
              color = closeness),    # color scale for closeness
          alpha = 0.8) +
  scale_size(range = c(0.5, 4)) +  # adjust as needed
  scale_color_viridis_c() +
  labs(size  = "Weighted Degree",
       color = "Closeness",
       title = "Intersections by Weighted Degree (size) & Closeness (color)") +
  theme_minimal()


library(ggrepel)

top5_bet <- d$nodes %>% 
  top_n(5, wt = betweenes) # pick top 5 by betweenness

ggplot() +
  geom_sf(data = montreal, color = "grey90", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = betweenes), 
          size = 1) +
  geom_label_repel(
    data = top5_bet,
    aes(label = node.id, geometry=geometry),
    stat = "sf_coordinates",
    size = 3
  ) +
  scale_color_viridis_c() +
  labs(color = "Betweenness") +
  theme_minimal()


library(leaflet)


montreal_sf <- st_transform(montreal, 4326)
leaflet(montreal_sf) %>%
  addTiles() %>%  # adds a background tile layer (OpenStreetMap)
  addPolylines(
    # Label for hover:
    label = ~NOM_VOIE,
    # Popup for click:
    popup = ~NOM_VOIE,
    color = "blue",
    weight = 2,
    # Optional highlight style when hovered
    highlightOptions = highlightOptions(color = "red", weight = 3)
  )

head(d$df)

montreal %>%
  filter(start_pt %in% d$df$geometry[1]) %>% dim()
#---------------------------------------------------------
d$nodes %>% 
  as.data.frame() %>%
  rename(node_id = node.id) %>%
  left_join(as.data.frame(d$df), "node_id") ->
  dcomb
#---------------------------------------------------------
# Analysis for April 3rd
coords = st_coordinates(d$nodes)
nodes_xy <- d$nodes %>%
  mutate(x = coords[,1],
         y = coords[,2]) %>% as.data.frame()

d$edges %>%
  left_join(nodes_xy, by=c('node_id_start' ='node.id')) %>%
  left_join(nodes_xy, by=c('node_id_end' ='node.id'), suffix=c(".from",".to")) ->
  edges_nodes



  ggplot() +
  geom_segment(aes(x.from, y.from, xend=x.to, yend=y.to),
               color = "grey70", size = 0.3,
               data = edges_nodes) +
    geom_point(data = nodes_xy, aes(x=x, y=y), color="black", size=1) +
    # coord_fixed() +
    theme_minimal() +
    labs(title = "Montreal Network of Intersections with sensor data and edges")

#--
# compute correlation in dcomb
# closeness is highest for centrally located intersections.
# betweeness is highly right skewed but a few serve as key bridges between areas
#Intersections with high degree tend to also have high eigenvector centrality
#(they connect to other well-connected nodes), and often higher closeness
# (since being well-connected usually means shorter paths to others).
# intersections with higher degree or closeness tend to have higher vehicle volumes
  
