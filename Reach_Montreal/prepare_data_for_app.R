source("../src/load_libs.R")

montreal <- st_read("../data/geobase_city_of_montreal.json")
montreal %<>% st_transform(crs = 2950) %>%
  mutate(start_pt = lwgeom::st_startpoint(geometry),
         end_pt = lwgeom::st_endpoint(geometry))
montreal$length <- as.numeric(st_length(montreal))
head(montreal)

#------------------------------------------
# edges
data.frame(
  from = montreal$start_pt,
  to = montreal$end_pt,
  weight = montreal$length
) -> edges
#nodes
st_sf(
  geometry = c(
    st_geometry(montreal$start_pt),
    st_geometry(montreal$end_pt)),
  crs =  2950 #st_crs(montreal)
) %>%
  distinct() %>%
  mutate(node.id = row_number()) ->
  nodes
# graph
G <- graph_from_data_frame(
  edges,
  FALSE,
  nodes
)
E(G)$weight <- as.numeric(edges$weight)
# compute all shortest_paths between all nodes
compute_distances = FALSE
if(compute_distances){
  dist_matrix <- distances(G, mode="all", weights = E(G)$weight)
  dist_matrix <- dist_matrix / 1000 # store in Kilometers
  saveRDS(dist_matrix, "../data/Montreal_distances_weighted.rds")
}else
  dist_matrix <- readRDS( "../data/Montreal_distances_weighted.rds")

dist_matrix <- as.matrix(as.integer(round(dist_matrix * 100)))
mat_f <- fl

dist_matrix[1:10, 1:10]
#----------------------------------------------
library(shiny)
library(leaflet)

ui <- fluidPage(
  titlePanel("Montreal Reach Centrality Explorer"),
  sidebarLayout(
    sidebarPanel(
      sliderInput("radius", "Reach radius (meters):", 
                  min = 0, max = 5000, value = 1000, step = 100),
      helpText("Click intersections on the map to select or deselect them.")
      # (Future: inputs for custom weights could be added here)
    ),
    mainPanel(
      leafletOutput("map", height = "600px")
    )
  )
)


server <- function(input, output, session) {
  # Reactive value to store selected node IDs
  selectedNodes <- reactiveVal(character(0))
  
  # Observe map clicks to update selectedNodes
  observeEvent(input$map_marker_click, {
    node_id <- input$map_marker_click$id  # intersection ID of clicked marker
    req(node_id)
    current <- selectedNodes()
    # Toggle selection
    if (node_id %in% current) {
      current <- setdiff(current, node_id)
    } else {
      current <- c(current, node_id)
    }
    selectedNodes(current)
  })
  
  # Reactive expression to compute reach centrality for all nodes given radius r
  reach_values <- reactive({
    req(g)  # ensure the graph is available
    r <- input$radius
    # Compute shortest path distances from all nodes (matrix form)
    dist_matrix <- distances(g, mode = "all", weights = E(g)$weight)
    # Calculate reach count for each node (excluding itself)
    apply(dist_matrix, 1, function(dists) sum(dists <= r, na.rm = TRUE) - 1)
    # (If custom weights W were in use, sum W[j] for dists <= r instead of counting)
  })



# Render the initial map with road lines and intersection markers
output$map <- renderLeaflet({
  # Get coordinates for intersections (as an sf points data frame)
  # Derive node coordinates by extracting geometry of intersections from roads_sf:
  nodes_sf <- st_as_sf( 
    # combine start and end points of each road segment
    rbind(
      st_sf(id = roads_sf$DEB_GCH, geom = st_startpoint(roads_sf)),
      st_sf(id = roads_sf$FIN_GCH, geom = st_endpoint(roads_sf))
    ), 
    crs = st_crs(roads_sf)
  )
  nodes_sf <- nodes_sf[!duplicated(nodes_sf$id), ]  # unique intersections by ID
  
  leaflet() %>%
    addTiles() %>%
    addPolylines(data = roads_sf, color = "#999999", weight = 1, opacity = 0.5) %>%
    addCircleMarkers(data = nodes_sf, layerId = ~id,  # use intersection ID for click
                     radius = 5, color = "blue", fillOpacity = 0.7)
})

# Observer to update map when reach values or selection changes
observe({
  req(reach_values())
  nodes_sf$reach <- reach_values()           # attach computed reach values
  # Define color palette for reach
  pal <- colorNumeric(palette = c("green","yellow","red"), domain = nodes_sf$reach)
  # Prepare base and highlight styles
  normalNodes <- nodes_sf[ !(nodes_sf$id %in% selectedNodes()), ]
  selectedNodes_sf <- nodes_sf[ nodes_sf$id %in% selectedNodes(), ]
  
  leafletProxy("map") %>%  # update existing map
    clearMarkers() %>%
    # Add unselected nodes with color fill
    addCircleMarkers(data = normalNodes, layerId = ~id,
                     radius = 6, stroke = FALSE,
                     fillColor = ~pal(reach), fillOpacity = 0.8) %>%
    # Add selected nodes on top with a black outline
    addCircleMarkers(data = selectedNodes_sf, layerId = ~id,
                     radius = 8, color = "black", weight = 2,
                     fillColor = ~pal(reach), fillOpacity = 1.0) %>%
    # Add legend for reach values
    addLegend(pal = pal, values = nodes_sf$reach, title = paste("Reach (r =", input$radius, "m)"),
              position = "bottomright")
})
}
