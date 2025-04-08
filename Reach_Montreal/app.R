#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    https://shiny.posit.co/
#
library(shiny)
library(leaflet)
library(sf)
library(dplyr)
library(matrixStats)

# read data : montreal, nodes, and distances
dist_matrix <- readRDS( "../data/Montreal_distances_weighted.rds")
montreal <- readRDS("../data/Montreal_processed_app.rds")
nodes <- readRDS("../data/nodes_montreal_app.rds") %>%
  rename(id = node.id)


#source("../Reach_Montreal/prepare_data_for_app.R")

# Define UI for application that draws a histogram
ui <- fluidPage(

    # Application title
    titlePanel("Reach Montreal"),

    # Sidebar with a slider input for number of bins 
    sidebarLayout(
        sidebarPanel(
            sliderInput("radius",
                        "Maximum Tolerable Distance (Kilometers):",
                        min = 0,
                        max = 5,
                        value = 2.5,
                        step = 0.5),
            helpText("Select intersections on the map ....."),
            actionButton("reset", "Reset Selection") # sets L to be empty
        ),

        # Show a plot of the generated distribution
        mainPanel(
          leafletOutput("map", height="600px")
        )
    )
    
)


server <- function(input, output, session){
  
  # store L
  selectedNodes <- reactiveVal(character(0))

  
  
  # observe for reset button
  observeEvent(input$reset, {
    selectedNodes(character(0)) # Reset selected nodes to empty
  })
  # observe map clicks to update
  observeEvent(input$map_marker_click, {
    click <- input$map_marker_click
    if(is.null(click$id)) return()
    node_id <- as.numeric(click$id) # intersection id
    #req(node_id)
    current <- selectedNodes()
    
    if(node_id %in% current){
      # if selecting one that's already chosen, remove it
      current <- setdiff(current, node_id)
    }else {
      # else, add it to the list
      current <- c(current, node_id)
    }
    # update the minimum r >>
    new_min_radius <- max(dist_matrix[current, current]) / 100
    new_max_radius <- new_min_radius + 5
    print(new_min_radius)
    # Update the radius slider's minimum limit 
    updateSliderInput(session, "radius", min = new_min_radius,
                      max = new_max_radius)
    
    # if current radius is below the new minimum, raise it to new minimum
    if (input$radius > new_max_radius || input$radius < new_min_radius) {
      updateSliderInput(session, "radius", value = new_min_radius)
    }
    #--- done
    selectedNodes(current)
  })
  
  reach_values <- reactive({
    # the following will compute the reach centrality for all nodes
    # test:
    #r = 1; L = sample(nodes$id, 5)
    
    r <- input$radius
    L <- selectedNodes()
    # convert radius from kilometers into  kilometers * 100
    r <- floor(as.numeric(r) * 100)
    req(dist_matrix)
    # compute the reach centrality
    if(length(L) == 0){
      reach_count <- max_dist <- rep(0L, nrow(nodes))
    } else {
    dist_submat <- dist_matrix[L,,drop=FALSE] # rows of selected
    max_dist <- colMaxs(dist_submat)
    reachable <- dist_submat <= r
    reach_count <- colSums(reachable, na.rm=T)
    reach_count <- as.integer(reach_count)
    # later, we will add weight which will sum W[j] instead of counting.
    }
    list(reach_count, max_dist)
  })

  output$map <- renderLeaflet({
    req(nodes) # we need nodes matrix to draw
    req(montreal)
    # base color pallette. 0 reach by default.
    init_pal <- colorNumeric(c("green","yellow","red"),c(0,1))
    leaflet(options = leafletOptions(preferCanvas = TRUE)) %>%  # use canvas for performance with many points
      addTiles() %>%
      # Add road network lines for context (Montreal roads in gray)
      addPolylines(data = montreal, color = "#888888", weight = 1, opacity = 0.6, group = "Roads") %>%
      # Add all intersection nodes as circle markers (initially all unselected, treat reach=0)
      addCircleMarkers(data = nodes,
                       layerId = ~id,  # use unique node id for referencing on click
                       radius = 5,
                       fillColor = ~init_pal(0), fillOpacity = 0.8,
                       stroke = FALSE,  # no border
                       group = "Nodes") %>%
      # Add an initial legend (0 as min, 1 as max for placeholder)
      addLegend(pal = init_pal, values = c(0,1), title = "Reach (POIs within radius)",
                position = "bottomright", opacity = 1)
    
  })
    
  # observe to update when user interacts
  observe({
    req(input$map_bounds)
    # req(reach_values())
    reach <- reach_values()
    max_dist <- reach[[2]]
    reach <- reach[[1]]
    rng <- range(reach, na.rm=T)
    if(diff(rng) == 0){ # if all values are identical - no points selected
      rng <- c(rng[1], rng[1]+1) # so that it works
    }
    # color palette
    pal <- colorNumeric(c("green", "yellow", "red"), rng)
    # Use leaflet proxy to update the existing map, instead of re-drawing it completely.
    # Update circle marker colors based on new reach values.
    # We'll map each node's fill color using the updated palette and the reach values.
    fillOpacity = 
    leafletProxy("map", data = nodes) %>%

      clearMarkers() %>%   # remove existing node markers
      addCircleMarkers(layerId = ~id,
                       radius = 5,
                       fillColor = ~pal(reach), fillOpacity = ~ifelse(reach==0, 0.0, 0.2),
                       stroke = FALSE,
                       label = ~paste0("Node ", id, ": reach = ", reach),  # tooltip showing reach value
                       group = "Nodes") %>%
      addCircleMarkers(data = nodes[selectedNodes(),], # show elements in L in black
                       layerId = ~id,
                       radius = 5,
                       fillColor = "black", fillOpacity = 1,
                       stroke = FALSE,
                       label = ~paste0("max dist = ", max_dist,
                                       ": reach = ", reach,
                                       ": arrond = ", montreal$ARR_DRT),  
                       group = "Nodes") %>%
      clearControls() %>%  # remove old legend
      addLegend(pal = pal, values = reach, title = "Reach (POIs within radius)",
                position = "bottomright", opacity = 1)
    
    
    
    
    # 
    # # nodes not highlighted
    # normalNodes <- nodes[ !(nodes$node.id %in% selectedNodes()), ]
    # # nodes highlighted
    # selectedNodes <- nodes[ nodes$node.id %in% selectedNodes(), ]
    # 
    # # we now update the map
    # leafletProxy("map") %>%
    #   clearMarkers() %>%
    #   # add unselected nodes
    #   addCircleMarkers(data=normalNodes, layerId = ~node.id,
    #                    radius = 6, stroke=FALSE,
    #                    fillColor = ~pal(reach), fillOpacity = 0.8) %>%
    #   # add selected nodes
    #   addCircleMarkers(data = selectedNodes, layerId = ~node.id,
    #                    radius = 6, color = "black", weight=2,
    #                    fillColor = ~pal(reach), fillOpacity = 1) #%>%
      # add legend later
    
  })
  
}


# Run the application 
shinyApp(ui = ui, server = server)
