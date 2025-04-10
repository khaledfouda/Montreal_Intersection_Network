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
library(DT) # for table

# read data : montreal, nodes, and distances
dist_matrix <- readRDS( "../data/Montreal_distances_cluster.rds")
montreal <- readRDS("../data/Montreal_processed_app.rds")
nodes <- readRDS("../data/nodes_cluster_montreal_app.rds") %>%
  rename(id = node.id) %>%
  mutate(id = 1:nrow(.))


#source("../Reach_Montreal/prepare_data_for_app.R")

# Define UI for application that draws a histogram
ui <- fluidPage(

    # Application title
    titlePanel("Reach Montreal"),

    # Sidebar with a slider input for number of bins 
    sidebarLayout(
        sidebarPanel(
          #sidebarPanel(
            checkboxInput("use_weights", "With Weights?", FALSE),
            
            conditionalPanel(
              "input.use_weights == true",
              h4("Selected Locations and Weights"),
              DTOutput("weights_table")
            ),
          #),  
          
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
  selectedNodes <- reactiveValues(id = numeric(0),
                                  weight = numeric(0))

  
  
  # observe for reset button
  observeEvent(input$reset, {
    selectedNodes$id <- numeric(0)   
    selectedNodes$weight <- numeric(0)
    selectedNodes_df(data.frame(
      order = integer(0),
      id = numeric(0),
      weight = numeric(0),
      stringsAsFactors = FALSE
    ))
  })
  
  selectedNodes_df <- reactiveVal(
    # print(seq_along(selectedNodes$ids))
    # print(selectedNodes$id)
    # print(selectedNodes$weight)
    data.frame(
      order = integer(0),#seq_along(as.numeric(selectedNodes$id)),
      id = numeric(0),#as.numeric(selectedNodes$id),
      weight = numeric(0),#as.numeric(selectedNodes$weight),
      stringsAsFactors = FALSE
    )
  )
  
  # observe map clicks to update
  observeEvent(input$map_marker_click, {
    click <- input$map_marker_click
    if(is.null(click$id)) return()
    
    node_id <- as.numeric(click$id) # intersection id
    #current <- selectedNodes$ids
    
    df <- selectedNodes_df() # with the table values
    
    if(node_id %in% selectedNodes$id){
      # if selecting one that's already chosen, remove it
      #current <- setdiff(current, node_id)
      idx <- which(selectedNodes$id == node_id)
      selectedNodes$id <- selectedNodes$id[-idx]
      selectedNodes$weight <- selectedNodes$weight[-idx]
      df <- df[df$id != node_id]
      df$order <- seq_len(nrow(df))
      
    }else {
      # else, add it to the list
      #current <- c(current, node_id)
      selectedNodes$id <- c(selectedNodes$id, node_id)
      selectedNodes$weight <- c(selectedNodes$weight, 1.0)
      new_row <- data.frame(order = length(selectedNodes$id),
                            id = node_id, weight = 1, stringsAsFactors = F)
      print(new_row)
      print(df)
      df <- rbind(df, new_row)
    }
    selectedNodes_df(df)
    print(selectedNodes$ids)
    # selectedNodes(as.numeric(current))
    
    # update map for the seected nodes [label]
    # leafletProxy("map") %>% clearGroup("labels")
    # # add a label next to each selected node
    # if (length(selectedNodes$ids) > 0) {
    #   # Get data for selected nodes in the order they were selected
    #   sel_order <- seq_along(selectedNodes$id)
    #   sel_data <- nodes[nodes$id %in% selectedNodes$id, ]
    #   # Match the order of sel_data to the order of selectedNodes$ids
    #   sel_data <- sel_data[match(selectedNodes$id, sel_data$id), ]
    #   leafletProxy("map") %>% addLabelOnlyMarkers(
    #     lng = sel_data$lon, lat = sel_data$lat,
    #     label = sel_order %>% as.character(),  # label text as character numbers
    #     group = "labels",
    #     labelOptions = labelOptions(noHide = TRUE, direction = "top", textOnly = TRUE)
    #   )
    # }
  })
  
 
  
  # render the data table fothe weights
  output$weights_table <- DT::renderDT({
    selectedNodes_df()
  }, rownames = FALSE, editable = list(target = "cell",
                                       columns = 3
                                       #disable = list(columns = c(1,2))
                                       ),
  options = list(dom = 't', paging = FALSE))
  
  # Handle edits in the weights table: update the weight in reactive values
  observeEvent(input$weights_table_cell_edit, {
    info <- input$weights_table_cell_edit
    if (is.null(info)) return()
    df <- selectedNodes_df()
    i <- info$row
    j <- info$col
    
    if(j == 1){ # allow only edit third column
      val <- suppressWarnings(as.numeric(info$value))
      if (is.na(val) || val <= 0) {
        # If no value or invalid value entered, default to 1.0
        val <- 1.0
      }
      df$weight[i] <- val
      # Only update if editing the Weight column (column index 3 in our table)
      #if (j == 3) {
      selectedNodes$weight[i] <- val
      selectedNodes_df(df)
    }
  })
  
  
  
  reach_values <- reactive({
    # the following will compute the reach centrality for all nodes
    # test:
    #r = 1; L = sample(nodes$id, 5)
    df <- selectedNodes_df()
    r <- input$radius
    L <- df$id#as.numeric(selectedNodes$id)
    # convert radius from kilometers into  kilometers * 100
    r <- floor(as.numeric(r) * 100)
    req(dist_matrix)
    # compute the reach centrality
    if(length(L) == 0){
      reach_count <- max_dist <- rep(0L, nrow(nodes))
    } else {
      if(input$use_weights){
        w <- df$weight#as.numeric(selectedNodes$weight)
      }else
        w <- rep(1.0, length(L))
    w[is.na(w)|!is.numeric(w)] <- 1.0 # for missing values
    
    
    dist_submat <- dist_matrix[L,,drop=FALSE] # rows of selected
    reachable <- w * (dist_submat <= r)
    reach_count <- colSums(reachable, na.rm=T)
    reach_count <- as.integer(reach_count)
    # later, we will add weight which will sum W[j] instead of counting.
    #print(reach_count)
    # update the minimum r >>
    max_dist <- colMaxs(dist_submat) /100
    new_min_radius <- min(max_dist)
    new_max_radius <- new_min_radius + 5
    print(new_min_radius)
    # Update the radius slider's minimum limit 
    updateSliderInput(session, "radius", min = new_min_radius,
                      max = new_max_radius)
    
    # if current radius is below the new minimum, raise it to new minimum
    if (input$radius > new_max_radius || input$radius < new_min_radius) {
    updateSliderInput(session, "radius", value = new_min_radius + 1)
    
    }
    #--- done
   
    
    
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
                       fillColor = ~pal(reach), fillOpacity = ~ifelse(reach==0, 0.01, 0.2),
                       stroke = FALSE,
                       label = ~paste0("max dist = ", max_dist,
                                       "km: reach = ", reach,
                                       ": arrond = ", ARR_GCH),
                       group = "Nodes") %>%
      addCircleMarkers(data = nodes[selectedNodes$id,], # show elements in L in black
                       layerId = ~id,
                       radius = 5,
                       label = ~paste0("Node ", id, ": arrond = ", ARR_GCH),
                       fillColor = "black", fillOpacity = 1,
                       stroke = FALSE,
                       
                       group = "Nodes") %>%
      clearControls() %>%  # remove old legend
      addLegend(pal = pal, values = reach, title = "Reach (POIs within radius)",
                position = "bottomright", opacity = 1)
    
    
    
    
  })
  
}


# Run the application 
shinyApp(ui = ui, server = server)
