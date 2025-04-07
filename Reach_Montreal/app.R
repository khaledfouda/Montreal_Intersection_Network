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
                        max = 30,
                        value = 5)
        ),

        # Show a plot of the generated distribution
        mainPanel(
          leafletOutput("map", height="600px")
           #plotOutput("distPlot")
        )
    )
)


server <- function(input, output, session){
  
  # store L
  selectedNodes <- reactiveVal(character(0))
  
  # observe map clicks to update
  observeEvent(input$map_marker_click, {
    node_id <- input$map_marker_click$id # intersection id
    req(node_id)
    current <- selectedNodes()
    
    if(node_id %in% current){
      # if selecting one that's already chosen, remove it
      current <- setdiff(current, node_id)
    }else {
      # else, add it to the list
      current <- c(current, node_id)
    }
    selectedNodes(current)
  })
  
  # the following will compute the reach centrality for all nodes
  req(G) # make sure that the graph is there
  r <- input$radius
  req(dist_matrix)
  # compute the reach centrality
  apply(dist_matrix, 1, function(dists) sum(dists <= r, na.rm=T) - 1)
  # later, we will add weight which will sum W[j] instead of counting.
}




# Define server logic required to draw a histogram
server <- function(input, output) {

    output$distPlot <- renderPlot({
        # generate bins based on input$bins from ui.R
        x    <- faithful[, 2]
        bins <- seq(min(x), max(x), length.out = input$bins + 1)

        # draw the histogram with the specified number of bins
        hist(x, breaks = bins, col = 'darkgray', border = 'white',
             xlab = 'Waiting time to next eruption (in mins)',
             main = 'Histogram of waiting times')
    })
}

# Run the application 
shinyApp(ui = ui, server = server)
