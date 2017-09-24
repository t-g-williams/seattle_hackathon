# simplify polygon files

input.dir <- 'F:/UrbanDataProject/seattle_hackathon/data/block_data'
lyr <- 'sea_blocks_wgs84'
input.fn <- 'F:/UrbanDataProject/seattle_hackathon/data/Council_Districts/Council_Districts'
input.fn <- 'F:/UrbanDataProject/seattle_hackathon/data/Neighborhoods/Neighborhoods'

library(rgeos)
library(sp)

# import data
# sp.data <- SpatialLinesDataFrame()

sp.data <- readOGR(dsn=input.dir, layer = lyr, verbose = FALSE)

sp.data2 <- gSimplify(sp.data, 0.5, topologyPreserve = TRUE)

# export



for (i in 1:100) {
  plot(sp.data[i,])
  lines(sp.data[i,], col='red')
  
  print(length(sp.data[i,]@polygons[[1]]@Polygons[[1]]@coords))
  print(length(sp.data2[i,]@polygons[[1]]@Polygons[[1]]@coords))
  print('...') 
}

