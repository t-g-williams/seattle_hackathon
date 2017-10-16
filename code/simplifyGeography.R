# simplify polygon files

input.dir <- 'F:/UrbanDataProject/seattle_hackathon/data/Council_Districts'
outname <- 'district_data.shp'
lyr <- 'Council_Districts'
thresh <- 0.0005

input.dir <- 'F:/UrbanDataProject/seattle_hackathon/data/Neighborhoods'
outname <- 'neighborhood_data.shp'
lyr <- 'Neighborhoods'
thresh <- 0.0005

input.dir <- 'F:/UrbanDataProject/seattle_hackathon/data/block_data'
outname <- 'block_data.shp'
lyr <- 'sea_blocks_wgs84'
thresh <- 0.5

library(rgeos)
library(sp)

setwd('F:/UrbanDataProject/seattle_hackathon/data/boundaries')

# import data
# sp.data <- SpatialLinesDataFrame()

sp.data <- readOGR(dsn=input.dir, layer = lyr, verbose = FALSE)
sp.data2 <- gSimplify(sp.data, thresh, topologyPreserve = TRUE)
sp.data2 <- as(sp.data2, "SpatialPolygonsDataFrame")
sp.data2@data <- sp.data@data
plot(sp.data)
lines(sp.data2, col='red')
# export
WriteShp(sp.data2, outname)


for (i in 1:100) {
  plot(sp.data[i,])
  lines(sp.data[i,], col='red')

  print(length(sp.data[i,]@polygons[[1]]@Polygons[[1]]@coords))
  print(length(sp.data2[i,]@polygons[[1]]@Polygons[[1]]@coords))
  print('...')
}



WriteShp <- function(spatial.dataframe,filename.shp){
  # write the data to shapefile
  
  # get the saving directory and filename
  save_dir <- getwd()
  
  # we save once with writeOGR so we have .proj file
  writeOGR(spatial.dataframe,save_dir, filename.shp, driver = 'ESRI Shapefile', overwrite_layer=TRUE)
  # we overwrite with writeSpatialShape so the col names aren't truncated
  writeSpatialShape(spatial.dataframe, paste(save_dir,'/',filename.shp,sep=''))
}

