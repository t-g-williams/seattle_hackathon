# Create a neartable for every parcel to every other parcel within x distance
library(pbapply)
library(rgdal)
library(raster)
library(rgeos)
library(spdep)
library(pracma)
library(maptools)
library(parallel)
library(httr)
library(FNN)
library(data.table)
library(sp)
library(tictoc)
library(doSNOW)
library(itertools)

# directories
# dir.work<- 'F:/UrbanDataProject/seattle_hackathon'
# dir.work<- '/home/tlogan/seattle_hackathon'
dir.work <- '/mnt/StorageArray/tlogan/sea_hackathon'
setwd(dir.work)

# variables
buf.dist <- 5000 # 5km
kNoCores.nt <- 15
chunk.factor <- 4

###
# Import data
###
print('importing the data...')

# import the city
fn.city <- 'sea_city'
city <- readOGR(dsn =  file.path(dir.work,'data'), layer = fn.city, verbose = FALSE)

# import blocks
fn.blocks <- 'sea_blocks_wgs84'
blocks <- readOGR(dsn = file.path(dir.work, 'data', 'block_data'), layer = fn.blocks, verbose = FALSE)
# convert to points
blocks <- SpatialPointsDataFrame(coords=coordinates(blocks), data=blocks@data, proj4string = CRS(proj4string(city)))

# transform to meters
blocks <- spTransform(blocks, CRS(proj4string(city)))


print('import complete')

###
# Define functions - code continues below
###

CreateNearTable <- function(blocks, buf.dist){
  # create the near table for querying
  print('generating near table (parallel)')
  
  # break data into dest and origin
  orig.pts <- blocks
  dest.pts <- blocks
  

  # Note: if size on cores exceeds available memory, increase the chunk factor (defined in constants at top)
  chunk.num <- kNoCores.nt * chunk.factor
  tic()
  # init the cluster
  cl <- makePSOCKcluster(kNoCores.nt, outfile = "log.txt")
  registerDoSNOW(cl)
  # init the progress bar
  pb <- txtProgressBar(max = 100, style = 3)
  progress <- function(n) setTxtProgressBar(pb, n)
  opts <- list(progress = progress)
  
  near.table <- foreach(m = isplitRows(orig.pts, chunks=chunk.num),
                        i = icount(chunk.num),
                        .combine='rbind',
                        .packages=c('rgeos', 'data.table', 'FNN', 'sp', 'rgdal'),
                        .export=c('chunkNearTable', 'SingleOriginNearPoints'),
                        .options.snow=opts) %dopar% {
                          chunkNearTable(m, i, dest.pts,  buf.dist)
                        }
  
  # close progress bar
  close(pb)
  # stop cluster
  stopCluster(cl) 
  toc()

  # save full near table
  saveRDS(near.table, file = 'neartable_full_blocks.rds')# save compressed object
  
  print('finished creating near table')
  # return(near.table)
}


chunkNearTable <- function(chunk.orig.pts, i, dest.pts, buf.dist) {
  # create near table for a chunk of origin points
  neartable.chunk <- lapply(1:nrow(chunk.orig.pts), function(j) SingleOriginNearPoints(chunk.orig.pts[j,],
                                                                                    dest.pts,
                                                                                    buf.dist))
  # Convert from list to data table
  neartable.chunk <- rbindlist(neartable.chunk)
  
  # save
  saveRDS(neartable.chunk, file = paste0('block_neartable_part_', i, '.rds'))
  
  return(neartable.chunk)
}


SingleOriginNearPoints <- function(p1, dest.pts, buf.dist){
  # for a bldg, find the destination points within x meters 
  # and, if necessary, points further away of sufficient area/crime threshold
  
  # create the buffer
  p1.buf <- gBuffer(p1, width = buf.dist)
  
  # get points within the buffer
  dests.p1 <- dest.pts[p1.buf,]
  
  # convert to lat/lon
  CRS.new <- CRS("+init=epsg:4326") # WGS 84
  p1 <- spTransform(p1, CRS.new)
  dests.p1 <- spTransform(dests.p1, CRS.new)
  
  # create near table for this origin
  near.table <- data.table('orig_id' = rep(p1$MAF_ID, times=length(dests.p1)),
                           'dest_id' = dests.p1$MAF_ID,
                           'orig_lon' = p1@coords[,1],
                           'orig_lat' = p1@coords[,2],
                           'dest_lon' = dests.p1@coords[,1],
                           'dest_lat' = dests.p1@coords[,2],
                           'rank' = rep(NA, times=length(dests.p1))
                           )
  
  return(near.table)
}


###
# Create near table
###
print('building near table')

CreateNearTable(blocks,  buf.dist)