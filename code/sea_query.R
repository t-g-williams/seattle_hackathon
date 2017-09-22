
###
### Output origin, destination coordinate pairs for cities
### 
### INPUT:
###      - shapefile of origin points
###      - shapefile of destination points OR polygons
###      - cityName = string, e.g. 'baltimore', or 'detroit'
### OUTPUT:
###      - 
###      - shapefile containing same information
###
###      
### AUTHOR:
###      Tom Logan
###      Andrew Nisbet
###      Tim Williams
###
### NOTES:
###      A function for R 
### 

####
## Libraries
####
library(pbapply)
library(rgdal)
library(raster)
library(rgeos)
library(ggplot2)
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
library(sendmailR) 

####
## User defined inputs
####

# city and mode
name.city <- 'sea'
# name.orig <- 'bldg'
# name.dest <- 'parks'
# mode <- 'walk'

# workdir.root <- '/mnt/StorageArray/tlogan/research/urban_data/green_space_access'#picturedrocks
dir.work<- '/home/tlogan/seattle_hackathon'
# workdir.root <- 'F:/UrbanDataProject/green_space_access'#isle royale

# workdir <- file.path(workdir.root, 'data', name.city)

query.continue <- FALSE # if number >1 then a neartable and results table must already exist. if 1, any existing results will be overwritten
query.groupno <- 10e6 # number of queries to run between saving results

# constants
kNoCores <<- floor(detectCores() - 6)  # Calculate the number of cores
chunk.factor <<- 100
kPolygonBuffer <<- 5  # number of meters to shave off edge of polygon destinations
grid.size <<- 50  # meters of grid
grid.size.sparse <<- 200  # meters of grid
kNearestNeighbors <<- 10  # closest Euclidean number of destinations to report to origin (outside 2km zone)
cluster.dist.threshold <<- 20 # maximum distance that any building can be from the cluster centroid
buf.dist <<- 2000 # 2km from building
area.threshs <<- seq(0.2, 3, by=0.2) * 10000
crime.threshs <<- seq(0.25, 3, by=0.25)

# OSRM parameters:
# If you have an OSRM server already running, provide
# osrm.url and that will be used.
osrm.url <- 'http://localhost:5000' # NULL  # e.g. 'https://router.project-osrm.org'
# Otherwise a server can be created locally
# for Windows, just provide the path to an osm.pbf file, the path to a .lua
# routing profile, and the location to the osrm windows binaries included in
# this folder.
osm.pbf.path <- 'f:/UrbanDataProject/city_access/OSRM/walking/washington-latest.osm.pbf'
osrm.profile.path <- 'f:/UrbanDataProject/city_access/analysis/code/lib/osrm/profiles/foot.lua'
osrm.binary.path <- 'f:/UrbanDataProject/city_access/analysis/code/lib/osrm/'

# file names
# filename.dest <- paste(name.city,'_', name.dest, sep = "")
# filename.orig <- paste(name.city,'_',name.orig, '_clust', sep = "")
# filename.full <- paste(name.city,'_fullresults.csv', sep = "")


main <- function(){
  # changes to work directory
  setwd(dir.work) 
  # Check that not overwriting results
  # CheckExistingResults()
  
  # Start OSRM if needed.
  started.osrm.server <- FALSE
  if (is.null(osrm.url)) {
    print('No OSRM url provided, starting a server.')
    osrm.url <- StartOSRMServer(osm.pbf.path, osrm.profile.path, osrm.binary.path)
    started.osrm.server <- TRUE
  }
  
  # the.data <- ImportData()
    
  ### 
  
  # Add a loop here
  for (i in seq(38,60)){
  ###
  
    filename.neartable <- paste0('neartable_part_', i, '.rds')
    fn.save <- paste0('results_part_', i, '.rds')
    
    print(paste0('importing near table ', i, '...'))
    
    # import the neartable
    near.table <- readRDS(filename.neartable) # import it
    print('...near table imported')
    near.table$near_id <- rownames(near.table)
  
    # query OSRM to return time and distance
    tic()
    result.table <- Query_and_join(near.table, osrm.url, i)
    exectime <- toc()
    
    # write to csv
    WriteResults(result.table, fn.save)
    
  
  ###
  
  # End loop
  }
  ###
  
  
  # Cleanup server.
  if (started.osrm.server) {
    KillOSRMServer()
  }
  
  print('made it to the end!')
  NotifyCompletion()
}


CheckExistingResults <- function(){
  # does the filename exist
  results.exist <- file.exists(filename.full)
  # if overwriting, confirm
  if (!query.continue){
    if (results.exist){
      x <- readline("* \n CAUTION: You want to overwrite existing results. Please confirm by typing Y \n* \n")  
      if (x != 'Y'){
        stop('overwriting cancelled by user')
      }
    }
  } else {
    # if continuing, does the neartable exist
    neartable.exist <- file.exists(filename.neartable)
    if (!neartable.exist){
      stop('neartable doesnt exist, cannot continue')
    }
  }
}


StartOSRMServer <- function (osm.pbf.path, osrm.profile.path, osrm.binary.path) {
  
  # Only works on windows.
  platform <- .Platform$OS.type
  if (platform != 'windows') {
    print('Wanring: starting OSRM may only work on Windows, provide osrm.url for other platforms.')
  }
  
  # Create requried temp directory.
  dir.create('c:/temp', showWarnings = FALSE)
  
  # Extract the data with the required profile.
  print('Extracting the map data.')
  extract.path <- file.path(osrm.binary.path, 'osrm-extract.exe')
  cmd <- paste(extract.path, osm.pbf.path, '-p', osrm.profile.path, sep = " ")
  system2('cmd.exe', input = cmd)
  
  # Contract the graph.
  print('Contracting the route graph.')
  osrm.file.path <- gsub('\\.osm\\.pbf$', '.osrm', osm.pbf.path)
  contract.path <- file.path(osrm.binary.path, 'osrm-contract.exe')
  cmd <- paste(contract.path, osrm.file.path, sep = " ")
  system2('cmd.exe', input = cmd)
  
  # Start the server.
  print('Starting the OSRM server.')
  routed.path <- file.path(osrm.binary.path, 'osrm-routed.exe')
  cmd <- paste(routed.path, osrm.file.path, '--port', '5000', sep = " ")
  system2('cmd.exe', input = cmd, wait = FALSE)
  
  # Return the url
  url <- 'http://localhost:5000'
  print(paste('OSRM server running at', url, sep = " "))
  return(url)
  
}


KillOSRMServer <- function() {
  cmd <- paste('Taskkill', '/IM', 'osrm-routed.exe', '/F')
  system2('cmd.exe', input = cmd)
}


ImportData <- function(){
  print('importing the data...')
  # create building clusters if necessary
  orig.fileloc <- paste(workdir, '/buildings/', name.city, '_bldg_clust', sep='')
  if (!file.exists(paste(orig.fileloc, '/', name.city, '_bldg_clust.shp', sep=''))) {
    # Load building cluster functions
    source(paste(workdir.root, 'code/data_code/format/cluster_buildings.R', sep='')) 
    # create building clusters
    BldgClusterMain(name.city, kNoCores, getwd()) 
  }
  
  # open all of the files
  orig.shp = readOGR(dsn=orig.fileloc, layer = filename.orig, verbose = FALSE)
  dest.shp = readOGR(dsn=paste(workdir, '/parks', sep=''), layer = filename.dest,verbose = FALSE)
  
  the.data <- c(orig.shp, dest.shp)
  print('Data import complete')
  return (the.data)
}


ProcessPolygon <- function(dest.shp,grid.size, sparse){
  # if the destination is a polygon, then we need to process it
  
  # output filename
  parks.wd <- file.path(getwd(), 'parks')
  filename.shp <- paste(substr(name.city,1,3),'_',name.dest,'_pts_',grid.size, sep = "")
  
  if (file.exists(paste(parks.wd, '/', filename.shp, '.shp', sep=''))) {
    # import it
    print('importing gridded parks..')
    dest.pts <- readOGR(dsn=parks.wd, layer=filename.shp, verbose=FALSE)
  } else {
    # do the boundary gridding
    print('gridding the parks..')
    dest.class <- class(dest.shp)[1]
    if (dest.class == "SpatialPolygonsDataFrame"){
      # create a negative buffer into the polygon
      dest.shp.buf <- gBuffer(dest.shp,width=-kPolygonBuffer,byid=TRUE)
      
      # dest_ids are excluded 
      excld_dest <- setdiff(seq(1,length(dest.shp)),dest.shp.buf$park_id)
      
      # convert the polygon into the boundary line
      dest.line <- as(dest.shp.buf,"SpatialLinesDataFrame")
      
      # regularly spaced points along lines
      dest.pts <- CreateGrid(grid.size,dest.line)
      
      # if there are any polygons which disappear because of buffer
      if (! length(excld_dest)==0){
        # get the centroid for all excluded polygons 
        dest.cntr <- gCentroid(dest.shp[excld_dest,],byid=TRUE)
        # relate the polygon data with the centroid
        dest.cntr <- SpatialPointsDataFrame(dest.cntr,data=dest.shp[excld_dest,]@data)
        # merge boundaries with centroids
        dest.pts <- do.call('rbind', c(dest.pts, dest.cntr))
      }
      
    } else {
      dest.pts <- dest.shp
    }
    
    # add ids to the points
    if (sparse) {
      dest.pts$dest_id <- paste('s', seq(1,length(dest.pts)), sep='')
    } else {
      dest.pts$dest_id <- seq(1,length(dest.pts))  
    }
    
    # write a .shp for the pts
    old.wd <- getwd()
    setwd(parks.wd)
    WriteShp(dest.pts,filename.shp)
    print('Destination processing complete')
    setwd(old.wd)
  }
  
  return(dest.pts)
}


CreateGrid = function(grid.size,dest.line){
  ## takes the grid size input
  ## returns the polygons with gridded boundaries
  print('Boundary gridding begun')
  
  # return the spatial point dataframes
  
  # split sf into kNoCores subsets
  sf.nums <- seq(1,length(dest.line))
  sf.subindx <- split(sf.nums, ceiling(sf.nums/ceiling(length(dest.line)/kNoCores)))
  
  # for each park, turn the boundary line to series of points
  cl <- makeCluster(kNoCores,outfile="")
  clusterExport(cl, c("grid.size","dest.line", "GridLine"), envir=environment())
  clusterEvalQ(cl, c(library(raster), library(rgeos), library(sp)))
  spdf_list = parLapply(cl,sf.nums,function(j) GridLine(j))
  stopCluster(cl)
  
  # merge the spatialpointdataframes
  dest.pts <- rbind(spdf_list)

  return(dest.pts)
}


GridLine <- function(j){
  # intersect a single park polygon
  
  # park line
  p1 <- dest.line[j,]
  
  # number of points on line
  line.pts <- sum(SpatialLinesLengths(p1))/grid.size
  if(line.pts <= 2){line.pts = 2}
  
  # line to points
  dest.pt <- spsample(p1,line.pts,'regular')
  if (is.null(dest.pt)){dest.pt <- SpatialPoints(coords = coordinates(gCentroid(p1)), proj4string = CRS(proj4string(dest.line)))}
  
  # data to add
  df <- p1@data[rep(1,length(dest.pt)),]
  
  # convert to spatialPointsDataFrame
  dest.pt <- SpatialPointsDataFrame(dest.pt,data=df)
  
  return(dest.pt)
}


CreateNearTable <- function(the.data, area.threshs, crime.threshs, buf.dist, kNearestNeighbors){
  # create the near table for querying
  print('generating near table (parallel)')
  
  # break data into dest and origin
  orig.pts <- the.data[[1]]
  dest.shp <- the.data[[2]]
  
  # check projections
  if (proj4string(orig.pts) != proj4string(dest.shp)) {
    dest.shp <- spTransform(dest.shp, proj4string(orig.pts))
  }
  
  # if destination or origin is a polygon, process to a series of points
  dest.pts <- ProcessPolygon(dest.shp,grid.size, sparse=FALSE)
  dest.pts.sparse <- ProcessPolygon(dest.shp,grid.size.sparse, sparse=TRUE)
  
  # Note: if size on cores exceeds available memory, increase the chunk factor (defined in constants at top)
  chunk.num <- kNoCores * chunk.factor
  tic()
  # init the cluster
  cl <- makePSOCKcluster(kNoCores)
  registerDoSNOW(cl)
  # init the progress bar
  pb <- txtProgressBar(max = 100, style = 3)
  progress <- function(n) setTxtProgressBar(pb, n)
  opts <- list(progress = progress)
  
  near.table <- foreach(m=isplitRows(orig.pts, chunks=chunk.num),
                        .combine='rbind',
                        .packages=c('rgeos', 'data.table', 'FNN', 'sp', 'rgdal'),
                        .export=c('chunkNearTable', 'CreateBldgNearTable'),
                        .options.snow=opts) %dopar% {
                          chunkNearTable(m, dest.pts, dest.pts.sparse, area.threshs, crime.threshs, buf.dist, kNearestNeighbors)
                        }
  
  # close progress bar
  close(pb)
  # stop cluster
  stopCluster(cl) 
  toc()
  
  print('saving rds object...')
  tic()
  saveRDS(near.table, file = 'neartable_temp_unlisted.rds')# save compressed object
  toc()

  print('finished creating near table')
  near.table$near_id <- seq(1,nrow(near.table))
  # write it
  fwrite(near.table, file=filename.neartable, row.names=FALSE)
  
  return(near.table)
}


chunkNearTable <- function(chunk.orig.pts, dest.pts,dest.pts.sparse, area.threshs, crime.threshs, buf.dist, kNearestNeighbors) {
  # create near table for a chunk of origin points
  neartable.chunk <- lapply(1:nrow(chunk.orig.pts), function(j) CreateBldgNearTable(chunk.orig.pts[j,],
                                                                                    dest.pts,dest.pts.sparse, 
                                                                                    area.threshs, crime.threshs, 
                                                                                    buf.dist, kNearestNeighbors))
  # Convert from list to data table
  neartable.chunk <- rbindlist(neartable.chunk)
  
  return(neartable.chunk)
}


CreateBldgNearTable <- function(p1, dest.pts,dest.pts.sparse, area.threshs, crime.threshs, buf.dist, kNearestNeighbors){
  # for a bldg, find the destination points within x km 
  # and, if necessary, points further away of sufficient area/crime threshold
  
  # if there is no column name called 'area_m2' raise an error
  if (! 'area_m2' %in% colnames(dest.pts@data)){
    stop('no column named "area_m2" is in the park shapefile')
  }

  # create the buffer
  p1.buf <- gBuffer(p1, width = buf.dist)
  
  # get points within the buffer
  dests.p1 <- dest.pts[p1.buf,]
  
  # bldg park max area
  if (length(dests.p1) > 0) {
    max.area <- max(dests.p1$area_m2)   
  } else {
    max.area <- 0
  }
  
  # For points further than buf.dist, use the sparse destination points
  remaining.pts <- dest.pts.sparse
  
  ### check that we have sufficient area first
  while (max.area < tail(area.threshs, n=1)) {
    # find the next area threshold that is larger than all current parks
    min.area.to.query <- area.threshs[which(area.threshs > max.area)[1]]
    # include only the parks greater than this area - using remaining sparse destination points
    parks.to.include <- remaining.pts[which(remaining.pts$area_m2 > min.area.to.query),]
    # find the k nearest park points
    new.pts <- get.knnx(parks.to.include@coords, p1@coords, k=kNearestNeighbors)
    # add these destinations to our list
    dests.p1 <- rbind(dests.p1, remaining.pts[c(new.pts$nn.index),])
    # remove these from the remaining park pts
    remaining.pts <- remaining.pts[-c(new.pts$nn.index),]
    # check maximum area
    max.area <- max(dests.p1$area_m2)
  }
  
  ### now check the crime rates
  min.crime <- min(dests.p1$crime_dens, na.rm=TRUE)
  # exclude all parks with NA crime rates
  remaining.pts <- remaining.pts[-which(is.na(remaining.pts$crime_dens)),]
  
  while (min.crime > crime.threshs[1]) {
    # find the next crime threshold that is smaller than for all current parks
    max.crime.to.query <- crime.threshs[tail(which(crime.threshs < min.crime), n=1)]
    # exclude parks with crime greater than this
    parks.to.include <- remaining.pts[which(remaining.pts$crime_dens < max.crime.to.query),]
    # print(paste('next max dist: ', length(parks.to.include)))
    # find k nearest park points
    new.pts <- get.knnx(parks.to.include@coords, p1@coords, k=kNearestNeighbors)
    # add these destinations
    dests.p1 <- rbind(dests.p1, remaining.pts[c(new.pts$nn.index),])
    # remove these from the remaining park pts
    remaining.pts <- remaining.pts[-c(new.pts$nn.index),]
    # check minimum crime
    min.crime <- min(dests.p1$crime_dens, na.rm=TRUE)
  }
  
  # convert to lat/lon
  CRS.new <- CRS("+init=epsg:4326") # WGS 84
  p1 <- spTransform(p1, CRS.new)
  dests.p1 <- spTransform(dests.p1, CRS.new)
  
  # create near table for this origin
  near.table <- data.table('orig_id' = rep(p1$orig_id, times=length(dests.p1)),
                           'dest_id' = dests.p1$dest_id,
                           'park_id' = dests.p1$park_id,
                           'orig_lon' = p1@coords[,1],
                           'orig_lat' = p1@coords[,2],
                           'dest_lon' = dests.p1@coords[,1],
                           'dest_lat' = dests.p1@coords[,2],
                           'rank' = rep(NA, times=length(dests.p1)),
                           'dest_area' = dests.p1$area_m2,
                           'dest_inner' = dests.p1$inr_buf_m,
                           'dest_crime' = dests.p1$crime_dens)
  
  return(near.table)
}


Query_and_join <- function(near.table, osrm.url, i){
  # check if we have any results already
  int.results.file <- paste('results/', name.city, '_int_results_', i, '.rds', sep='')
  int.results.exists <- file.exists(int.results.file)
  if (int.results.exists) {
    print('importing existing query results...')
    prev.query.results <- readRDS(int.results.file)
    prev.query.data <- merge(near.table, prev.query.results, by='near_id')
    # find remaining query ids
    ids.to.query <- seq(1, nrow(near.table))[-prev.query.results$near_id]
    print(paste('...import complete. Still need to query', length(ids.to.query)/nrow(near.table)*100, '% of the data'))
  } else {
    # initialize intermediate output file
    temp <- data.frame(matrix(ncol=3, nrow=0))
    colnames(temp) <- c('near_id', 'distance', 'duration')
    saveRDS(temp, int.results.file) # xxxx sort this out
    # query all points
    ids.to.query <- seq(1, nrow(near.table))
  }

  # subset
  near.table <- near.table[ids.to.query,]
  
  # initialize result columns
  near.table[,'distance'] <- NA
  near.table[,'duration'] <- NA
  
  # Do the querying
  query.results <- DoQuerying(near.table,osrm.url,int.results.file)
  
  # combine previous query results if necessary
  if (int.results.exists) {
    query.results <- rbind(prev.query.data, query.results)
  }
  
  # sort the results based on near_id
  query.results <- query.results[order(query.results$near_id),]

  # print('saving query results rds..')
  # tic()
  # saveRDS(query.results, file = paste0(name.city,'_results.rds'))
  # toc()
  return(query.results)
}


DoQuerying <- function(near.table,osrm.url,int.results.file){
  # Query the routing algorithm using do parallel
  
  coord.table <- near.table[,c('near_id', 'orig_lon','orig_lat','dest_lon','dest_lat')]
  # Note: if size on cores exceeds available memory, increase the chunk factor (defined with constants at top)
  chunk.num <- kNoCores * chunk.factor
  tic()
  # init the cluster
  cl <- makePSOCKcluster(kNoCores)
  registerDoSNOW(cl)
  # init the progress bar
  pb <- txtProgressBar(max = 100, style = 3)
  progress <- function(n) setTxtProgressBar(pb, n)
  opts <- list(progress = progress)
  # conduct the parallelisation
  travel.queries <- foreach(m=isplitRows(coord.table, chunks=chunk.num),
                            .combine='cbind',
                            .packages=c('httr','data.table'),
                            .export=c("QueryOSRM", "GetSingleTravelInfo"), 
                            .options.snow = opts) %dopar% {
                              QueryOSRM(m,osrm.url,int.results.file)
                            }
  # close progress bar
  close(pb)
  # stop cluster
  stopCluster(cl) 
  toc()
  
  # unlist results
  travel.info = t(matrix(unlist(travel.queries),nrow=2))
  
  # add results to the near.table
  near.table[,'duration'] = travel.info[,1]
  near.table[,'distance'] = travel.info[,2]
  
  return(near.table)
}


QueryOSRM <- function(m,osrm.url,int.results.file){
  # query the routing algorithm to determine the travel distance and duration
  
  # number of queries
  query.no <- dim(m)[1]
  
  # do queries
  travel.queries = sapply(seq(1,query.no),function(i) GetSingleTravelInfo(m[i,], osrm.url))
  
  # save these results
  m$duration <- travel.queries[1,]
  m$distance <- travel.queries[2,]
  fwrite(m[,c('near_id','distance','duration')], int.results.file, append=TRUE)
  
  return(travel.queries)
}


GetSingleTravelInfo <- function(route, osrm.url){
  # query OSRM once and return the travel distance and duration
  # route <- near.table[j,]
  orig.lat <- route[,'orig_lat']
  orig.lon <- route[,'orig_lon']
  dest.lon <- route[,'dest_lon']
  dest.lat <- route[,'dest_lat']
  
  # get the url  
  query.url <- sprintf('%s/route/v1/walking/%.6f,%.6f;%.6f,%.6f?overview=false',osrm.url, orig.lon, orig.lat, dest.lon, dest.lat)
  
  # return the parsed json  
  result <- content(GET(query.url),"parsed")
  
  # extract time and distance
  duration <- result$routes[[1]]$legs[[1]]$duration
  distance <- result$routes[[1]]$legs[[1]]$distance
  
  result <- c('duration' = duration, 'distance' = distance)
  return(result)
}


WriteResults <- function(result.table, fn.save){
  
  print('writing results...')
  # write the full result table to rds
  dir.save <- file.path(getwd(), 'results')
  saveRDS(result.table, file = file.path(dir.save, fn.save))
  print('writing full results complete')
  # determine closest
  # closest.table <- result.table[result.table[ , .I[which.min(distance)], by = c('orig_id')]$V1]
  # write closest to csv
  # fwrite(closest.table,file=paste(save.dir, name.city,'_closest_results.csv', sep = ""), row.names=FALSE)
  # print('writing closest results complete')
  # convert both closest table to spatialdataframe
  # get the coordinates
  # closest.coords <- closest.table[,c('orig_lon','orig_lat')]
  # create the spatial points dataframes
  # closest.spdf <- SpatialPointsDataFrame(coords = closest.coords, data = closest.table, proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
  
  # write to shapefiles
  # filename.closest.shp <- paste(name.city, '_closest_results', sep = "")
  # WriteShp(closest.spdf,filename.closest.shp)
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


NotifyCompletion <- function() {
  # email function for finishing process

  # define server
  mailControl <- list(smtpServer="ASPMX.L.GOOGLE.COM") #, smtpPort = '465')

  from <- "<tom.mcleod.logan@gmail.com>"
  to <- paste0("<", 'tomlogan@umich.edu',">")
  sub <- paste(name.city, 'green space queries')
  msg <- paste('Dear Mr Tumnus, queries for', name.city, 'have completed.\n\nNarnia is saved.')

  sendmail(from = from, 
           to = to, 
           subject =  sub, 
           msg = msg,
           control = mailControl)
}

# runs the code if called from the terminal.
if(!interactive()){
  main()
}

main()
