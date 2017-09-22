####
# Seattle Parcel-parcel
####

library(rgdal)
library(leaflet)
library(htmlwidgets)
library(rgeos)
library(raster)
library(maptools)

latlong = "+init=epsg:4326"

# directories
dir <- 'F:/UrbanDataProject/city_access/seattle_hackathon'
setwd(dir)
# read file
fn.parcels <- 'Master_Address_File' # https://data.seattle.gov/dataset/Master-Address-File/3vsa-a788
parcels <- readOGR(dsn = file.path(dir,'data','WGS84'), layer = fn.parcels, verbose = FALSE)
parcels <- spTransform(parcels,CRS(latlong))



# plot using leaflet
m <- leaflet()  %>% 
  # base groups
  addProviderTiles(providers$Esri.WorldImagery, group = 'Satellite') %>% 
  addProviderTiles(providers$OpenStreetMap, group = "OSM") %>%
  addProviderTiles(providers$Esri.WorldStreetMap, group = "Esri streets") %>%
  # overlay groups
  # addPolygons(data = city, group = 'city', fill=FALSE, smoothFactor=0.3, stroke = 2, color = 'Black') %>%
  addCircleMarkers(data = parcels, group = 'parcels', popup = 'parcel',
                   stroke = TRUE, fillOpacity = 0.8, color = 'Orange',
                   radius = 1,
                   clusterOptions = markerClusterOptions()) %>%
  addLayersControl(
    baseGroups = c('Satellite', "OSM", "Esri streets"),
    overlayGroups = c('city','parcels'),
    options = layersControlOptions(collapsed = FALSE)
  )  
# get centroid for plot
city.cent <- gCentroid(parcels)
m <- m %>% 
  setView(lng = city.cent$x, lat = city.cent$y, zoom = 11)

# save leaflet file
saveWidget(m, file = 'seattle_parcels.html', selfcontained = F)
