var http = require('http');

var server = http.createServer(function(request, response) {




var lat = 47.609037;
var lng = -122.334544;

var citySelector = document.getElementById('citySelector');

var dataSelector = document.getElementById('dataSelector');

var urlDistricts = '../assets/data/district_data.json';
var urlNeighborhoods = '../assets/data/neighborhood_data.json';

var dataUrl = urlDistricts;



// var baseColors = {
//   zero: ['#e5a2a2', '#e57c7c', '#e55a5a', '#e53b3b', '#e52020', '#e50000'],
//   fifty: ['#e5e5a2', '#e5e57c', '#e5e55a', '#e5e53b', '#e5e520', '#e5e500'],
//   seventy: ['#a2e5a2', '#7ce57c', '#5ae55a', '#3be53b', '#20e520', '#00e500'],
//   ninety: ['#a2a2e5', '#7c7ce5', '#5a5ae5', '#3b3be5', '#2020e5', '#0000e5']
// };

// var dataType = '0to49';


var currentSet;

function reset() {
  // initialization
  var data = currentSet.data.filter(d => d.id === 'all')[0];
  updateCharts(data);
  updateNumbers(data, currentSet);
}

var dataSeattle = axios.get(dataUrl).then(function(response){
  mapData(response);
  currentSet = response;
  reset();
});

function deselectDistricts(){
  map.eachLayer(function(layer){
    if(layer.options.type === 'dataPoint'){
      layer.setStyle({fillOpacity: 0.3});
    }
  });
}

function setDistrict(district){
  district.setStyle({fillOpacity: 0.7});
}

function clearMap(){
  map.eachLayer(function(layer){
    if(layer.options.type === 'dataPoint'){
      map.removeLayer(layer);
    }
  });
}

/*
Andy,
The following function is the function called in the event handlers that can be used for hooking in district selection to the rest of the chart number crunching things that happen elsewhere in the site
*/

function useDistrictData(districtId, district){
  // DO COOL STUFF HERE WITH THE DATA
  // console.log("District ID: " + districtId);
  // console.log("District Mean: " + district.options.mean);
  var data = currentSet.data[+districtId-1];
  updateCharts(data);
  updateNumbers(data, currentSet);
}

function onMapClick(event){
  deselectDistricts();
  if(event.target.options.id){
    setDistrict(event.target);
    useDistrictData(event.target.options.id, event.target);
  }
}

// Dropdown menu event handler
dataSelector.onchange = function(event){
  clearMap();
  var newMapType = event.target.value;
  console.log(newMapType);
  if(newMapType === "neighborhoods"){
    dataUrl = urlNeighborhoods;
    dataSeattle = axios.get(dataUrl).then(function(response){
      mapData(response);
      currentSet = response;
      reset();
    });
  } else if(newMapType === "districts"){
    dataUrl = urlDistricts;
    dataSeattle = axios.get(dataUrl).then(function(response){
      mapData(response);
      currentSet = response;
      reset();
    });
  }
}

// dataSelector.onchange = function(event){
//   deselectDistricts();
//   map.eachLayer(function(layer){
//     if(layer.options.type === 'dataPoint' && layer.options.id === dataSelector.value){
//       layer.setStyle({fillOpacity: 0.7});
//       useDistrictData(layer.options.id, layer);
//     }
//   });
// }

var map = L.map('map').setView([lat, lng], 12);



L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoiYWFyb25rYnJvd24iLCJhIjoiY2o3eHlrdzRjN2VoODMybnBvYmhqczBmZiJ9.HggNGKHEV6mFUV0PKbTlVA', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'your.mapbox.access.token'
}).addTo(map);

function getColor(value){
    //value from 0 to 1
    var hue = ((1 - value) * 120).toString(10);
    return ["hsl(",hue,",100%,50%)"].join("");
}

function mapData(dataSet){

  dataSet.data.map(function(data){
    var mean = data.score_mean;
    var max = 40;
    console.log(mean);
    var color = getColor(mean / max);
    console.log(mean / max);
    var polygon = L.polygon(data.coordinates, {
      color: color,
      opacity: 0.5,
      fillColor: color,
      fillOpacity: 0.3,
      type: 'dataPoint',
      score49: data.score0_49,
      score69: data.score50_69,
      score89: data.score70_89,
      score100: data.score90_100,
      mean: data.mean,
      bubblingMouseEvents: false,
      id: data.id
    }).addTo(map);
    polygon.on('click', onMapClick);
    map.on('click', onMapClick);
  });
}

// function mapData(dataSet, dataType){

//   dataSet.data.map(function(data){
//     console.log("Scores: " + data.score0_49 + ' ' + data.score50_69 + ' ' + data.score70_89 + ' ' + data.score90_100);
//     var color = '#000';
//     var popSum = data.score0_49 + data.score50_69 + data.score70_89 + data.score90_100;
//     if(dataType === '0to49'){
//       if(data.score0_49 / popSum > 0.99){
//         color = baseColors.zero[5];
//       } else if(data.score0_49 / popSum > 0.95){
//         color = baseColors.zero[4];
//       } else if(data.score0_49 / popSum > 0.9){
//         color = baseColors.zero[3];
//       } else if(data.score0_49 / popSum > 0.8){
//         color = baseColors.zero[2];
//       } else if(data.score0_49 / popSum > 0.7){
//         color = baseColors.zero[1];
//       } else {
//         color = baseColors.zero[0];
//       }
//     } else if(dataType === '50to69'){
//       if(data.score50_69 / popSum < 0.05){
//         color = baseColors.fifty[5];
//       } else if(data.score50_69 / popSum < 0.1){
//         color = baseColors.fifty[4];
//       } else if(data.score50_69 / popSum < 0.15){
//         color = baseColors.fifty[3];
//       } else if(data.score50_69 / popSum < 0.2){
//         color = baseColors.fifty[2];
//       } else if(data.score50_69 / popSum < 0.25){
//         color = baseColors.fifty[1];
//       } else {
//         color = baseColors.fifty[0];
//       }
//     } else if(dataType === '70to89'){
//       if(data.score70_89 / popSum < 0.01){
//         color = baseColors.seventy[0];
//       } else if(data.score70_89 / popSum < 0.025){
//         color = baseColors.seventy[1];
//       } else if(data.score70_89 / popSum < 0.05){
//         color = baseColors.seventy[2];
//       } else if(data.score70_89 / popSum < 0.075){
//         color = baseColors.seventy[3];
//       } else if(data.score70_89 / popSum < 0.1){
//         color = baseColors.seventy[4];
//       } else {
//         color = baseColors.seventy[5];
//       }
//     } else {
//       if(data.score90_100 / popSum < 0.005){
//         color = baseColors.ninety[0];
//       } else if(data.score90_100 / popSum < 0.01){
//         color = baseColors.ninety[1];
//       } else if(data.score90_100 / popSum < 0.015){
//         color = baseColors.ninety[2];
//       } else if(data.score90_100 / popSum < 0.025){
//         color = baseColors.ninety[3];
//       } else if(data.score90_100 / popSum < 0.05){
//         color = baseColors.ninety[4];
//       } else {
//         color = baseColors.ninety[5];
//       }
//     }
//     var polygon = L.polygon(data.coordinates, {
//       color: color,
//       opacity: 0.5,
//       fillColor: color,
//       fillOpacity: 0.3,
//       type: 'dataPoint',
//       score49: data.score0_49,
//       score69: data.score50_69,
//       score89: data.score70_89,
//       score100: data.score90_100,
//       mean: data.mean,
//       id: data.id
//     }).addTo(map);
//     polygon.on('click', onMapClick);
//   });
// }



});

var port = process.env.PORT || 1337;
server.listen(port);

console.log("Server running at http://localhost:%d", port);
