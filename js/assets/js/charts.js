var donutConfig = {
    title: {
        text: '15%',
        style: {
            fontSize: '40px'
        },
        align: 'center',
        verticalAlign: 'middle',
        y: 15
    },
    tooltip: {
        pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
    },
    plotOptions: {
        pie: {
            dataLabels: {
                enabled: true,
            }
        }
    },
    series: [{
        animation: {
            duration: 3000
        },
        type: 'pie',
        name: 'Browser share',
        size: "100%",
  			innerSize: "90%",
  			pointPadding: 0,
  			groupPadding: 0,
        data: [
            ['Firefox',   10.38],
            ['IE',       56.33]
        ]
    }],
    credits: {
      enabled: false
    }
}

var histogramConfig = {
    chart: {
        type: 'column'
    },
    title: {
        text: 'Distribution'
    },
    xAxis: {
        type: 'category',
        labels: {
            rotation: -45,
            style: {
                fontSize: '13px',
                fontFamily: 'Verdana, sans-serif'
            }
        }
    },
    yAxis: {
        min: 0,
        title: {
            text: 'Population %'
        }
    },
    tooltip: {
        pointFormat: 'Population: <b>{point.y:.1f}</b>'
    },
    series: [{
        name: 'Regional Population %',
        dataLabels: {
            enabled: false,
            format: '{point.y:.1f}%'
        }
    },{
        name: 'City Population %',
        dataLabels: {
            enabled: false,
            format: '{point.y:.1f}%'
        }
    }],
    credits: {
      enabled: false
    }
}

var chartSelectors = ['ageOver65', 'ageUnder10', 'gender', 'race'];
var numberSelectors = ['population', 'funding', 'services', 'score'];

function updateNumbers(data, currentSet) {
  numberSelectors.forEach(s => {
    var text = 'Coming Soon';
    if (data) {
      if (s === 'population') {
        text = data.pop_total.toLocaleString();
      }
      if (s === 'funding') {
        text = '$' + Math.floor(data.investment).toLocaleString();
      }
      if (s === 'score') {
        text = data.score_mean;
        histogramConfig.series[0].name = currentSet.data.length > 8 ? "Neighborhood Population %" : "District Population %";
        histogramConfig.series[0].data = [
          ['Poor', toPercent(data.score0_49, data.pop_total, true)],
          ['Fair', toPercent(data.score50_69, data.pop_total, true)],
          ['Good', toPercent(data.score70_89, data.pop_total, true)],
          ['Excellent', toPercent(data.score90_100, data.pop_total, true)]
        ];
        var all = currentSet.data.filter(d => d.id === 'all')[0];
        histogramConfig.series[1].data = [
          ['Poor', toPercent(all.score0_49, all.pop_total, true)],
          ['Fair', toPercent(all.score50_69, all.pop_total, true)],
          ['Good', toPercent(all.score70_89, all.pop_total, true)],
          ['Excellent', toPercent(all.score90_100, all.pop_total, true)]
        ];
        Highcharts.chart('scoreDistribution', histogramConfig);
      }
    }
    $('#' + s).text(text);
  });
}

function toPercent(num1, num2, asNumber) {
  if (asNumber) {
    return Math.floor(num1 / num2 * 100);
  }
  return (num1 / num2 * 100).toFixed(1) + "%";
}

function updateCharts(data) {
  if (!data) return;
  var config = donutConfig;
  chartSelectors.forEach(s => {
      config.series[0].name = s.charAt(0).toUpperCase() + s.slice(1);
      if (s === 'ageOver65') {
        config.series[0].name = "Age";
        config.title.text = toPercent(data.pop_over_65, data.pop_total);
        config.series[0].data = [
          ['Over 65', data.pop_over_65],
          ['Under 65', data.pop_total - data.pop_over_65]
        ];
      }
      if (s === 'gender') {
        config.title.text = toPercent(data.pop_female, data.pop_total);
        config.series[0].data = [
          ['Female', data.pop_female],
          ['Male', data.pop_total - data.pop_female]
        ];
      }
      if (s === 'race') {
        config.title.text = toPercent(data.pop_color, data.pop_total);
        config.series[0].data = [
          ['Population of color', data.pop_color],
          ['Other', data.pop_total - data.pop_color]
        ];
      }
      if (s === 'ageUnder10') {
        config.series[0].name = "Age";
        config.title.text = toPercent(data.pop_below_10, data.pop_total);
        config.series[0].data = [
          ['Under 10', data.pop_below_10],
          ['Over 10', data.pop_total - data.pop_below_10]
        ];
      }
      Highcharts.chart(s, config);
  });
}
