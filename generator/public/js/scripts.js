var map;

function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 11,
        center: {
            lat: 53.3169421,
            lng: -6.210036
        },
        mapTypeControl: false
    });

   $(document).ready(function(){

// createGridHeatMap({lat: 53.189579, lng: -6.421509}, {lat:53.447171, lng: -6.017761});

    createHeatMap();

    //createMovementMap();

    //createWordCloud();

   });

   
}

function createGridHeatMap(start, end){

    var grid = [];

    var step = (end.lat - start.lat) / 20;
    var lat = start.lat;

    while(lat < end.lat){
        var lng = start.lng;

        while(lng < end.lng){

            grid.push(new google.maps.Rectangle({
                strokeColor: '#000000',
                strokeOpacity: 1,
                strokeWeight: 1,
                fillColor: '#00f',
                fillOpacity: Math.random(),
                map: map,
                bounds: {
                    north: lat,
                    south: lat + step,
                    west: lng,
                    east: lng + step
                }
            }));

            lng += step;
        }

        lat += step;
    }

}

function createHeatMap(){
    query('heatmap', function(points){
        heatmap = new google.maps.visualization.HeatmapLayer({
            data: getPoints(points),
            map: map
        });
    });    
}

function getPoints(points){

    var gmPoints = [];
    for(var i=0,l=points.length;i<500;i++){
        var p = points[i];
        //console.log(p);
        gmPoints.push(new google.maps.LatLng(p[0], p[1]));
        /*new google.maps.Marker({
            position: {lat: p[0], lng: p[1]},
            map: map
          });*/
    }

    console.log(gmPoints);

    return gmPoints;
}

function getFilters(){
    //get boundary
    /*console.log(map);
    var boundary = map.getBounds();
    var NE = boundary.getNorthEast();
    var SW = boundary.getSouthWest();*/

    //get times
    var st = $( "#slider-time" ).slider( "values", 0 );
    var et = $( "#slider-time" ).slider( "values", 1 );
    
    //get keyword
    var keyword = $( "input#keyword" ).val();

    return {
        slt: '53.189579',
        sln: '-6.421509',
        elt: '53.447171',
        eln: '-6.017761',
        st: st,
        et: et,
        keyword: '',
        filter: 'global'
    };
}

function query(type, success){

    var data = getFilters();

    $.get(
        'json/' + type,
        data,
        function(data){
            console.log("Success:");
            //console.log(data);
            success(data);
        }
    );
}

function createMovementMap(){

    var paths = [];

    for(var i=0;i<10;i++){

        var length = Math.random() * 9 + 1;
        var points = [];
        for(var j=0;j<length;j++){
            var lat = Math.random() * (53.447171 - 53.189579) + 53.189579;
            var lng = Math.random() * (-6.421509 - -6.017761) + -6.017761;
            var point = {lat: lat, lng: lng};
            points.push(point);
        }

        var color = 'rgb(' + Math.floor(Math.random() * 255)
            + ', ' + Math.floor(Math.random() * 255)
            + ', ' + Math.floor(Math.random() * 255) + ')';

        var path = new google.maps.Polyline({
            path: points,
            geodesic: true,
            strokeColor: color,
            strokeOpacity: 1.0,
            strokeWeight: 3
          });
        path.setMap(map);
        paths.push(path);
    }

}

function createWordCloud(){
    WordCloud(document.getElementById('cloud-canvas'), { list: getWords() } );
}

function getWords(){

    var words = "abandonee,abandoner,abandoners,abandoning,abandonment,abandonments,abandons,abase,abased,abasedly,abasement,abaser,abasers,abases,abash,abashed,abashedly,abashes".split(",");
    var list = [];
    for(var i=0,l=words.length;i<l;i++){
        list.push([words[i], Math.random()*9 + 1]);
    }

    return list;
}