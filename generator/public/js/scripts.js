var map;
var heatmap;
var paths;
var grid;

var boundary;
var originBounds = [{lat: 53.447171, lng: -6.421509}, {lat: 53.189579, lng: -6.017761}];

function initMap() {
    boundary = new google.maps.LatLngBounds(originBounds[0], originBounds[1]);

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 11,
        center: {
            lat: 53.3169421,
            lng: -6.210036
        },
        mapTypeControl: false
    });

   $(document).ready(function(){
    
    createWordCloud();
    $("input#radio1").click();
    createHeatMap();

    $("input#radio1").click(function(){

        if($(this).is(":checked")){
            reset();
            createHeatMap();
            createWordCloud();
        }

    });

    $("input#radio2").click(function(){

        if($(this).is(":checked")){
            reset();
            createGridHeatMap();
        }

    });

    $("input#radio3").click(function(){

        if($(this).is(":checked")){
            reset();
            createMovementMap();
        }

    });

    $("button#filter-keyword").click(function(){
        reset();
        $("input#radio1").click();
        createWordCloud();
    });
    
   });

   
}

function reset(){
    removeHeatMap();
    removeMovementMap();
    removeGridHeatMap();
}

function createGridHeatMap(){

    map.fitBounds(boundary);

    start = {lat: boundary.getNorthEast().lat(), lng: boundary.getSouthWest().lng()};
    end = {lat: boundary.getSouthWest().lat(), lng: boundary.getNorthEast().lng()};

    grid = [];

    var dimension = 5;
    var gridIntensity = [];
    for(var i=0;i<dimension;i++){
        var row = [];
        for(var j=0;j<dimension;j++){
            row.push(0);
        }
        gridIntensity.push(row);
    }
    
    var step = Math.abs(end.lat - start.lat) / dimension;
    var lat = start.lat;
    var lng = start.lng;

    query('grid', function(points){

        //console.log(points.length);

        var max = 0;

        //count density
        for(var i=0,l=points.length;i<l;i++){

            var point = points[i];
            var plt = Math.abs(point[0] - lat);
            var pln = Math.abs(point[1] - lng);

            var x = -1;
            var y = -1;

            while(x < dimension - 1 && plt > 0){
                plt -= step;
                x++;
            }

            while(y < dimension - 1 && pln > 0){
                pln -= step;
                y++;
            }

            var inten = gridIntensity[x][y];
            inten++;
            gridIntensity[x][y] = inten;
            if(inten > max){
                max = inten;
            }

        }

        //console.log(gridIntensity);
        //console.log(max);

        //create grid
        for(var i=0;i<dimension;i++){
            for(var j=0;j<dimension;j++){
                
                var rect = new google.maps.Rectangle({
                    strokeColor: '#000000',
                    strokeOpacity: 1,
                    strokeWeight: 1,
                    fillColor: '#f00',
                    fillOpacity: gridIntensity[i][j] / max,
                    map: map,
                    bounds: {
                        north: lat + i*step,
                        south: lat + (i+1)*step,
                        west: lng + j*step,
                        east: lng + (j+1)*step
                    }
                });

                grid.push(rect);

                google.maps.event.addListener(rect, 'click', function (event) {
                    reset();
                    
                    boundary = this.getBounds();
                    
                    createGridHeatMap();
                });
            }
        }

    });
}

function removeGridHeatMap(){

    if(!grid) return;

    for(var i=0,l=grid.length;i<l;i++){
        grid[i].setMap(null);
    }

    boundary = new google.maps.LatLngBounds(originBounds[0], originBounds[1]);
}

function createHeatMap(){
    query('heatmap', function(points){
        heatmap = new google.maps.visualization.HeatmapLayer({
            data: getPoints(points),
            map: map,
            maxIntensity: 5
        });
    });    
}

function removeHeatMap(){
    if(!heatmap) return;

    heatmap.setMap(null);
}

function getPoints(points){

    var gmPoints = [];
    for(var i=0,l=points.length;i<l;i++){
        var p = points[i];
        //console.log(p);
        gmPoints.push(new google.maps.LatLng(p[0], p[1]));
        /*new google.maps.Marker({
            position: {lat: p[0], lng: p[1]},
            map: map
          });*/
    }

    return gmPoints;
}

function getFilters(filters){
    //get boundary
    /*console.log(map);
    var boundary = map.getBounds();
    var NE = boundary.getNorthEast();
    var SW = boundary.getSouthWest();*/

    //get times
    var st = $( "#slider-time" ).slider( "values", 0 ) + ":00:00";
    var et = $( "#slider-time" ).slider( "values", 1 ) + ":00:00";
    
    //get keyword
    var keyword = $( "input#keyword" ).val();

    //console.log(st);
    //console.log(et);

    return {
        slt: boundary.getNorthEast().lat(),
        sln: boundary.getSouthWest().lng(),
        elt: boundary.getSouthWest().lat(),
        eln: boundary.getNorthEast().lng(),
        st: st,
        et: et,
        keyword: keyword,
        filter: 'global'
    };
}

function query(type, filters, success){

    if(typeof filters === 'function'){
        success = filters;
        filters = null;
    }

    var data = getFilters(filters);

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

    paths = [];
    query('movement', function(points){

        for(var i=0,l=points.length;i<l;){

            var temp = points[i];
            var gmPoints = [];
            while(i<l && points[i][0] == temp[0]){
                var next = points[i];
                gmPoints.push({lat: next[1], lng: next[2]});

                i++;
            }

            var user = temp[0];
            var r = user % 256;
            user = Math.floor(user/256);
            var g = user % 256;
            user = Math.floor(user/256);
            var b = user % 256;

            var color = 'rgb(' + r + ', ' + g + ', ' + b + ')';

            var path = new google.maps.Polyline({
                path: gmPoints,
                geodesic: true,
                strokeColor: color,
                strokeOpacity: 0.5,
                strokeWeight: 1
              });
            path.setMap(map);
            paths.push(path);

            google.maps.event.addListener(path, 'mouseover', function (event) {
                 this.setOptions({
                     strokeOpacity: 1,
                     strokeWeight: 3
                 });
             });

             google.maps.event.addListener(path, 'mouseout', function (event) {
                 this.setOptions({
                     strokeOpacity: 0.5,
                     strokeWeight: 1
                 });
             });

             google.maps.event.addListener(path, 'click', function (event) {
                 this.setMap(null);
             });
        }

    });

}

function removeMovementMap(){

    if(!paths) return;

    for(var i=0,l=paths.length;i<l;i++){
        paths[i].setMap(null);
    }

}

function createWordCloud(){

    query('cloud', function(tweets){

        var list = {};

        for(var i=0,l=tweets.length;i<l;i++){

            var tweet = tweets[i];
            var text = tweet[0];
            var words = text.split(" ");

            for(var j=0,k=words.length;j<k;j++){
                var word = words[j];
                if(list.hasOwnProperty(word)){
                    list[word]++;
                }else{
                    list[word] = 1;
                }
            }
        }

        var sortable = [];
        for(var key in list){
            sortable.push([key, list[key]]);
        }

        sortable.sort(function(a,  b){
            return b[1] - a[1];
        });

        list = [];
        for(var i=0;i<10;i++){
            list.push(sortable[i]);
            //console.log(sortable[i]);
        }

       WordCloud(document.getElementById('cloud-canvas'), { list: list } ); 
    });

    
}