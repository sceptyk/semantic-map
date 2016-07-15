var map;
var heatmap;
var paths;
var grid;

var boundary;
var originBounds = [{lat: 53.447171, lng: -6.421509}, {lat: 53.189579, lng: -6.017761}];

var scope = window.location.hash || '#heatmap'; //#heatmap, #gridmap, #movement

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

    function onHeatMap(){
        reset();
        createHeatMap();
        createPopularityMap();
        setUrl('#heatmap');

        //TODO set url #heatmap

        //hide word cloud container
        $(".heatmap-show").show();
        $(".gridmap-show").hide();
    }

    function onGridMap(){

        reset();
        createGridHeatMap();
        createWordCloud();
        setUrl('#gridmap');

        $(".gridmap-show").show();
        $(".heatmap-show").hide();
    }


    $("input#radio1").click(function(){

        if($(this).is(":checked")){
            onHeatMap();
        }

    });

    $("input#radio2").click(function(){

        if($(this).is(":checked")){
            onGridMap();
        }

    });

    $("input#radio3").click(function(){

        if($(this).is(":checked")){
            reset();
            createMovementMap();
        }

    });

    $("button#filter-keyword").click(function(){
        $("input#radio1").click();
    });

    //START APP
    if(scope == '#heatmap'){
        $("input#radio1").click();
        onHeatMap();
    }
    else if (scope == '#gridmap'){
        $("input#radio2").click();
        onGridMap();
    }

    
    
   });

   
}

function reset(){
    removeHeatMap();
    removeMovementMap();
    removeGridHeatMap();
    removePopularityMap();
}

/**********************************************
 *
 *---------------GRID MAP
 *
 * ********************************************/
function createGridHeatMap(){

    map.fitBounds(boundary);

    grid = [];

    query('grid', function(sqs){

        //sqs: [slat, slng, elat, elng, weight]
        console.log(sqs);

        var max = sqs[0][4];
        for(var i=1,l=sqs.length;i<l;i++){
            if(sqs[i][4] > max) max = sqs[i][4];
        }

        //create grid
        for(var i=0,l=sqs.length;i<l;i++){
            
            var sq = sqs[i];

            var rect = new google.maps.Rectangle({
                strokeColor: '#000000',
                strokeOpacity: 1,
                strokeWeight: 1,
                fillColor: '#f00',
                fillOpacity:  sq[4] / max,
                map: map,
                bounds: {
                    north: sq[0],
                    south: sq[2],
                    west: sq[1],
                    east: sq[3]
                }
            });

            grid.push(rect);

            google.maps.event.addListener(rect, 'click', function (event) {
                reset();
                
                boundary = this.getBounds();
                
                createGridHeatMap();
            });
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

/**********************************************
 *
 *----------------WORD CLOUD
 *
 * ********************************************/
function createWordCloud(){

    query('cloud', function(keywords){

        var list = [];
        console.log(keywords);
        var minFontSize = 16;
        var min = keywords[0][1];

        for(var i=0,l=keywords.length;i<l;i++){
            if(keywords[i][1] < min) min = keywords[i][1];
        }

        for(var i=0,l=keywords.length;i<l;i++){
            var keyword = keywords[i];
            keyword[1] = keyword[1] / min * minFontSize;
            list.push(keyword);
        }

        WordCloud(document.getElementById('cloud-canvas'), { 
            list: list
        }); 
    });

    
}


/**********************************************
 *
 *----------------HEAT MAP
 *
 * ********************************************/
function createHeatMap(){
    query('heatmap', function(points){

        //console.log(points.length);

        var gmPoints = [];
        for(var i=0,l=points.length;i<l;i++){
            var p = points[i];
            //console.log(p);
            gmPoints.push(new google.maps.LatLng(p[0], p[1]));
        }

        heatmap = new google.maps.visualization.HeatmapLayer({
            data: gmPoints,
            map: map,
            maxIntensity: 5
        });
    });    
}

function removeHeatMap(){
    if(!heatmap) return;

    heatmap.setMap(null);
}

/**********************************************
 *
 *----------------POPULARITY
 *
 * ********************************************/
function createPopularityMap(){

    query('popularity', function(hours){

        var max = 0;
        var day = [];
        for(var i=0;i<24;i++) day[i] = 0;

        for(var i=0,l=hours.length;i<l;i++){

            var hour = hours[i];
            var counter = day[hour] + 1;

            if(counter > max) max = counter
            day[hour] = counter;

        }

        //print columns
        var $wrap = $("#popularity-canvas");
        var columnWidth = $wrap.width() / 24;
        var maxHeight = $wrap.height();
        for(var i=0;i<24;i++){
            $("<div></div>")
                .addClass('column')
                .text(i)
                .height(day[i] / max * maxHeight)
                .width(columnWidth)
                .appendTo($wrap);
        }

    });

}

function removePopularityMap(){
    //remove columns
    $("#popularity-canvas").empty();
}


/**********************************************
 *
 *----------------MOVEMENT MAP
 *
 * ********************************************/
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

function getFilters(filters){
    //FIXME get only needed values

    //get times
    var st = $( "#slider-time" ).slider( "values", 0 ) + ":00:00";
    var et = $( "#slider-time" ).slider( "values", 1 ) + ":00:00";

    //get recent limit
    var recent = new Date();
    recent.setHours(recent.getHours() - $("#slider-recent").slider("value"));

    var recStr = recent.toISOString();
    recStr = recStr.split("T");
    recStr = recStr[0] + " " + recStr[1].split(".")[0];

    var re = $( "#recent-switch" ).is(":checked") ? recStr : "0000-00-00 00:00:00";

    //get keyword
    var keyword = $( "input#keyword" ).val();

    //get days
    var days = [];
    if("input#checkbox-monday:checked") days.push("MON");
    if("input#checkbox-tuesday:checked") days.push("TUE");
    if("input#checkbox-wednesday:checked") days.push("WED");
    if("input#checkbox-thursday:checked") days.push("THU");
    if("input#checkbox-friday:checked") days.push("FRI");
    if("input#checkbox-saturday:checked") days.push("SAT");
    if("input#checkbox-sunday:checked") days.push("SUN");
    days = JSON.stringify(days);

    //get layer
    layer = 3;
    //TODO

    return {
        slt: boundary.getNorthEast().lat(),
        sln: boundary.getSouthWest().lng(),
        elt: boundary.getSouthWest().lat(),
        eln: boundary.getNorthEast().lng(),
        st: st,
        et: et,
        re: re,
        days: days,
        layer: layer,
        keyword: keyword
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

function setUrl(_scope){
    scope = _scope;
    window.location.hash = _scope;
}

function getUrl(){
    var url = window.location.hash;
    url = url.substr(1);
}