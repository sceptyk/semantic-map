var map;
var heatmap;
var paths;
var grid;

var layer = 4;
var boundary;
var originBounds = [{lat: 53.447171, lng: -6.421509}, {lat: 53.189579, lng: -6.017761}];
var clickPoint = null;


var scope = window.location.hash || '#heatmap'; //#heatmap, #gridmap, #movement

var thread = 'free';
var timer = null;

function initMap() {
    boundary = new google.maps.LatLngBounds(originBounds[0], originBounds[1]);

    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 12,
        center: {
            lat: 53.3478,
            lng: -6.2597
        },
        mapTypeControl: false,
        disableDefaultUI: true,
        draggableCursor: 'crosshair'
    });

    var timer_1 = null;
    map.addListener('zoom_changed', function(){
        var zoom = map.getZoom();
        var _layer;

        if(zoom < 11){
            _layer = 4;
        }
        else if(zoom < 13){
            _layer = 3;
        }
        else if(zoom < 15){
            _layer = 2;
        }
        else if(zoom < 17){
            _layer = 1;
        }
        else{
            _layer = 0;
        }

        clearTimeout(timer_1);
        timer_1 = setTimeout(function(_layer){
            console.log(_layer, layer);
            if(_layer != layer){
                layer = _layer;

                onWordCloud();
            }
        }, 500, _layer);
    });

    map.addListener('click', function(e){
        var p = e.latLng;
        var step = 0.01;

        //console.log("Click");
        ////TODO boundary based on layer
        boundary = new google.maps.LatLngBounds({lat: p.lat() + step, lng: p.lng() - step}, {lat: p.lat() - step, lng: p.lng() + step});
        clickPoint = p;

        //onMovement();
    });

   $(document).ready(function(){

        onHeatMap();
        onWordCloud();
        onPopularity();
   });

   
}

/**********************************************
 *
 *---------------UI CHANGE
 *
 * ********************************************/

function onHeatMap(){
    
    
    createHeatMap();
    
}

function onWordCloud(){

    //reset word cloud
    
    //createwordcloud
    createWordCloud();
}

function onPopularity(){

    //reset popularity chart

    createPopularityMap();
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

    //map.fitBounds(boundary);

    //TODO remove it, catch onclick event send for movement and wordcloud update
    grid = [];

    query('grid', function(sqs){

        //sqs: [slat, slng, elat, elng, weight]
        //console.log(sqs);
        
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
                map.fitBounds(boundary);
                
                //createGridHeatMap();
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

    query('cloud', {
        boundary: true,
        layer: true,
        days: true,
        time: true
    },function(keywords){

        /*function rand(){
            return Math.random() * 10000 + 100;
        }

        var keywords = [
            ["satisfy", rand()],
            ["silent", rand()],
            ["rabid", rand()],
            ["simplistic", rand()],
            ["magical", rand()],
            ["frequent", rand()],
            ["undress", rand()],
            ["wing", rand()],
            ["excited", rand()],
            ["pest", rand()],
            ["juicy", rand()],
            ["close", rand()]
        ];*/
        
        var max = 0;

        var preview = $('#canvas-word-cloud-preview');
        var view = $('#canvas-word-cloud-view');

        for(var i=0,l=keywords.length;i<l;i++){
            if(keywords[i][1] > max) max = keywords[i][1];
        }

        console.log(keywords);

        function setCanvas($el){
            var h = $el.height();
            WordCloud($el.get(0), { 
                list: keywords,
                weightFactor: function(size){
                    return h*0.2*size/max+h*0.01;
                }
            });
        }

        setCanvas(preview);

        var OnHoverToken = null;
        $("#word-cloud.preview").hover(function(){

            clearTimeout(OnHoverToken);
            preview.hide();
            OnHoverToken = setTimeout(function(){
                preview.show();
                setCanvas(preview);
            }, 1000);
        });
    });

    
}


/**********************************************
 *
 *----------------HEAT MAP
 *
 * ********************************************/
function createHeatMap(){
    query('heatmap', {
        boundary: true
    }, function(points){

        //console.log(points);

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

    query('popularity', {
        boundary: true
    }, function(hours){

        //split
        var labels = [];
        var data = [];

        for(var i=0,l=hours.length;i<l;i++){
            var hour = hours[i];
            labels.push(hour[0]);
            data.push(hour[1]);
        }

        //console.log(labels);

        //print preview chart
        new Chart(document.getElementById('canvas-popularity-preview'), {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    backgroundColor: 'rgba(49,157,38,0.7)'
                }]
            },
            options: {
                tooltips: {
                    enabled: false
                },
                legend: {
                    display: false
                },
                scales: {
                    yAxes: [{
                        display: false
                    }]
                }
            }
        });

        /*var max = 0; //TODO

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
                .width(100/25 + "%") //columnWidth)
                .appendTo($wrap);
        }*/

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
        console.log("Movement map", points);
        
        /*var center = {lat:0,lng:0};
        for(var i=0,l=points.length;i<l;i++){
            var temp = points[0];
            center.lat += temp[0];
            center.lng += temp[1];
        }

        center.lat /= points.length;
        center.lng /= points.length;*/

        for(var i=0,l=points.length;i<l;i++){

            var temp = points[i];
            var gmPoints = [
                clickPoint,
                {lat: temp[0], lng: temp[1]},
                {lat: temp[2], lng: temp[3]}
            ];

            if(temp[4] && temp[5]){
                gmPoints.push({lat: temp[4], lng: temp[5]});
            }

            var user = temp[6];
            var r = user % 256;
            user = Math.floor(user/256);
            var g = user % 256;
            user = Math.floor(user/256);
            var b = user % 256;

            var color = 'rgb(' + r + ', ' + g + ', ' + b + ')';

            //color = '#6789AB';

            var path = new google.maps.Polyline({
                path: gmPoints,
                geodesic: true,
                strokeColor: color,
                strokeOpacity: 0.5,
                strokeWeight: 3
              });
            path.setMap(map);
            paths.push(path);
        }

    });

}

function removeMovementMap(){

    if(!paths) return;

    for(var i=0,l=paths.length;i<l;i++){
        paths[i].setMap(null);
    }

}

/**********************************************
 *
 *--------------QUERY HELPERS
 *
 * ********************************************/

function _formatDate(date){
    var sDAte = date.toISOString();
    sDate = sDate.split("T");
    sDate = sDate[0] + " " + sDate[1].split(".")[0];

    return sDate;
}

function getFilters(filters){

    var processed = {};

    if(filters !== undefined && filters != null){

        //get date
        if(filters.date){
            var date = {
                start: _filterDate(new Date($( '#input-date' ).slider('values', 0))),
                end: _filterDate(new Date($( '#input-date' ).slider('values', 1)))
            }
            processed.d = date;
        }

        //get times
        if(filters.time){
            var time = {
                start: $( "#input-time" ).slider( "values", 0 ) + ":00:00",
                end: $( "#input-time" ).slider( "values", 1 ) + ":00:00"
            };

            processed.t = time;
        }

        //get keyword
        if(filters.keyword){
            var keywords = $( "#input-keywords" ).val().split(/,*\s/);
            processed.k = keywords;
        }

        //get days
        if(filters.day){
            var days = [];
            if("#input-day-mo:checked") days.push(1);
            if("#input-day-tu:checked") days.push(2);
            if("#input-day-we:checked") days.push(3);
            if("#input-day-th:checked") days.push(4);
            if("#input-day-fr:checked") days.push(5);
            if("#input-day-sa:checked") days.push(6);
            if("#input-day-su:checked") days.push(7);

            processed.ds = days;
        }

        if(filters.boundary){
            processed.b = boundary.toJSON();
        }

        if(filters.layer){

            processed.l = layer;
        }


        for(var key in filters){
            if(filters.hasOwnProperty(key) && processed.hasOwnProperty(key)){
                var filter = filters[key];

                if(typeof filter === 'function'){
                    processed[key] = filter(processed[key]);
                }else{
                    processed[key] = filter;
                }
            }
        }

        for(var key in processed){
            if(processed.hasOwnProperty(key)){
                processed[key] = JSON.stringify(processed[key]);
            }
        }

    }

    //console.log(processed);
    return processed;
}

function query(type, filters, success){

    if(typeof filters === 'function'){
        success = filters;
        filters = null;
    }

    var data = getFilters(filters);

    //console.log("Query: ", type);

    $.get(
        'json/' + type,
        data,
        function(data){
            console.log("Success: ", type);
            //console.log(data);
            success(data);
        }
    );
}

/**********************************************
 *
 *---------------URL HELPERS
 *
 * ********************************************/

function setUrl(_scope){
    scope = _scope;
    window.location.hash = _scope;
}

function getUrl(){
    var url = window.location.hash;
    url = url.substr(1);
}