var map;
var heatmap;
var paths;
var grid;

var layer = 4;
var boundary;
var originBounds = [{lat: 53.447171, lng: -6.421509}, {lat: 53.189579, lng: -6.017761}];
var clickPoint = null;

var Utils = utils();

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
        draggableCursor: 'crosshair',
        styles: [
            {
                featureType: 'all',
                stylers: [
                    { saturation: -80 }
                ]
            },{
            featureType: 'road.arterial',
            elementType: 'geometry',
            stylers: [
                { hue: '#00ffee' },
                { saturation: 50 }
            ]
            },{
                featureType: 'poi.business',
                elementType: 'labels',
                stylers: [
                    { visibility: 'off' }
                ]
            }
        ]
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

        initMenu();

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

function initMenu(){

    var timer_2 = null;
    function updateWithKeywords(){

        clearTimeout(timer_2);
        timer_2 = setTimeout(function(){
            console.log("updateWithKeywords")
            //onPopularity();
            onHeatMap();

        }, 500);
    }
    
    $("#toggle-details").click(updateWithKeywords);
    $("#input-keywords").keypress(updateWithKeywords);
    $("#submit-search").click(updateWithKeywords);
    $("#filter-day input").change(updateWithKeywords);
    $("#input-time").on("slidechange", updateWithKeywords);
    $("#input-date").on("slidechange", updateWithKeywords);

}

function onHeatMap(){
    
    removeHeatMap();
    createHeatMap();
}

function onWordCloud(){

    //reset word cloud
    
    //createwordcloud
    createWordCloud();
}

function onPopularity(){

    createPopularityMap();
    
}

function reset(){
    removeHeatMap();
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
        center: true,
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

        var limit = 20; //limit words number
        //console.log(keywords);

        function setCanvas($el){
            var h = $el.height();
            limit = limit == 20 ? 10: 20;
            WordCloud($el.get(0), { 
                list: keywords.slice(0, limit),
                weightFactor: function(size){
                    return h*0.2*size/max+h*0.01;
                },
                color: 'rgba(0, 0, 59, 1)'
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
        boundary: true,
        details: true,
        keywords: true,
        date: true,
        time: true,
        day: true,
        layer: true
    }, function(points, filters){

        //console.log(points);

        var gmPoints = [];
        if(filters.details){
            for(var i=0,l=points.length;i<l;i++){
                var p = Util.decodeHash(points[i][0]);
                //console.log(p);
                var point = new google.maps.LatLng(p[0], p[1]);
                var weight = points[i][1];
                gmPoints.push(new google.maps.visualization.WeightedLocation(point, weight));
            }
        }else{
            for(var i=0,l=points.length;i<l;i++){
                var p = points[i];
                gmPoints.push(new google.maps.LatLng(p[0], p[1]));
            }
        }

        heatmap = new google.maps.visualization.HeatmapLayer({
            data: gmPoints,
            map: map,
            maxIntensity: 5,
            gradient: [
                'rgba(255, 255, 255, 0)',
                'rgba(255, 255, 255, 1)',
                'rgba(235, 235, 255, 1)',
                'rgba(196, 196, 255, 1)',
                'rgba(157, 157, 255, 1)',
                'rgba(118, 118, 255, 1)',
                'rgba(78, 78, 255, 1)',
                'rgba(39, 39, 255, 1)',
                'rgba(0, 0, 255, 1)',
                'rgba(0, 0, 216, 1)',
                'rgba(0, 0, 177, 1)',
                'rgba(0, 0, 137, 1)',
                'rgba(0, 0, 98, 1)',
                'rgba(0, 0, 59, 1)'
            ]
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
        boundary: true,
        details: true,
        keywords: true,
        date: true,
        time: true,
        day: true,
        layer: true
    }, function(hours, filters){

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
                    backgroundColor: 'rgba(0, 0, 59, 1)'
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
    var sDate = date.toISOString();
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
                start: _formatDate(new Date($( '#input-date' ).slider('values', 0))),
                end: _formatDate(new Date($( '#input-date' ).slider('values', 1)))
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
        if(filters.keywords){
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

        if(filters.center){
            var ceneter = null;
            if(!clickPoint){
                center = map.getCenter();
            }else{
                center = [clickPoint.lat, clickPoint.lng];
            }

            console.log(center);

            processed.c = center;
        }

        if(filters.details){
            processed.dl = $("#toggle-details").hasClass("active");
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

    var filters = getFilters(filters);

    //console.log("Query: ", type);

    $.get(
        'json/' + type,
        filters,
        function(data){
            console.log("Success: ", type);
            //console.log(data);
            success(data, filters);
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

/**********************************************
 *
 *---------------UTILS
 *
 * ********************************************/
 
function utils(){

    var digits = {
        'A':0, 'B':1, 'C':2, 'D':3, 'E':4, 'F':5, 'G':6, 'H':7, 'I':8, 'J':9, 'K':10, 'L':11, 'M':12, 'N':13, 'O':14, 'P':15, 'Q':16, 'R':17, 'S':18, 'T':19, 'U':20,
        'V':21, 'W':22, 'X':23, 'Y':24, 'Z':25,
        'a':26, 'b':27, 'c':28, 'd':29, 'e':30, 'f':31, 'g':32, 'h':33, 'i':34, 'j':35, 'k':36, 'l':37, 'm':38, 'n':39, 'o':40, 'p':41, 'q':42, 'r':43, 's':44, 't':45, 'u':46,
        'v':47, 'w':48, 'x':49, 'y':50, 'z':51,
        '0':52, '1':53, '2':54, '3':55, '4':56, '5':57, '6':58, '7':59, '8':60, '9':61, '+':62, '/':63
    };

    var precisions = [0, 0.2, 0.6, 1.2, 50.0];

    function from_64(s){
        
        var n = 0, m = 1;
        while(s != 0){
            var d = s % 10;
            n += m*digits[d];
            m *= 64
            s /= 10;
        }

        return n;
    }

    function to_deg_lat(km){
        return km / 111
    }

    function to_deg_lng(lat, km){
        return abs(km / (111 * math.cos(lat)))
    }

    function layer_precision(index){
        return precisions[index];
    }

    return {
        decodeHash: function(hash){
            var precision = layer_precision(layer);
            var deg_lat = to_deg_lat(precision);
            var deg_lng = to_deg_lng(Math.floor(deg_lat), precision);
            var cols = Math.floor(360 / deg_lng);

            var n = from_64(hash);
            var lng = n % cols;
            var lat = n / cols;

            lat *= deg_lat;
            lng *= deg_lng;

            lat -= 180;
            lng -= 180;

            return [lat, lng];
        }
    };
}