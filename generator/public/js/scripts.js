/**********************************************
 *
 *---------------GLOBALS
 *
 * ********************************************/
var map;
var heatmap;
var chart;

var layer = 4;
var boundary;
var originBounds = [{lat: 53.447171, lng: -6.421509}, {lat: 53.189579, lng: -6.017761}];
var clickPoint = null;

var Utils = utils();
var scope = {
    zoom: 12,
    center: [53.3478, -6.2597],
    type: "recent"
}; //url address params TODO

/**********************************************
 *
 *---------------INIT
 *
 * ********************************************/
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
                featureType: 'poi',
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
        }else if(zoom < 13){
            _layer = 3;
        }
        else if(zoom < 15){
            _layer = 2;
        }
        else{
            _layer = 1;
        }

        clearTimeout(timer_1);
        timer_1 = setTimeout(function(_layer){
            //console.log(_layer, layer);
            if(_layer != layer){
                layer = _layer;
                clickPoint = null; //remove click point in order to get map center
                onWordCloud();
            }
        }, 500, _layer);
    });

    var timer_2 = null;
    map.addListener('click', function(e){

        clearTimeout(timer_2);
        timer_2 = setTimeout(function(point){
            clickPoint = point;
            onWordCloud();
        }, 500, e.latLng);
        
    });

   $(document).ready(function(){

        initMenu();

        onHeatMap();
        onPopularity();
        onWordCloud();
   });

   
}

/**********************************************
 *
 *---------------UI CHANGE
 *
 * ********************************************/

function initMenu(){

    var timer_3 = null;
    function updateWithKeywords(){

        clearTimeout(timer_3);
        timer_3 = setTimeout(function(){
            //console.log("updateWithKeywords");
            onPopularity();
            onHeatMap();

        }, 750);
    }
    
    $("#toggle-details").click(function(){
        $("#filter-date").slideToggle(); //hide date filter
        updateWithKeywords();
    });
    $("#input-keywords").keypress(updateWithKeywords);
    $("#submit-search").click(updateWithKeywords);
    $("#filter-day input").change(updateWithKeywords);
    $("#input-time").on("slidechange", updateWithKeywords);
    $(".input-date").datepicker("option", "onSelect", updateWithKeywords);

}

function onHeatMap(){
    
    removeHeatMap();
    enqueue();
    createHeatMap(function(){
        dequeue();
    });
}

function onWordCloud(){

    //reset word cloud
    
    //createwordcloud
    enqueue();
    createWordCloud(function(){
        dequeue();
    });
}

function onPopularity(){

    enqueue();
    removePopularityMap();
    createPopularityMap(function(){
        dequeue();
    });
    
}

function reset(){
    removeHeatMap();
    removeGridHeatMap();
    removePopularityMap();
}


/**********************************************
 *
 *----------------WORD CLOUD
 *
 * ********************************************/
function createWordCloud(done){

    query('cloud', {
        center: true,
        layer: true
    }, function(keywords){

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
        
        //console.log("keywords", keywords);

        var max = 0;

        var preview = $('#canvas-word-cloud-preview');
        var view = $('#canvas-word-cloud-view');

        for(var i=0,l=keywords.length;i<l;i++){
            if(keywords[i][1] > max) max = keywords[i][1];
        }

        var limit = 10; //limit words number
        //console.log(keywords);

        function setCanvas($el, oldHeight){
            var h = $el.height();
            if(h > oldHeight) limit = 20;
            else limit = 10;

            WordCloud($el.get(0), { 
                list: keywords.slice(0, limit),
                weightFactor: function(size){
                    return h*0.2*size/max+h*0.1;
                },
                color: 'rgba(0, 0, 59, 1)'
            });
        }

        var OnHoverToken = null;
        $("#word-cloud.preview").hover(function(){

            clearTimeout(OnHoverToken);
            preview.hide();
            var height = preview.height();
            OnHoverToken = setTimeout(function(height){
                preview.show();
                preview.toggleClass("hoverAnimation");
                setCanvas(preview, height);
            }, 1000, height);
        });

        setCanvas(preview);
        done();
    });

    
}


/**********************************************
 *
 *----------------HEAT MAP
 *
 * ********************************************/
function createHeatMap(done){
    query('heatmap', {
        boundary: true,
        details: true,
        keywords: true,
        date: true,
        time: true,
        day: true
    }, function(points, filters){

        //console.log(points);
        //console.log(filters);

        var isDetails = JSON.parse(filters.dl);

        var gmPoints = [];
        if(isDetails){
            var minWeight = Number.MAX_VALUE;

            for(var i=0,l=points.length;i<l;i++){
                if(points[i][1] < minWeight) minWeight = points[i][1];
            }

            for(var i=0,l=points.length;i<l;i++){
                var p = Utils.decodeHash(points[i][0], 1);
                var point = new google.maps.LatLng(p[0], p[1]);
                var weight = Math.floor(points[i][1]/minWeight);
                gmPoints.push({location: point, weight: weight});
            }
        }else{
            for(var i=0,l=points.length;i<l;i++){
                var p = points[i];
                gmPoints.push(new google.maps.LatLng(p[0], p[1]));
            }
        }

        //console.log(gmPoints);

        heatmap = new google.maps.visualization.HeatmapLayer({
            data: gmPoints,
            map: map,
            maxIntensity: 10,
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

        done();
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
function createPopularityMap(done){

    query('popularity', {
        boundary: true,
        details: true,
        keywords: true,
        date: true,
        time: true,
        day: true
    }, function(hours, filters){

        //split
        var labels = [];
        var data = [];
        var isDetails = JSON.parse(filters.dl);

        for(var i=0,l=hours.length;i<l;i++){
            var hour = hours[i];
            labels.push(hour[0]);
            data.push(hour[1]);
        }

        if(isDetails){
            for(var i=0,l=labels.length;i<l;i++){
                labels[i] = Utils.decodeDaytime(labels[i]);
            }
        }

        //console.log(labels);

        //print preview chart
        chart = new Chart(document.getElementById('canvas-popularity-preview'), {
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

        done();
    });

}

function removePopularityMap(){
    //remove columns
    if(!chart) return

    chart.destroy();
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

function _formatHour(hour){
    if(parseInt(hour) < 10){
        hour = "0" + hour;
    }
    return hour;
}

function getFilters(filters){

    var processed = {};

    if(filters !== undefined && filters != null){

        //get date
        if(filters.date){
            var date = [
               _formatDate(new Date(Date.parse($( '#input-date-start' ).datepicker('getDate')))),
               _formatDate(new Date(Date.parse($( '#input-date-end' ).datepicker('getDate'))))
            ];

            processed.d = date;
        }

        //get times
        if(filters.time){
            var time = [
                _formatHour($( "#input-time" ).slider( "values", 0 )) + ":00:00",
                _formatHour($( "#input-time" ).slider( "values", 1 )) + ":00:00"
            ];

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
            if($("#input-day-mo").is(":checked")) days.push(1);
            if($("#input-day-tu").is(":checked")) days.push(2);
            if($("#input-day-we").is(":checked")) days.push(3);
            if($("#input-day-th").is(":checked")) days.push(4);
            if($("#input-day-fr").is(":checked")) days.push(5);
            if($("#input-day-sa").is(":checked")) days.push(6);
            if($("#input-day-su").is(":checked")) days.push(7);

            processed.ds = days;
        }

        if(filters.boundary){
            processed.b = boundary.toJSON();
        }

        if(filters.layer){

            processed.l = layer;
        }

        if(filters.center){
            var center = null;
            if(!clickPoint){
                center = map.getCenter();
            }else{
                center = clickPoint;
            }
            center = center.toJSON();

            processed.c = [center.lat, center.lng];
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

    //console.log("Processed", processed);
    return processed;
}

function query(type, filters, success){

    if(typeof filters === 'function'){
        success = filters;
        filters = null;
    }

    var filtered = getFilters(filters);

    //console.log("Query: ", type);
    //console.log("Filtered", filtered);

    $.get(
        'json/' + type,
        filtered,
        function(data){
            //console.log("Success: ", type);
            //console.log(data);
            success(data, filtered);
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
    params = url.split("/");
}

/**********************************************
 *
 *---------------LOADER
 *
 * ********************************************/

var queue = 0
function enqueue(){
    if(queue == 0){
        //show loader
        $("#loader").show();
    }
    queue++;
    //console.log("Queue", queue);
}

function dequeue(){
    if(queue == 1){
        //hide loader
        $("#loader").hide();
    }
    if(queue > 0) queue--;
    //console.log("Dequeue", queue);
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

    var precisions = [0, 0.2, 1.0, 5.0];

    function from64(s){
        var n = 0, m = 1;
        for(var l=s.length,i=l-1;i>=0;i--){
            var d = s[i];
            n += m*digits[d];
            m *= 64
        }

        return n;
    }

    function toDeg(km){
        return (km / 40000) * 360;
    }

    function layerPrecision(index){
        return precisions[index];
    }

    return {
        decodeHash: function(hash, layer){
            var precision = layerPrecision(layer);
            var degs = toDeg(precision);
            var cols = Math.floor(360 / degs);

            var n = from64(hash);
            var lng = n % cols;
            var lat = n / cols;

            lat *= degs;
            lng *= degs;

            //move to real grid
            lat -= 180;
            lng -= 180;

            //find center of grid square
            lat += degs/2;
            lng += degs/2;

            return [lat, lng];
        },

        decodeDaytime: function(daytime){
            if(daytime == 0) return '0-4';
            else if(daytime == 1) return '4-12';
            else if(daytime == 2) return '12-17';
            else if(daytime == 3) return '17-22';
            else if(daytime == 4) return '22-24';
            else return '0';
        }
    };
}