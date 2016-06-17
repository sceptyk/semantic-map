function initMap() {
    var map = new google.maps.Map(document.getElementById('map'), {
        zoom: 11,
        center: {
            lat: 53.3169421,
            lng: -6.210036
        },
        mapTypeControl: false
    });
    var rectangle = new google.maps.Rectangle({
        strokeColor: '#FF0000',
        strokeOpacity: 0.8,
        strokeWeight: 2,
        fillColor: '#ffff00',
        fillOpacity: 0.15,
        map: map,
        bounds: {
            north: 53.189579,
            south: 53.447171,
            east: -6.017761,
            west: -6.421509
        }
    });
}

$(function() {
    $("#slider-time").slider({
        range: true,
        min: 0,
        max: 24,
        step: 6,
        values: [6, 18],
        slide: function( event, ui ) {
	        $( "#range-time" ).val( ui.values[ 0 ] + ":00 - " + ui.values[ 1 ] + ":00");
	      }
    });
    $( "#range-time" ).val( $( "#slider-time" ).slider( "values", 0 ) +
      ":00 - " + $( "#slider-time" ).slider( "values", 1 ) + ":00");

    $( "#radio" ).buttonset();
});