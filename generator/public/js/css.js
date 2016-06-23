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
    $( "#range-time" ).text( $( "#slider-time" ).slider( "values", 0 ) +
      ":00 - " + $( "#slider-time" ).slider( "values", 1 ) + ":00");

    $( "#radio" ).buttonset();
});