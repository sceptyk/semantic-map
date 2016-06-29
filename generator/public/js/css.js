$(function() {
    $("#slider-time").slider({
        range: true,
        min: 0,
        max: 24,
        step: 1,
        values: [0, 24],
        slide: function( event, ui ) {
	        $( "#range-time" ).text( ui.values[ 0 ] + ":00 - " + ui.values[ 1 ] + ":00");
	      }
    });
    $( "#range-time" ).text( $( "#slider-time" ).slider( "values", 0 ) +
      ":00 - " + $( "#slider-time" ).slider( "values", 1 ) + ":00");

    $( "#radio" ).buttonset();
});