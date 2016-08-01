$(function() {

    //SLIDER TIME
    $("#input-time").slider({
        range: true,
        min: 0,
        max: 24,
        step: 1,
        values: [0, 24],
        slide: function(event, ui) {
            $("#input-time-value").text(ui.values[0] + ":00 - " + ui.values[1] + ":00");
        }
    });
    
    $("#input-time-value").text($("#input-time").slider("values", 0) + ":00 - " + $("#input-time").slider("values", 1) + ":00");

    //SLIDER DATE
    $("#input-date").slider({
        range: true,
        min: 0,
        max: 24,
        step: 1,
        values: [0, 24],
        slide: function(event, ui) {
            $("#input-date-value").text(ui.values[0] + ":00 - " + ui.values[1] + ":00");
        }
    });
    
    $("#input-date-value").text($("#input-time").slider("values", 0) + ":00 - " + $("#input-time").slider("values", 1) + ":00");
    
});