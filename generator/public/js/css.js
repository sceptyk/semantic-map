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

    //DATE
    $("#input-date-start").datepicker();
    $("#input-date-start").datepicker("setDate", "-14d");
    $("#input-date-end").datepicker();
    $("#input-date-end").datepicker("setDate", "0d");
    
    $("#input-date-value").text($("#input-time").slider("values", 0) + ":00 - " + $("#input-time").slider("values", 1) + ":00");
    
    $("#toggle-filters").click(function(){
        $(".menu-items").slideToggle();
    });

    $("#toggle-details").click(function(){
        $(this).toggleClass("active");
    });

    /*var PINNED = false;
    $(".preview").click(function(){
        PINNED = PINNED ? false : true;
    });
    $(".preview").hover(function(){
        if(!PINNED){
            $(this).siblings('.view').show();
        }
    }, function(){
        if(!PINNED){
            $(this).siblings('.view').hide();
        }
    });*/
});