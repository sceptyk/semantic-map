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

    //SLIDER DAYTIME
    var times = [0, 4, 12, 17, 22, 24];
    $("#input-daytime").slider({
        range: true,
        min: 0,
        max: 5,
        step: 1,
        values: [0, 5],
        slide: function(event, ui) {
            $("#input-time-value").text(times[ui.values[0]] + ":00 - " + times[ui.values[1]] + ":00");
            $("#input-time").slider("option", "values", [times[ui.values[0]], times[ui.values[1]]]);
        }
    });

    
    //DATE
    $("#input-date-start").datepicker({minDate: "-14d"});
    $("#input-date-start").datepicker("setDate", "-14d");
    $("#input-date-end").datepicker({minDate: "-14d"});
    $("#input-date-end").datepicker("setDate", "0d");
    
    $("#input-date-value").text($("#input-time").slider("values", 0) + ":00 - " + $("#input-time").slider("values", 1) + ":00");
    
    $("#toggle-filters").click(function(){
        $(".menu-items").slideToggle();
    });

    $("#toggle-details").click(function(){
        $(this).toggleClass("active");

        if($(this).hasClass("active")){
            $("#input-time").hide();
            $("#input-daytime").show();
        }else{
            $("#input-time").show();
            $("#input-daytime").hide();
        }
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