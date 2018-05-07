var collect_data = function() {
	var collected_data = [];
	$.getJSON("../data/log_1524222080.json", function (data) {
        $.each(data, function (index, value) {
        	 dt = new Date(value["time"]);
        	 var new_dp = [dt , value["value"]];
        	 collected_data.push(new_dp);
        });
    });
    return collected_data;
}