'use strict';

/* Services */


// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('rdfGraphServices', [])

.factory('TimeLineService', function(){
	
	var timelineData = {};
	
	return {
        setData: function(data){
        	timelineData = data;
        },
        getData: timeLineData
    };
})
.factory('LifeCycleService', function(){
	
	var hardware = {
        retired: [],
        sunset: [],
        supported: [],
        ga: [],
        unknown: []
    };

    var allHardware = [];

    function convertMonthsToMiliSec(mth) {
        //The parameter 'mth' is actually an interval of 3 so we multiply it by the number of days in 3 months (91.25 days);
        //multiply the number by 3 months in days then by 24 hours in a day then by 60 mins in an hour then by 60 seconds in a min then by 1000 miliseconds in a second
        return mth * 91.25 * 24* 60 * 60 * 1000;
    }

    function convertMSToYears (ms) {
        if (isNaN(ms)){
            return "NA";
        } else {
            var years = ms/1000/60/60/24/365;
            return years; 
        }
    }

    function getTodaysDate () {
        //creating todays date variable
        var d = new Date();
        var td = d.getFullYear() + '-'
        + ('0' + String(d.getMonth()+1)).substr(-2) + '-'
        + ('0' + String(d.getDate())).substr(-2);

        return new Date(td);
    }
	
	return {
        setInitData: function(data){

	        for (var key in data){
	            allHardware.push({
	                "name": key,
	                "endOfLifeDate": data[key][0],
	                "upgradeHW": data[key][1],
	                "quantity": data[key][2],
	                "cost": data[key][3],
	                "totalCost": data[key][2]*data[key][3]
	            });
	        }
        },
        getData: hardware,
        //currently not used
        refreshData: function(startDate) {
        	for(var i = 0; i < hardware.retired.length; i++) {
        		hardware.retired.splice(i,1);
        	}
        	for(var i = 0; i < hardware.sunset.length; i++) {
        		hardware.sunset.splice(i,1);
        	}
        	for(var i = 0; i < hardware.supported.length; i++) {
        		hardware.supported.splice(i,1);
        	}
        	for(var i = 0; i < hardware.ga.length; i++) {
        		hardware.ga.splice(i,1);
        	}
        	for(var i = 0; i < hardware.unknown.length; i++) {
        		hardware.unknown.splice(i,1);
        	} 

	        //start code to see what timeframe the hardware lands in
	        for (var i=0; i<allHardware.length; i++) {
	            
	            //get the time left before software needs to be retired
	            var hwAge = convertMSToYears((new Date(allHardware[i].endOfLifeDate) - startDate));

	            if (hwAge < .5) {
	                hardware.retired.push(allHardware[i]);
	            } else if (hwAge >= .5 && hwAge < 1) {
	                hardware.sunset.push(allHardware[i]);
	            } else if (hwAge >= 1 && hwAge < 3) {
	                hardware.supported.push(allHardware[i]);
	            } else if (hwAge >= 3) {
	                hardware.ga.push(allHardware[i]);
	            } else {
	                hardware.unknown.push(allHardware[i]);
	            }
	        }
        },
        getHardwareLifeCycles: hardware,
        convertMonthsToMiliSec: function(mth) {
        	return mth * 91.25 * 24* 60 * 60 * 1000;
        },
        convertMSToYears: function(ms) {
        	if (isNaN(ms)){
	            return "NA";
	        } else {
	            var years = ms/1000/60/60/24/365;
	            return years;
	        }
        },
        getTodaysDate: function () {
	        //creating todays date variable
	        var d = new Date();
	        var td = d.getFullYear() + '-'
	        + ('0' + String(d.getMonth()+1)).substr(-2) + '-'
	        + ('0' + String(d.getDate())).substr(-2);

	        return new Date(td);
	    }
    };
});
