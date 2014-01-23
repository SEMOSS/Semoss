'use strict';

angular.module('rdfGraphDirectives', [])

.directive('timeslider', function(LifeCycleService) {
	
	return {
		restrict: 'A',
		scope: {
			parentfunc: '&'
		},
		link: function(scope, elem, attr, controller) {
			elem.selectToUISlider({
		        labels: 6,
		        sliderOptions: {
		            change:function(e, ui) {
		                //refresh the life cycle data, based on the slider value
		                if(ui.value == 0){
		                    scope.parentfunc({startdate: LifeCycleService.getTodaysDate()});	
		                } else {
		                    //convert to milliseconds
		                    var millisecs = LifeCycleService.convertMonthsToMiliSec(ui.value);
		                    scope.parentfunc({startdate: LifeCycleService.getTodaysDate().getTime() + millisecs});
		                    
		                }
		            	scope.$apply();    
		            }
		        }
		    }).hide().next();
		}
	};
	
});
