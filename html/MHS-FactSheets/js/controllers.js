'use strict';

var controllers = angular.module('app.controllers', [])

	controllers.controller('IndexCtrl', ['$scope', '$http', '$location', '$anchorScroll', function($scope, $http, $location, $anchorScroll) {

    	// Uncomment this part of the code to test JSON data locally
    	/**$http.get("capabilitylist.json").success(function(jsonData) {
        	$scope.list = jsonData;
    	});*/
    	
    	$http.get("export.json").success(function(jsonData) {
        	$scope.data = jsonData;
    	});

        $scope.scrollTo = function(id) {
			var old = $location.hash();
            $location.hash(id);
            $anchorScroll();
			$location.hash(old);
        }

		$scope.setJSONData = function(data) {
        	$scope.$apply(function () {
            	$scope.list = jQuery.parseJSON(data);
        	});
    	}; 

        $scope.getFactSheet = function(capName) {
            $scope.data = jQuery.parseJSON(singleCapFactSheet(capName));
            /* generate data for pie charts
             $scope.pieChartData = [
                {"key":"Create",
                "y":"data.dataSeries.CapabilityOverviewSheet.DATA_COUNT_QUERY[2]"},
                {"key":"Read",
                "y":"data.dataSeries.CapabilityOverviewSheet.DATA_COUNT_QUERY[1]"}
            ];
            alert(pieChartData); */
            
            //$location.url("cap");
        };

	}]);