'use strict';

angular.module('app.controllers', [])

	.controller('IndexCtrl', ['$scope', '$http', function($scope, $http) {

    	/* // Uncomment this part of the code to test JSON data locally
    	$http.get("data2.json").success(function(jsonData) {
        	$scope.data = jsonData;
    	}); */

		$scope.setJSONData = function (data) {
		
        	$scope.$apply(function () {
            
            	$scope.data = jQuery.parseJSON(data);

        	});

    	}; 

	}]);