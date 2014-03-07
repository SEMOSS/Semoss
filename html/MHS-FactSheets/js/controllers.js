'use strict';

var controllers = angular.module('app.controllers', [])

	controllers.controller('IndexCtrl', ['$scope', '$http', '$location', '$anchorScroll', function($scope, $http, $location, $anchorScroll) {

    	// Uncomment this part of the code to test JSON data locally
    	/* $http.get("capabilitylist.json").success(function(jsonData) {
        	$scope.list = jsonData;
    	}); */

        $scope.scrollTo = function(id) {
            $location.hash(id);
            $anchorScroll();
        }

		$scope.setJSONData = function (data) {
        	$scope.$apply(function () {
            	$scope.list = jQuery.parseJSON(data);
        	});
    	}; 

        $scope.getFactSheet = function(capName) {
            $scope.data = jQuery.parseJSON(singleCapFactSheet(capName));
            //$location.url("cap");
        }

	}]);