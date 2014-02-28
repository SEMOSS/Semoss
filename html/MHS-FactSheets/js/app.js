'use strict';

// Declare app level module which depends on filters, and services
var app = angular.module('app', []);

app.controller('IndexCtrl', ['$scope', function($scope) {
	$scope.test = "yo";

	$scope.setJSONData = function (data) {
		
        $scope.$apply(function () {
            
            $scope.data = jQuery.parseJSON(data);
            
        });
    };

}]);