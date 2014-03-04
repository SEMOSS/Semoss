'use strict';

// Declare app level module which depends on filters, and services
var app = angular.module('app', []);

app.controller('IndexCtrl', ['$scope', '$http', function($scope, $http) {

    /* $http.get("data.json").success(function(jsonData) {
        $scope.data = jsonData;
    }); */

	$scope.setJSONData = function (data) {
		
        $scope.$apply(function () {
            
            $scope.data = jQuery.parseJSON(data);

        });

    }; 

}]);

app.filter('replaceUnderscores', function() {
    // Takes the string after the last slash and replaces underscores with spaces
    return function(str) {
        str = String(str);
        var myRe = new RegExp("([^/]*)$");
        var shortStr = myRe.exec(str);
        var result = shortStr[0].replace(/_/g, " ");
        
        return result;
    }
});
