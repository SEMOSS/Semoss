'use strict';

// Declare app level module which depends on filters, and services
var app = angular.module('app', []);

app.controller('IndexCtrl', ['$scope', '$http', function($scope, $http) {

    $http.get("data.json").success(function(jsonData) {
        $scope.data = jsonData;
    });

	/* $scope.setJSONData = function (data) {
		
        $scope.$apply(function () {
            
            $scope.data = jQuery.parseJSON(data);

            var capability = data.CAPABILITY_GROUP_QUERY; // this query includes capability, cap desc, cap group, and cap group desc
            var capDesc = data.CAPABILITY_GROUP_QUERY;

            $scope.conops = data.CONOPS_SOURCE_QUERY;
            var date = data.DATE_GENERATED_QUERY;

            var capGroup = data.CAPABILITY_GROUP_QUERY; 
            var capGroupDesc = data.CAPABILITY_GROUP_QUERY;
            var missionOutcomes = data.MISSION_OUTCOME_QUERY;
            var participants = data.PARTICIPANTS_QUERY;

            var busStds = data.BS_COUNT_QUERY;
            var busReqs = data.BR_COUNT_QUERY;
            var busProcs = data.BP_COUNT_QUERY;
            var techStds = data.TS_COUNT_QUERY;
            var techReqs = data.TR_COUNT_QUERY;
            var sysCount = data.SYSTEM_COUNT_QUERY;
            var dataCount = data.DATA_COUNT_QUERY; // just need provide and consume
            var bluCount = data.BLU_COUNT_QUERY;
            var taskCount = data.TASK_COUNT_QUERY;

            var dataObjects = data.DATA_OBJECT_QUERY;
            var dataObjectsCRM = data.DATA_OBJECT_QUERY;
            var fGaps = data.FUNCTIONAL_GAP_QUERY;
            var BLUs = data.BLU_QUERY;

        });

    }; */

}]);

app.filter('replaceUnderscores', function() {
    //will take the string after the last slash and will replaces underscores with spaces
    return function(str) {
        str = String(str);
        var myRe = new RegExp("([^/]*)$");
        var shortStr = myRe.exec(str);
        var result = shortStr[0].replace(/_/g, " ");
        
        return result;
    }
});
