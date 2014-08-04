'use strict';

/* Controllers */
app.controller('dataCtrl', function($scope, $http) {
    console.log("in controller");

    $scope.setJSONData = function (data) {
        $scope.$apply(function () {
            $scope.data = data;
        });
    };

    $scope.setGroupData = function(groupData) {
        $scope.$apply(function(){
            $scope.groupData = groupData;
        });
    };

    $scope.setNodeData = function(nodeData) {
        $scope.$apply(function(){
            $scope.nodeData = nodeData;
            console.log(nodeData);
        });
    };
});