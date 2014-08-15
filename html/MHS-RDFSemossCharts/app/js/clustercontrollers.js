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
//            $scope.groupData = {};

            $scope.groupData = groupData;
//            console.log($scope.groupData);
        });
    };

    $scope.setNodeData = function(nodeData) {
        $scope.$apply(function(){
            $scope.nodeData = nodeData;
            console.log(nodeData);
        });
    };
});