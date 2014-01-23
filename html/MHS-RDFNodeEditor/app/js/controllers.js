'use strict';

/* Controllers */

function IndexCtrl($scope, $rootScope, $location) {
	
	// param passed from "FoxHole" application
    $scope.setJsonData = function(param) {
         $scope.$apply(function() {
              console.log("setJsonData function!" + param);
         });
    };
    
    $scope.alert = {
        "type": "loading",
        "title": "",
        "content": "",
        closed: false
    };
    
    $rootScope.$on("$routeChangeStart", function (event, next, current) {
        $scope.alertType = "";
        $scope.alertMessage = "Loading...";
        $scope.active = "progress-striped active progress-warning";
        $scope.stage = { status: "loading" };
        
        $scope.alert = {
                "type": "loading",
                "title": "Loading...",
                "content": "retrieving data",
                closed: false
        };
    });
    
    $rootScope.$on("$routeChangeSuccess", function (event, current, previous) {
        $scope.alertType = "success";
        $scope.alertMessage = "Successfully loaded data";
        $scope.active = "progress-success";
        $scope.stage = { status: "success" };
        
        $scope.alert = {
                "type": "success",
                "title": "Success!",
                "content": "retrieved data",
                closed: false
        };
        $scope.newLocation = $location.path();
    });
    
    $rootScope.$on("$routeChangeError", function (event, current, previous, rejection) {
        console.log("ROUTE CHANGE ERROR: " + rejection);
        $scope.alertType = "error";
        $scope.alertMessage = "Failed to load data";
        $scope.active = "";
        $scope.stage = { status: "error" };
        
        $scope.alert = {
                "type": "error",
                "title": "ERROR!",
                "content": "Failed to load data",
                closed: false
        };
    });
    
    $scope.toSearch = function(){
        $location.url("/search");
        $scope.searchResults = []; //creates a NEW search array on page refresh
    }
}

function searchCtrl($scope, $q, searchNodeService) {
	$scope.searchResults = []; //creates a NEW search array on page refresh
    /**
     * @name getURI
     * @param search 
     * set of characters currently input into the search field
     * @description
     * Gets back a list of URIs based on the search parameter
     */            
	$scope.getURIList = function(search){
		var deferred = $q.defer();
		
		searchNodeService.search(search).then(function(data){
			deferred.resolve(data);
		});
			
		return deferred.promise;
	}
    
    $scope.getSearchURIList = function(search){
		var deferred = $q.defer();
		
		searchNodeService.search(search).then(function(data){
			$scope.searchResults = data;
            deferred.resolve(data);
		});
			
		return deferred.promise;
	}
	
	//Search pagination
	$scope.currentPage4 = 0;
	$scope.pageSize4 = 10;
	$scope.numberOfPagesSearch=function(){
	    if($scope.searchResults.length > 0){
		 return Math.ceil($scope.searchResults.length/$scope.pageSize4);
	    }else{
		   return 1;
	    }
	}	
    
}


/*RdfEditCtrl.resolve = {
					loadInboundRelationshipData: function($q, $route, $filter, nodeService) {
						var uri = ($route.current.params.uri).replace(new RegExp(/\^/g), '/');
						alert(uri);
						return nodeService.getInboundRelationships (uri);
					}, 
					loadOutboundRelationshipData: function($q, $route, $filter, nodeService) {
						var uri = ($route.current.params.uri).replace(new RegExp(/\^/g), '/');
						return nodeService.getOutboundRelationships (uri);
					},
					loadPropertiesAndValuesData: function($q, $route, $filter, nodeService) {
						var uri = ($route.current.params.uri).replace(new RegExp(/\^/g), '/');
						return nodeService.getNodeValues(uri);
					}
				}*/

function RdfEditCtrl($scope, $routeParams, $q, nodeService, editNodeService, editRelationshipService, $filter/*, loadInboundRelationshipData, loadOutboundRelationshipData, loadPropertiesAndValuesData*/) {
    // retrieve uri paramter and replace all '_' with forward slashes
    //var uri = ($routeParams.uri).replace(new RegExp(/\^/g), '/');
	//var uri = '<http://health.mil/ontologies/Concept/BusinessProcess/Clinical_Laboratory>'
    var nodeProperty = {};
    $scope.propertyTypeSelected = '';
    $scope.nodePropertyOptions = [{name: "String", value: ""}, {name: "Double", value: "^^<http://www.w3.org/2001/XMLSchema#double>"}, {name: "Integer", value: "^^<http://www.w3.org/2001/XMLSchema#integer>"}, {name: "Date", value: "^^<http://www.w3.org/2001/XMLSchema#dateTime>"}, {name: "Boolean", value: "^^<http://www.w3.org/2001/XMLSchema#boolean>"}];
    $scope.nodeProperties = [];
    $scope.inboundRelationships = [];
    $scope.outboundRelationships = [];
    $scope.alerts = [];
    $scope.nodeURI = '';
    $scope.searchResults = [];
    
    //the other lists i'll need to put together the create and edit modals
    $scope.outNodeTypeOptions = [];
    $scope.outNodeTypeSelection = '';
    $scope.outNodeInstanceOptions = [];
    $scope.outNodeInstanceSelection = '';
    $scope.outNodeVerbOptions = [];
    $scope.outNodeVerbSelection = '';
    $scope.inNodeTypeOptions = [];
    $scope.inNodeTypeSelection = '';
    $scope.inNodeInstanceOptions = [];
    $scope.inNodeInstanceSelection = '';
    $scope.inNodeVerbOptions = [];
    $scope.inNodeVerbSelection = '';
    

    var inSubjectPromise, inPredicatePromise, inObjectPromise, inTriplePromise, outSubjectPromise, outPredicatePromise, outObjectPromise, outTriplePromise;

	$scope.setNodeData = function (uri, nodeName, nodeType) {
		$scope.$apply(function() {
			$scope.nodeURI = uri;
			$scope.nodeType = nodeType;
			$q.all([
			    nodeService.getNodeValues($scope.nodeURI),
			    nodeService.getOutboundRelationships($scope.nodeURI),
			    nodeService.getInboundRelationships($scope.nodeURI)
			]).then(function(data){
				//set scope variables with the return data from the $q.all 
				$scope.outboundRelationships = data[1];
				$scope.inboundRelationships = data[2];
				for (var i=0; i<data[0].length; i++) {
					var shortVal = $filter("shortenValueFilter")(String(data[0][i][2]));
					$scope.nodeProperties.push([data[0][i][0], data[0][i][1], shortVal, data[0][i][2]]);
				}
			});
		});
	};
    
	$scope.getOutNodeInstanceOptions = function () {
		nodeService.getInstanceOptions($scope.outNodeTypeSelection).then(function (data){
				$scope.outNodeInstanceOptions = data;
		});
	}
	
	$scope.getInNodeInstanceOptions = function () {
		nodeService.getInstanceOptions($scope.inNodeTypeSelection).then(function (data) {
			$scope.inNodeInstanceOptions = data;
		});
	}
	
	$scope.getOutNodeVerbOptions = function () {
		nodeService.getVerbOptions($scope.nodeType, $scope.outNodeTypeSelection).then(function(data){
				$scope.outNodeVerbOptions = data;
		});
	}
	
	$scope.getInNodeVerbOptions = function () {
		nodeService.getVerbOptions($scope.inNodeTypeSelection, $scope.nodeType).then(function(data){
				$scope.inNodeVerbOptions = data;
		});
	}
	
    $scope.createProperty = function () {
    	editNodeService.createNewProperty($scope.nodeURI, $scope.propertyName, $scope.propertyValue, $scope.propertyTypeSelected.value).then(function(data){
    		var propValWithType = '"' + $scope.propertyValue + '"^^' + $scope.propertyTypeSelected.value;
    		if ($scope.propertyTypeSelected.value == "") {
    			propValWithType = "string";
    		}
    		var newRecord = [$scope.nodeURI, 'http://semoss.org/ontologies/Relation/Contains/' + $scope.propertyName, $scope.propertyValue, propValWithType];
            $scope.nodeProperties.unshift(newRecord);
            $scope.alerts.push({ type: 'success', msg: 'Congratulations, you have successfully added a new Node Property.' });
            $scope.closeModal();
        }, function(){
            $scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to create a new property.' });
            $scope.closeModal();
        });
    };
    
    //looks through the array of nodeProperties and matches the nodeProperty that needs to be deleted and deletes it
    $scope.deleteProp = function () {
        editNodeService.deleteProperty($scope.nodeURI, $scope.deleteNodeProperty[1], $scope.deleteNodeProperty[2]).then(function(data){
            $scope.nodeProperties.splice($scope.deleteNodeIndex, 1);
            $scope.alerts.push({ type: 'success', msg: 'You have successfully deleted a Node Property...I hope you feel good about yourself.' });
            $scope.closeModal();
        }, function(){
            $scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to delete this property.' });
            $scope.closeModal();
        });
    };
    
    $scope.inboundDeleteRelationship = function () {
    	editRelationshipService.deleteRelationship($scope.inboundRelationships[$scope.deleteInboundIndex][0], $scope.inboundRelationships[$scope.deleteInboundIndex][1], $scope.inboundRelationships[$scope.deleteInboundIndex][3], $scope.inboundRelationships[$scope.deleteInboundIndex][4], $scope.inboundRelationships[$scope.deleteInboundIndex][5]).then(function () {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully deleted an inbound relationship...I hope you feel good about yourself.' });
    		$scope.inboundRelationships.splice($scope.deleteInboundIndex, 1);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to delete this relationship.' });
    		$scope.closeModal();
    	})
    };
    
    $scope.outboundDeleteRelationship = function () {
    	editRelationshipService.deleteRelationship($scope.outboundRelationships[$scope.deleteOutboundIndex][0], $scope.outboundRelationships[$scope.deleteOutboundIndex][1], $scope.outboundRelationships[$scope.deleteOutboundIndex][3], $scope.outboundRelationships[$scope.deleteOutboundIndex][4], $scope.outboundRelationships[$scope.deleteOutboundIndex][5]).then(function () {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully deleted an outbound relationship...I hope you feel good about yourself.' });
    		$scope.outboundRelationships.splice($scope.deleteOutboundIndex, 1);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to delete this relationship.' });
    		$scope.closeModal();
    	})
    };
    
    
    $scope.inboundCreateRelationship = function () {
    	editRelationshipService.createInboundRelationship($scope.inNodeTypeSelection, $scope.inNodeInstanceSelection, $scope.inNodeVerbSelection, $scope.nodeType, $scope.nodeURI).then(function () {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully created this Inbound Relationship.' });
    		var newRecord = [$scope.inNodeTypeSelection, $scope.inNodeInstanceSelection, "", $scope.inNodeVerbSelection, $scope.nodeType, $scope.nodeURI];
    		$scope.inboundRelationships.unshift(newRecord);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to create this relationship.' });
    		$scope.closeModal();
    	})
    }
    
    $scope.outboundCreateRelationship = function () {
    	editRelationshipService.createOutboundRelationship($scope.nodeType, $scope.nodeURI, $scope.outNodeVerbSelection, $scope.outNodeTypeSelection, $scope.outNodeInstanceSelection).then(function (data) {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully created this Outbound Relationship.' });
    		var newRecord = [$scope.nodeType, $scope.nodeURI, "", $scope.outNodeVerbSelection, $scope.outNodeTypeSelection, $scope.outNodeInstanceSelection];
    		$scope.outboundRelationships.unshift(newRecord);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to create this relationship.' });
    		$scope.closeModal();
    	})
    }
    
    $scope.inboundEditRelationship = function () {
    	editRelationshipService.editInboundRelationship($scope.editInRelationship[0], $scope.newEditInInstance, $scope.editInRelationship[3], $scope.editInRelationship[4], $scope.editInRelationship[5], $scope.editInRelationship[1]).then(function () {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully edited this Inbound Relationship.' });
    		$scope.inboundRelationships.splice($scope.editInboundIndex, 1);
    		var newRecord = [$scope.editInRelationship[0], $scope.newEditInInstance, "", $scope.editInRelationship[3], $scope.editInRelationship[4], $scope.editInRelationship[5]];
    		$scope.inboundRelationships.unshift(newRecord);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to edit this relationship.' });
    		$scope.closeModal();
    	})
    }
    
    $scope.outboundEditRelationship = function () {
    	editRelationshipService.editOutboundRelationship($scope.editOutRelationship[0], $scope.editOutRelationship[1], $scope.editOutRelationship[3], $scope.editOutRelationship[4], $scope.newEditOutInstance, $scope.editOutRelationship[5]).then(function () {
    		$scope.alerts.push({ type: 'success', msg: 'You have successfully edited this Outbound Relationship.' });
    		$scope.outboundRelationships.splice($scope.editOutboundIndex, 1);
    		var newRecord = [$scope.editOutRelationship[0], $scope.editOutRelationship[1], "", $scope.editOutRelationship[3], $scope.editOutRelationship[4], $scope.newEditOutInstance];
    		$scope.outboundRelationships.unshift(newRecord);
    		$scope.closeModal();
    	}, function(err){
    		$scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to edit this relationship.' });
    		$scope.closeModal();
    	})
    }
    
    
	//------------------------------Modal Windows----------------------------------//
    // Will get the parameter value from the url based on the 'name' string
    // Opens create modal

    $scope.modalCreateOpen = function () {
          $scope.createModal = true;
          $scope.propertyName = "";
          $scope.propertyValue = "";
          $scope.propertyTypeSelected = "";
    };
   
    //Opens Inbound Create Modal
    $scope.inboundModalCreateOpen = function () {
          $scope.inboundCreateModal = true;
          //clear the list values
          $scope.inNodeTypeSelection = [];
          $scope.inNodeInstanceSelection = [];
          $scope.inNodeVerbSelection = [];
          
          nodeService.getInboundTypeOptions($scope.nodeType).then(function(data) {
        	  $scope.inNodeTypeOptions = data;
          }, function(err) {
        	  $scope.alerts.push({ type: 'error', msg: 'Error:  Unable to retrieve Inbound Node Types' });
          });
    };
   
    //Opens Outbound Create Modal
    $scope.outboundModalCreateOpen = function () {
          $scope.outboundCreateModal = true;
        //clear the list values
          $scope.outNodeTypeSelection = [];
          $scope.outNodeInstanceSelection = [];
          $scope.outNodeVerbSelection = [];
          nodeService.getOutboundTypeOptions($scope.nodeType).then(function(data) {
        	  $scope.outNodeTypeOptions = data;
          }, function(err) {
        	  $scope.alerts.push({ type: 'error', msg: 'Error:  Unable to retrieve Outbound Node Types' });
          });
    };
   
    //opens delete modal and also takes the values of the property selected to be deleted and puts them in nodeProperty
    $scope.modalDeleteOpen = function (index) {
          $scope.deleteModal = true;
          $scope.deleteNodeProperty = this.property;
          //the index is based on the page so you have to add the previous pages items (5 per page)
          $scope.deleteNodeIndex = $scope.currentPage1 * 5 + index;
    };
   
    //opens inbound edit module
    $scope.inboundModalEditOpen = function (index) {
    	  
          $scope.inboundEditModal = true;
          $scope.editInRelationship = this.inboundRelationship;
          //the index is based on the page so you have to add the previous pages items (5 per page)
          $scope.editInboundIndex = $scope.currentPage2 * 5 + index;
          nodeService.getInstanceOptions(this.inboundRelationship[0]).then(function(data) {
        	  $scope.inEditNodeInstanceOptions = data;
        	  for (var i=0; i<$scope.inEditNodeInstanceOptions.length; i++) {
        		  if ($scope.inEditNodeInstanceOptions[i] == $scope.editInRelationship[1]) {
        			  $scope.newEditInInstance = $scope.inEditNodeInstanceOptions[i];
        		  }
        	  }
          }, function(err) {
        	  $scope.alerts.push({ type: 'error', msg: 'Error:  Unable to retrieve Inbound Instance Types' });
          });
    };
    
    //opens inbound delete module
    $scope.inboundModalDeleteOpen = function (index) {
        $scope.inboundDeleteModal = true;
        $scope.deleteInRelationship = this.inboundRelationship;
        //the index is based on the page so you have to add the previous pages items (5 per page)
        $scope.deleteInboundIndex = $scope.currentPage2 * 5 + index;
    };
    
    //opens outbound edit module
    $scope.outboundModalEditOpen = function (index) {
        $scope.outboundEditModal = true;
        $scope.editOutRelationship = this.outboundRelationship;
        //the index is based on the page so you have to add the previous pages items (5 per page)
        $scope.editOutboundIndex = $scope.currentPage3 * 5 + index;
        nodeService.getInstanceOptions(this.outboundRelationship[4]).then(function(data) {
        	$scope.outEditNodeInstanceOptions = data;
        	for (var i=0; i<$scope.outEditNodeInstanceOptions.length; i++) {
      		  if ($scope.outEditNodeInstanceOptions[i] == $scope.editOutRelationship[5]) {
      			  $scope.newEditOutInstance = $scope.outEditNodeInstanceOptions[i];
      		  }
      	  }
        }, function(err) {
      	  $scope.alerts.push({ type: 'error', msg: 'Error:  Unable to retrieve Outbound Instance Types' });
        });
    };
    
    //opens outbound delete module
    $scope.outboundModalDeleteOpen = function (index) {
        $scope.outboundDeleteModal = true;
        $scope.deleteOutRelationship = this.outboundRelationship;
        //the index is based on the page so you have to add the previous pages items (5 per page)
        $scope.deleteOutboundIndex = $scope.currentPage3 * 5 + index;
    };

    //Closes the create modal
    $scope.closeModal = function () {
        $scope.createModal = false;
        $scope.deleteModal = false;
        $scope.inboundCreateModal = false;
        $scope.outboundCreateModal = false;
        $scope.inboundDeleteModal = false;
        $scope.outboundDeleteModal = false;
        $scope.inboundEditModal = false;
        $scope.outboundEditModal = false;
    };

    $scope.closeAlert = function(index) {
        $scope.alerts.splice(index, 1);
    };
    
    $scope.isDisabled = function(){
        return $scope.form.$invalid;
    }
    
    //Get the number of pages for the pagination
    $scope.currentPage1 = 0;
    $scope.pageSize1 = 5;
    $scope.numberOfPagesNodeProperties=function(){
        if ($scope.nodeProperties.length > 0){
            return Math.ceil($scope.nodeProperties.length/$scope.pageSize1);
        }else{
            return 1;
        }
    }
    
    $scope.currentPage2 = 0;
    $scope.pageSize2 = 5;
    $scope.numberOfPagesInbound=function(){
        if($scope.inboundRelationships.length > 0){
            return Math.ceil($scope.inboundRelationships.length/$scope.pageSize2);
        }else{
            return 1;
        }
    }
    
    $scope.currentPage3 = 0;
    $scope.pageSize3 = 5;
    $scope.numberOfPagesOutbound=function(){
        if($scope.outboundRelationships.length > 0){
          return Math.ceil($scope.outboundRelationships.length/$scope.pageSize3);
        }else{
            return 1;
        }
    }
    
}
