'use strict';

describe("testing controllers: ", function() {	
	describe("RdfEditCtrl: ", function() {
	  var scope;
	  
	  beforeEach(inject(function($rootScope, $routeParams, $q, nodeService, $filter, $http) {
		scope = $rootScope.$new();
		testNodeService = { 
			getOutboundRelationships: function() {
				var deferred = $q.defer();
				$http.get('testdata.json').success(function(data) {	
					deferred.resolve(data);
				});
				return deferred.promise;
			},
			getNodeValues: function() {
				var deferred = $q.defer();
				$http.get('testdata.json').success(function(data) {	
					deferred.resolve(data);
				});
				return deferred.promise;
			},
			getInboundRelationships: function() {
				var deferred = $q.defer();
				$http.get('testdata.json').success(function(data) {	
					deferred.resolve(data);
				});
				return deferred.promise;
			}
		}
		
		
		  
		$controller(RdfEditCtrl, {$scope: scope, $routeParams: $routeParams, $q: $q, nodeService: testNodeService, $filter: $filter});
		  
	  }));

		
	  /*it("Login button is disabled in Local mode", function() {
		//username and password is already blank
		scope.user = {username: '', password: ''};
		scope.loginSelection = "Local";
		
		//recreate the logic from the template and controller that determines when form is no longer invalid
		scope.form = {$invalid: true}; 
		if(scope.user.username && scope.user.password && scope.loginSelection){
			scope.form.$invalid = false;
		}
		expect(scope.isDisabled()).toBe(true);
	  });*/
	 
	  afterEach(function() {
		scope = null;
	  });
	});
	
	
});
	



