'use strict';

angular.module('rdfeditor', ['ui', 'rdfeditor.filters', 'rdfeditor.services', 'rdfeditor.directives', 'ui.bootstrap', '$strap.directives']).
    config(['$routeProvider', '$httpProvider', function($routeProvider, $httpProvider) {
      $routeProvider.
        when('/search/', {
            templateUrl: 'partials/search.html',
            controller: searchCtrl
        }).
	   when('/rdfnode/:uri', {
		   templateUrl: 'partials/rdfEdit.html',
             controller: RdfEditCtrl
             //resolve: RdfEditCtrl.resolve
		}).
	   otherwise({
             templateUrl: 'partials/404.html'
	   });
      
        // Use x-www-form-urlencoded Content-Type
        $httpProvider.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded;charset=utf-8';
         
        // Override $http service's default transformRequest
        $httpProvider.defaults.transformRequest = [function(data)
        {
            return angular.isObject(data) && String(data) !== '[object File]' ? jQuery.param(data) : data;
        }];
  }]);

