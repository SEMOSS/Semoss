'use strict';

var app = angular.module('app', ['app.controllers',
                                'app.filters',
                             	'app.directives',
                             	'nvd3ChartDirectives',
                             	'ngRoute'
                                ]);

// Routing stuff 
app.config(['$routeProvider', function($routeProvider) {

	$routeProvider.
		when('/', {
			templateUrl: 'partials/list.html',
			controller: 'IndexCtrl'
		}).
		when('/cap', {
			templateUrl: 'partials/capfactsheet.html',
			controller: 'IndexCtrl'
		}).
		otherwise({
			redirectTo: '/'
		});

}])  