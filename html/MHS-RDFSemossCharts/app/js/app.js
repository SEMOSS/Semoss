'use strict';

angular.module('rdfgraph', ['ui', 'ui.bootstrap', 'rdfGraphFilters', 'rdfGraphServices', 'rdfGraphDirectives']).
  config(['$routeProvider', function($routeProvider) {
    $routeProvider.
		when('/', {
			controller: indexCtrl,
			templateUrl: 'index.html'
		}).
        when('/grid', {
			controller: gridCtrl,
			templateUrl: 'grid.html'
		}).
        when('/singlechart', {
			controller: SingleChartCtrl,
			templateUrl: 'singlechart.html'
		}).
		when('/timeline', {
			controller: TimelineCtrl,
			templateUrl: 'timeline.html'
		}).
		when('/lifecycle', {
			templateUrl: 'lifecycle.html'
		}).
		when('/singlechartgrid', {
			controller: SingleChartCtrl,
			templateUrl: 'singlechartgrid.html'
		}).
		otherwise({
			redirectTo: '/index.html'
		});
  }]);
