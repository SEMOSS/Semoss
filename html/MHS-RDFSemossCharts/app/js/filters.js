'use strict';

/* Filters */

angular.module('rdfGraphFilters', [])
    .filter('removeUnderScoreAddSpaces', function() {
	//gets last value in url after the /
	return function(str) {
        if (str) {
            var shortKey = '';
            shortKey = str.replace(/_/g, " ")
            return shortKey;
        } else{
            return;
        }
        
	}
}).filter('shortenDecimalPlaces', function() {
	//gets last value in url after the /
	return function(num) {
        if (num) {
            return parseFloat(num).toFixed(2);
        } else {
            return;
        }
        
	}
});
