'use strict';

var filters = angular.module('app.filters', [])
	filters.filter('replaceUnderscores', function() {
    	// Takes the string after the last slash and replaces underscores with spaces
    	return function(str) {
        	str = String(str);
        	var myRe = new RegExp("([^/]*)$");
        	var shortStr = myRe.exec(str);
        	var result = shortStr[0].replace(/_/g, " ");
        
        	return result;
    	}
});