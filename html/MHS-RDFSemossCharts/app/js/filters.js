'use strict';

/* Filters */

angular.module('rdfGraphFilters', [])
    .filter('removeUnderScoreAddSpaces', function () {
        //gets last value in url after the /
        return function (str) {
            if (str) {
                var shortKey = '';
                shortKey = str.replace(/_/g, " ");
                return shortKey;
            }

            return;
        };
    }).filter('shortenDecimalPlaces', function () {
        //gets last value in url after the /
        return function (num) {
            if (num) {
                return parseFloat(num).toFixed(2);
            }

            return;
        };
    }).filter('replaceUnderscores', function () {
        return function (str) {
            str = String(str);
            var myRe = new RegExp("([^/]*)$"),
                shortStr = myRe.exec(str),
                result = shortStr[0].replace(/_/g, " ");

            return result;
        };
    }).filter('replaceSpaces', function () {
        return function (str) {
            str = String(str);
            var myRe = new RegExp("([^/]*)$"),
                shortStr = myRe.exec(str),
                result = shortStr[0].replace(/ /g, "_");

            return result;
        };
    }).filter('shortenAndReplaceUnderscores', function () {
        //will take the string after the last slash and will replaces underscores with spaces
        return function (str) {
            str = String(str);
            var returnStr = "",
                shortStr = "",
                result = "",
                myRe;

            if (str.indexOf('"') === 0) {
                shortStr = str.substring(1, str.length);
                returnStr = shortStr.substring(0, shortStr.indexOf('"')).replace(/_/g, " ");
                return returnStr;
            }
            if (str.indexOf('http://') !== -1) {
                returnStr = '';
                myRe = new RegExp("([^/]*)$");
                shortStr = myRe.exec(str);
                result = shortStr[0].replace(/_/g, " ");
                /*if (result.length > 20) {
                 returnStr = result.replace(/.{20}\S*\s+/g, "$&@").split(/\s+@/);
                 if (returnStr.length > 1) {
                 return returnStr[0] + "...";
                 }

                 return returnStr[0];
                 }*/

                return result;
            }

            return str;
        };
    });
