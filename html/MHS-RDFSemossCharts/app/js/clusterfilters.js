'use strict';

/* filters */

app.filter('shortenValueFilter', function() {
    //gets last value in url after the /
    return function(str) {
        //convert to string
        str = String(str);
        if (str.indexOf('"') == 0) {
            var shortStr = str.substring(1, str.length);
            var returnStr = shortStr.substring(0, shortStr.indexOf('"'));
            return returnStr;
        }else if (str.indexOf('http://') !== -1){
            var myRe = new RegExp("([^/]*)$");
            var returnStr = myRe.exec(str);
            return returnStr[0];
        } else{
            return str;
        }
    }
});

app.filter('replaceForwardSlashWithCaretFilter', function() {
    return function(str) {
        //console.log('replaceForwardSlashWithUnderscoreFilter ' + str);
        if(str){
            return str.replace(new RegExp(/\//g), '^');
        }else{
            return str;
        }
    }
});

app.filter('namespaceFilter', function() {
    //returns value in url before the last / and then takes the last letter off that value
    return function(str) {
        if(str){
            var myRegExp = new RegExp("^(.*[/])");
            var str = myRegExp.exec(str);
            return str[0].substring(0, str[0].length - 1);
        }else{
            return str;
        }
    }
});

app.filter('relationshipsFilter', function($filter) {
    //gets last value in url after the /
    return function(data) {
        var returnData = [];
        if(data){
            for (var i = 0; i < data.length; i++){
                if(data[i].binding[1].uri[0].Text.indexOf('http://health.mil/ontologies/dbcm/Relation/') !== -1 && data[i].binding[1].uri[0].Text.indexOf('http://health.mil/ontologies/dbcm/Relation/Contains') == -1 && data[i].binding[1].uri[0].Text.indexOf($filter('shortenValueFilter')(data[i].binding[0].uri[0].Text)) == -1){
                    returnData.push(data[i]);
                }
            }
            return returnData;
        }else{
            return data;
        }

    }
});

app.filter('startFrom', function() {
    return function(input, start) {
        if(input){
            start = +start; //parse to int
            return input.slice(start);
        }else{
            return;
        }
    }
});

app.filter('beforeFilter', function() {
    //returns everything before the removePt character
    return function(str, removePt) {
        var returnStr = '';
        if(str){
            if(str.indexOf(removePt) !== -1) {
                var endPt = str.indexOf(removePt);
                returnStr = str.substring(0, endPt);
                return returnStr;
            } else {
                return str;
            }
        }else{
            return str;
        }
    }
});

app.filter('afterFilter', function() {
    //returns everything after the removePt character
    return function(str, removePt) {
        var returnStr = '';
        if(str){
            if(str.indexOf(removePt) !== -1) {
                var startPt = str.indexOf(removePt) + 1;
                returnStr = str.substring(startPt, str.length-1);
                return returnStr;
            } else {
                return str;
            }
        }else{
            return str;
        }
    }
});

app.filter('shortenAndReplaceUnderscores', function() {
    //will take the string after the last slash and will replaces underscores with spaces
    return function(str) {
        str = String(str);
        if (str.indexOf('"') == 0) {
            var shortStr = str.substring(1, str.length);
            var returnStr = shortStr.substring(0, shortStr.indexOf('"')).replace(/_/g, " ");
            return returnStr;
        }else if (str.indexOf('http://') !== -1){
            var returnStr = '';
            var myRe = new RegExp("([^/]*)$");
            var shortStr = myRe.exec(str);
            var result = shortStr[0].replace(/_/g, " ");
            if(result.length > 20) {
                returnStr = result.replace(/.{20}\S*\s+/g, "$&@").split(/\s+@/);
                if(returnStr.length > 1){
                    return returnStr[0] + "...";
                }else {
                    return returnStr[0];
                }
            } else {
                returnStr = result;
            }

            return returnStr;
        } else{
            return str;
        }
    }
});

app.filter('replaceUnderscores', function() {
    return function(str) {
        str = String(str);
        var myRe = new RegExp("([^/]*)$");
        var shortStr = myRe.exec(str);
        var result = shortStr[0].replace(/_/g, " ");

        return result;
    }
});

app.filter('propertyTypeFilter', function() {
    //returns everything after the removePt character
    return function(str) {
        var returnStr = '';
        if (str.indexOf("^^") == -1) {
            return "string";
        }
        if(str){
            if(str.indexOf("#") !== -1) {
                var startPt = str.indexOf("#") + 1;
                returnStr = str.substring(startPt, str.length-1);
                return returnStr;
            } else {
                return str;
            }
        }else{
            return str;
        }
    }
});

app.filter('reduceStringLength', function() {
    //returns the string
    return function(str, length) {

        if (!length) {
            length = 15;
        }

        if (str.length > length) {
            return str.substring(0, length) + "...";
        } else {
            return str;
        }
    }
});