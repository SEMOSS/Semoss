(function () {
    "use strict";

    angular.module("app.services.utility", [])
        .factory("utilityService", utilityService);

    utilityService.$inject = ["$filter"];

    function utilityService($filter) {
        return {
            createButtonParamCheck: function (visualOptionsList) {

                var alertFlag = false,
                    blankOptions = [];

                if (visualOptionsList !== {} || visualOptionsList !== "") {
                    for (var i = 0; i < visualOptionsList.length; i++) {
                        if (visualOptionsList[i].selected === "" && !visualOptionsList[i].optional) {
                            console.log(visualOptionsList[i].name.search("Optional"));
                            alertFlag = true;
                            blankOptions.push(visualOptionsList[i].name);
                        }
                    }
                }
                if (alertFlag) {
                    var alertString = "";
                    for (var i = 0; i < blankOptions.length; i++) {
                        if (i === 0) {
                            alertString = blankOptions[i];
                        } else if (i !== blankOptions.length - 1) {
                            alertString = alertString + ", " + blankOptions[i];
                        } else {
                            alertString = alertString + " and " + blankOptions[i];
                        }
                    }
                    alert("Please provide input for " + alertString);
                }
                return !alertFlag;
            },
            //formats table data, also sets up a filtered data object that removes underscores and uris from keys/values and headers/data
            formatTableData: function (headers, data, removeUri) {
                var formattedData = {
                        headers: [],
                        data: []
                    },
                    filteredData = [];


                //create array of objects from the scope.data.data array using the headers
                data.forEach(function(item) {
                    var newObjUri = {},
                        newObjNoUri = {};

                    for (var i=0; i<item.length; i++) {
                        if (0===item[i]) {
                            newObjUri[headers[i]] = item[i].uriString;
                            newObjNoUri[$filter("replaceUnderscores")(headers[i])] = $filter("shortenAndReplaceUnderscores")(item[i].uriString);

                        } else {
                            newObjUri[headers[i]] = $filter("replaceUnderscores")(item[i]);
                            newObjNoUri[$filter("replaceUnderscores")(headers[i])] = $filter("replaceUnderscores")(item[i]);
                        }
                    }
                    if (removeUri) {
                        filteredData.push(newObjNoUri);
                    }

                    formattedData.data.push(newObjUri);
                });

                //add the headers as objects
                headers.forEach(function(header) {
                    var newObj = {title: header, filteredTitle: $filter("replaceUnderscores")(header), filter: {}};
                    newObj.filter[header] = "";
                    formattedData.headers.push(newObj);
                });

                //only want to return the filteredData if they asked for it
                if (removeUri) {
                    formattedData.filteredData = filteredData;
                }

                return formattedData;
            },
            //filters data, removes all underscores from keys and values
            filterTableUriData: function(uriData) {
                var filteredData = [],
                    headerKeys = _.keys(uriData[0]);

                uriData.forEach(function(item) {
                    var newObj = {};

                    for (var i=0; i<headerKeys.length; i++) {
                        newObj[$filter("replaceUnderscores")(headerKeys[i])] = $filter("shortenAndReplaceUnderscores")(item[headerKeys[i]]);
                    }

                    filteredData.push(newObj);
                });

                return filteredData;
            },
            //need this for the time beinf until we switch to new table platform
            //have to have underscores for filtering tables
            addUnderscoresToTableData: function(data) {
                var formattedData = [];
                for (var i=0; i<data.length; i++) {
                    var newObj = {};
                    for (var key in data[i]) {
                        newObj[$filter("replaceSpaces")(key)] = $filter("replaceSpaces")(data[i][key]);
                    }
                    formattedData.push(newObj);
                }

                return formattedData;
            }
        };
    }
})();