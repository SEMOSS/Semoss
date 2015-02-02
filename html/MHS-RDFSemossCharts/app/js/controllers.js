'use strict';

/* Controllers */

function indexCtrl($scope, $http) {
    $scope.types = {};
    $scope.types[0] = [];
    $scope.instances = {};
    $scope.properties = {};
    $scope.values = {};
    $scope.selectedInstances = {};
    $scope.typesSelected = {};
    $scope.instancesSelected = {};
    $scope.instancesSelected[0] = [];
    $scope.propertiesSelected = {};
    $scope.propertiesSelected[0] = [];
    $scope.groupby = 'Property';
    $scope.grid = {selected: false};
    
    
    $scope.setJSONData = function (data) {
        $scope.$apply(function () {
            
            $scope.data = jQuery.parseJSON(data);
            
            var key;
            for (key in $scope.data.Nodes) {
                $scope.types[0].push({'name': $scope.data.Nodes[key][0].propHash.VERTEX_TYPE_PROPERTY, 'selected': false});
            }
            
        });
    };
    
    //----------Comment the below $http.get when using in Java
    /*$http.get('lib/longDataNames.json').success(function(jsonData) {
        $scope.data = jsonData;
    
        var key;
        for (key in $scope.data.Nodes) {
            $scope.types[0].push({'name': $scope.data.Nodes[key][0].propHash.VERTEX_TYPE_PROPERTY, 'selected': false});
        }
    });*/

    
    $scope.typesChecked = function(){
        if(this.type.selected == true){

            var dim = this.dimension.index;
            var type = this.type.name;
            $scope.instances[dim] = [];
            $scope.instancesSelected[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.properties[dim] = [];
            $scope.values[dim] = [];
            var typeInstances = [];
            this.dimension.selectedIntance = false;
            
            
            if(!$scope.typesSelected[dim]){
                $scope.typesSelected[dim] = [];
            }
    
            
            $scope.typesSelected[dim].push(type);
            for(var i=0; i<$scope.typesSelected[dim].length; i++){
                for(var j=0; j<$scope.data.Nodes[$scope.typesSelected[dim][i]].length; j++){
                    typeInstances.push({'name': $scope.data.Nodes[$scope.typesSelected[dim][i]][j], 'selected': false});
                }
            }
            $scope.instances[dim] = typeInstances;
            
        }else if(this.type.selected == false){
            
            var dim = this.dimension.index;
            var type = this.type.name;
            $scope.typesSelected[dim] = _.without($scope.typesSelected[dim], type);
            $scope.instances[dim] = [];
            $scope.instancesSelected[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.properties[dim] = [];
            $scope.values[dim] = [];
            this.dimension.selectedType = false;
            this.dimension.selectedInstance = false;
            
            var typeInstances = [];
            for(var i=0; i<$scope.typesSelected[dim].length; i++){
                for(var j=0; j<$scope.data.Nodes[$scope.typesSelected[dim][i]].length; j++){
                    typeInstances.push({'name': $scope.data.Nodes[$scope.typesSelected[dim][i]][j], 'selected': false});
                }
            }
            
            $scope.instances[dim] = typeInstances;
            
            if($scope.typesSelected[dim].length == 0){
                $scope.instances[dim] = [];
            }
        }
    }    
    
    $scope.instancesChecked = function(){
        if(this.instance.selected == true){

            var dim = this.dimension.index;
            var instance = this.instance.name;
            $scope.propertiesSelected[dim] = [];
            $scope.values[dim] = [];
            $scope.instancesSelected[dim].push(instance);
            
            var key;
            var reducedProps = [];
            $scope.properties[dim] = [];
            for(var i=0; i<$scope.instancesSelected[dim].length; i++){
                for (key in $scope.instancesSelected[dim][i].propHash) {
                    reducedProps.push(key);
                }
            }
            reducedProps = _.uniq(reducedProps);
            
            for(var i=0; i<reducedProps.length; i++){
                $scope.properties[dim].push({'name':reducedProps[i], 'selected': false});
            }
            
        }else if(this.instance.selected == false){
            //declare and set variables
            var dim = this.dimension.index;
            var instance = this.instance.name;
            var key;
            var reducedProps = [];
            $scope.properties[dim] = [];
            $scope.values[dim] = [];
            this.allInstances = false;
            this.dimension.selectedInstance = false;
            
            $scope.instancesSelected[dim] = _.without($scope.instancesSelected[dim], instance);
            
            for(var i=0; i<$scope.instancesSelected[dim].length; i++){
                for (key in $scope.instancesSelected[dim][i].propHash) {
                    reducedProps.push(key);
                }
            }
            
            reducedProps = _.uniq(reducedProps);
            
            for(var i=0; i<reducedProps.length; i++){
                $scope.properties[dim].push({'name':reducedProps[i], 'selected': false});
            }
            
            if($scope.instancesSelected[dim].length == 0){
                $scope.instancesSelected[dim] = [];
                $scope.propertiesSelected[dim] = [];
                $scope.values[dim] = [];
            }
            
        }
    }
    
    //when radio button is clicked, this function executes
    $scope.propertiesChecked = function(){
        //If a radio button is selected, the ng model value (propety.name) is always defined.
        if(this.property.name !== undefined){
            //var dim is the variable which holds which dimension is being used.
            var dim = this.dimension.index;
            //This takes the properties names which have been selected inside the dimension and adds them to the propertiesSelected array. 
            $scope.propertiesSelected[dim] = [];
            $scope.propertiesSelected[dim].push(this.property.name);
            //This defines the values array
            $scope.values[dim] = [];
            //This fills the values array with all of the names of the properties that have been selected
            for(var i=0; i<$scope.instancesSelected[dim].length; i++){
               $scope.values[dim].push({'name': $scope.instancesSelected[dim][i].propHash[this.property.name], 'selected': false});
            }
        }
    }
    
    $scope.selectAllTypes = function(){
        if(this.dimension.selectedType == true){
            //declare variables
            var dim = this.dimension.index;
            $scope.typesSelected[dim] = [];
            $scope.instances[dim] = [];
            $scope.instancesSelected[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.properties[dim] = [];
            $scope.values[dim] = [];
            var typeInstances = [];
            this.dimension.selectedInstance = false;
            
            //Loops
            for(var i=0; i<$scope.types[dim].length; i++){
                $scope.typesSelected[dim].push($scope.types[dim][i].name);
                $scope.types[dim][i].selected = true;
            }
            for(var i=0; i<$scope.typesSelected[dim].length; i++){
                for(var j=0; j<$scope.data.Nodes[$scope.typesSelected[dim][i]].length; j++){
                    typeInstances.push({'name': $scope.data.Nodes[$scope.typesSelected[dim][i]][j], 'selected': false});
                }
            }
            $scope.instances[dim] = typeInstances;
        }else{
            var dim = this.dimension.index;
            $scope.instances[dim] = [];
            $scope.instancesSelected[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.properties[dim] = [];
            $scope.values[dim] = [];
            $scope.instances[dim] = [];
            this.dimension.selectedInstance = false;
            
            for(var i=0; i<$scope.types[dim].length; i++){
                $scope.types[dim][i].selected = false;
            }
            
        }
    }
    
    $scope.selectAllInstances = function(){
        if(this.dimension.selectedInstance == true){
            //declare variables
            var dim = this.dimension.index;
            $scope.instancesSelected[dim] = [];
            $scope.properties[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.values[dim] = [];
            var key;
            var reducedProps = [];
            
            for(var i=0; i<$scope.instances[dim].length; i++){
                $scope.instancesSelected[dim].push($scope.instances[dim][i].name);
                $scope.instances[dim][i].selected = true;
            }
                
            for(var i=0; i<$scope.instancesSelected[dim].length; i++){
                for (key in $scope.instancesSelected[dim][i].propHash) {
                    reducedProps.push(key);
                }
            }
            reducedProps = _.uniq(reducedProps);
            
            for(var i=0; i<reducedProps.length; i++){
                $scope.properties[dim].push({'name':reducedProps[i], 'selected': false});
            }
            
        }else{
            var dim = this.dimension.index;
            $scope.properties[dim] = [];
            $scope.instancesSelected[dim] = [];
            $scope.propertiesSelected[dim] = [];
            $scope.values[dim] = [];
            this.dimension.selectedInstance = false;
            for(var i=0; i<$scope.instances[dim].length; i++){
                $scope.instances[dim][i].selected = false;
            }
        }
    }
    
    
    
    $scope.drawGraph = function(chartType){
        var wrapper;
        var hAxisTitle = '';
        var vAxisTitle = 'Count';
        var axisParams = [];
        var yAxesVal = [];
        var seriesVal = [];
        var series1Val = [];
        var series2Val = [];
        var reducedInstances = [];
        var colors = Highcharts.getOptions().colors;
        var sortedPropertiesSelected = _.sortBy($scope.propertiesSelected, function(prop) {
                return prop;
            });
        //The start of massaging the data
        if($scope.groupby == 'Instance'){
            //Declaring variables
            hAxisTitle = 'Property';
            //var reducedInstances = [];
            
            //loops through each dimension then each instance array and pushes all the selected instances into reducedInstances array
            for(var i=0; i<$scope.dimensions.length; i++){
                if($scope.instancesSelected[i]){
                    for(var j=0; j<$scope.instancesSelected[i].length; j++){
                        reducedInstances.push($scope.instancesSelected[i][j]);
                    }
                }
            }
            //returns all unique instances to reducedInstances
            reducedInstances = _.uniq(reducedInstances);

            reducedInstances = _.sortBy(reducedInstances, function(instance) {
                return instance.propHash.VERTEX_LABEL_PROPERTY;
            });

            
            //adds all the selected unique instances to the columns
            //Params pushed in order of dimensions added
            //Params applied in order of [x axis, y axis, 3rd dimension, ... ]
            for(var i=0; i<reducedInstances.length; i++){
            	axisParams.push(reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY);
            }
            //set cols variable
            var cols = axisParams.length;
            
            //check if pie chart
            if(chartType == 'pie'){
                //check if first dimensions
                if($scope.dimensions.length ==1){
                    var instanceParams = [];
                    //loops through and adds every Instance + Property value to the pie chart
                    for(var j=0; j<reducedInstances.length; j++){
                        for(var i=0; i<$scope.dimensions.length; i++){
                            if(sortedPropertiesSelected[i]){
                                if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                    instanceParams.push([reducedInstances[j].propHash.VERTEX_LABEL_PROPERTY + '-' + sortedPropertiesSelected[i][0], reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]]);
                                }else{
                                    instanceParams.push('', 0);
                                }
                            } 
                        }
                    }
                    seriesVal.push({'name': 'Count', 'data': instanceParams});
                    
                //all other dimensions start here
                }else{
                    //loop through to get the inner circles values into series1Val
                    for(var i=0; i<$scope.dimensions.length; i++){
                        if(sortedPropertiesSelected[i]){
                            var totalPropVal = 0;
                            for(var j=0; j<cols; j++){
                                if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                    totalPropVal += parseInt(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]);
                                }else{
                                    totalPropVal += 0;
                                }
                            }
                            series1Val.push({'name': sortedPropertiesSelected[i][0], 'y': totalPropVal, 'color': colors[i]});
                        }
                    }
                    
                    //loop through to get the outer circles values into series2Val
                    for(var j=0; j<$scope.dimensions.length; j++){
                        if(sortedPropertiesSelected[j]){
                            for(var i=0; i<reducedInstances.length; i++){
                                if(reducedInstances[i].propHash[sortedPropertiesSelected[j][0]]){
                                    var brightness = 0.2 - (i / reducedInstances.length) /5;
                                    series2Val.push({'name': reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, 'y': reducedInstances[i].propHash[sortedPropertiesSelected[j][0]], 'color': Highcharts.Color(series1Val[j].color).brighten(brightness).get()});
                                }else{
                                    series2Val.push({'name': reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, 'y': 0});
                                }
                            }
                        }
                    }
                    
                    //setting up the series correctly with the 2 pie charts data sets: series1Val and series2Val
                    seriesVal = [{
                        name: 'Property',
                        data: series1Val,
                        size: '60%',
                        dataLabels: {
                            formatter: function() {
                                var shortName = this.point.name.length > 18 ? this.point.name.substring(0,18) + '...' : this.point.name;
                                return '<b>'+ shortName +':</b> '+ this.y;
                            },
                            color: 'white',
                            distance: -50
                        }
                    }, {
                        name: 'Instance',
                        data: series2Val,
                        size: '80%',
                        innerSize: '60%',
                        dataLabels: {
                            formatter: function() {
                                var shortName = this.point.name.length > 18 ? this.point.name.substring(0,18) + '...' : this.point.name;
                                // display only if larger than 1
                                return this.y > 0 ? '<b>'+ shortName +':</b> '+ this.y : null;
                            }
                        }
                    }];
                        
                }
            }else{
                for(var i=0; i<$scope.dimensions.length; i++){
                    if(sortedPropertiesSelected[i]){
                        var instanceParams = [];
                        for(var j=0; j<cols; j++){
                            if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                instanceParams.push(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]);
                            }else{
                                instanceParams.push(0);
                            }
                        }
                        seriesVal.push({'name': sortedPropertiesSelected[i][0], 'data': instanceParams});
                    }
                }
            }
            
        //if anything but instance is selected, the data is parsed below
        }else{
            
            for(var i=0; i<$scope.dimensions.length; i++){
                if(sortedPropertiesSelected[i]){
                    for(var j=0; j<sortedPropertiesSelected[i].length; j++){
                    	axisParams.push(sortedPropertiesSelected[i][0]);
                    }
                }
            }
            
            for(var i=0; i<$scope.dimensions.length; i++){
                if($scope.instancesSelected[i]){
                    for(var j=0; j<$scope.instancesSelected[i].length; j++){
                        reducedInstances.push($scope.instancesSelected[i][j]);
                    }
                }
            }
            reducedInstances = _.uniq(reducedInstances);

            reducedInstances = _.sortBy(reducedInstances, function(instance) {
                return instance.propHash.VERTEX_LABEL_PROPERTY;
            });
            
            var cols = axisParams.length;
            //check if chart type selected is 'pie'
            if(chartType == 'pie'){
                //check the number of dimensions
                if($scope.dimensions.length == 1){
                    for(var i=0; i<$scope.dimensions.length; i++){
                        if(sortedPropertiesSelected[i]){
                            var propParams = [];
                            for(var j=0; j<reducedInstances.length; j++){
                                if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                    propParams.push([reducedInstances[j].propHash.VERTEX_LABEL_PROPERTY, reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]]);
                                }else{
                                    propParams.push(0);
                                }
                            }
                            seriesVal.push({'name': sortedPropertiesSelected[i][0], 'data': propParams});
                        }
                    }
                //for all pie charts with dimensions more than 1
                }else{
                    //loop to set series1Val with inner circle values
                    for(var i=0; i<reducedInstances.length; i++){
                        var totalInstanceVal = 0;
                        for(var j=0; j<cols; j++){
                            if(sortedPropertiesSelected[j]){
                                if(reducedInstances[i].propHash[sortedPropertiesSelected[j][0]]){
                                    totalInstanceVal += parseInt(reducedInstances[i].propHash[sortedPropertiesSelected[j][0]]);
                                }else{
                                    totalInstanceVal += 0;
                                }
                            }
                        }
                        series1Val.push({'name': reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, 'y': totalInstanceVal, 'color': colors[i]});
                    }
                    
                    //loop to set series2Val with outer circle values
                    for(var j=0; j<reducedInstances.length; j++){
                        for(var i=0; i<$scope.dimensions.length; i++){
                            if(sortedPropertiesSelected[i]){
                                if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                    var brightness = 0.2 - (i / $scope.dimensions.length) /5;
                                    series2Val.push({'name': sortedPropertiesSelected[i][0], 'y': reducedInstances[j].propHash[sortedPropertiesSelected[i][0]], 'color': Highcharts.Color(series1Val[j].color).brighten(brightness).get()});
                                }else{
                                    series2Val.push({'name': sortedPropertiesSelected[i][0], 'y': 0});
                                }
                            }
                        }
                    }
                    
                    //setting up the series correctly with the 2 pie charts data sets
                    seriesVal = [{
                        name: 'Instance',
                        data: series1Val,
                        size: '60%',
                        dataLabels: {
                            formatter: function() {
                                var shortName = this.point.name.length > 18 ? this.point.name.substring(0,18) + '...' : this.point.name;
                                return '<b>'+ shortName +':</b> '+ this.y;
                            },
                            color: 'white',
                            distance: -30
                        }
                    }, {
                        name: 'Property',
                        data: series2Val,
                        size: '80%',
                        innerSize: '60%',
                        dataLabels: {
                            formatter: function() {
                                var shortName = this.point.name.length > 18 ? this.point.name.substring(0,18) + '...' : this.point.name;
                                // display only if larger than 1
                                return this.y > 0 ? '<b>'+ shortName +':</b> '+ this.y: null;
                            }
                        }
                    }];
                    
                }
            //all other chart types
            }else if(chartType == 'scatter' && $scope.dimensions.length == 2){
                var propertyVals = [];
                for(var i=0; i<$scope.dimensions.length; i++){
                    if(sortedPropertiesSelected[i]){
                        propertyVals[i] = [];
                        for(var j=0; j<reducedInstances.length; j++){
                            if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                propertyVals[i].push(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]);
                            }else{
                                propertyVals[i].push(0);
                            }
                        }
                    }
                }
                var data = [];
                for(var i=0; i<reducedInstances.length; i++){
                     data.push({name: reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, x: propertyVals[0][i], y: propertyVals[1][i]});
                }

                seriesVal.push({'name': 'Instances', 'data': data});
            //sets the data for bubble charts with 3 dimensions
            }else if(chartType == 'bubble' && $scope.dimensions.length == 3){
                var propertyVals = [];
                for(var i=0; i<$scope.dimensions.length; i++){
                    if(sortedPropertiesSelected[i]){
                        propertyVals[i] = [];
                        for(var j=0; j<reducedInstances.length; j++){
                            if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                propertyVals[i].push(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]);
                            }else{
                                propertyVals[i].push(0);
                            }
                        }
                    }
                }
                var data = [];
                for(var i=0; i<reducedInstances.length; i++){
                     //data.push({name: reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, x: propertyVals[0][i], y: propertyVals[1][i], marker: { radius: propertyVals[2][i]}});
                    data.push({name: reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, data: [[propertyVals[0][i], propertyVals[1][i],propertyVals[2][i]]]});
                }
                //seriesVal.push({'name': 'Instances', 'data': data});
                seriesVal = data;
            //all other chart types data is set here
            }else{
                //loop to set series value with name and data
                for(var i=0; i<reducedInstances.length; i++){
                    var propParams = [];
                    for(var j=0; j<cols; j++){
                        if(sortedPropertiesSelected[j]){
                            if(reducedInstances[i].propHash[sortedPropertiesSelected[j][0]]){
                                propParams.push(reducedInstances[i].propHash[sortedPropertiesSelected[j][0]]);
                            }else{
                                propParams.push(0);
                            }
                        }
                    }
                    seriesVal.push({'name': reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY, 'data': propParams});
                }
            }
        //end of the '$scope.groupby' else
        }        

        createChart(chartType, axisParams, seriesVal);
        
        function createChart(cType, axisParams, dataSeries){
            //creating the chart
            jQuery('#visualization').highcharts({
                chart: {
                    type: cType
                },
                credits: {
                    text: ''
                },
                title: {
                    text: ''
                },
                xAxis: getxAxisOptions(axisParams),
                yAxis: getyAxisOptions(axisParams),
                plotOptions: {
                    pie: {
                        dataLabels: getDataLabels(),
                    },
                    series: {
                        cursor: 'pointer',
                        point: {
                            events: {
                                click: function() {
                                    clickChartPoint(this);
                                }
                            }
                        }
                    }
                },
                legend: getLegend(),
                tooltip: getTooltipOptions(),
                series: dataSeries
            });
        }
        
        function getxAxisOptions(axisParams){
            //check to see which charts is selected and how many dimensions
            if(chartType == 'column' || chartType == 'line' || chartType == 'bar' || (chartType == 'scatter' && $scope.dimensions.length == 1)){
                return {
                    categories: axisParams,
                    labels: {
                        rotation: getRotation(),
                        align: 'right',
                        formatter : function() {
                                        return this.value.length > 16 ? this.value.substring(0,16) + '...' : this.value;
                                    }
                    }
                }
            //check to see which charts is selected and how many dimensions
            }else if ((chartType == 'scatter' && $scope.dimensions.length > 1 && $scope.groupby == 'Property') || (chartType == 'bubble')){
                //
                if($scope.grid.selected == true){
                    var xlineVal = getxLineVal();
                    //adds a line for the quadrant grid and returns the title of the xAxis
                    return {
                        plotLines: [{
                            color: 'blue',
                            width: 2,
                            value: xlineVal
                        }],
                        title: {
                            text: axisParams[0]
                        },
                    };
                }else{
                    //returns the xAxis title
                    return {
                        title: {
                            text: axisParams[0]
                        },
                    };
                }
                
            //all other graph types
            }else{
                return {categories: axisParams};
            }
        }
        
        function getyAxisOptions(axisParams){
            if((chartType == 'scatter' && $scope.dimensions.length > 1 && $scope.groupby == 'Property') || (chartType == 'bubble')){
                if($scope.grid.selected == true){
                    var ylineVal = getyLineVal();
                    //adds a line for the quadrant grid and returns the title of the xAxis
                    return {
                        plotLines: [{
                            color: 'blue',
                            width: 2,
                            value: ylineVal
                        }],
                        title: {
                            text: axisParams[1]
                        },
                    };
                }else{
                    return {
                    	title: {
                    		text: axisParams[1]
                    	},
                    	enabled: true
                    };
                }
            }else{
                return {
                	title: {
                		text: "Values"
                	},
                	enabled: true
                };
            }
        }
        
        function getTooltipOptions(){
            if(chartType == 'scatter' && $scope.dimensions.length == 2 && $scope.groupby == 'Property'){
                return {
                        formatter: function(){

                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            var shortKey = this.key.length > 25 ? this.key.substring(0,25) + '...' : this.key;
                            
                            var dividedxAxisName = '';
                            var brokenxAxisArray = this.series.xAxis.axisTitle.text.replace(/_/g, ' ');
                            brokenxAxisArray = brokenxAxisArray.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenxAxisArray.length; i++) {
                                dividedxAxisName += '<b>' + brokenxAxisArray[i] + '</b>';
                                if(i!==brokenxAxisArray.length-1) {
                                    dividedxAxisName += '<br/>';
                                }
                            }

                            var dividedyAxisName = '';
                            var brokenyAxisArray = this.series.yAxis.axisTitle.text.replace(/_/g, ' ');
                            brokenyAxisArray = brokenyAxisArray.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenyAxisArray.length; i++) {
                                dividedyAxisName += '<b>' + brokenyAxisArray[i] + '</b>';
                                if(i!==brokenyAxisArray.length-1) {
                                    dividedyAxisName += '<br/>';
                                }
                            }
                            
                            return '<b>' + shortKey + '</b><br/>' + dividedxAxisName + ': ' + xVal + '<br/>' + dividedyAxisName + ': ' + yVal + '<br/>';
                        }
                    };
            }else if(chartType == 'bubble' && $scope.dimensions.length == 3 && $scope.groupby == 'Property'){
                 return {
                        formatter: function(){


                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            var zVal = this.point.options.z.toString().substring(0,11);
                            var shortKey = this.series.userOptions.name.length > 18 ? this.series.userOptions.name.substring(0,18) + '...' : this.series.userOptions.name;
                            
                            var dividedxAxisName = '';
                            var brokenxAxisArray = this.series.xAxis.axisTitle.text.replace(/_/g, ' ');
                            brokenxAxisArray = brokenxAxisArray.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenxAxisArray.length; i++) {
                                dividedxAxisName += '<b>' + brokenxAxisArray[i] + '</b>';
                                if(i!==brokenxAxisArray.length-1) {
                                    dividedxAxisName += '<br/>';
                                }
                            }

                            var dividedyAxisName = '';
                            var brokenyAxisArray = this.series.yAxis.axisTitle.text.replace(/_/g, ' ');
                            brokenyAxisArray = brokenyAxisArray.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenyAxisArray.length; i++) {
                                dividedyAxisName += '<b>' + brokenyAxisArray[i] + '</b>';
                                if(i!==brokenyAxisArray.length-1) {
                                    dividedyAxisName += '<br/>';
                                }
                            }

                            var dividedzAxisName = '';
                            var brokenzAxisArray = sortedPropertiesSelected[2][0].replace(/_/g, ' ');
                            brokenzAxisArray = brokenzAxisArray.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenzAxisArray.length; i++) {
                                dividedzAxisName += '<b>' + brokenzAxisArray[i] + '</b>';
                                if(i!==brokenzAxisArray.length-1) {
                                    dividedzAxisName += '<br/>';
                                }
                            }
                            
                            return '<b>' + shortKey + '</b><br/>' + dividedxAxisName + ': ' + xVal + '<br/>' + dividedyAxisName + ': ' + yVal + '<br/>' + dividedzAxisName + ' (radius): ' + zVal;
                        }
                    };
            }else{
                if(chartType == 'column' || chartType == 'bar' || chartType == 'scatter' || chartType == 'line'){
                    return {
                        enabled: true,
                        formatter: function(){
                            var shortKey = this.key.length > 25 ? this.key.substring(0,25) + '...' : this.key;
                            var shortName = this.series.name.length > 18 ? this.series.name.substring(0,18) + '...' : this.series.name;
                            return '<b>' + shortKey + '</b><br/><b>' + shortName + '</b>: ' + this.y;
                        }
                    };
                }else if(chartType == 'pie' && $scope.dimensions.length == 1){
                    return {
                        enabled: true,
                        formatter: function(){
                            var shortKey = this.key.length > 25 ? this.key.substring(0,25) + '...' : this.key;
                            var shortName = this.series.name.length > 18 ? this.series.name.substring(0,18) + '...' : this.series.name;
                            return '<b>' + shortKey + '</b><br/><b>' + shortName + '</b>: ' + this.y;
                        }
                    };
                }else if(chartType == 'pie' && $scope.dimensions.length == 2){
                    return {
                        enabled: true,
                        formatter: function(){
                            var shortKey = this.key.length > 25 ? this.key.substring(0,25) + '...' : this.key;
                            var shortName = this.series.name.length > 18 ? this.series.name.substring(0,18) + '...' : this.series.name;
                            return '<b>' + shortKey + '</b><br/><b>' + shortName + '</b>: ' + this.y;
                        }
                    };
                }
            }
        }
        
        function getRotation(){
            if(chartType == 'column' || chartType == 'line' || chartType == 'scatter'){
                return -45;
            }else{
                return 0;
            }
        }
        
        function getLegend(){
            if(chartType == 'bubble'){
                return {enabled: false};
            }else{
                return {enabled: true,
                        labelFormatter: function() {
                        return this.name.length > 15 ? this.name.substring(0,15) + '...' : this.name;
                    }
                };
            }
        }
        
        function getDataLabels(){
            if(chartType == 'pie'){
                return{
                    enabled: true,
                    formatter: function() {
                        var shortName = this.point.name.length > 20 ? this.point.name.substring(0,20) + '...' : this.point.name;
                        return '<b>'+ shortName +':</b> '+ this.y;
                    },
                    color: 'black',
                    distance: -30
                }
            }else{
                return {enabled: false};
            }
        }
        
        //returns the value on the x axis where the vertical line of the application health grid will be locatied
        function getxLineVal(){
            return '';
        }
        //returns the value on the y axis where the horizontal line of the application health grid will be locatied
        function getxLineVal(){
            return '';
        }
        
        
        function clickChartPoint(that){
            if(chartType == 'pie' && $scope.dimensions.length > 1){
                if($scope.groupby == 'Property'){
                    var  reducedInstances = [];
                    var axisParams = [];
                    var seriesVal = [];
                    //loops through each dimension then each instance array and pushes all the selected instances into reducedInstances array
                    for(var i=0; i<$scope.dimensions.length; i++){
                        if($scope.instancesSelected[i]){
                            for(var j=0; j<$scope.instancesSelected[i].length; j++){
                                reducedInstances.push($scope.instancesSelected[i][j]);
                            }
                        }
                    }
                    //returns all unique instances to reducedInstances
                    reducedInstances = _.uniq(reducedInstances);
                    reducedInstances = _.filter(reducedInstances, function(inst){return inst ? inst.propHash.VERTEX_LABEL_PROPERTY == that.name : null});
                    
                    //checks to see if reducedInstances is empty and will stop the function if so
                    if(reducedInstances.length == 0){
                        return;
                    }
                    
                    //adds all the selected unique instances to the columns
                    for(var i=0; i<reducedInstances.length; i++){
                        axisParams.push(reducedInstances[i].propHash.VERTEX_LABEL_PROPERTY);
                    }
                    
                    for(var j=0; j<reducedInstances.length; j++){
                        if(sortedPropertiesSelected[i]){
                            var propParams = [];
                            for(var i=0; i<$scope.dimensions.length; i++){
                                if(reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]){
                                    propParams.push([sortedPropertiesSelected[i][0], reducedInstances[j].propHash[sortedPropertiesSelected[i][0]]]);
                                }else{
                                    propParams.push(0);
                                }
                            }
                            seriesVal.push({'name': reducedInstances[j].propHash.VERTEX_LABEL_PROPERTY, 'data': propParams});
                        }
                    }
                    createChart(chartType, axisParams, seriesVal);
                }else if($scope.groupby == 'Instance'){
                    var  reducedInstances = [];
                    var axisParams = [];
                    var seriesVal = [];
                    
                    for(var i=0; i<$scope.dimensions.length; i++){
                        if($scope.instancesSelected[i]){
                            for(var j=0; j<$scope.instancesSelected[i].length; j++){
                                reducedInstances.push($scope.instancesSelected[i][j]);
                            }
                        }
                    }
                    reducedInstances = _.uniq(reducedInstances);
                    


                    var propParams = [];
                    for(var j=0; j<reducedInstances.length; j++){
                        if(reducedInstances[j].propHash[that.name]){
                            propParams.push([reducedInstances[j].propHash.VERTEX_LABEL_PROPERTY, reducedInstances[j].propHash[that.name]]);
                        }else{
                            return;
                        }
                    }
                    seriesVal.push({'name': that.name, 'data': propParams});


                    createChart(chartType, that.name, seriesVal);
                }
            }
        }
        
    }
    
    /*------when to disable chart types-------*/
    $scope.isPieDisabled = function(){
        if(($scope.groupby =='Instance' && $scope.dimensions.length == 1) || ($scope.groupby == undefined && $scope.dimensions.length == 1) || $scope.dimensions.length > 2){
            return true;
        }else{
            return false;
        }
    }
    
    $scope.isScatterDisabled = function(){
        if($scope.groupby == 'Instance' || $scope.dimensions.length > 2){
            return true;
        }else{
            return false;
        }
    }
    
    $scope.isBubbleDisabled = function(){
        if($scope.groupby == 'Property' && $scope.dimensions.length == 3){
            return false;
        }else{
            return true;
        }
    }
    
    $scope.isDeleteShow = function(){
        if(this.dimension.index > 0){
            return true;
        }else{
            return false;
        }
    }
    
    function forLoop(array1, array2){
        var returnArray = [];
        for(var i=0; i<array1.length; i++){
            for(var j=0; j<array2[array1[i]].length; j++){
                returnArray.push({'name': array2[array1[i]][j], 'selected': false});
            }
        }
        return returnArray;
    }
    
     /*---------Accordion---------*/
    $scope.oneAtATime = false;
    $scope.dimensions = [{name: 'Dimension 1', index: 0, selectedType: false, selectedInstance: false}];
     
    //add new accordion group
    $scope.addDimension = function() {
        var newDimensionNo = $scope.dimensions.length + 1;
        $scope.dimensions.push({name: 'Dimension ' + newDimensionNo, index: $scope.dimensions.length, selectedType: false, selectedInstance: false});
        
        var key;
        $scope.types[$scope.dimensions.length -1] = [];
        for (key in $scope.data.Nodes) {
            $scope.types[$scope.dimensions.length -1].push({'name': $scope.data.Nodes[key][0].propHash.VERTEX_TYPE_PROPERTY, 'selected': false});
        }
        
    };
    
    //delete accordion group
    $scope.deleteDim = function(){
        $scope.dimensions.splice(this.dimension.index, 1);
        
        for(var i=0; i<$scope.dimensions.length; i++){
            $scope.dimensions[i].name = "Dimension " + (i+1);
            $scope.dimensions[i].index = i;
        }
    }
}
    

//grid.html controller
function gridCtrl($scope, $http) {
    $scope.extStab = true;
    $scope.techStand = true;

    $scope.isDisabled = function() {
        if ($scope.extStab == false && $scope.techStand == false) {
            return true;
        } else {
            return false;
        }
    }
    
    //get data from function on grid.html page
    $scope.setJSONData = function (data) {
        $scope.$apply(function () {
            
            $scope.data = jQuery.parseJSON(data);
            
            $scope.healthGrid('bubble');
        });
    };
    
    //----------Comment the below $http.get when using in Java
    /*$http.get('lib/singleChartGridData.json').success(function(jsonData) {
        $scope.data = jsonData;
        $scope.healthGrid('bubble');
    });*/

    

    //creates alliance health grid chart
    $scope.healthGrid = function(chartType){
        //creates object that holds all graph option values
            var graphOptions = {
                xAxis: '',
                yAxis: '',
                zAxis: '',
                chartTitle: '',
                chartType: '',
                stackType: '',
                xAxisCategories: '',
                xLineVal: '',
                yLineVal: '',
                xLineWidth: '',
                yLineWidth: '',
                xMax: undefined,
                xMin: undefined,
                xInterval: undefined,
                yMax: undefined,
                yMin: undefined,
                yInterval: undefined,
                series: [],
                test: [],
                highlight: '',
                showZTooltip: true
            };
            $scope.yExtraLegend = [];
            $scope.xExtraLegend = [];
            //arrays to hold differen health grid data


            //variables for determining points amount
            var busValMultiplier = '';

            if ($scope.data.dataSeries) {
                setChartData($scope.data);
            } else {
                setOldChartData();
            }
        
        
            //takes the "data" parameter passed and sorts all the data appropriately
            function setChartData(data) {

                if (data.yLineLegend) {
                    //declare the scope variable that will hold all the extra legend information
                    for (var i=0; i<data.yLineLegend.length; i++) {
                        if( Object.prototype.toString.call( data.yLineVal ) === '[object Array]' ) {
                            $scope.yExtraLegend.push(data.yLineVal[i] + " = " + data.yLineLegend[i]);
                        }else {
                            $scope.yExtraLegend.push(data.yLineVal + " = " + data.yLineLegend[i]);
                        }
                        
                    }
                }
                
                if (data.xLineLegend) {
                    //declare the scope variable that will hold all the extra legend information
                    for (var i=0; i<data.xLineLegend.length; i++) {
                        if( Object.prototype.toString.call( data.xLineVal ) === '[object Array]' ) {
                            $scope.xExtraLegend.push(data.xLineVal[i] + " = " + data.xLineLegend[i]);
                        }else {
                            $scope.xExtraLegend.push(data.xLineVal + " = " + data.xLineLegend[i]);
                        }
                        
                    }
                }
                

                if ($scope.techStand == true && $scope.extStab == true) {
                    busValMultiplier = .33;
                } else if ($scope.techStand == false && $scope.extStab == true) {
                    busValMultiplier = 1;
                } else if ($scope.techStand == true && $scope.extStab == false) {
                    busValMultiplier = 0;
                } else if ($scope.techStand == false && $scope.extStab == false) {
                    busValMultiplier = 0;
                }

                //declare and set variables
                graphOptions.xLineLabel = data.xLineLabel;
                graphOptions.yLineLabel = data.yLineLabel;
                graphOptions.xAxis = data.xAxisTitle;
                graphOptions.yAxis = data.yAxisTitle;
                graphOptions.zAxis = data.zAxisTitle;
                graphOptions.chartTitle = data.title;
                //graphOptions.chartType = data.type;
                graphOptions.chartType = chartType;
                graphOptions.stackType = data.stack;
                graphOptions.xAxisCategories = data.xAxis;
                graphOptions.showZTooltip = data.showZTooltip;
                if(data.highlight) {
                    graphOptions.highlight = data.highlight;
                }

                //set values if they are not null
                if(data.xAxisMax || data.xAxisMax == 0){
                    graphOptions.xMax = data.xAxisMax;
                }
                if(data.xAxisMin || data.xAxisMin == 0){
                    graphOptions.xMin = data.xAxisMin;
                }
                if(data.xAxisInterval || data.xAxisInterval == 0){
                    graphOptions.xInterval = data.xAxisInterval;
                }
                if(data.yAxisMax || data.yAxisMax == 0){
                    graphOptions.yMax = data.yAxisMax;
                }
                if(data.yAxisMin || data.yAxisMin == 0){
                    graphOptions.yMin = data.yAxisMin;
                }
                if(data.yAxisInterval || data.yAxisInterval == 0){
                    graphOptions.yInterval = data.yAxisInterval;
                }
                if(data.xLineVal || data.xLineVal == 0){
                    graphOptions.xLineVal = data.xLineVal;
                    graphOptions.xLineWidth = 1;
                }
                if(data.yLineVal || data.yLineVal == 0){
                    graphOptions.yLineVal = data.yLineVal;
                    graphOptions.yLineWidth = 1;
                }

                //loops through each object within dataSeries
                for (var key in data.dataSeries) {

                    //very specific to health grid data
                    if (data.dataSeries[key][0].length > 4) {
                        var lifeCyclePhase = {};
                        //get the all the different system lifecycle values
                        for (var i=0; i< data.dataSeries[key].length; i++) {
                            lifeCyclePhase[data.dataSeries[key][i][4]] = [];
                        }
                    }
                    
                    //check to see whether the xAxis values are already defined
                    if(graphOptions.xAxisCategories){
                        //checks to see if the color option is available or not and sets it appropriately
                        if(data.colorSeries){
                            graphOptions.series.push({name: key, color: data.colorSeries[key], data: data.dataSeries[key]});
                        } else {
                            graphOptions.series.push({name: key, data: data.dataSeries[key]});
                        }
                    } else {

                        var series = [];

                        if (graphOptions.zAxisTitle) {
                            //organize the data by z value so that the smallest points are plotted last
                            var sortedData = _.sortBy(data.dataSeries[key], function(point){
                                                    //determines if it is health grid data dependent on the length
                                                    if(point.length > 4) {
                                                        return -1*point[3];
                                                    } else{
                                                        return -1*point[2]; 
                                                    }
                                                }
                                            );
                        } else {
                            var sortedData = data.dataSeries[key];
                        }

                        //dependent on the length of the data.dataSeries[key] array, the data needs to be set up differently
                        for (var i = 0; i< sortedData.length; i++) {

                            if (sortedData[i].length == 2) {

                                //set the data series and add to series array
                                series.push({x: sortedData[i][0], y: sortedData[i][1]});
                            }
                            if (sortedData[i].length == 3 && !graphOptions.zAxisTitle) {

                                //set the data series and add to series array
                                series.push({x: sortedData[i][0], y: sortedData[i][1], name: sortedData[i][2]});
                            }
                            if (sortedData[i].length == 3 && graphOptions.zAxisTitle) {

                                //set the data series and add to series array
                                series.push({x: sortedData[i][0], y: sortedData[i][1], z: sortedData[i][2]});
                            }
                            if (sortedData[i].length == 4) {
                                
                                //set the data series and add to series array
                                series.push({x: sortedData[i][0], y: sortedData[i][1], z: sortedData[i][2], name: sortedData[i][3]});
                            }
//**********This will need to be changed when the data comes back the correct way***********
//literally just a hack for this data
                            if (sortedData[i].length > 4) {

                                var techMat = sortedData[i][1] * busValMultiplier + (1 - busValMultiplier) * sortedData[i][2];

                                //check for the system to be highlighted and set it as its own series
                                if (graphOptions.highlight && graphOptions.highlight == sortedData[i][5]) {
                                    graphOptions.test.push({x: sortedData[i][0], y: techMat, z: sortedData[i][3], name: sortedData[i][5]});
                                }
                                for (var lcKey in lifeCyclePhase) {
                                    if (sortedData[i][4] == lcKey) {
                                        lifeCyclePhase[lcKey].push({x: sortedData[i][0], y: techMat, z: sortedData[i][3], name: sortedData[i][5]});
                                    }
                                }
                            }
                        }

                        //checks to see if the color option is available or not and sets it appropriately
                        if(data.colorSeries){
                            graphOptions.series.push({name: key, color: data.colorSeries[key], data: series});
                        } else if (lifeCyclePhase) {
                            for (var lcKey in lifeCyclePhase) {
                                if (lcKey == 'Supported') {
                                    graphOptions.series.push({name: lcKey, color: Highcharts.getOptions().colors[2], data: lifeCyclePhase[lcKey]})
                                } else if (lcKey == 'Retired_(Not_Supported)') {
                                    graphOptions.series.push({name: lcKey, color: Highcharts.getOptions().colors[3], data: lifeCyclePhase[lcKey]})
                                } else if (lcKey !== 'Retired_(Not_Supported)' && lcKey !== 'Supported') {
                                    graphOptions.series.push({name: "Retired_(Not_Supported)", color: Highcharts.getOptions().colors[6], data: lifeCyclePhase[lcKey]})
                                }
                            }
                            if (graphOptions.highlight !== '') {
                                graphOptions.series.push({name: graphOptions.highlight, color: Highcharts.getOptions().colors[0], marker: {symbol: "triangle-down"}, data: graphOptions.test})
                            }
                        } else {
                            graphOptions.series.push({name: key, data: series});
                        }
                    }
                }

                //get the median for the x and y axis
                if(graphOptions.chartType == 'scatter' || graphOptions.chartType == 'bubble') {
                    //if xline is empty get the median
                    if(!data.xLineVal)
                    {
                        graphOptions.xLineVal = getMedian(sortedData, 0);
                        graphOptions.xLineWidth = 1;
                    }
                    //if yline is empty get the median
                    if(!data.yLineVal)
                    {
                        graphOptions.yLineVal = getMedian(sortedData, 1);
                        graphOptions.yLineWidth = 2;
                    }
                }
                
                createChart();
            }

            function updateChartOption(option, newValue){
                graphOptions[option] = newValue;
            }

            function getMedian(array, sortIdx) {
                var medianData = _.sortBy(array, function(point){
                                                    return point[sortIdx]; 
                                                }
                                            );

                var half = Math.floor(medianData.length/2);

                if(medianData.length % 2){
                    return medianData[half][sortIdx];
                } else {
                    return (medianData[half-1][sortIdx] + medianData[half][sortIdx]) / 2.0;
                }
                    
            }


            //creates alliance health grid chart
            function createChart (newType) {
                    
                if(newType){
                    graphOptions.chartType = newType
                }
                if(newType === 'bubble' && graphOptions.series[0].data[0].length < 3) {
                    graphOptions.chartType = 'scatter';
                }

                //creating the chart
                jQuery('#visualization').highcharts({
                    chart: {
                        type: graphOptions.chartType,
                        spacingRight: 15
                    },
                    credits: {
                        text: ''
                    },
                    title: {
                        text: graphOptions.chartTitle
                    },
                    plotOptions: {
                        spline: {
                            marker: {
                                enabled: false
                            }
                        },
                        column: {
                        stacking: graphOptions.stackType
                        }
                    },
                    xAxis: {
                        max: graphOptions.xMax,
                        min: graphOptions.xMin,
                        tickInterval: graphOptions.xInterval,
                        labels: {
                            rotation: -45,
                            align: 'right',
                            formatter: function(){
                                return this.value;       
                            }
                        },
                        title: {
                            text: graphOptions.xAxis
                        },
                        categories: graphOptions.xAxisCategories,
                        plotLines: getXAxisPlotLines()
                    },
                    yAxis: {
                        max: graphOptions.yMax,
                        min: graphOptions.yMin,
                        tickInterval: graphOptions.yInterval,
                        title: {
                            text: graphOptions.yAxis
                        },
                        plotLines: getYAxisPlotLines()
                    },
                    tooltip: getToolTip(),
                    series: graphOptions.series
                }, function(chart) {
                    if(graphOptions.highlight !== '') {
                        var last = chart.series.length
                        chart.tooltip.refresh(chart.series[last-1].points[0]);
                    }
                    
                });
                
            }//end of createChart()

            function getToolTip(){
                if (graphOptions.chartType == 'scatter'){
                    if (graphOptions.highlight !== '') {
                        var tipPosition = function() {
                            return {x:300, y:30};
                        }
                    } else {
                        var tipPosition = null;
                    }
                    return {
                        formatter: function(){
                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            var shortKey = '';
                            var brokenKeyArray = this.key.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenKeyArray.length; i++) {
                                shortKey += '<b>' + brokenKeyArray[i] + '</b><br/>';
                            }
                            
                            return '<b>' + shortKey + '<b>' + graphOptions.xAxis + '</b>: ' + xVal + '<br/><b>' + graphOptions.yAxis + '</b>: ' + yVal + '<br/>';
                        },
                        positioner: tipPosition
                    };
                }else if (graphOptions.chartType == 'bubble'){
                    if (graphOptions.highlight !== '') {
                        var tipPosition = function() {
                            return {x:300, y:30};
                        }
                    } else {
                        var tipPosition = null;
                    }

                    return{
                        formatter: function(){
                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            var zInfo = '';
                            var shortKey = '';
                            if (graphOptions.zAxis){
                            	if (graphOptions.showZTooltip == false) {
                            		zInfo = '<br/><b>Radius: </b>' + graphOptions.zAxis;
                            		if (this.point.options.z == -1) {
                            			zInfo = '<br/><b>Radius: </b>' + graphOptions.zAxis + ' <b>Unknown</b>';
                            		}
                            	} else {
                            		zInfo = '<br/><b>' + graphOptions.zAxis + '</b> (radius): ' + this.point.options.z;
                            	}
                            }

                            var brokenKeyArray = this.key.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                            for (var i = 0; i<brokenKeyArray.length; i++) {
                                shortKey += '<b>' + brokenKeyArray[i] + '</b><br/>';
                            }
                            
                            return '<b>' + shortKey + '<b>' + graphOptions.xAxis + '</b>: ' + xVal + '<br/><b>' + graphOptions.yAxis + '</b>: ' + yVal + zInfo;
                        },
                        positioner: tipPosition
                    };
                } else {
                    return {enabled: true};
                }
            }

            function getXAxisPlotLines(){
                if( Object.prototype.toString.call( graphOptions.xLineVal ) === '[object Array]' ) {
                    var returnLines = [];
                    var xLabel = '';

                    for(var i=0; i<graphOptions.xLineVal.length; i++) {
                        if(graphOptions.xLineLabel == "true") {
                            if(graphOptions.xMin == graphOptions.xLineVal[i] || graphOptions.xMax == graphOptions.xLineVal[i]) {
                                xLabel = '';
                            } else {
                                xLabel = graphOptions.xLineVal[i];
                            }
                        }
                        returnLines.push({
                            color: 'black',
                            width: graphOptions.xLineWidth,
                            value: graphOptions.xLineVal[i],
                            label: {
                                text: xLabel,
                                verticalAlign: 'bottom',
                                textAlign: 'right',
                                rotation: 0,
                                y: -5,
                                x: -2
                            }
                        })
                    }
                    return returnLines;
                } else {
                    var xLabel = '';
                    if(graphOptions.xLineLabel == "true") {
                        if(graphOptions.xMin == graphOptions.xLineVal[i] || graphOptions.xMax == graphOptions.xLineVal[i]) {
                            xLabel = '';
                        } else {
                            xLabel = graphOptions.xLineVal;
                        }
                    }
                    return [{
                        color: 'black',
                        width: graphOptions.xLineWidth,
                        value: graphOptions.xLineVal,
                        label: {
                            text: xLabel,
                            verticalAlign: 'bottom',
                            textAlign: 'right',
                            rotation: 0,
                            y: -5,
                            x: -2
                        }
                    }];
                }        
            }

            function getYAxisPlotLines(){
                if( Object.prototype.toString.call( graphOptions.yLineVal ) === '[object Array]' ) {
                    var returnLines = [];
                    var yLabel = '';
                    
                    for(var i=0; i<graphOptions.yLineVal.length; i++) {
                        if(graphOptions.yLineLabel == "true") {
                            if(graphOptions.yMin == graphOptions.yLineVal[i] || graphOptions.yMax == graphOptions.yLineVal[i]) {
                                xLabel = '';
                            } else {
                                yLabel = graphOptions.yLineVal[i];
                            }
                        }
                        returnLines.push({
                            color: 'black',
                            width: graphOptions.yLineWidth,
                            value: graphOptions.yLineVal[i],
                            label: {
                                text: yLabel,
                                verticalAlign: 'bottom',
                                textAlign: 'right',
                                y: -5
                            }
                        })
                    }
                    return returnLines;
                } else {
                    var yLabel = '';
                    if(graphOptions.yLineLabel == "true") {
                        if(graphOptions.yMin == graphOptions.yLineVal[i] || graphOptions.yMax == graphOptions.yLineVal[i]) {
                            xLabel = '';
                        } else {
                            yLabel = graphOptions.yLineVal;
                        }
                    }
                    return [{
                        color: 'black',
                        width: graphOptions.yLineWidth,
                        value: graphOptions.yLineVal,
                        label: {
                            text: yLabel,
                            verticalAlign: 'bottom',
                            textAlign: 'right',
                            y: -5
                        }
                    }];
                }        
            }








//*******************************Needs to be deprecated********************************************
        function setOldChartData() {
            var data = [];
            var otherData = [];
            var supportedData = [];
            var unSupportedData = [];
            var unknownData = [];
            var xAxis = 'BusinessValue'
            var yAxis = 'TechnicalMaturity';
            
            var sum = 0;
            for(var i=0; i<$scope.data.Nodes.System.length; i++){
                if($scope.data.Nodes.System[i].propHash['BusinessValue']){
                    sum += $scope.data.Nodes.System[i].propHash['BusinessValue'];
                }
            };
            
            var xLineVal = sum/$scope.data.Nodes.System.length;
            var xLineLabel = xLineVal.toString().substring(0,5) + '...';
            var yLineVal = 5;
            
            //getting the life cycle phase of the system
            var lifeCycle = [];
            if($scope.data.Nodes.LifeCycle){
                for(var i=0; i<$scope.data.Nodes.LifeCycle.length; i++){
                    for(var j=0; j<$scope.data.Nodes.LifeCycle[i].outEdge.length; j++){
                        
                        var myRe = new RegExp("([^/]*)$");
                        var sysLifeCycStr = myRe.exec($scope.data.Nodes.LifeCycle[i].outEdge[j].propHash.EDGE_NAME);
                        
                        var sysLifeCycArray = sysLifeCycStr[0].split(":");
                        lifeCycle.push({system: sysLifeCycArray[0], lifecycle: sysLifeCycArray[1]});
                    }
                }
            }
            
            //have to sort the systems so that the largest points get plotted first so their z index will be lower
            var sortedSystems = _.sortBy($scope.data.Nodes.System, function(system){
                                            if(!system.propHash['SustainmentBudget']){
                                                system.propHash['SustainmentBudget'] = 0;
                                            }
                                            return -1*system.propHash['SustainmentBudget']; 
                                        }
                                    );
            
            
            for(var i=0; i<sortedSystems.length; i++){
                var pointColor = '';
                 
                if(!sortedSystems[i].propHash['TechnicalMaturity']){
                    sortedSystems[i].propHash['TechnicalMaturity'] = 0;
                }
                if(!sortedSystems[i].propHash['BusinessValue']){
                    sortedSystems[i].propHash['BusinessValue'] = 0;
                }
                if(!sortedSystems[i].propHash['SustainmentBudget']){
                    sortedSystems[i].propHash['SustainmentBudget'] = 0;
                }
                for(var j=0; j<lifeCycle.length; j++){
                    if(lifeCycle[j].system == sortedSystems[i].propHash.VERTEX_LABEL_PROPERTY){
                        if(lifeCycle[j].lifecycle == 'Supported'){
                            pointColor = Highcharts.getOptions().colors[2];
                        }
                        else if(lifeCycle[j].lifecycle == 'Retired_(Not_Supported)'){
                            pointColor = Highcharts.getOptions().colors[3];
                        }
                    }
                }
                
                if(pointColor == '' && lifeCycle.length > 0){
                    pointColor = Highcharts.getOptions().colors[6];
                }else if(pointColor == '' && lifeCycle.length == 0){
                    pointColor = Highcharts.getOptions().colors[0];
                };
                
                //check chart type to determine data model
                if(chartType == 'bubble'){
                    var seriesData = {name: sortedSystems[i].propHash.VERTEX_LABEL_PROPERTY, x: sortedSystems[i].propHash['BusinessValue'], y: sortedSystems[i].propHash['TechnicalMaturity'],  z: sortedSystems[i].propHash['SustainmentBudget']};
                    
                    if(pointColor == Highcharts.getOptions().colors[2]){
                        supportedData.push(seriesData);
                    }else if(pointColor == Highcharts.getOptions().colors[3]){
                        unSupportedData.push(seriesData);
                    }else if(pointColor == Highcharts.getOptions().colors[6]){
                        unknownData.push(seriesData);
                    }else{
                        otherData.push(seriesData);
                    }
                }else if(chartType == 'scatter'){
                    var seriesData = {name: sortedSystems[i].propHash.VERTEX_LABEL_PROPERTY, x: sortedSystems[i].propHash['BusinessValue'], y: sortedSystems[i].propHash['TechnicalMaturity']}
                    //if scatter plot, set the data to different arrays based on color (which is based on life cycle)
                    if(pointColor == Highcharts.getOptions().colors[2]){
                        supportedData.push(seriesData);
                    }else if(pointColor == Highcharts.getOptions().colors[3]){
                        unSupportedData.push(seriesData);
                    }else if(pointColor == Highcharts.getOptions().colors[6]){
                        unknownData.push(seriesData);
                    }else{
                        otherData.push(seriesData);
                    }
                }
            }
            
            if(otherData.length == 0){
                if(supportedData.length > 0){
                    data.push({name: 'Supported', color: Highcharts.getOptions().colors[2], data: supportedData});
                }
                if(unSupportedData.length > 0){
                    data.push({name: 'Retired_(Not_Supported)', color: Highcharts.getOptions().colors[3], data: unSupportedData});
                }
                if(unknownData.length > 0){
                    data.push({name: 'Unknown', color: Highcharts.getOptions().colors[6], data: unknownData});
                }
            }else if (otherData.length > 0){
                data.push({name: 'Systems', data: otherData});
            }
            
            
                //creating the chart
                jQuery('#visualization').highcharts({
                    chart: {
                        type: chartType,
                        zoomType: 'xy'
                    },
                    credits: {
                        text: ''
                    },
                    title: {
                        text: 'Health Grid'
                    },
                    //legend: getLegend(),
                    plotOptions: {
                    },
                    xAxis: {
                            plotLines: [{
                                color: 'black',
                                width: 1,
                                value: xLineVal,
                                label: {
                                    text: xLineLabel,
                                    align: 'right',
                                    verticalAlign: 'bottom',
                                    rotation: '0',
                                    x: 55,
                                    y: -2
                                }
                            }],
                            title: {
                                text: xAxis
                            }
                        },
                    yAxis: {
                            plotLines: [{
                                color: 'black',
                                width: 2,
                                value: yLineVal
                            }],
                            title: {
                                text: yAxis
                            }
                        },
                    tooltip: getToolTipOld(),
                    series: data
                });
            
            function getLegend(that){
                if(chartType == 'scatter'){
                    return {enabled: true};
                }else if(chartType == 'bubble'){
                    return{
                        labelFormatter: function() {
                            return getLegendLabels(this);
                        }
                    }
                }
            }
            
            function getLegendLabels(that){
                if(that.color == Highcharts.getOptions().colors[2]){
                    return "Supported";
                }else if(that.color == Highcharts.getOptions().colors[3]){
                    return "Retired_(Not_Supported)";
                }else if(that.color == Highcharts.getOptions().colors[6]){
                    return "Unknown";
                }
            }
            
            function getToolTipOld(){
                if(chartType == 'scatter'){
                    return {
                            formatter: function(){
                                var xVal = this.x.toString().substring(0,6);
                                var yVal = this.y.toString().substring(0,6);
                                
                                return '<b>' + this.key + '</b><br/><b>Business Value</b>: ' + xVal + '...<br/><b>Tech Maturity</b>: ' + yVal + '...<br/>';
                            }
                        };
                }else if(chartType == 'bubble'){
                    return{
                            formatter: function(){
                                var xVal = this.x.toString().substring(0,6);
                                var yVal = this.y.toString().substring(0,6);
                                
                                return '<b>' + this.key + '</b><br/><b>Business Value</b>: ' + xVal + '...<br/><b>Tech Maturity</b>: ' + yVal + '...<br/><b>Sustainment Budget</b> (radius): ' + this.point.options.z;
                            }
                        }
                }
            }

        }
    }//end of healthGrid()
    
    //creates alliance health grid chart
    $scope.serviceGrid = function(chartType){
        var xAxis = 'BusinessValue'
        var yAxis = 'TechnicalMaturity';
        var xLineVal = .05;
        var yLineVal = 5;
        var systems = [];
        var dataProv = [];
        var bluProv = [];
        
        //get all the systems and their respective Technical Maturity
        for(var i=0; i<$scope.data.Nodes.System.length; i++){
            systems.push({name: $scope.data.Nodes.System[i].propHash.VERTEX_LABEL_PROPERTY, TechMat: $scope.data.Nodes.System[i].propHash.TechnicalMaturity});
        }
        
        //var uniqEdges = _.uniq($scope.data.Nodes.Service[i].outEdge);
        
        //get all the services and their respective business value
        for(var i=0; i<$scope.data.Nodes.Service.length; i++){
            for(var j=0; j<systems.length; j++){
                var edgeName = systems[j].name + '-' + $scope.data.Nodes.Service[i].propHash.VERTEX_LABEL_PROPERTY;
                var uniqEdges = _.uniq($scope.data.Nodes.Service[i].outEdge, function(edge){ return edge.propHash.EDGE_NAME; });
                for(var k=0; k<uniqEdges.length; k++){
                    //check to make sure all keys have values
                    if(!$scope.data.Nodes.Service[i].propHash.BusinessValue){
                        $scope.data.Nodes.Service[i].propHash.BusinessValue = 0;
                    }
                    if(!systems[j].TechMat){
                        systems[j].TechMat = 0;
                    }
                    if(!$scope.data.Nodes.Service[i].propHash.ICDCount){
                        $scope.data.Nodes.Service[i].propHash.ICDCount = 0;
                    }
                    if(chartType =='bubble'){
                        //setting the data to be added to the specific array
                        var seriesData = {name: edgeName, x: $scope.data.Nodes.Service[i].propHash.BusinessValue, y: systems[j].TechMat, z: $scope.data.Nodes.Service[i].propHash.ICDCount}
                        if(uniqEdges[k].propHash.EDGE_TYPE == "BLUProvider" && uniqEdges[k].propHash.EDGE_NAME == edgeName){
                            //pointColor = Highcharts.getOptions().colors[1];
                            bluProv.push(seriesData);
                        }
                        if(uniqEdges[k].propHash.EDGE_TYPE == "DataProvider" && uniqEdges[k].propHash.EDGE_NAME == edgeName){
                            //pointColor = Highcharts.getOptions().colors[2];
                            dataProv.push(seriesData);
                        }
                    }else if(chartType == 'scatter'){
                        //setting the data to be added to the specific array
                        var seriesData = {name: edgeName, x: $scope.data.Nodes.Service[i].propHash.BusinessValue, y: systems[j].TechMat}
                        if(uniqEdges[k].propHash.EDGE_TYPE == "BLUProvider" && uniqEdges[k].propHash.EDGE_NAME == edgeName){
                            //pointColor = Highcharts.getOptions().colors[1];
                            bluProv.push(seriesData);
                        }
                        if(uniqEdges[k].propHash.EDGE_TYPE == "DataProvider" && uniqEdges[k].propHash.EDGE_NAME == edgeName){
                            //pointColor = Highcharts.getOptions().colors[2];
                            dataProv.push(seriesData);
                        }
                    }
                }
            }
        }
        
        //make array of all the edge names
        var allServiceNames = [];
        for(var i=0; i<$scope.data.Nodes.Service.length; i++){
            allServiceNames.push($scope.data.Nodes.Service[i].propHash.VERTEX_LABEL_PROPERTY);
        }
        //returns only unique edge names
        allServiceNames = _.uniq(allServiceNames);
        
        
        //get the average technical maturity from the systems based on the specific service
        function getAveragePointsArray(array, dataType){
            var returnArray = [];
            var returnArraySorted = [];
            for(var i=0; i<allServiceNames.length; i++){     
                var count = 0;
                var total = 0;

                //loop through given array to find all the average tech maturity for a specific service
                for(var j=0; j<array.length; j++){
                    if(array[j].name.indexOf(allServiceNames[i]) > -1){
                        count++;
                        total += array[j].y;
                        var busVal = array[j].x;
                        if(array[j].z){
                            var icdCount = array[j].z
                        }
                    }
                    if(array.length == (j+1) && count > 0){
                        var avg = total/count;
                        if(chartType == 'bubble'){
                            if(dataType == 'BLU Provider'){
                                returnArray.push({name: allServiceNames[i], x: busVal, y: avg, z: count});
                            }else if(dataType == 'Data Provider'){
                                returnArray.push({name: allServiceNames[i], x: busVal, y: avg, z: icdCount});
                            }
                        }else if(chartType == 'scatter'){
                            returnArray.push({name: allServiceNames[i], x: busVal, y: avg});
                        }
                    }
                }
            }
            if(chartType == 'bubble'){
                //sort the data so it is plotted correctly
                returnArraySorted = _.sortBy(returnArray, function(point){
                                            if(!point.z){
                                                point.z = 0;
                                            }
                                            return -1*point.z; 
                                        }
                                    );
                return returnArraySorted;
            }else{
                return returnArray;
            }
            
        }
        
        //get the values for the arrays
        var bluProvAvg = getAveragePointsArray(bluProv, 'BLU Provider');
        var dataProvAvg = getAveragePointsArray(dataProv, 'Data Provider');
        
        var series = [];
        //set the data to the series array but only add data that isn't empty
        if(dataProvAvg.length > 0){
            series.push({name: 'Data Provider', color: Highcharts.getOptions().colors[2], data: dataProvAvg});
        }
        if(bluProvAvg.length > 0){
            series.push({name: 'BLU Provider', color: Highcharts.getOptions().colors[4], data: bluProvAvg});
        }
        
        //find the total business value stored in sum variable
        var sum = 0;
        sum += getSum(bluProvAvg);
        sum += getSum(dataProvAvg);
        
        //totals all the x values and returns
        function getSum(array){
            var returnSum = 0;
            for(var i=0; i<array.length; i++){
                if(array[i].x){
                    returnSum += array[i].x;
                }
            };
            return returnSum;
        }
        
        //set the x axis for the health grid that is plotted on the chart by finding the business value average
        var xLineVal = sum/(dataProvAvg.length + bluProvAvg.length);
        var xLineLabel = xLineVal.toString().substring(0,6) + '...';
        var yLineVal = 5;
        
        
            //creating the chart
            jQuery('#visualization').highcharts({
                chart: {
                    type: chartType,
                    zoomType: 'xy'
                },
                credits: {
                    text: ''
                },
                title: {
                    text: 'Health Grid'
                },
                /* legend: {
                    labelFormatter: function() {
                        return getLegend(this);
                    }
                },*/
                plotOptions: {
                    series: {
                        cursor: 'pointer',
                        point: {
                            events: {
                                click: function() {
                                    highlightPoints(this);
                                }
                            }
                        }
                    }
                },
                xAxis: {
                        events: {
                            afterSetExtremes: function(e){
                                if(e.min === e.max){
                                    var xMax = e.min + (e.min * 0.5);
                                    var xMin = e.min - (e.min * 0.5);
                                    this.setExtremes(xMin, xMax);
                                }
                            }
                        },
                        plotLines: [{
                            color: 'black',
                            width: 1,
                            value: xLineVal,
                            label: {
                                text: xLineLabel,
                                align: 'right',
                                verticalAlign: 'bottom',
                                rotation: '0',
                                x: 55,
                                y: -2
                            }
                        }],
                        title: {
                            text: xAxis
                        }
                    },
                yAxis: {
                        plotLines: [{
                            color: 'black',
                            width: 2,
                            value: yLineVal
                        }],
                        title: {
                            text: yAxis
                        }
                    },
                tooltip: getToolTip(),
                series: series
            });
        
        function getLegend(that){
            if(that.color == Highcharts.getOptions().colors[2]){
                return "Supported";
            }else if(that.color == Highcharts.getOptions().colors[3]){
                return "Retired_(Not_Supported)";
            }else if(that.color == Highcharts.getOptions().colors[6]){
                return "Unknown";
            }
        }
        
        function getToolTip(){
            if(chartType == 'scatter'){
                return {
                        formatter: function(){
                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            
                            return '<b>' + this.key + '</b><br/><b>Business Value</b>: ' + xVal + '...<br/><b>Tech Maturity</b>: ' + yVal + '...<br/>';
                        }
                    };
            }else if(chartType == 'bubble'){
                return{
                        formatter: function(){
                            var xVal = this.x.toString().substring(0,6);
                            var yVal = this.y.toString().substring(0,6);
                            var zLabel = '';
                            if(this.point.series.color == Highcharts.getOptions().colors[4]){
                                zLabel = '<br/><b>System Count</b> (radius): ' + this.point.options.z;
                            }else if(this.point.series.color == Highcharts.getOptions().colors[2]){
                                zLabel = '<br/><b>ICD Count</b> (radius): ' + this.point.options.z;;
                            }
                            
                            return '<b>' + this.key + '</b><br/><b>Business Value</b>: ' + xVal + '...<br/><b>Tech Maturity</b>: ' + yVal + '...' + zLabel;
                        }
                    }
            }
        
        }
        
        function highlightPoints(that){
            var highlightedPoints = [];
            var highlightedBLU = [];
            var highlightedDataProv = [];
            var chart = jQuery('#visualization').highcharts();
            //finds all the system-service
            for(var i=0; i<dataProv.length; i++){
                var dataProvServiceName = dataProv[i].name.substring(dataProv[i].name.lastIndexOf('-')+1);
                if(dataProvServiceName == that.name){
                    highlightedDataProv.push(_.pick(dataProv[i], 'name', 'x', 'y'));
                }
            }
            for(var i=0; i<bluProv.length; i++){
                var bluServiceName = bluProv[i].name.substring(bluProv[i].name.lastIndexOf('-')+1);
                if(bluServiceName == that.name){
                    highlightedBLU.push(_.pick(bluProv[i], 'name', 'x', 'y'));
                }
            }
            //set the x axis min and max values so that the data is centered
            var xMax = that.x + (that.x * 0.5);
            var xMin = that.x - (that.x * 0.5);
            //set the subtitle
            chart.setTitle(null, { text: that.name});
            
            //sets the new x axis and new data 
            chart.yAxis[0].setExtremes(0, 10);
            chart.xAxis[0].setExtremes(xMin, xMax);
            var serie;
            var highlightedData = [];
            var seriesCount = chart.series.length;
            //loops through current data series
            for(var i = 0; i < seriesCount; i++){
                highlightedData = [];
                serie = chart.series[i];
                var seriesColor = '';
                //sets the new data to highlightedData array
                if(serie.name == 'BLU Provider'){
                    highlightedData = highlightedBLU;
                    seriesColor = Highcharts.getOptions().colors[4];
                }else if(serie.name == 'Data Provider'){
                    highlightedData = highlightedDataProv;
                    seriesColor = Highcharts.getOptions().colors[2];
                }
                
                if(highlightedData.length > 0){
                    //adds the new series with only that service's systems
                    chart.addSeries({
                        type: 'scatter',
                        name: serie.name,
                        marker: {symbol: 'diamond'},
                        color: seriesColor,
                        data: highlightedData
                    }, false);
                }
                //chart.series[0].remove(false);
            }
            //have to remove the old series based on the original chart.series.length, you always remove 0 index in the array because the array shifts after deletion
            for(var i = 0; i < seriesCount; i++){
                chart.series[0].remove(false);
            }
            //redraws the chart
            chart.redraw();           
        }
    }//end of serviceGrid
}

function SingleChartCtrl($scope, $http) {
    
    //creates object that holds all graph option values
    var graphOptions = {
        xAxis: '',
        yAxis: '',
        zAxis: '',
        chartTitle: '',
        chartType: '',
        stackType: '',
        xAxisCategories: '',
        xLineVal: '',
        yLineVal: '',
        xLineWidth: '',
        yLineWidth: '',
        xMax: undefined,
        xMin: undefined,
        xInterval: undefined,
        yMax: undefined,
        yMin: undefined,
        yInterval: undefined,
        series: []
    };
    $scope.yExtraLegend = [];
    $scope.xExtraLegend = [];
    $scope.switchAxes = false;
    $scope.chartType = '';
    

    //function used to interact with the inline javascript
    $scope.setJSONData = function (data) {
        $scope.$apply(function () {
            graphOptions.series = [];
            var jsonData = jQuery.parseJSON(data);

            setChartData(jsonData);
        });
    };
    
    //----------Comment the below $http.get when using in Java
    /*$http.get('lib/lineData.json').success(function(jsonData) {
        setChartData(jsonData);
    });*/
    
    //takes the "data" parameter passed and sorts all the data appropriately
    function setChartData(data) {

        if (data.yLineLegend) {
            //declare the scope variable that will hold all the extra legend information
            for (var i=0; i<data.yLineLegend.length; i++) {
                if( Object.prototype.toString.call( data.yLineVal ) === '[object Array]' ) {
                    $scope.yExtraLegend.push(data.yLineVal[i] + " = " + data.yLineLegend[i]);
                }else {
                    $scope.yExtraLegend.push(data.yLineVal + " = " + data.yLineLegend[i]);
                }
                
            }
        }
        
        if (data.xLineLegend) {
            //declare the scope variable that will hold all the extra legend information
            for (var i=0; i<data.xLineLegend.length; i++) {
                if( Object.prototype.toString.call( data.xLineVal ) === '[object Array]' ) {
                    $scope.xExtraLegend.push(data.xLineVal[i] + " = " + data.xLineLegend[i]);
                }else {
                    $scope.xExtraLegend.push(data.xLineVal + " = " + data.xLineLegend[i]);
                }
                
            }
        }
        
        $scope.chartType = data.type;
        
        //declare and set variables
        graphOptions.xLineLabel = data.xLineLabel;
        graphOptions.yLineLabel = data.yLineLabel;
        graphOptions.xAxis = data.xAxisTitle;
        graphOptions.yAxis = data.yAxisTitle;
        graphOptions.zAxis = data.zAxisTitle;
        graphOptions.chartTitle = data.title;
        $scope.chartTitle = data.title;
        graphOptions.chartType = data.type;
        graphOptions.stackType = data.stack;
        graphOptions.xAxisCategories = data.xAxis;

        //set values if they are not null
        if(data.xAxisMax || data.xAxisMax == 0){
            graphOptions.xMax = data.xAxisMax;
        }
        if(data.xAxisMin || data.xAxisMin == 0){
            graphOptions.xMin = data.xAxisMin;
        }
        if(data.xAxisInterval || data.xAxisInterval == 0){
            graphOptions.xInterval = data.xAxisInterval;
        }
        if(data.yAxisMax || data.yAxisMax == 0){
            graphOptions.yMax = data.yAxisMax;
        }
        if(data.yAxisMin || data.yAxisMin == 0){
            graphOptions.yMin = data.yAxisMin;
        }
        if(data.yAxisInterval || data.yAxisInterval == 0){
            graphOptions.yInterval = data.yAxisInterval;
        }
        if(data.xLineVal || data.xLineVal == 0){
            graphOptions.xLineVal = data.xLineVal;
            graphOptions.xLineWidth = 1;
        }
        if(data.yLineVal || data.yLineVal == 0){
            graphOptions.yLineVal = data.yLineVal;
            graphOptions.yLineWidth = 1;
        }

        //loops through each object within dataSeries
        for (var key in data.dataSeries) {

            //check to see whether the xAxis values are already defined
            if(graphOptions.xAxisCategories){
                //checks to see if the color option is available or not and sets it appropriately
                if(data.colorSeries){
                    graphOptions.series.push({name: key, color: data.colorSeries[key], data: data.dataSeries[key]});
                } else {
                    graphOptions.series.push({name: key, data: data.dataSeries[key]});
                }
            } else if (graphOptions.chartType == 'pie') {
            	graphOptions.series.push({name: key, data: data.dataSeries[key]});
            } else {

                var series = [];

                if (graphOptions.zAxisTitle) {
                    //organize the data by z value so that the smallest points are plotted last
                    var sortedData = _.sortBy(data.dataSeries[key], function(point){
                                            return -1*point[2]; 
                                        }
                                    );
                } else {
                    var sortedData = data.dataSeries[key];
                }

                //dependent on the length of the data.dataSeries[key] array, the data needs to be set up differently
                for (var i = 0; i< sortedData.length; i++) {

                    if (sortedData[i].length == 2) {

                        //set the data series and add to series array
                        series.push({x: sortedData[i][0], y: sortedData[i][1]});
                    }
                    if (sortedData[i].length == 3 && !graphOptions.zAxisTitle) {

                        //set the data series and add to series array
                        series.push({x: sortedData[i][0], y: sortedData[i][1], name: sortedData[i][2]});
                    }
                    if (sortedData[i].length == 3 && graphOptions.zAxisTitle) {

                        //set the data series and add to series array
                        series.push({x: sortedData[i][0], y: sortedData[i][1], z: sortedData[i][2]});
                    }
                    if (sortedData[i].length == 4) {
                        
                        //set the data series and add to series array
                        series.push({x: sortedData[i][0], y: sortedData[i][1], z: sortedData[i][2], name: sortedData[i][3]});
                    }
                }

                //checks to see if the color option is available or not and sets it appropriately
                if(data.colorSeries){
                    graphOptions.series.push({name: key, color: data.colorSeries[key], data: series});
                } else {
                    graphOptions.series.push({name: key, data: series});
                }
            }
        }

        //get the median for the x and y axis
        if(graphOptions.chartType == 'scatter' || graphOptions.chartType == 'bubble') {
            //if xline is empty get the median
        	if(!data.xLineVal)
        	{
	            graphOptions.xLineVal = getMedian(sortedData, 0);
    	        graphOptions.xLineWidth = 1;
    	    }
            //if yline is empty get the median
    	    if(!data.yLineVal)
    	    {
                graphOptions.yLineVal = getMedian(sortedData, 1);
                graphOptions.yLineWidth = 2;
            }
        }
        
        $scope.createChart();
    }

    function updateChartOption(option, newValue){
        graphOptions[option] = newValue;
    }

    function getMedian(array, sortIdx) {
        var medianData = _.sortBy(array, function(point){
                                            return point[sortIdx]; 
                                        }
                                    );

        var half = Math.floor(medianData.length/2);

        if(medianData.length % 2){
            return medianData[half][sortIdx];
        } else {
            return (medianData[half-1][sortIdx] + medianData[half][sortIdx]) / 2.0;
        }
            
    }
    
    $scope.switchGraphAxesData = function() {
    	var newSeries = [];
    	var newXAxisCategories = [];
    	
    	for (var i=0; i<graphOptions.xAxisCategories.length; i++) {
    		var name = graphOptions.xAxisCategories[i];
    		var data = [];
    		
    		for (var j=0; j<graphOptions.series.length; j++) {
                data.push(graphOptions.series[j].data[i]);
                if (i == 0) {
                	newXAxisCategories.push(graphOptions.series[j].name);
                }
			}
    		newSeries.push({name: name, data: data});
    	}
    	    	
    	graphOptions.xAxisCategories = newXAxisCategories;
    	graphOptions.series = newSeries;
    	$scope.createChart();
    }

    //creates alliance health grid chart
    $scope.createChart = function (newType) {
    		
        if(newType){
            graphOptions.chartType = newType;
        }
        if(newType === 'bubble' && graphOptions.series[0].data[0].length < 3) {
            graphOptions.chartType = 'scatter';
        }

        //creating the chart
        jQuery('#visualization').highcharts({
            chart: {
                type: graphOptions.chartType,
            },
            credits: {
                text: ''
            },
            title: {
                text: graphOptions.chartTitle
            },
            plotOptions: {
            	spline: {
                    marker: {
                        enabled: false
                    }
                },
				column: {
                stacking: graphOptions.stackType
				}
            },
            xAxis: {
                gridLineWidth: getxGridLineWidth(),
                max: graphOptions.xMax,
                min: graphOptions.xMin,
                tickInterval: graphOptions.xInterval,
                labels: {
                    rotation: -45,
                    align: 'right',
                    formatter: function(){
                        return this.value;       
                    }
                },
                title: {
                    text: graphOptions.xAxis
                },
                categories: getXAxisCategories(),
                plotLines: getXAxisPlotLines()
            },
            yAxis: {
                gridLineWidth: getyGridLineWidth(),
                max: graphOptions.yMax,
                min: graphOptions.yMin,
                tickInterval: graphOptions.yInterval,
                title: {
                    text: graphOptions.yAxis
                },
                plotLines: getYAxisPlotLines()
            },
            tooltip: getToolTip(),
            series: graphOptions.series
        });
        
    }//end of createChart()

    function getToolTip(){
        if (graphOptions.chartType == 'scatter'){
            return {
                formatter: function(){
                    var xVal = this.x.toString().substring(0,6);
                    var yVal = this.y.toString().substring(0,6);
                    var shortKey = '';
                    var brokenKeyArray = this.key.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                    for (var i = 0; i<brokenKeyArray.length; i++) {
                        shortKey += '<b>' + brokenKeyArray[i] + '</b><br/>';
                    }
                    
                    return '<b>' + shortKey + '<b>' + graphOptions.xAxis + '</b>: ' + xVal + '<br/><b>' + graphOptions.yAxis + '</b>: ' + yVal + '<br/>';
                }
            };
        }else if (graphOptions.chartType == 'column'){
            return {
                formatter: function(){
					return '<b>' + "Count" + '</b>: ' + this.y.toString();
                }
            };
        }else if (graphOptions.chartType == 'bubble'){
            return{
                formatter: function(){
                    var xVal = this.x.toString().substring(0,6);
                    var yVal = this.y.toString().substring(0,6);
                    var zInfo = '';
                    var shortKey = '';
                    if (graphOptions.zAxis){
                        zInfo = '<br/><b>' + graphOptions.zAxis + '</b> (radius): ' + this.point.options.z;
                    }

                    var brokenKeyArray = this.key.replace(/.{25}\S*\s+/g, "$&@").split(/\s+@/);
                    for (var i = 0; i<brokenKeyArray.length; i++) {
                        shortKey += '<b>' + brokenKeyArray[i] + '</b><br/>';
                    }
                    
                    return '<b>' + shortKey + '<b>' + graphOptions.xAxis + '</b>: ' + xVal + '<br/><b>' + graphOptions.yAxis + '</b>: ' + yVal + zInfo;
                }
            };
        } else {
            return {enabled: true};
        }
    }

    function getXAxisPlotLines(){
        if( Object.prototype.toString.call( graphOptions.xLineVal ) === '[object Array]' ) {
            var returnLines = [];
            var xLabel = '';
            for(var i=0; i<graphOptions.xLineVal.length; i++) {
                if(graphOptions.xLineLabel == "true") {
                    if(graphOptions.xMin == graphOptions.xLineVal[i] || graphOptions.xMax == graphOptions.xLineVal[i]) {
                        xLabel = '';
                    } else {
                        xLabel = graphOptions.xLineVal[i];
                    }
                }
                returnLines.push({
                    color: 'black',
                    width: graphOptions.xLineWidth,
                    value: graphOptions.xLineVal[i],
                    label: {
                        text: xLabel,
                        verticalAlign: 'bottom',
                        textAlign: 'right',
                        rotation: 0,
                        y: -5,
                        x: -2
                    }
                })
            }
            return returnLines;
        } else {
            var xLabel = '';
            if(graphOptions.xLineLabel == "true") {
                if(graphOptions.xMin == graphOptions.xLineVal || graphOptions.xMax == graphOptions.xLineVal[i]) {
                    xLabel = '';
                } else {
                    xLabel = graphOptions.xLineVal;
                }
            }
            return [{
                color: 'black',
                width: graphOptions.xLineWidth,
                value: graphOptions.xLineVal,
                label: {
                    text: xLabel,
                    verticalAlign: 'bottom',
                    textAlign: 'right',
                    rotation: 0,
                    y: -5,
                    x: -2
                }
            }];
        }        
    }

    function getYAxisPlotLines(){
        if( Object.prototype.toString.call( graphOptions.yLineVal ) === '[object Array]' ) {
            var returnLines = [];
            var yLabel = '';
            for(var i=0; i<graphOptions.yLineVal.length; i++) {
                if(graphOptions.yLineLabel == "true") {
                    if(graphOptions.yMin == graphOptions.yLineVal[i] || graphOptions.yMax == graphOptions.yLineVal[i]) {
                        yLabel = '';
                    } else {
                        yLabel = graphOptions.yLineVal[i];
                    }
                }
                returnLines.push({
                    color: 'black',
                    width: graphOptions.yLineWidth,
                    value: graphOptions.yLineVal[i],
                    label: {
                        text: yLabel,
                        verticalAlign: 'bottom',
                        textAlign: 'right',
                        y: -5
                    }
                })
            }
            return returnLines;
        } else {
            var yLabel = '';
            if(graphOptions.yLineLabel == "true") {
                if(graphOptions.yMin == graphOptions.yLineVal || graphOptions.yMax == graphOptions.yLineVal[i]) {
                    yLabel = '';
                } else {
                    yLabel = graphOptions.yLineVal;
                }
            }
            return [{
                color: 'black',
                width: graphOptions.yLineWidth,
                value: graphOptions.yLineVal,
                label: {
                        text: yLabel,
                        verticalAlign: 'bottom',
                        textAlign: 'right',
                        y: -5
                    }
            }];
        }        
    }

    function getxGridLineWidth() {
        if (graphOptions.xLineVal) {
            return 0;
        } else{
            return 1;
        } 
    }

    function getyGridLineWidth() {
        if (graphOptions.yLineVal) {
            return 0;
        } else{
            return 1;
        } 
    }
    
    function getXAxisCategories () {
    	var shortAxisNames = [];
        if (graphOptions.xAxisCategories) {
        	for (var i=0; i<graphOptions.xAxisCategories.length; i++) {
        		if (graphOptions.xAxisCategories[i].length > 25) {
        			shortAxisNames.push(graphOptions.xAxisCategories[i].substring(0, 25) + "...");
        		} else {
        			shortAxisNames.push(graphOptions.xAxisCategories[i]);
        		}
        	}
        	return shortAxisNames;
        } else {
            return undefined;
        }
    	
    }
    
}

function TimelineCtrl($scope, $http) {


    //----------Comment the below $http.get when using in Java
    /*$http.get('lib/timelineData.json').success(function(timeData) {
        var jsonData = timeData;
        createTimeline(jsonData);
    });*/

    
    $scope.setJSONData = function (timeData) {
        $scope.$apply(function () {
            var jsonData = jQuery.parseJSON(timeData);
			
            createTimeline(jsonData);
        });
    };

    function createTimeline(data){

        //console.log(data);
        var eventsData = [];
        for (var key in data){

            //declare string to find
            var find = '~';
            //set the RegExp to find variable 'find' and make it find all instances using 'g'
            var re = new RegExp(find, 'g');
            //replaces all values of '~' to '\n' for the description string
            data[key].description = data[key].description.replace(re, '\n');
            eventsData.push(data[key]);
        }
		
        /****** potential functionality to set the initialized date that the timeline will start on*/
        //this will sort the dates in ascending order
        var sortedData = _.sortBy(data, function(timeEvent){
        	var splitDate = timeEvent.start.split();
        	var m = parseInt(splitDate[0]) - 1;
        	var d = parseInt(splitDate[1]);
        	var y = parseInt(splitDate[2]);
        	return new Date(y,m,d);
        });
        //get the median value to set where the timelines will begin
        var lastItem = sortedData.length - 1;
        var bandInitDate = sortedData[lastItem].start + " 00:00:00 GMT";
        

        var timeline_data = {
            'dateTimeFormat': 'Gregorian',
            'events': eventsData
        }

        var tl;
        var eventSource = new Timeline.DefaultEventSource();
        var bandInfos = [
            Timeline.createBandInfo({
                eventSource:    eventSource,
                date:           bandInitDate,
                width:          "75%", 
                intervalUnit:   Timeline.DateTime.MONTH, 
                intervalPixels: 100
            }),
            Timeline.createBandInfo({
                overview: true,
                eventSource:    eventSource,
                date:           bandInitDate,
                width:          "25%", 
                intervalUnit:   Timeline.DateTime.YEAR, 
                intervalPixels: 200
            })
        ];

        bandInfos[1].syncWith = 0;
        bandInfos[1].highlight = true;
        tl = Timeline.create(document.getElementById("my-timeline"), bandInfos);

        //using json example from above
        var url = '.';
        eventSource.loadJSON(timeline_data, url);
    }
    
}

function HWLifecycleCtrl($scope, $http, LifeCycleService) {

    $scope.hardware = {
        retired: [],
        sunset: [],
        supported: [],
        ga: [],
        unknown: []
    };
    var hardware = [];
    var jsonData;

    //function setting the json data that is sent from the Java application
    $scope.setJSONData = function (data) {
        $scope.$apply (function () {
            initData(jQuery.parseJSON(data));
        });
    };
    
    //----------Comment the below $http.get when using in Java
    /*$http.get('lib/lifecycleData.json').success(function(jsonData) {
        //orginal
        initData(jsonData);
    });*/

    function initData(data) {
        var todaysDate = LifeCycleService.getTodaysDate();

        for (var key in data){
            hardware.push({
                "name": key,
                "endOfLifeDate": data[key][0],
                "upgradeHW": data[key][1],
                "quantity": data[key][2],
                "cost": data[key][3],
                "totalCost": data[key][2]*data[key][3]
            });
        }
        $scope.setlifecycledata(todaysDate);
    }


    //initial set up of lifecycle data
    $scope.setlifecycledata = function(startDate) {
        $scope.hardware.retired = [];
        $scope.hardware.sunset = [];
        $scope.hardware.supported = [];
        $scope.hardware.ga = [];
        $scope.hardware.unknown = [];

        //start code to see what timeframe the hardware lands in
        for (var i=0; i<hardware.length; i++) {
            
            //get the time left before software needs to be retired
            var hwAge = LifeCycleService.convertMSToYears((new Date(hardware[i].endOfLifeDate) - startDate));

            if (hwAge < .5) {
                $scope.hardware.retired.push(hardware[i]);
            } else if (hwAge >= .5 && hwAge < 1) {
                $scope.hardware.sunset.push(hardware[i]);
            } else if (hwAge >= 1 && hwAge < 3) {
                $scope.hardware.supported.push(hardware[i]);
            } else if (hwAge >= 3) {
                $scope.hardware.ga.push(hardware[i]);
            } else {
                $scope.hardware.unknown.push(hardware[i]);
            }
        }
        setPieChartData();
    }

    function setPieChartData() {
        var data = [];

        if($scope.hardware.retired.length > 0){
            data.push({name: 'Retired_(Not_Supported)', y: $scope.hardware.retired.length, color: '#ee7d7d'});
        }
        if($scope.hardware.sunset.length > 0){
            data.push({name: 'Sunset_(End_of_Life)', y: $scope.hardware.sunset.length, color: '#eec76a'});
        }
        if($scope.hardware.supported.length > 0){
            data.push({name: 'Supported', y: $scope.hardware.supported.length, color: '#6fd531'})
        }
        if($scope.hardware.ga.length > 0){
            data.push({name: 'GA_(Generally_Available)', y: $scope.hardware.ga.length, color: '#18a600'});
        }
        if($scope.hardware.unknown.length > 0){
            data.push({name: 'Unknown', y: $scope.hardware.unknown.length, color: '#b70000'});
        }

        var series = [];
        series.push({type: 'pie', name:'Life Cycle', data: data});

        createChart(series);
    }


    //creates chart
    function createChart (series) {
        
            //creating the chart
            jQuery('#visualization').highcharts({

                credits: {
                    text: ''
                },
                title: {
                    text: ''
                },
                series: series
            });
        
    }//end of createChart()
    
}