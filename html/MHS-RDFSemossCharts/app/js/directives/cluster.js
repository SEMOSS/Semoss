'use strict';

/* Directives */
app.directive('d3Cluster', function() {
    return {
        restrict: 'E',
        scope: {
            data: '=',
            setGroupData: "&",
            setNodeData: "&",
            containerId: "="
        },
        link: function(scope) {
            var clusterData = {};
            var numGroups = 0;
            var groupingCategory = "ClusterID";
            var nodeName = "nodeName";
            var groupingCategoryInstances = {};

            d3.select(window).on('resize', resize);

            scope.$watch('data', function() {
                if (scope.data == undefined || scope.data == null || scope.data.length == 0 || scope.data == '') {
                    clusterData = {};
                } else {
                    if (clusterData != scope.data.dataSeries) {
                        clusterData = {};
                        clusterData = scope.data.dataSeries;
                        update(clusterData);

                    }
                }
            });

            var getCategoryInstances = function(cat, nodeData){
                var categoryInstances = {};
                var j = 0;
                for(var i = 0; i<nodeData.length; i++){
                    if(!(nodeData[i][cat] in categoryInstances)) {
                        categoryInstances[nodeData[i][cat]] = j;
                        j++;
                    }
                }
                numGroups = j + 1;
                return categoryInstances;
            };

            var getCategoryArray = function(catData){
                var array = [];
                for(var key in catData){
                    array.push(key);
                }
                return array;
            };

            function structureBarData(d, groupingCategory, nodeName){
                var clusterPropertyCategories = {};
                var clusterPropertyCategoriesInverse = {};
                var n = 0;
                var output = {};
                for(var key in d.values[0]){
                    if(key != "x" && key != "y" && key != "px" && key != "py"){
                        if(key != "weight" && key != "fixed" && key != "index" && key != groupingCategory && key != nodeName){
                            clusterPropertyCategories[key] = n;
                            clusterPropertyCategoriesInverse[n] = key;
                            n++;
                        }
                    }
                }
                for(var h=0; h < n; h++){
                    var propToAugment = clusterPropertyCategoriesInverse[h];
                    output[propToAugment] = {'names':[propToAugment,'Frequency'],'dataSeries':[[]]};
                }
                //loop through each circle
                for(var i=0; i < d.values.length; i++){
                    //loop through each property
                    for(var j=0; j < n; j++){
                        var propToAugment = clusterPropertyCategoriesInverse[j];
                        var xName = d.values[i][propToAugment];
                        var matchFound = false;
                        var newObject = {
                            seriesName: 'Frequency',
                            x: xName,
                            y: 1,
                            y0:0
                        };
                        //if xName doesn't exist, add new object to output
                        for(var s = 0; s<output[propToAugment]["dataSeries"][0].length; s++){
                            if(output[propToAugment]["dataSeries"][0][s]['x'] == xName){
                                matchFound = true;
                            }
                        }
                        if(!matchFound){
                            output[propToAugment]["dataSeries"][0].push(newObject);
                        }else{
                            //locate object with matching x value
                            for(var t = 0; t < output[propToAugment]["dataSeries"][0].length; t++){
                                if(output[propToAugment]["dataSeries"][0][t]['x'] == xName){
                                    output[propToAugment]["dataSeries"][0][t]['y']++;
                                }
                            }
                        }
                    }
                }
                scope.setGroupData({groupData: output});
            }

            function structureNodeData(d, groupingCategory, nodeName){
                var output = {};

                for(var key in d){
                    if(key != "x" && key != "y" && key != "px" && key != "py"){
                        if(key != "weight" && key != "fixed" && key != "index" && key != groupingCategory && key != nodeName){
                            output[key] = d[key];
                        }
                    }
                }

                scope.setNodeData({nodeData: output});
            }


            var w = parseInt(d3.select('#' + scope.containerId).style('width'));
            var h = parseInt(d3.select('#' + scope.containerId).style('height')) - 5;
            var vis = d3.select('#' + scope.containerId).append("svg")
                .attr("width", w)
                .attr("height", h);

            function update(updateData) {
                var data = updateData;
                var nodes = data.map(Object);

                var fill = d3.scale.category20();

                groupingCategoryInstances = getCategoryInstances(groupingCategory, nodes);

                var groups = d3.nest().key(function (d) {
                    return groupingCategoryInstances[d[groupingCategory]];
                }).entries(nodes);

                var groupPath = function (d) {
                    var groupPathReturn = "";
                    if(d.values.length == 1){
                        groupPathReturn = ("M" + (d.values[0].x + 0.04) + "," + d.values[0].y + "L" + (d.values[0].x - 0.03) + "," + (d.values[0].y + 0.03) + "L" + (d.values[0].x - 0.03) + "," + (d.values[0].y - 0.03) + "Z");
                    }else if(d.values.length == 2){
                        groupPathReturn = ("M" + (d.values[1].x) + "," + d.values[1].y + "L" + (d.values[0].x -0.01) + "," + (d.values[0].y +0.01) + "L" + (d.values[0].x -0.01) + "," + (d.values[0].y - 0.01) + "Z");
                    }else{
                        groupPathReturn = ("M" +
                            d3.geom.hull(d.values.map(function (i) {
                                return [i.x, i.y];
                            }))
                                .join("L")
                            + "Z");
                    }
                    return groupPathReturn;
                };

                var groupFill = function (d, i) {
                    return fill(d.key);
                };



                var force = d3.layout.force()
                    .nodes(nodes)
                    .links([])
                    .size([w, h])
                    .start();

                var node = vis.selectAll("circle.node")
                    .data(nodes)
                    .enter().append("circle")
                    .attr("class", "node")
                    .attr("cx", function (d) {
                        return d.x;
                    })
                    .attr("cy", function (d) {
                        return d.y;
                    })
                    .attr("r", 8)
                    .style("fill", function (d, i) {
                        return fill(groupingCategoryInstances[d[groupingCategory]]);
                    })
                    .style("stroke", function (d, i) {
                        return "#777";
                    })
                    .style("stroke-width", 1.5)
                    .call(force.drag);

                vis.style("opacity", 1e-6)
                    .transition()
                    .duration(1000)
                    .style("opacity", 1);

                node.on("click", function(d){
                    structureNodeData(d, groupingCategory, nodeName);
                    var allCircles = d3.selectAll("circle.node"),
                        selectedCircle = d3.select(this);
                    //set all circles (and previously selected nodes) to default stroke & stroke-width
                    allCircles.style({
                        "stroke": "#777",
                        "stroke-width": 1.5
                    });
                    //set selected node to <color> and <border> size
                    selectedCircle.style({
                        "stroke": "red",
                        "stroke-width": 3.0
                    });
                });

                force.on("tick", function (e) {
                    var k = 6 * e.alpha;
                    var theta = 2 * Math.PI / numGroups;

                    nodes.forEach(function (o, i) {
                        o.y += Math.sin(groupingCategoryInstances[o[groupingCategory]] * theta) * 2 * k;
                        o.x += Math.cos(groupingCategoryInstances[o[groupingCategory]] * theta) * 2 * k;
                    });

                    node.attr("cx", function (d) {
                        return d.x;
                    })
                        .attr("cy", function (d) {
                            return d.y;
                        });

                    vis.selectAll("path")
                        .data(groups)
                        .attr("d", groupPath)
                        .enter().insert("path", "circle")
                        .style("fill", groupFill)
                        .style("stroke", groupFill)
                        .style("stroke-width", 40)
                        .style("stroke-linejoin", "round")
                        .style("opacity", .2)
                        .attr("d", groupPath)
                        .on("click", function(d){

                            structureBarData(d, groupingCategory, nodeName);

                            var allPaths = d3.selectAll("#" + scope.containerId + " path"),
                                selectedPath = d3.select(this);
                            //set all circles (and previously selected nodes) to default stroke & stroke-width
                            allPaths.style({
                                "stroke": groupFill,
                                "fill": groupFill,
                                "stroke-width": 40,
                                "opacity":.2
                            });
                            //set selected node to <color> and <border> size
                            selectedPath.style({
                                "stroke": groupFill,
                                "fill": groupFill,
                                "opacity":.8,
                                "stroke-width": 50
                            });
                        });
                });
            }

            function resize(){
                w = parseInt(d3.select('#' + scope.containerId).style('width'));
                h = parseInt(d3.select('#' + scope.containerId).style('height')) - 5;
                d3.select('#clusterContainer svg').attr("width", w).attr("height", h);
            }
        }
    }
});