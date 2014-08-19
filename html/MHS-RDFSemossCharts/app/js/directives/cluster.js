'use strict';

/* Directives */
app.directive('d3Cluster', function() {
    return {
        restrict: 'E',
        scope: {
            data: '=',
            setGroupData: "&",
            setNodeData: "&",
            containerId: "=",
            isClusterPropActive: "="
        },
        link: function(scope) {
            var clusterData = {};
            var numGroups = 0;
            var groupingCategory = "ClusterID";
            var nodeName = "nodeName";
            var groupingCategoryInstances = {};
            var numberOfClusters = 0;
            var barData = [];
            var w = parseInt(d3.select('#' + scope.containerId).style('width'));
            var h = parseInt(d3.select('#' + scope.containerId).style('height')) - 5;


            scope.$watch('data', function() {
                if (scope.data == undefined || scope.data == null || scope.data.length == 0 || scope.data == '') {
                    clusterData = {};
                } else {
                    if (clusterData != scope.data.dataSeries) {
                        clusterData = {};
                        clusterData = scope.data.dataSeries;
                        barData = scope.data.barData;
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

            function structureBarData(d, barDataArray){
                var barDataCopy = jQuery.extend(true, {}, barDataArray);
                var output = [];
                output = barDataCopy[d.key];
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

            var fill = d3.scale.category20();
            var vis = d3.select('#' + scope.containerId).append("svg")
                .attr("width", w)
                .attr("height", h);

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

            vis.style("opacity", 1e-6)
                .transition()
                .duration(1000)
                .style("opacity", 1);

            var force = d3.layout.force();

            var node;

            var pathElements;

            function update(updateData) {
                var data = updateData;
                var nodes = data.map(Object);

                groupingCategoryInstances = getCategoryInstances(groupingCategory, nodes);
                numberOfClusters = Object.keys(groupingCategoryInstances).length;

                var groups = d3.nest().key(function (d) {
                    return d[groupingCategory];
                }).entries(nodes);

                force
                    .nodes(nodes)
                    .links([])
                    .size([w, h])
                    .charge(-10)
                    .start();

                node = vis.selectAll("circle.node")
                    .data(nodes);
                node
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
                        return fill(d[groupingCategory]);
                    })
                    .style("stroke", function (d, i) {
                        return "#777";
                    })
                    .style("stroke-width", 1.5)
                    .call(force.drag);

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

                node.attr("cx", function (d) {
                    return d.x;
                })
                    .attr("cy", function (d) {
                        return d.y;
                    });

                force.on("tick", function (e) {
                    var k = 6 * e.alpha;
                    var heightAmplification;
                    var heightOffset = -h/2;
                    var widthAmplification;
                    var widthOffset = -w/2;
//                    var aspectRatio = w/h;

                    if(numGroups > 20){
                        heightAmplification = 0.010;
                        widthAmplification = 0.010;
                    }else if(numGroups > 10){
                        heightAmplification = 0.008;
                        widthAmplification = 0.010;
                    }else if(numGroups > 5){
                        heightAmplification = 0.007;
                        widthAmplification = 0.006;
                    }else{
                        heightAmplification = 0.005;
                        widthAmplification = 0.005;
                    }

                    var theta = 2 * Math.PI / numGroups;

                    function calculatePositionIncrements(i){
//                        if(aspectRatio < 0.668){
//                            var yPositionIncrement = Math.sin(i * theta) * 4 * k;
//                            var xPositionIncrement = Math.cos(i * theta) * 4 * k;
//                        }else if(aspectRatio < 1.497){
                            var n = Math.ceil(Math.sqrt(numberOfClusters)+1);
                            var j = Math.ceil((Math.pow(n, 2)/numberOfClusters)*i);
                            var yPositionIncrement = (heightOffset + Math.ceil(j / n)*(h / n)) * heightAmplification * k;
                            var xPositionIncrement = (widthOffset + (j % n)*(w / n)) * widthAmplification * k;
//                        }else{
//                            var yPositionIncrement = Math.sin(i * theta) * 2 * k - 5 * k;
//                            var xPositionIncrement = Math.cos(i * theta) * 2 * k - 5 * k;
//                        }
                        return {
                            yPositionIncrement: yPositionIncrement,
                            xPositionIncrement: xPositionIncrement
                        }
                    }

                    nodes.forEach(function (o, i) {
                        var index = groupingCategoryInstances[o[groupingCategory]];
                        var increments = calculatePositionIncrements(index);
                        o.y += increments.yPositionIncrement;
                        o.x += increments.xPositionIncrement;

                    });

                    node.attr("cx", function (d) {
                        return d.x;
                    })
                        .attr("cy", function (d) {
                            return d.y;
                        });

                    pathElements = vis.selectAll("path")
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
                            structureBarData(d, barData);
                            var allPaths = d3.selectAll("#" + scope.containerId + " path"),
                                selectedPath = d3.select(this);
                            //set all circles (and previously selected nodes) to default stroke & stroke-width
                            allPaths.style({
                                "stroke-width": 40,
                                "opacity":.2
                            });
                            //set selected node to <color> and <border> size
                            selectedPath.style({
                                "opacity":.8,
                                "stroke-width": 50
                            });
                        });
                });
            }

            function resize() {
                w = parseInt(d3.select('#' + scope.containerId).style('width'));
                h = parseInt(d3.select('#' + scope.containerId).style('height')) - 5;
                console.log("resizing");
                d3.select('#clusterContainer svg').attr("width", w).attr("height", h);
                force.size([w, h]).resume();


            }

            d3.select(window).on('resize', resize);
            scope.$watch('isClusterPropActive', function() {
                setTimeout(resize, 300);
            });
        },
        template:"<div></div>"
    }
})