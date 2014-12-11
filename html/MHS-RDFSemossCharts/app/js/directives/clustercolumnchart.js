app.directive('columnchart', function($filter, $rootScope) {

    return {
        restrict: 'AE',
        scope: {
            data: "=",
            containerClass: "=",
            barChartResized: "="
        },
        link: function(scope, ele, attrs) {
            var margin = {top: 20, right: 40, bottom: 100, left: 80},
                container = {width: 0, height: 0},
                dataSeriesObject = [],
                dataSeriesKeys = [],
                dataSeriesLabels = {},
                filteredLevels = [], legendArray = [],
                names = [], nodes = [], edges = [],
                xAxisText = "", yAxisText = "",
                standardBarOpacity = .65,
                barsTransitionDuration = 230, axisTransitionDuration = 1000,
                axisLabelPadding = 15, currentLeftMargin = 0, currentWidth = 0,
                absoluteBarPadding = 1, barPadding = 0.05,
                yGroupMax = 0, yStackMax = 0, layers = {}, stacked = true;
            var columnData = {};
            var barChartSignal = false;
            var zScoreFlag = false;
            var zScoreData = [];
            var thresholdwidth = 960,
                thresholdheight = 500,
                formatNumber = d3.format(".f");
            var threshold;
            var thresholdg;
            var thresholdxScale2;
            var thresholdxAxis2;


//            var zScoreDataTest = [-3.75,-3,-2,-1,0,1.05];

            scope.internalControl = scope.control || {};
            scope.$on('filterValue', function(event, args){
                highlightColumn(args);
            });

            //set container width and height
            containerSize(scope.containerClass, container, margin);

            function containerSize(containerClass, containerObj, marginObj) {
                if(containerClass) {
                    containerObj.width = parseInt(d3.select('.' + containerClass).style('width'));
                    containerObj.height = parseInt(d3.select('.' + containerClass).style('height'));
                } else {
                    containerObj.width = parseInt(d3.select('.graph-canvas').style('width'));
                    containerObj.height = parseInt(d3.select('.graph-canvas').style('height'));
                }

                containerObj.width = containerObj.width - marginObj.left - marginObj.right;
                containerObj.height = containerObj.height - marginObj.top - marginObj.bottom;
            }

            scope.$watch('data', function() {
                if (scope.data == undefined || scope.data == null || scope.data ==  [] || scope.data == '') {
                    console.log("no data");
                } else {
                    if(columnData != scope.data){
                        columnData = scope.data;
                        if(columnData.zScore != null){
                            zScoreFlag = true;
                            zScoreData = columnData.zScore;
                        }
                        dataSeriesObject.length = 0;
                        dataSeriesKeys.length = 0;
                        names.length = 0;
                        dataSeriesObject = columnData.dataSeries;
                        dataSeriesKeys = dataSeriesObject[0].map(function(d) { return d.x});
                        xAxisText = columnData.names[0];
                        names = columnData.names;
                        legendArray.length = 0;
                        update();
                    }
                }
            });



//            $rootScope.$on('gridster-resized', function(newSizes){
//                //need to check and make sure the containerClass that is trying to resize actually has values in it
//                if($("." + scope.containerClass).length > 0) {
//                    resize();
//                }
//            });

            var color;
            //d3.scale.linear()
            //.domain([0, dataSeriesObject.length - 1])
            //.range(["#aad", "#556"]);

            var createCount = 0;

            //setup x
            var xScale = d3.scale.ordinal()
                .rangeRoundBands([0, container.width ], barPadding);



            //setup y
            var yScale = d3.scale.linear()
                .rangeRound([container.height, 0]);


            var yAxis = d3.svg.axis()
                .scale(yScale)
                .orient("left")
                .tickSize(0);

            // add the graph canvas to the body of the webpage
            var svg = d3.select(ele[0]).append("svg")
                .attr("width", d3.max([container.width + margin.left + margin.right, 300]))
                .attr("height", container.height + margin.top + margin.bottom)
                .on('click', function() {
                    var clicked = d3.select(d3.event.target).data()[0];
                    var uriToSend = getClickedURI(clicked);
                    if(uriToSend !== ""){
                        scope.$emit('registerClick', uriToSend);
                    }
                    else
                        scope.$emit('registerClick', "");
                })
                .on('dblclick', function() {
                    var clicked = d3.select(d3.event.target).data()[0];
                    var uriToSend = getClickedURI(clicked);
                    if(uriToSend !== ""){
                        scope.sendUri({uri:uriToSend});
                        scope.$apply();
                    }
                    else
                        console.log("NOT showing context menu as no edge or node was clicked");
                })
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
                .attr("width", container.width)
                .attr("height", container.height);

            function getClickedURI(clicked){
                var uriToSend = "";
                if(clicked){
                    if (clicked.x in nodes || clicked.x in edges) {
                        uriToSend = clicked.x;
                    }
                    else if (clicked in nodes || clicked in edges) {
                        uriToSend = clicked;
                    }
                }
                return uriToSend;
            }

            var layer = svg.selectAll(".layer");
            var rect = layer.selectAll(".bar");


            //x axis
            svg.append("svg:g")
                .attr("class", "xaxis")
                .attr("transform", "translate(0," + container.height + ")");
            //x axis text/label
            svg.selectAll("g.xaxis").append("text")
                .attr("class", "label")
                .attr("x", container.width/2)
                .attr("y", margin.bottom - 6)
                .style("text-anchor", "end")
                .text("");
            //x axis line
            svg.selectAll("g.xaxis").append("svg:line")
                .attr("class", "x-axis-line")
                .attr("x1", 0)
                .attr("y1", 0)
                .attr("x2", 0)
                .attr("y2", 0)
                .style("stroke", "black")
                .style("stroke-width", 1)
                .style("shape-rendering", "crispEdges");

            //y axis
            svg.append("g")
                .attr("class", "y axis");

            svg.selectAll("g.y.axis.line")
                .style("stroke", "black")
                .style("stroke-width", 1)
                .style("shape-rendering", "crispEdges");


            var legendGroup = svg.append("g")
                .attr("class", "legend");

            //where all the magic happens
            function update() {

                if(zScoreFlag){

                    threshold = d3.scale.threshold()
                        .domain(zScoreData);

// A position encoding for the key only.
                    thresholdxScale2 = d3.scale.linear()
                        .domain([zScoreData[0], zScoreData[zScoreData.length - 1]])
                        .range([0, container.width]);

                    thresholdxAxis2 = d3.svg.axis()
                        .scale(thresholdxScale2)
                        .orient("bottom")
                        .tickSize(5)
                        .tickValues(threshold.domain())
                        .tickFormat(function(d) { return formatNumber(d)});


                    thresholdg = svg.append("svg:g")
                        .attr("class", "key")
                        .attr("transform", "translate(0," + container.height + ")");
                    //x axis text/label
//                    svg.selectAll("g.key").append("text")
//                        .attr("class", "label")
//                        .attr("x", container.width / 2)
//                        .attr("y", margin.bottom - 6)
//                        .style("text-anchor", "end")
//                        .text("");
                    //x axis line
//                    svg.selectAll("g.key").append("svg:line")
//                        .attr("class", "x-axis-line")
//                        .attr("x1", 0)
//                        .attr("y1", 40)
//                        .attr("x2", 0)
//                        .attr("y2", 40)
//                        .style("stroke", "black")
//                        .style("stroke-width", 1)
//                        .style("shape-rendering", "crispEdges");

                    thresholdg.call(thresholdxAxis2).append("text")
                        .attr("class", "caption")
                        .attr("y", -6)
                }

                //reset the color scale
                color = d3.scale.category20();

                //establish colors
                setLayers(dataSeriesObject);

                //SET X DOMAIN
                xScale.domain(dataSeriesObject[0].map(function (d) {
                    return d.x;
                }));

                //UPDATE CURRENT MARGINS BASED ON LARGEST LABEL
                //y axis -- left margin
                var maxWidth = 0;
                svg.selectAll("text.foo")
                    .data(yScale.ticks())
                    .enter().append("text")
                    .text(function (d) {
                        return yScale.tickFormat()(d);
                    })
                    .each(function (d) {
                        maxWidth = Math.max(this.getBBox().width + yAxis.tickSize() + yAxis.tickPadding(), maxWidth);
                    })
                    .remove();
                currentLeftMargin = Math.max(margin.left + axisLabelPadding, maxWidth + axisLabelPadding)
                currentWidth = container.width - (currentLeftMargin - margin.left);
                svg.attr("transform", "translate(" + currentLeftMargin + "," + margin.top + ")");
                //update xScale based on new marginat
                xScale
                    .rangeRoundBands([0, currentWidth ], barPadding);

                //BUILD AXIS::::::
                // Truncate Labels
                for (i = 0; i < dataSeriesKeys.length; i++) {
                    var key = dataSeriesKeys[i];
                    var instanceName = $filter('shortenValueFilter')(key);
                    if (instanceName.length > 20) {
                        dataSeriesLabels[key] = (instanceName.substring(0, 20) + '...');
                    } else {
                        dataSeriesLabels[key] = instanceName;
                    }
                }

                //x axis title
                var xAxisTitle = svg.selectAll("g.xaxis text")
                    .data([xAxisText]);
                xAxisTitle
                    .text(function (d) {
                        return d
                    });
                xAxisTitle
                    .exit().remove();

                //x axis ticks
                var xAxis = svg.select("g.xaxis");
                var xTick = xAxis.selectAll("tick")
                    .data(dataSeriesKeys);
                xTick.enter().append("text");
                xTick.text(function (d) {
                    return dataSeriesLabels[d];
                })
                    .style("text-anchor", "end")
                    .attr("x", 0)
                    .attr("y", 15)
                    .style("font-size", getFontSize())
                    .attr("class", "tick")
                    .attr("transform", function (d, i) {
                        return "translate(" + (xScale(d) + (xScale.rangeBand()) / 2) + ", 0)rotate(-25)"
                    })
                    .attr("opacity", 0).transition().duration(axisTransitionDuration)
                    .attr("opacity", 1);
                xTick
                    .exit()
                    .attr("opacity", 1).transition().duration(axisTransitionDuration)
                    .attr("opacity", 0)
                    .remove();

                svg.selectAll(".x-axis-line")
                    .attr("x2", currentWidth);

                //y axis
                svg.selectAll("g.y.axis")
                    .transition()
                    .duration(axisTransitionDuration)
                    .call(yAxis);

                //y axis text
                svg.select("g.y.axis text").text(yAxisText);

//                drawLegend();

                //BUILD BARS:::::
                if (createCount === 0) {
                    buildBars();
                    createCount++;
                }
                else {
                    //delay bars
                    svg.selectAll(".bar")
                        .transition().duration(barsTransitionDuration)
                        .attr("y", container.height)
                        .attr("height", 0)
                        .each("end", buildBars);
                }

                function drawLegend(){
                    //draw legend
                    positionLegend();
                    var legendData = legendGroup.selectAll("g.legendEntry")
                        .data(legendArray, function(d) {
                            return color(d);
                        });

                    //enter legend g
                    var legendEnter = legendData.enter().append("g");
                    legendEnter
                        .attr("transform", function(d, i) {
                            return "translate(0 ," + i * 20 + ")";
                        })
                        .attr("class", "legendEntry");

                    // enter legend colored rectangles
                    legendEnter.append("rect");
                    legendEnter.append("text");

                    legendData.select("rect")
                        .attr("x",  - 13)
                        .attr("y", 0)
                        .attr("width", 13)
                        .attr("height", 13)
                        .attr("fill-opacity", standardBarOpacity)
                        .style("stroke", color)
                        .style("stroke-width", "1px")
                        .style("fill", color)
                        .on("click", function(d, i) {
                            var rect = d3.select(this);
                            var previouslyFiltered = filteredLevels.some(function(entry) {
                                return entry === i;
                            });
                            if(!previouslyFiltered) {
                                filteredLevels.push(i);
                                if(filteredLevels.length === dataSeriesObject.length) {
                                    filteredLevels.length = 0;
                                    legendData.selectAll("rect").style("fill",color);
                                }
                                else rect.style("fill", "white");
                            }
                            else {
                                filteredLevels.splice(filteredLevels.indexOf(i), 1);
                                rect.style("fill",color);
                            }
                            filterData(d);
                        });

                    // enter legend text
                    legendData.select("text")
                        .attr("x", - 24)
                        .attr("y", 7)
                        .attr("dy", ".25em")
                        .style("text-anchor", "end")
                        .text(function(d) {
                            return d;
                        });

                    legendData.exit().remove();
                }// end of drawLegend

                d3.selectAll("path.domain").style("shape-rendering", 'crispedges');

                resize();
            } // end of update

            function buildBars() {

                //this contains all generic bar building
                //specific building exists in buildStacked and buildGrouped
                layer = svg.selectAll(".layer")
                    .data(layers);
                rect = layer.selectAll(".bar")
                    .data(function (d) {
                        return d;
                    });
                rect
                    .enter().append("rect")
                    .attr("y", container.height)
                    .attr("height", 0);
                rect
                    .attr("class", "bar")
                    .attr("opacity", standardBarOpacity);

                if(stacked)
                    buildStacked(true);
                else
                    buildGrouped(true);


            } // end of buildBars

            function filterData(d){
                var newObject = [];
                for (var i = 0; i<dataSeriesObject.length; ++i){
                    var filtered = filteredLevels.some(function(entry) {
                        return entry === i;
                    });
                    if(!filtered) {
                        newObject.push(dataSeriesObject[i]);
                    }
                }
                setLayers(newObject);

                svg.select('g.y.axis')
                    .transition().duration(axisTransitionDuration)
                    .call(yAxis);

                buildBars();

            }

            function setLayers(data) {
                var stack = d3.layout.stack();
                layers = stack(data);

                yGroupMax = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return Number(d.y); }); });
                yStackMax = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return d.y0 + Number(d.y); }); });

                if(stacked)
                    yScale.domain([0, yStackMax]);
                else
                    yScale.domain([0, yGroupMax]);

                var layer = svg.selectAll(".layer")
                    .data(layers);
                layer
                    .enter().append("g");
                layer
                    .attr("class", "layer")
                    .style("fill", function (d) {
                        legendArray.push(d[0].seriesName);
                        return color(d[0].seriesName);
                    });
                layer
                    .exit()
                    .selectAll(".bar")
                    .transition().duration(barsTransitionDuration)
                    .delay(function (d, i) {
                        return i * 10;
                    })
                    .attr("y", container.height)
                    .attr("height", 0)
                    .remove();
            }

            function positionLegend(){
                legendGroup
                    .attr("transform", function(d, i) {
                        return "translate(" + currentWidth + ", 0)";
                    });
            }

            //this is called when initially building bars or resizing window
            //this animation is different than transition bars animation--thus different function
            function buildGrouped(transition) {
                if(transition) {
                    rect
                        .transition()
                        .delay(function (d, i) {
                            return i * 10;
                        })
                        .attr("x", function(d, i, j) {
                            return xScale(d.x) + xScale.rangeBand() / layers.length * j; })
                        .attr("width", xScale.rangeBand() / layers.length)
                        .attr("y", function(d) { return yScale(Number(d.y)); })
                        .attr("height", function(d) { return container.height - yScale(Number(d.y)); });
                }
                else {
                    rect
                        .attr("x", function(d, i, j) {
                            return xScale(d.x) + xScale.rangeBand() / layers.length * j; })
                        .attr("width", xScale.rangeBand() / layers.length - absoluteBarPadding)
                        .attr("y", function(d) { return yScale(Number(d.y)); })
                        .attr("height", function(d) { return container.height - yScale(Number(d.y)); });
                }
            }

            //this is called when the toggle between stacked and grouped is switched
            //this animation is different than build bars animation--thus different function
            scope.internalControl.transitionGrouped = function transitionGrouped() {
                stacked = false;
                yScale.domain([0, yGroupMax]);

                rect.transition()
                    .duration(500)
                    .delay(function(d, i) { return i * 10; })
                    .attr("x", function(d, i, j) {
                        return xScale(d.x) + xScale.rangeBand() / layers.length * j; })
                    .attr("width", xScale.rangeBand() / layers.length)
                    .transition()
                    .attr("y", function(d) { return yScale(Number(d.y)); })
                    .attr("height", function(d) { return container.height - yScale(Number(d.y)); });

                //y axis
                svg.selectAll("g.y.axis")
                    .transition()
                    .duration(axisTransitionDuration)
                    .call(yAxis);
            };

            //this is called when initially building bars or resizing window
            //this animation is different than transition bars animation--thus different function
            function buildStacked(transition) {
                rect
                    .attr("x", function (d) {
                        return xScale(d.x);
                    })
                    .attr("width", xScale.rangeBand() - absoluteBarPadding);
                if(transition) {
                    rect
                        .transition()
                        .duration(250)
                        .delay(function (d, i) {
                            return i * 10;
                        })
                        .attr("y", function (d) {
                            return yScale(d.y0 + Number(d.y));
                        })
                        .attr("height", function (d) {
                            return yScale(d.y0) - yScale(d.y0 + Number(d.y));
                        });
                }
                else {
                    rect
                        .attr("y", function (d) {
                            return yScale(d.y0 + Number(d.y));
                        })
                        .attr("height", function (d) {
                            return yScale(d.y0) - yScale(d.y0 + +d.y);
                        });
                }
            }

            //this is called when the toggle between stacked and grouped is switched
            //this animation is different than build bars animation--thus different function
            scope.internalControl.transitionStacked = function transitionStacked() {
                stacked = true;
                yScale.domain([0, yStackMax]);

                rect.transition()
                    .duration(500)
                    .delay(function(d, i) { return i * 10; })
                    .attr("y", function(d) { return yScale(d.y0 + Number(d.y)); })
                    .attr("height", function(d) { return yScale(d.y0) - yScale(d.y0 + Number(d.y)); })
                    .transition()
                    .attr("x", function(d) { return xScale(d.x); })
                    .attr("width", xScale.rangeBand() - absoluteBarPadding);

                //y axis
                svg.selectAll("g.y.axis")
                    .transition()
                    .duration(axisTransitionDuration)
                    .call(yAxis);
            };

            //font size is dynamically calculated based on bar width (available space)
            function getFontSize(){
                var font = Math.round(xScale.rangeBand() / 2);
                if(font >12) {
                    font = 12;
                }
                if(font <7) {
                    font = 7;
                }
                return font + "px";
            }

            //this is called when window is resized
            //adjust positioning of all items
            function resize() {
                //set container width and height
                containerSize(scope.containerClass, container, margin);

                currentWidth = d3.max([container.width - (currentLeftMargin - margin.left), 170]);

                d3.select(ele[0]).select("svg")
                    .attr("width", d3.max([container.width + margin.left + margin.right, 300]))
                    .attr("height", container.height + margin.top + margin.bottom);

                //update xScale based on new margin
                xScale
                    .rangeRoundBands([0, currentWidth ], barPadding);

                if(zScoreFlag) {
                    thresholdxScale2.range([0, currentWidth]);
                    thresholdxAxis2.scale(thresholdxScale2);
                    thresholdg.call(thresholdxAxis2);
                }

                //setup y
                yScale
                    .range([container.height, 0]);
                yGroupMax = d3.max(layers, function(lay) { return d3.max(lay, function(d) { return Number(d.y); }); });
                yStackMax = d3.max(layers, function(lay) { return d3.max(lay, function(d) { return d.y0 + Number(d.y); }); });

                updatePositioning();
            }

            //updates positioning based on new scales, height, and currentWidth
            function updatePositioning(){
                positionLegend();

                if(stacked) {
                    yScale.domain([0, yStackMax]);
                    buildStacked(false);
                }
                else {
                    yScale.domain([0, yGroupMax]);
                    buildGrouped(false);
                }

                svg.selectAll(".x-axis-line")
                    .attr("x2", currentWidth);

                ///loook over here
//                svg.selectAll(".x-axis-line")
//                    .attr("x2", currentWidth);


                if(zScoreFlag) {
                    var xAxis = svg.select("g.xaxis")
                        .attr("transform", "translate(0," + container.height + ")");
                    xAxis.selectAll(".tick")
                        .style("font-size", getFontSize())
                        .attr("transform", function(d, i) { return "translate(" + (xScale(d) + (xScale.rangeBand()) / 2) + ", 15)rotate(-25)" });
                    xAxis.select(".label")
                        .attr("x", currentWidth/2);

                }else{
                    var xAxis = svg.select("g.xaxis")
                        .attr("transform", "translate(0," + container.height + ")");
                    xAxis.selectAll(".tick")
                        .style("font-size", getFontSize())
                        .attr("transform", function(d, i) { return "translate(" + (xScale(d) + (xScale.rangeBand()) / 2) + ", 0)rotate(-25)" });
                    xAxis.select(".label")
                        .attr("x", currentWidth/2);

                }








                yAxis
                    .ticks(Math.max(container.height/30, 2));
                svg.select('g.y.axis')
                    .call(yAxis);
            }

            //highlights column based on x-axis name
            function highlightColumn(xAxisName){
                if(xAxisName !== "") {
                    rect
                        .on('mouseover', function(d) {
//                            tip.show(d);
                        })
                        .on('mouseout', function(d) {
//                            tip.hide(d);
                        })
                        .transition()
                        .duration(500)
                        .attr("opacity", function (d) {
                            if (d.x === xAxisName) {
                                return 1;
                            }
                            else
                                return .3
                        });
                }
                else {
                    rect
                        .on('mouseover', function(d) {
                            var bar = d3.select(this);
                            bar.transition()
                                .duration(100).attr("opacity", 1);
//                            tip.show(d);
                        })
                        .on('mouseout', function(d) {
                            var bar = d3.select(this);
                            bar.transition()
                                .duration(400).attr("opacity", standardBarOpacity);
//                            tip.hide(d);
                        })
                        .attr("opacity", standardBarOpacity);
                }
            }
            resize();
            scope.$watch('barChartResized', function() {
                    if(barChartSignal != scope.barChartResized){
                        barChartSignal = scope.barChartResized;
                        resize();
                    }
            });

        }
    }
});