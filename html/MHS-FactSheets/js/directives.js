'use strict';

var directives = angular.module('app.directives', [])

directives.directive('d3Pie', function() {
    return {
        restrict: 'A',
        scope: {
            data: '=',
            d3Width: '=',
            d3Height: '=',
            onClick: '&'
        },
        link: function(scope, ele, attrs) {
            var dataString = {};
            scope.$watch('data', function() {
                if (!(scope.data == undefined || scope.data == null || scope.data == {} || scope.data == '')) {
                    dataString = {};
                    dataString = scope.data;
                    update(dataString);
                }
            }, true);
                
            function update(dataString) {
                var data = [{"label": "Provide", "value":dataString.DATA_COUNT_QUERY[2]}, {"label": "Consume", "value": dataString.DATA_COUNT_QUERY[1]}];
                
                var width = 400,
                    height = 353,
                    radius = Math.min(width, height) / 2;
                 
                
                
                
                //var color = d3.scale.category20();
                var color = function(iterator) {
                    if (iterator === 0) {
                        return ("#FD8D3C")
                    } else {
                        return ("#E31A1C");
                    }
                }
                
                
                var pie = d3.layout.pie()
                  .value(function(d){return d.value})
                  .sort(null);
                
                var arc = d3.svg.arc()
                  .innerRadius(0)
                  .outerRadius(100);
                
                var svg = d3.select(ele[0]).append("svg")
                  .attr("width", width)
                  .attr("height", height)
                  .append("g")
                  .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
              
                  
                 var g = svg.selectAll(".arc")
                  .data(pie(data))
                  .enter().append("g")
                  .attr("x", 50)
                  .attr("y", 100)
                  .attr("class", "arc");
            
                  g.append("path")
                    .attr("d", arc)
                    .attr("fill", function(d, i) { return color(i); });
            
                  g.append("text")
                    .attr("transform", function(d) { return "translate(" + arc.centroid(d) + ")"; })
                    .attr("dy", ".35em")
                    .style("text-anchor", "middle")
                    .text(function(d) {
                        if(d.value>=1){
                            return d.value
                        } else {
                            return "";
                        }
                        
                        })
                    .attr("fill", "white");
                    
                // add legend   
                var legend = svg.append("g")
                  .attr("class", "legend")
                  .attr("height", 100)
                  .attr("width", 100)
                  .attr('transform', 'translate(-20,50)')    
                  
                
                legend.selectAll('rect')
                  .data(data)
                  .enter()
                  .append("rect")
                  .attr("x", -120)
                  .attr("y", function(d, i){return i * 20 - 210})
                  .attr("width", 10)
                  .attr("height", 10)
                  .style("fill", function(d,i) { 
                    return color(i);
                  })
                  
                legend.selectAll('text')
                  .data(data)
                  .enter()
                  .append("text")
                  .attr("x", -100)
                  .attr("y", function(d, i){ return i *  20 - 200;})
                  .text(function(d) {
                    return d.label;
                  });
           

            }
        }
    }
    
    
    
});

directives.directive('d3Heatmap', function() {
    return {
        restrict: 'A',
        scope: {
            data: '=',
            d3Width: '=',
            d3Height: '=',
            onClick: '&'
        },
        link: function(scope, ele, attrs) {
            //put heatmap code here
            //var jsonData = jQuery.parseJSON(dataString);
            //var data = jsonData.dataSeries
            var dataString = {};
            
            scope.$watch('data', function() {
                if (!(scope.data == undefined || scope.data == null || scope.data == {} || scope.data == '')) {
                    dataString = {};
                    dataString = scope.data;
                    update(dataString);
                }
            }, true);
            
            function update(dataString) {
                
                var data = dataString.dataSeries;
                var xAxisName = dataString.xAxisTitle;
                var yAxisName = dataString.yAxisTitle;
                var value = dataString.value;
                
                var xAxisArray = [];
                var yAxisArray = [];
                var dataArray = [];
                var truncY = [];
                var truncX = [];
                var domainArray = [];
                
                for (var key in data) {
                    xAxisArray.push(data[key][xAxisName]);
                    yAxisArray.push(data[key][yAxisName]);
                    var round = (Math.round(data[key][value] * 100) / 100);
                    //This array stores the values as numbers
                    dataArray.push({yAxis: data[key][yAxisName], Value: round, xAxis: data[key][xAxisName], xAxisName: data[key][xAxisName], yAxisName: data[key][yAxisName]});
                };
                  
                var uniqueX = _.uniq(xAxisArray);
                var uniqueY = _.uniq(yAxisArray);
                xAxisArray = uniqueX.sort();
                yAxisArray = uniqueY.sort();
                  
                /* Assign each name a number and place matrix coordinates inside of dataArray */
                for (var i = 0; i<dataArray.length;i++) {
                    for (var j = 0; j<xAxisArray.length; j++) {
                        if (xAxisArray[j] == dataArray[i].xAxis) {
                            dataArray[i].xAxis = j;
                        }
                    }
                                            
                    for (var j = 0; j<yAxisArray.length; j++) {
                        if (yAxisArray[j] == dataArray[i].yAxis) {
                            dataArray[i].yAxis = j;
                        }
                    }
                };
                  
                /* Truncate Values */
                for (i = 0; i < yAxisArray.length; i++) {
                    if (yAxisArray[i].length > 20) {
                      truncY.push(yAxisArray[i].substring(0, 20) + '...');
                    } else {
                      truncY.push(yAxisArray[i]);
                    }
                }
                  
                for (i = 0; i < xAxisArray.length; i++) {
                    if (xAxisArray[i].length > 30) {
                      truncX.push(xAxisArray[i].substring(0, 30) + '...');
                    } else {
                      truncX.push(xAxisArray[i]);
                    }
                }
                  
                
                var margin = { top: 200, right: 150, bottom: 100, left: 200 },
                    xAxisData = xAxisArray,
                    yAxisData = yAxisArray,
                    gridSize = 15;
                
                var width = xAxisData.length * gridSize,
                    height = yAxisData.length * gridSize,
                    legendElementWidth = 60,
                    buckets = 9,
                    colors = ["#FFFFCC","#FFEDA0","#FED976","#FEB24C","#FD8D3C","#FC4E2A","#E31A1C","#BD0026","#800026"];
                
                if (xAxisData.length < 35) {
                    legendElementWidth = 40
                    if (xAxisData.length < 25) {
                        legendElementWidth = 25
                        if (xAxisData.length < 15) {
                          legendElementWidth = 15
                        }
                    }
                }
               
                var colorScale = d3.scale.quantile()
                    .domain([ 0, buckets - 1, d3.max(dataArray, function (d) { return d.Value; })])
                    .range(colors);

                var svg = d3.select(ele[0]).append("svg")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                    .append("g")
                    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                
                var yAxis = svg.selectAll(".yAxis")
                    .data(truncY)
                    .enter().append("text")
                    .text(function (d) { return d; })
                    .attr("x", 0)
                    .attr("y", function (d, i) { return i * gridSize; })
                    .style("text-anchor", "end")
                    .attr("transform", "translate(-6," + gridSize / 1.5 + ")")
                    .attr("class", "yAxis");
                                                
                                
                var xAxis = svg.selectAll(".xAxis")
                    .data(truncX)
                    .enter().append("svg:g");
                                
                xAxis.append("text")
                    .text(function(d) { return d; })
                    .style("text-anchor", "start")
                    .attr("x", 6)
                    .attr("y", 7)
                    .attr("class", "xAxis")
                    .attr("transform", function(d, i) { return "translate(" + i * gridSize + ", -6)rotate(-45)" });
                                
                /* Initialize tooltip */
                //var tip = d3.tip()
                //.attr('class', 'd3-tip')
                //.html(function(d) { return "<div> <span class='light'>" + value + "</span> " + d.Value + "</div>" + "<div><span class='light'>" + xAxisName + ":</span> " + d.xAxisName + "</div>" + "<div> <span class='light'>" + yAxisName + ": </span>" + d.yAxisName + "</div>"; })
                                
                var heatMap = svg.selectAll(".heat")
                    .data(dataArray);
                                
                heatMap
                    .enter().append("rect");
                
                heatMap
                    .attr("x", function(d) { return (d.xAxis) * gridSize; })
                    .attr("y", function(d) { return (d.yAxis) * gridSize; })
                    .attr("rx", 2)
                    .attr("ry", 2)
                    .attr("class", "heat bordered")
                    .attr("width", gridSize)
                    .attr("height", gridSize)
                    .style("fill", colors[0]);
                    //.on('mouseover', tip.show)
                    //.on('mouseout', tip.hide);
                                
                heatMap
                    .transition()
                    .duration(1000)
                    .style("fill", function(d) { return colorScale(d.Value); });
                                
                /* Invoke the tooltip in the context of your visualization */
                //heatMap.call(tip);
                
                //vertical lines
                svg.selectAll(".vline").data(d3.range(xAxisData.length + 1)).enter()
                    .append("line")
                    .attr("x1", function (d) {
                        return d * gridSize;
                    })
                    .attr("x2", function (d) {
                        return d * gridSize;
                    })
                    .attr("y1", function (d) {
                        return 0;
                    })
                    .attr("y2", function (d) {
                        return height;
                    })
                    .style("stroke", "#eee");
                                
                // horizontal lines
                svg.selectAll(".hline").data(d3.range(yAxisData.length + 1)).enter()
                    .append("line")
                    .attr("y1", function (d) {
                        return d * gridSize;
                    })
                    .attr("y2", function (d) {
                        return d * gridSize;
                    })
                    .attr("x1", function (d) {
                        return 0;
                    })
                    .attr("x2", function (d) {
                        return width;
                    })
                    .style("stroke", "#eee");


                //heatMap.append("title").text(function(d) { return d.Value; });
                                                                              
                var legend = svg.selectAll(".legend")
                    .data([0].concat(colorScale.quantiles()), function(d) { return d; })
                    .enter().append("g")
                    .attr("class", "legend");

                legend.append("rect")
                    .attr("x", function(d, i) { return legendElementWidth * i; })
                    .attr("y", yAxisData.length * gridSize + 40)
                    .attr("width", legendElementWidth)
                    .attr("height", 20);
                                                                            
                legend.style("fill", function(d, i) { return colors[i]; });

                legend.append("text")
                    .attr("class", "mono")
                    .text(function(d) { return "" + Math.round(d); })
                    .attr("x", function(d, i) { return legendElementWidth * i; })
                    .attr("y", yAxisData.length * gridSize + 75);
            }
        }
    }
});
