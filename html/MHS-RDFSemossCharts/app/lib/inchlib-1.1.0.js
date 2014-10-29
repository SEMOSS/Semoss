/**
* InCHlib is an interactive JavaScript library which facilitates data
* visualization and exploration by means of a cluster heatmap. InCHlib
* is a versatile tool, and its use is not limited only to chemical or
* biological data. Source code, tutorial, documentation, and example
* data are freely available from InCHlib website <a
* href="http://openscreen.cz/software/inchlib"
* target=blank>http://openscreen.cz/software/inchlib</a>. At the
* website, you can also find a Python script <a
* href="http://openscreen.cz/software/inchlib/inchlib_clust"
* target=blank>inchlib_clust</a> which performs data clustering and
* prepares <a href="http://openscreen.cz/software/inchlib/input_format"
* target=blank>input data for InCHlib</a>.
* 
* @author <a href="mailto:ctibor.skuta@img.cas.cz">Ctibor Ĺ kuta</a>
* @author <a href="mailto:petr.bartunek@img.cas.cz">Petr BartĹŻnÄ›k</a>
* @author <a href="mailto:svozild@vscht.cz">Daniel Svozil</a>
* @version 1.1.0
* @category 1
* @license InCHlib - Interactive Cluster Heatmap Library http://openscreen.cz/software/inchlib Copyright 2014, Ctibor Ĺ kuta, Petr BartĹŻnÄ›k, Daniel Svozil Licensed under the MIT license.
* 
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
* 
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
* OTHER DEALINGS IN THE SOFTWARE.
* 
* @requires <a href='http://code.jquery.com/jquery-2.0.3.min.js'>jQuery Core 2.0.3</a>
* @dependency <script language="JavaScript" type="text/javascript" src="http://code.jquery.com/jquery-2.0.3.min.js"></script>
* 
* @requires <a href='http://kineticjs.com/'>KineticJS 5.1.0</a>
* @dependency <script language="JavaScript" type="text/javascript" src="http://openscreen.cz/software/inchlib/static/js/kinetic-v5.1.0.min.js"></script>
*
* @param {Object} options An object with the options for the InCHlib component.
*
* @option {string} target
*   identifier of the DIV tag where the component should be displayed

* @option {boolean} [column_dendrogram=false]
*   turn on/off the column dendrogram

* @option {boolean} [count_column=false]
*   turn on/off the count column

* @option {boolean} [dendrogram=true]
*   turn on/off the row dendrogram

* @option {string} [font="Trebuchet&nbsp;MS"]
*   font family

* @option {string} [heatmap_colors="Greens"]
*   the heatmap color scale

* @option {number} [heatmap_part_width=0.7]
*   define the heatmap part width from the width of the whole graph

* @option {string} [highlight_colors="Reds"]
*   color scale for highlighted rows

* @option {obejct} [highlighted_rows=[]]
*   array of row IDs to highlight

* @option {boolean} [independent_columns=true]
*   determines whether the color scale is based on the values from all columns together or for each column separately

* @option {string} [label_color=grey]
*   color of column label

* @option {number} [max_column_width=100]
*   maximum column width in pixels

* @option {number} [max_height=800]
*   maximum graph height in pixels

* @option {number} [max_row_height=25]
*   maximum row height in pixels

* @option {boolean} [metadata=false]
*   turn on/off the metadata

* @option {string} [metadata_colors="Oranges"]
*   the metadata color scale

* @option {number} [min_row_height=false]
*   minimum row height in pixels

* @option {number} [width="the width of target DIV"]
*   width of the graph in pixels

* @option {boolean} [heatmap=true]
*   turn on/off the heatmap

* @option {string} [heatmap_font_color="black"]
*   the color of the text values in the heatmap

* @option {string} [count_column_colors="Reds"]
*   the color scale of count column

* @option {boolean} [draw_row_ids=true]
*   draws the row IDs next to the heatmap when there is enough space to visualize them

* @option {number} [max_percentile=100]
*   the value percentile above which the color will be equal to the terminal color of the color scale

* @option {number} [min_percentile=0]
*   the value percentile below which the color will be equal to the beginning color of the color scale

* @option {number} [middle_percentile=50]
*   the value percentile which defines where the middle color of the color scale will be used

* 
* @example
*       window.instance = new InCHlib({
*                target : "YourOwnDivId",
*                metadata: true, 
*                max_height: 800,
*                width: 700,
*                metadata_colors: "RdLrBu"
*            });
*       instance.read_data_from_file("../biojs/data/chembl_gr.json");
*       instance.draw();
*/

var InCHlib, _this;
var _date = new Date();

(function($){
  InCHlib = function(settings){
      _this = this;
      _this.user_settings = settings;
      _this.target_element = $("#" + settings.target);
      var target_width = _this.target_element.width();
      _this.target_element.css({"position": "relative"});

      /**
      * Default values for the settings
      * @name InCHlib#settings
      */
      
      _this.settings = {
          "target" : "YourOwnDivId",
          "heatmap" : true,
          "heatmap_header": true,
          "dendrogram": true,
          "metadata": false,
          "column_metadata": false,
          "column_metadata_row_height": 8,
          "column_metadata_colors": "GnBu",
          "max_height" : 800,
          "width" : target_width,
          "heatmap_colors" : "Greens",
          "heatmap_font_color" : "black",
          "heatmap_part_width" : 0.7,
          "column_dendrogram" : false,
          "independent_columns" : true,
          "metadata_colors" : "Reds",
          "highlight_colors" : "Oranges",
          "highlighted_rows" : [],
          "label_color": "#9E9E9E",
          "count_column": false,
          "count_column_colors": "Reds",
          "min_row_height": false,
          "max_row_height": 25,
          "max_column_width": 150,
          "font": "Helvetica",
          "draw_row_ids": true,
          "show_export_button": true,
          "max_percentile": 100,
          "min_percentile": 0,
          "middle_percentile": 50,
      };

      $.extend(_this.settings, settings);
      _this.settings.width = (settings.max_width && settings.max_width < target_width)?settings.max_width:_this.settings.width;
      _this.settings.heatmap_part_width = (_this.settings.heatmap_part_width>0.9)?0.9:_this.settings.heatmap_part_width;

      _this.header_height = 150;
      _this.footer_height = 70;
      _this.dendrogram_heatmap_distance = 5;

      /**
      * Default function definitions for the InCHlib events
      * @name InCHlib#events
      */
      _this.events = {
          /**
            * @name InCHlib#row_onclick
            * @event
            * @param {function} function() callback function for click on the heatmap row event
            * @eventData {array} array array of object IDs represented by row
            * @eventData {object} event event object

            * @example 
            * instance.events.row_onclick = (
            *    function(object_ids, evt) {
            *       alert(object_ids);
            *    }
            * ); 
            * 
            */
          "row_onclick": function(object_ids, evt){
              return;
          },

          /**
            * @name InCHlib#row_onmouseover
            * @event
            * @param {function} function() callback function for mouse cursor over the heatmap row event
            * @eventData {array} array array of object IDs represented by row
            * @eventData {object} event event object

            * @example 
            * instance.events.row_onmouseover = (
            *    function(object_ids, evt) {
            *       alert(object_ids);
            *    }
            * ); 
            * 
            */
          "row_onmouseover": function(object_ids, evt){
              return;
          },

          /**
            * @name InCHlib#row_onmouseout
            * @event
            * @param {function} function() callback function for mouse cursor out of the heatmap row event
            * @eventData {object} event event object

            * @example 
            * instance.events.row_onmouseout = (
            *    function(evt) {
            *       alert("now");
            *    }
            * ); 
            * 
            */
          "row_onmouseout": function(evt){
              return;
          },

          /**
            * @name InCHlib#dendrogram_node_onclick
            * @event
            * @param {function} function() callback function for dendrogram node click event
            * @eventData {array} array array of object IDs represented by the node
            * @eventData {string} node_id Id of the dendrogram node
            * @eventData {object} event event object

            * @example 
            * instance.events.dendrogram_node_onclick = (
            *    function(object_ids, node_id, evt) {
            *    alert(node_id + ": " + object_ids.length+" rows");
            *    }
            * ); 
            * 
            */
          "dendrogram_node_onclick": function(object_ids, node_id, evt){
              return;
          },

          /**
            * @name InCHlib#column_dendrogram_node_onclick
            * @event
            * @param {function} function() callback function for column dendrogram click event
            * @eventData {array} array array of column indexes
            * @eventData {string} node_id Id of the dendrogram node
            * @eventData {object} event event object

            * @example 
            * instance.events.column_dendrogram_node_onclick = (
            *    function(column_ids, node_id, evt) {
            *    alert(node_id + ": " + column_ids.length+" columns");
            *    }
            * ); 
            * 
            */
          "column_dendrogram_node_onclick": function(column_indexes, node_id, evt){
              return;
          },

          /**
            * @name InCHlib#dendrogram_node_highlight
            * @event
            * @param {function} function() callback function for the dendrogram node highlight event
            * @eventData {array} array array of object IDs represented by row
            * @eventData {string} node_id Id of the dendrogram node
            * @eventData {object} event event object

            * @example 
            * instance.events.dendrogram_node_highlight = (
            *    function(object_ids, node_id, evt) {
            *       alert(node_id + ": " + object_ids.length+" rows");
            *    }
            * ); 
            * 
            */
          "dendrogram_node_highlight": function(object_ids, node_id){
              return;
          },

          /**
            * @name InCHlib#column_dendrogram_node_highlight
            * @event
            * @param {function} function() callback function for the column dendrogram node highlight event
            * @eventData {array} array array of column indexes
            * @eventData {string} node_id Id of the dendrogram node
            * @eventData {object} event event object

            * @example 
            * instance.events.column_dendrogram_node_highlight = (
            *    function(object_ids, node_id, evt) {
            *       alert(node_id + ": " + object_ids.length+" columns");
            *    }
            * ); 
            * 
            */
          "column_dendrogram_node_highlight": function(column_indexes, node_id){
              return;
          },

          /**
            * @name InCHlib#dendrogram_node_unhighlight
            * @event
            * @param {function} function() callback function for the dendrogram node unhighlight event
            * @eventData {string} node_id Id of the dendrogram node

            * @example 
            * instance.events.dendrogram_node_unhighlight = (
            *    function(node_id) {
            *       alert(node_id);
            *    }
            * ); 
            * 
            */
          "dendrogram_node_unhighlight": function(node_id){
              return;
          },

          /**
            * @name InCHlib#column_dendrogram_node_unhighlight
            * @event
            * @param {function} function() callback function for the column dendrogram node unhighlight event
            * @eventData {string} node_id Id of the column dendrogram node

            * @example 
            * instance.events.column_dendrogram_node_unhighlight = (
            *    function(node_id) {
            *       alert(node_id);
            *    }
            * ); 
            * 
            */
            "column_dendrogram_node_unhighlight": function(node_id){
                return;
            },

          /**
            * @name InCHlib#heatmap_onmouseout
            * @event
            * @param {function} function() callback function for mouse cursor out of hte heatmap area
            * @eventData {object} event event object

            * @example 
            * instance.events.heatmap_onmouseout = (
            *    function(evt) {
            *       alert("now");
            *    }
            * ); 
            * 
            */
          "heatmap_onmouseout": function(evt){
              return;
          },

          /**
            * @name InCHlib#on_zoom
            * @event
            * @param {function} function() callback function for zoom event
            * @eventData {string} node_id Id of the dendrogram node

            * @example 
            * instance.events.on_zoom = (
            *    function(node_id) {
            *       alert(node_id);
            *    }
            * ); 
            * 
            */
          "on_zoom": function(object_ids, node_id){
              return;
          },

          /**
            * @name InCHlib#on_unzoom
            * @event
            * @param {function} function() callback function for unzoom event
            * @eventData {string} node_id Id of the dendrogram node

            * @example 
            * instance.events.on_unzoom = (
            *    function(node_id) {
            *       alert(node_id);
            *    }
            * ); 
            * 
            */
          "on_unzoom": function(node_id){
              return;
          },

          /**
            * @name InCHlib#on_columns_zoom
            * @event
            * @param {function} function() callback function for columns zoom event
            * @eventData {array} array array of column indexes
            * @eventData {string} node_id Id of the column dendrogram node

            * @example 
            * instance.events.on_columns_zoom = (
            *    function(column_indexes, node_id) {
            *       alert(column_indexes, node_id);
            *    }
            * ); 
            * 
            */
          "on_columns_zoom": function(column_indexes, node_id){
              return;
          },

          /**
            * @name InCHlib#on_columns_unzoom
            * @event
            * @param {function} function() callback function for columns unzoom event
            * @eventData {string} node_id Id of the column dendrogram node

            * @example 
            * instance.events.on_columns_unzoom = (
            *    function(node_id) {
            *       alert(node_id);
            *    }
            * ); 
            * 
            */
          "on_columns_unzoom": function(node_id){
              return;
          },

          /**
            * @name InCHlib#on_refresh
            * @event
            * @param {function} function() callback function for refresh icon click event
            * @eventData {object} event event object
            * @example 
            * instance.events.on_refresh = (
            *    function() {
            *       alert("now");
            *    }
            * ); 
            * 
            */
          "on_refresh": function(){
              return;
          },

          /**
            * @name InCHlib#empty_space_onclick
            * @event
            * @param {function} function() callback function for click on empty(inactive) space in the visualization (e.g., around the heatmap)
            * @eventData {object} event event object

            * @example 
            * instance.events.empty_space_onclick = (
            *    function(evt) {
            *       alert("now");
            *    }
            * ); 
            * 
            */
          "empty_space_onclick": function(evt){
              return;
          }

      }

      /**
      * Default color scales
      * @name InCHlib#colors
      */
      _this.colors = {
              "YlGn": {"start": {"r":255, "g": 255, "b": 204}, "end": {"r": 35, "g": 132, "b": 67}},
              "GnBu": {"start": {"r":240, "g": 249, "b": 232}, "end": {"r": 43, "g": 140, "b": 190}},
              "BuGn": {"start": {"r":237, "g": 248, "b": 251}, "end": {"r": 35, "g": 139, "b": 69}},
              "PuBu": {"start": {"r":241, "g": 238, "b": 246}, "end": {"r": 5, "g": 112, "b": 176}},
              "BuPu": {"start": {"r":237, "g": 248, "b": 251}, "end": {"r": 136, "g": 65, "b": 157}},
              "RdPu": {"start": {"r":254, "g": 235, "b": 226}, "end": {"r": 174, "g": 1, "b": 126}},
              "PuRd": {"start": {"r":241, "g": 238, "b": 246}, "end": {"r": 206, "g": 18, "b": 86}},
              "OrRd": {"start": {"r":254, "g": 240, "b": 217}, "end": {"r": 215, "g": 48, "b": 31}},
              "Purples2": {"start": {"r":242, "g": 240, "b": 247}, "end": {"r": 106, "g": 81, "b": 163}},
              "Blues": {"start": {"r":239, "g": 243, "b": 255}, "end": {"r": 33, "g": 113, "b": 181}},
              "Greens": {"start": {"r":237, "g": 248, "b": 233}, "end": {"r": 35, "g": 139, "b": 69}},
              "Oranges": {"start": {"r":254, "g": 237, "b": 222}, "end": {"r": 217, "g": 71, "b": 1}},
              "Reds": {"start": {"r":254, "g": 229, "b": 217}, "end": {"r": 203, "g": 24, "b": 29}},
              "Greys": {"start": {"r":247, "g": 247, "b": 247}, "end": {"r": 82, "g": 82, "b": 82}},
              "PuOr": {"start": {"r":230, "g": 97, "b": 1}, "end": {"r": 94, "g": 60, "b": 153}},
              "BrBG": {"start": {"r":166, "g": 97, "b": 26}, "end": {"r": 1, "g": 133, "b": 113}},
              "RdBu": {"start": {"r":202, "g": 0, "b": 32}, "end": {"r": 5, "g": 113, "b": 176}},
              "RdGy": {"start": {"r":202, "g": 0, "b": 32}, "end": {"r": 64, "g": 64, "b": 64}},
              "BuYl": {"start": {"r": 5, "g": 113, "b": 176}, "end": {"r": 250, "g": 233, "b": 42}},
              "YlOrR": {"start": {"r":255, "g": 255, "b": 178}, "end": {"r": 227, "g": 26, "b": 28}, "middle": {"r": 204, "g": 76, "b": 2}},
              "YlOrB": {"start": {"r":255, "g": 255, "b": 212}, "end": {"r": 5, "g": 112, "b": 176}, "middle": {"r": 204, "g": 76, "b": 2}},
              "PRGn2": {"start": {"r":123, "g": 50, "b": 148}, "end": {"r": 0, "g": 136, "b": 55}, "middle": {"r":202, "g": 0, "b": 32}},
              "PiYG2": {"start": {"r":208, "g": 28, "b": 139}, "end": {"r": 77, "g": 172, "b": 38}, "middle": {"r":255, "g": 255, "b": 178},},
              "YlGnBu": {"start": {"r":255, "g": 255, "b": 204}, "end": {"r": 34, "g": 94, "b": 168}, "middle": {"r": 35, "g": 132, "b": 67}},
              "RdYlBu": {"start": {"r":215, "g": 25, "b": 28}, "end": {"r": 44, "g": 123, "b": 182}, "middle": {"r":255, "g": 255, "b": 178}},
              "RdYlGn": {"start": {"r":215, "g": 25, "b": 28}, "end": {"r": 26, "g": 150, "b": 65}, "middle": {"r":255, "g": 255, "b": 178}},
              "BuWhRd": {"start": {"r": 33, "g": 113, "b": 181}, "middle": {"r": 255, "g": 255, "b": 255}, "end": {"r":215, "g": 25, "b": 28}},
              "RdLrBu": {"start": {"r":215, "g": 25, "b": 28}, "middle": {"r":254, "g": 229, "b": 217}, "end": {"r": 44, "g": 123, "b": 182}},
              "RdBkGr": {"start": {"r":215, "g": 25, "b": 28}, "middle": {"r": 0, "g": 0, "b": 0}, "end": {"r": 35, "g": 139, "b": 69}},
              "RdLrGr": {"start": {"r":215, "g": 25, "b": 28}, "middle": {"r":254, "g": 229, "b": 217}, "end": {"r": 35, "g": 139, "b": 69}},
      };

      /**
      * Default kineticjs objects references
      * @name InCHlib#objects_ref
      */
      _this.objects_ref = {
          "tooltip_label": new Kinetic.Label({
                              opacity: 1,
                              listening: false,
                           }),

          "tooltip_tag": new Kinetic.Tag({
                              fill: _this.settings.label_color,
                              pointerWidth: 10,
                              pointerHeight: 10,
                              lineJoin: 'round',
                              listening: false,
                          }),
      
          "tooltip_text": new Kinetic.Text({
                              fontFamily: _this.settings.font,
                              fontSize: 12,
                              padding: 8,
                              fill: 'white',
                              fontStyle: "bold",
                              listening: false,
                              align: "center",
                              lineHeight: 1.2,
                          }),

          "node": new Kinetic.Line({
                              stroke: "grey",
                              strokeWidth: 2,
                              lineCap: 'sqare',
                              lineJoin: 'round',
                              listening: false
                          }),

          "node_rect" : new Kinetic.Rect({
                              fill: "white",
                              opacity: 0,
                          }),

          "icon_overlay": new Kinetic.Rect({
                              width: 32,
                              height: 32,
                              opacity: 0,
                          }),

          "heatmap_value": new Kinetic.Text({
                              fontFamily: _this.settings.font,
                              fill: _this.settings.heatmap_font_color,
                              fontStyle: "bold",
                              listening: false,
                          }),

          "heatmap_line": new Kinetic.Line({
                             lineCap: 'butt',
                             value: false,
                          }),

          "column_header": new Kinetic.Text({
                              fontFamily: _this.settings.font,
                              fontStyle: "bold",
                              fill: 'black',
                           }),

          "count": new Kinetic.Text({
                          fontSize: 10,
                          fill: "#6d6b6a",
                          fontFamily: _this.settings.font,
                          fontStyle: 'bold',
                          listening: false,
                       }),

          "cluster_overlay": new Kinetic.Rect({
                                  fill: "white",
                                  opacity: 0.5,
                              }),

          "cluster_border": new Kinetic.Line({
                                  stroke: "black",
                                  strokeWidth: 1,
                                  dash: [6,2]
                              }),

          "icon": new Kinetic.Path({
                      fill: "grey",
                  }),

          "rect_gradient": new Kinetic.Rect({
                              x: 0,
                              y: 80,
                              width: 100,
                              height: 20,
                              fillLinearGradientStartPoint: {x: 0, y: 80},
                              fillLinearGradientEndPoint: {x: 100, y: 80},
                              stroke: "#D2D2D2",
                              strokeWidth: "1px"
                          }),
      };

      _this.paths_ref = {
            "zoom_icon": "M22.646,19.307c0.96-1.583,1.523-3.435,1.524-5.421C24.169,8.093,19.478,3.401,13.688,3.399C7.897,3.401,3.204,8.093,3.204,13.885c0,5.789,4.693,10.481,10.484,10.481c1.987,0,3.839-0.563,5.422-1.523l7.128,7.127l3.535-3.537L22.646,19.307zM13.688,20.369c-3.582-0.008-6.478-2.904-6.484-6.484c0.006-3.582,2.903-6.478,6.484-6.486c3.579,0.008,6.478,2.904,6.484,6.486C20.165,17.465,17.267,20.361,13.688,20.369zM15.687,9.051h-4v2.833H8.854v4.001h2.833v2.833h4v-2.834h2.832v-3.999h-2.833V9.051z",
            "unzoom_icon": "M22.646,19.307c0.96-1.583,1.523-3.435,1.524-5.421C24.169,8.093,19.478,3.401,13.688,3.399C7.897,3.401,3.204,8.093,3.204,13.885c0,5.789,4.693,10.481,10.484,10.481c1.987,0,3.839-0.563,5.422-1.523l7.128,7.127l3.535-3.537L22.646,19.307zM13.688,20.369c-3.582-0.008-6.478-2.904-6.484-6.484c0.006-3.582,2.903-6.478,6.484-6.486c3.579,0.008,6.478,2.904,6.484,6.486C20.165,17.465,17.267,20.361,13.688,20.369zM8.854,11.884v4.001l9.665-0.001v-3.999L8.854,11.884z",
            "lightbulb": "M15.5,2.833c-3.866,0-7,3.134-7,7c0,3.859,3.945,4.937,4.223,9.499h5.553c0.278-4.562,4.224-5.639,4.224-9.499C22.5,5.968,19.366,2.833,15.5,2.833zM15.5,28.166c1.894,0,2.483-1.027,2.667-1.666h-5.334C13.017,27.139,13.606,28.166,15.5,28.166zM12.75,25.498h5.5v-5.164h-5.5V25.498z"
      };
      
  }

  InCHlib.prototype._update_user_settings = function(settings){
    var updated_settings = {}, key;
    for(var i = 0, keys=Object.keys(settings), len = keys.length; i < len; i++){
      key = keys[i];
      if(_this.user_settings[key] !== undefined && _this.user_settings[key] !== settings[key] && _this.user_settings[key] === true){
        updated_settings[key] = false;
      }
      else if(_this.user_settings[key] === undefined){
        updated_settings[key] = settings[key];
      }
    }
    $.extend(_this.settings, updated_settings);
  }

  /**
    * Read data from JSON variable.
    * 
    * @param {object} [variable] Clustering in proper JSON format.
    */
  InCHlib.prototype.read_data = function(json){
      _this.json = json;
      _this.data = _this.json.data;
      var settings = {};
      if(_this.json["metadata"] !== undefined){
        _this.metadata = _this.json.metadata;
        settings.metadata = true;
      }
      else{
        settings.metadata = false;
      }
      if(_this.json["column_dendrogram"] !== undefined){
        _this.column_dendrogram = _this.json.column_dendrogram;
        settings.column_dendrogram = true;
      }
      else{
        settings.column_dendrogram = false;
      }
      if(_this.json["column_metadata"] !== undefined){
        _this.column_metadata = _this.json.column_metadata;
        settings.column_metadata = true;
      }
      else{
        settings.column_metadata = false;
      }

      _this._update_user_settings(settings);
      _this._add_prefix();
  }

  /**
    * Read data from JSON file.
    * 
    * @param {string} [filename] Path to the JSON data file.
    *
    */
  InCHlib.prototype.read_data_from_file = function(json){      
      $.ajax({
          type: 'GET',
          url: json,
          dataType: 'json',
          success: function(json_file){
            _this.read_data(json_file);
          },
          async: false
      });
  }

  InCHlib.prototype._add_prefix = function(){
      _this.data.nodes = _this._add_prefix_to_data(_this.data.nodes);

      if(_this.settings.metadata){
        var metadata = {};
        for(var i = 0, keys = Object.keys(_this.metadata.nodes), len = keys.length; i < len; i++){
            id = [_this.settings.target, keys[i]].join("#");
            metadata[id] = _this.metadata.nodes[keys[i]];
        }
        _this.metadata.nodes = metadata;
      }

      if(_this.column_dendrogram){
        _this.column_dendrogram.nodes = _this._add_prefix_to_data(_this.column_dendrogram.nodes);
      }
  }

  InCHlib.prototype._add_prefix_to_data = function(data){
    var id, prefixed_data = {};

    for(var i = 0, keys = Object.keys(data), len = keys.length; i < len; i++){
        id = [_this.settings.target, keys[i]].join("#");
        prefixed_data[id] = data[keys[i]];
        
        if(prefixed_data[id]["parent"] !== undefined){
            prefixed_data[id].parent = [_this.settings.target, prefixed_data[id].parent].join("#");
        }

        if(prefixed_data[id]["count"] != 1){
            prefixed_data[id].left_child = [_this.settings.target, prefixed_data[id].left_child].join("#");
            prefixed_data[id].right_child = [_this.settings.target, prefixed_data[id].right_child].join("#");
        }
    }
    return prefixed_data;
  }

  InCHlib.prototype._get_root_id = function(nodes){
      var root_id;
      for(var i = 0, keys = Object.keys(nodes), len = keys.length; i < len; i++){
          if(nodes[keys[i]]["parent"] === undefined){
              root_id = keys[i];
              break;
          }
      }
      return root_id;
  }

  InCHlib.prototype._get_dimensions = function(){
      var dimensions = {"data": 0, "metadata": 0, "overall": 0}, key, keys, i;

      for(i = 0, keys = Object.keys(_this.data.nodes), len = keys.length; i < len; i++){
          key = keys[i];
          if(_this.data.nodes[key].count == 1){
              dimensions["data"] = _this.data.nodes[key].features.length;
              break;
          }
      }

      if(_this.settings.metadata){
        key = Object.keys(_this.metadata.nodes)[0];
        dimensions["metadata"] = _this.metadata.nodes[key].length;
      }
      
      dimensions["overall"] = dimensions["data"] + dimensions["metadata"];
      return dimensions;
  }

  InCHlib.prototype._get_min_max_middle = function(data){
      var i, len;
      var min_max_middle = [];
      var all = [];

      for(i = 0, len = data.length; i<len; i++){
          all = all.concat(data[i].filter(function(x){return x !== null}));
      }

      var len = all.length;
      all.sort(function(a,b){return a - b});
      min_max_middle.push((_this.settings.min_percentile > 0)?all[_this._hack_round(len*_this.settings.min_percentile/100)]:Math.min.apply(null, all));
      min_max_middle.push((_this.settings.max_percentile < 100)?all[_this._hack_round(len*_this.settings.max_percentile/100)]:Math.max.apply(null, all));
      min_max_middle.push((_this.settings.middle_percentile != 50)?all[_this._hack_round(len*_this.settings.middle_percentile/100)]:all[_this._hack_round((len-1)/2)]);
      return min_max_middle;
  }

  InCHlib.prototype._get_data_min_max_middle = function(data, axis){
      if(axis === undefined){
          axis = "column";
      }

      var i, j, value, len, columns;
      var data_length = data[0].length;

      if(axis == "column"){
          columns = [];

          for(i = 0; i<data_length; i++){
              columns.push([]);
          }

          for(i = 0; i<data.length; i++){
              for(j = 0; j < data_length; j++){
                  value = data[i][j];
                  if(value !== null && value !== undefined){
                      columns[j].push(value);
                  }
              }
          }
      }
      else{
        columns = data.slice(0);
      }

      var data2descs = {}
      var data_min_max_middle = [], min, max, middle;

      for(i = 0; i<columns.length; i++){
          if(_this._is_number(columns[i][0])){
              columns[i] = columns[i].map(parseFloat);
              columns[i].sort(function(a,b){return a - b});
              len = columns[i].length;
              max = (_this.settings.max_percentile < 100)?columns[i][_this._hack_round(len*_this.settings.max_percentile/100)]:Math.max.apply(null, columns[i]);
              min = (_this.settings.min_percentile > 0)?columns[i][_this._hack_round(len*_this.settings.min_percentile/100)]:Math.min.apply(null, columns[i]);
              middle = (_this.settings.middle_percentile != 50)?columns[i][_this._hack_round(len*_this.settings.middle_percentile/100)]:columns[i][_this._hack_round((len-1)/2)];
              data2descs[i] = {"min": min, "max": max, "middle": middle};
          }
          else{
              var hash_object = _this._get_hash_object(columns[i]);
              min = 0;
              max = _this._hack_size(hash_object)-1;
              middle = max/2;
              data2descs[i] = {"min": min, "max": max, "middle": middle, "str2num": hash_object};
          }
      }

      return data2descs;
  }

  InCHlib.prototype._get_hash_object = function(array){
      var i, count=0, hash_object = {};

      for(i = 0; i<array.length; i++){
          if(hash_object[array[i]] === undefined){
              hash_object[array[i]] = count;
              count++;
          }
      }
      return hash_object;
  }

  InCHlib.prototype._get_max_length = function(items){
      var lengths = items.map(function(x){return (""+x).length});
      var max = Math.max.apply(Math, lengths);
      return max;
  }

  InCHlib.prototype._get_max_value_length = function(){
      var nodes = _this.data.nodes;
      var max_length = 0;
      var node_data, key;

      for(var i = 0, keys = Object.keys(nodes), len = keys.length; i < len; i++){
          key = keys[i];
          if(nodes[key].count == 1){
              node_data = nodes[key].features;
              for(var j = 0, len_2 = node_data.length; j < len_2; j++){
                  if((""+node_data[j]).length > max_length){
                      max_length = (""+node_data[j]).length;
                  }
              }
          }
      }
      
      if(_this.settings.metadata){
          nodes = _this.metadata.nodes;
          for(var i = 0, keys = Object.keys(nodes), len = keys.length; i < len; i++){
              key = keys[i];
              node_data = nodes[key];
              for(var j = 0, len_2 = node_data.length; j < len_2; j++){
                  if((""+node_data[j]).length > max_length){
                      max_length = (""+node_data[j]).length;
                  }
              }
          }
      }
      return max_length;
  }

  InCHlib.prototype._preprocess_heatmap_data = function(){
      var heatmap_array = [], i, j = 0, keys, key, len, data, node;

      for(i = 0, keys = Object.keys(_this.data.nodes), len = keys.length; i < len; i++){
          key = keys[i];
          node = _this.data.nodes[key];
          if(node.count == 1){
              data = node.features;
              heatmap_array.push([key]);
              heatmap_array[j].push.apply(heatmap_array[j], data);
              if(_this.settings.metadata){
                  heatmap_array[j].push.apply(heatmap_array[j], _this.metadata.nodes[key]);
              }
              j++;
          }
      }
      return heatmap_array;
  }

  InCHlib.prototype._reorder_heatmap = function(column_index){
      _this.leaves_y_coordinates = {};
      column_index++;
      
      if(_this.ordered_by_index == column_index){
          _this.heatmap_array.reverse();
      }
      else{
          if(_this._is_number(_this.heatmap_array[0][column_index])){
            _this.heatmap_array.sort(function(a,b){return (a[column_index] == null)?-1:(b[column_index] == null)?1:a[column_index] - b[column_index]});
          }
          else{
            _this.heatmap_array.sort(function(a,b){return (a[column_index] == null)?-1:(b[column_index] == null)?1:(a[column_index] > b[column_index])?1:(a[column_index] < b[column_index])?-1:0});
          }
      }

      var y = _this.pixels_for_leaf/2 + _this.header_height;

      for(var i = 0, len = _this.heatmap_array.length; i<len; i++){
          _this.leaves_y_coordinates[_this.heatmap_array[i][0]] = y;
          y += _this.pixels_for_leaf;
      }

      _this.ordered_by_index = column_index;
  }

  /**
    * Draw already read data (from file/JSON variable).
    */
  InCHlib.prototype.draw = function(){
      _this.zoomed_clusters = {"row": [], "column": []};
      _this.last_highlighted_cluster = null;
      _this.current_object_ids = [];
      _this.current_column_ids = [];
      _this.heatmap_array = _this._preprocess_heatmap_data();

      if(_this.settings.heatmap){
        _this.last_column = null;
        _this.on_features = {"data":[], "metadata":[]}
        _this.dimensions = _this._get_dimensions();
        _this.column_metadata_rows = (_this.settings.column_metadata)?_this.column_metadata.features.length:0;
        _this.column_metadata_height = _this.column_metadata_rows * _this.settings.column_metadata_row_height;
        _this._set_heatmap_settings();
        _this._adjust_leaf_size(_this.heatmap_array.length);
      }
      else{
        _this._adjust_horizontal_sizes();
        _this.dimensions = {"data": 0, "metadata": 0, "overall": 0};
      }

      if(_this.settings.column_dendrogram && _this.heatmap_header){
          _this.footer_height = 150;
      }

      _this.stage = new Kinetic.Stage({
          container: _this.settings.target,
      });

      _this.settings.height = _this.heatmap_array.length*_this.pixels_for_leaf+_this.header_height+_this.footer_height;
      _this.stage.setWidth(_this.settings.width);
      _this.stage.setHeight(_this.settings.height);
      _this._draw_stage_layer();

      if(_this.settings.dendrogram){
        _this.timer = 0;
        _this._draw_dendrogram_layers();
        _this.root_id = _this._get_root_id(_this.data.nodes);
        _this._draw_row_dendrogram(_this.root_id);

        if(_this.settings.column_dendrogram && _this.settings.dendrogram){
          _this.column_root_id = _this._get_root_id(_this.column_dendrogram.nodes);
          _this.nodes2columns = false;
          _this.columns_start_index = 0;
          _this._draw_column_dendrogram(_this.column_root_id);
        }
      }
      else{
        _this.settings.column_dendrogram = false;
        _this._reorder_heatmap(0);
        _this.ordered_by_index = 0;
      }
      _this._draw_heatmap();
      _this._draw_heatmap_header();
      _this._draw_navigation();
      _this.highlight_rows(_this.settings.highlighted_rows);
  }

  InCHlib.prototype._draw_dendrogram_layers = function(){
    _this.cluster_layer = new Kinetic.Layer();
    _this.dendrogram_hover_layer = new Kinetic.Layer();
    _this.stage.add(_this.cluster_layer, _this.dendrogram_hover_layer);

    _this.cluster_layer.on("click", function(evt){
        _this.unhighlight_cluster();
        _this.unhighlight_column_cluster();
        _this.events.empty_space_onclick(evt);
    });
  };

  InCHlib.prototype._draw_row_dendrogram = function(node_id){
      _this.dendrogram_layer = new Kinetic.Layer();
      var node = _this.data.nodes[node_id];
      var count = node.count;

      _this.distance_step = _this.distance/node.distance;
      _this.leaves_y_coordinates = {};
      _this.objects2leaves = {};

      _this._adjust_leaf_size(count);
      _this.settings.height = count*_this.pixels_for_leaf+_this.header_height+_this.footer_height+_this.column_metadata_height;
      
      _this.stage.setWidth(_this.settings.width);
      _this.stage.setHeight(_this.settings.height);

      var current_left_count = 0;
      var current_right_count = 0;
      var y = _this.header_height + _this.column_metadata_height + _this.pixels_for_leaf/2;
      
      if(node.count > 1){
          current_left_count = _this.data.nodes[node.left_child].count;
          current_right_count = _this.data.nodes[node.right_child].count;
      }
      _this._draw_row_dendrogram_node(node_id, node, current_left_count, current_right_count, 0, y);
      _this.middle_item_count = (_this.min_item_count+_this.max_item_count)/2;
      _this._draw_distance_scale(node.distance);
      _this.stage.add(_this.dendrogram_layer);


      _this._bind_dendrogram_hover_events(_this.dendrogram_layer);
      
      _this.dendrogram_layer.on("click", function(evt){
          _this._dendrogram_layers_click(this, evt);
      });
      
      _this.dendrogram_layer.on("mousedown", function(evt){
        _this._dendrogram_layers_mousedown(this, evt);
      });

      _this.dendrogram_layer.on("mouseup", function(evt){
        _this._dendrogram_layers_mouseup(this, evt);
      });
  }

  InCHlib.prototype._draw_row_dendrogram_node = function(node_id, node, current_left_count, current_right_count, x, y){
      if(node.count != 1){
          var node_neighbourhood = _this._get_node_neighbourhood(node, _this.data.nodes);
          var right_child = _this.data.nodes[node.right_child];
          var left_child = _this.data.nodes[node.left_child];

          var y1 = _this._get_y1(node_neighbourhood, current_left_count, current_right_count);
          var y2 = _this._get_y2(node_neighbourhood, current_left_count, current_right_count);
          var x1 = _this._hack_round(_this.distance - _this.distance_step*node.distance);
          x1 = (x1 == 0)? 2: x1;
          
          var x2 = x1;
          var left_distance = _this.distance - _this.distance_step*_this.data.nodes[node.left_child].distance;
          var right_distance = _this.distance - _this.distance_step*_this.data.nodes[node.right_child].distance;

          if(right_child.count == 1){
              y2 = y2 + _this.pixels_for_leaf/2;
          }

          _this.dendrogram_layer.add(_this._draw_horizontal_path(node_id, x1, y1, x2, y2, left_distance, right_distance));
          _this._draw_row_dendrogram_node(node.left_child, left_child, current_left_count - node_neighbourhood.left_node.right_count, current_right_count + node_neighbourhood.left_node.right_count, left_distance, y1);
          _this._draw_row_dendrogram_node(node.right_child, right_child, current_left_count + node_neighbourhood.right_node.left_count, current_right_count - node_neighbourhood.right_node.left_count, right_distance, y2);
      }
      else{
          var objects = node.objects;
          _this.leaves_y_coordinates[node_id] = y;

          for(var i = 0, len = objects.length; i<len; i++){
              _this.objects2leaves[objects[i]] = node_id;
          }

          var count = node.objects.length;
          if(count<_this.min_item_count){
              _this.min_item_count = count;
          }
          if(count>_this.max_item_count){
              _this.max_item_count = count;
          }
      }
  }

  InCHlib.prototype._draw_stage_layer = function(){
      _this.stage_layer = new Kinetic.Layer();
      var stage_rect = new Kinetic.Rect({
                                  x: 0,
                                  y: 0,
                                  width: _this.settings.width,
                                  height: _this.settings.height,
                                  opacity: 0,
                              });
      _this.stage_layer.add(stage_rect);
      stage_rect.moveToBottom();
      _this.stage.add(_this.stage_layer);


      _this.stage_layer.on("click", function(evt){
          _this.unhighlight_cluster();
          _this.unhighlight_column_cluster();
          _this.events.empty_space_onclick(evt);
      });
  }

  InCHlib.prototype._draw_column_dendrogram = function(node_id){
      _this.column_dendrogram_layer = new Kinetic.Layer();
      _this.column_x_coordinates = {};
      var node = _this.column_dendrogram.nodes[node_id];
      _this.current_column_count = node.count;
      _this.vertical_distance = _this.header_height;
      _this.vertical_distance_step = _this.vertical_distance/node.distance;

      _this.last_highlighted_column_cluster = null;
      var current_left_count = _this.column_dendrogram.nodes[node.left_child].count;
      var current_right_count = _this.column_dendrogram.nodes[node.right_child].count;
      _this._draw_column_dendrogram_node(node_id, node, current_left_count, current_right_count, 0, 0);
      _this.stage.add(_this.column_dendrogram_layer);

      if(!_this.nodes2columns){
        _this.nodes2columns = _this._get_nodes2columns();
      }
      
      _this._bind_dendrogram_hover_events(_this.column_dendrogram_layer);

      _this.column_dendrogram_layer.on("click", function(evt){
          _this._column_dendrogram_layers_click(this, evt);
      });

      _this.column_dendrogram_layer.on("mousedown", function(evt){
        _this._column_dendrogram_layers_mousedown(this, evt);
      });

      _this.column_dendrogram_layer.on("mouseup", function(evt){
        _this._dendrogram_layers_mouseup(this, evt);
      });
  }

  InCHlib.prototype._get_nodes2columns = function(){
    var coordinates = [];
    var coordinates2nodes = {};
    var nodes2columns = {};
    var key, value, i;

    for(i = 0, keys = Object.keys(_this.column_x_coordinates), len = keys.length; i < len; i++){
      key = keys[i];
      value = _this.column_x_coordinates[key];
      coordinates2nodes[value] = key;
      coordinates.push(value);
    }
    coordinates.sort(function(a,b){return a - b});

    for(i = 0, len = coordinates.length; i<len; i++){
      nodes2columns[coordinates2nodes[coordinates[i]]] = i;
    }
    return nodes2columns;
  }

  InCHlib.prototype._bind_dendrogram_hover_events = function(layer){

      layer.on("mouseover", function(evt){
          _this._dendrogram_layers_mouseover(this, evt);
      });

      layer.on("mouseout", function(evt){
          _this._dendrogram_layers_mouseout(this, evt);
      });    
  }

  InCHlib.prototype._delete_layers = function(to_destroy, to_remove_children){
      for(var i = 0, len = to_destroy.length; i < len; i++){
        if(to_destroy[i] !== undefined){
          to_destroy[i].destroy();
        }
      }

      if(to_remove_children !== undefined){
        for(var i = 0, len = to_remove_children.length; i < len; i++){
          to_remove_children[i].removeChildren();
          to_remove_children[i].draw();
        }
      }
  }

  InCHlib.prototype._delete_all_layers = function(){
      _this.stage.destroyChildren();
  }

  InCHlib.prototype._adjust_leaf_size = function(leaves){
      _this.pixels_for_leaf = (_this.settings.max_height-_this.header_height-_this.footer_height-_this.column_metadata_height-5)/leaves;
      if(_this.pixels_for_leaf > 2*_this.pixels_for_dimension){
        _this.pixels_for_leaf = 2*_this.pixels_for_dimension;
      }

      if(_this.pixels_for_leaf > _this.settings.max_row_height){
          _this.pixels_for_leaf = _this.settings.max_row_height;
      }

      if(_this.settings.min_row_height > _this.pixels_for_leaf){
          _this.pixels_for_leaf = _this.settings.min_row_height;
      }
  }

  InCHlib.prototype._adjust_horizontal_sizes = function(dimensions){
      if(dimensions === undefined){
        dimensions = _this._get_visible_count();
      }
      _this.right_margin = 100;
      
      if(_this.settings.dendrogram){
        _this.heatmap_width = (_this.settings.width - _this.right_margin - _this.dendrogram_heatmap_distance)*_this.settings.heatmap_part_width;
        _this.distance = _this.settings.width - _this.heatmap_width - _this.right_margin;
        _this.heatmap_distance = _this.distance + _this.dendrogram_heatmap_distance;
      }
      else{
        _this.heatmap_width = _this.settings.width - _this.right_margin;
        _this.distance = _this.right_margin/2;
        _this.heatmap_distance = _this.distance;

      }
      _this.pixels_for_dimension = dimensions?_this.heatmap_width/dimensions:0;

      if(_this.settings.max_column_width && _this.settings.max_column_width < _this.pixels_for_dimension){
        _this.pixels_for_dimension = _this.settings.max_column_width;
        _this.heatmap_width = dimensions*_this.pixels_for_dimension;

        if(_this.settings.dendrogram){
          _this.distance = _this.settings.width - _this.heatmap_width - _this.right_margin - _this.dendrogram_heatmap_distance;
          _this.heatmap_distance = _this.distance + _this.dendrogram_heatmap_distance;
        }
        else{
          _this.distance = _this._hack_round((_this.settings.width - _this.heatmap_width)/2);
          _this.right_margin = _this.distance;
          _this.heatmap_distance = _this.distance;
        }
      }
  }

  InCHlib.prototype._set_color_settings = function(){
    var data = [];
    for(i = 0, keys = Object.keys(_this.data.nodes), len = keys.length; i < len; i++){
        node = _this.data.nodes[keys[i]];
        if(node.count == 1){
            data.push(node.features);
        };
    }
    
    _this.data_descs = {};
    if(_this.settings.independent_columns){
        _this.data_descs = _this._get_data_min_max_middle(data);
    }
    else{
        var min_max_middle = _this._get_min_max_middle(data);
        for(i = 0; i < _this.dimensions["data"]; i++){
          _this.data_descs[i] = {"min": min_max_middle[0], "max": min_max_middle[1], "middle": min_max_middle[2]};
        }
    }

    if(_this.settings.metadata){
        var metadata = [];

        for(i = 0, keys = Object.keys(_this.metadata.nodes), len = keys.length; i < len; i++){
            metadata.push(_this.metadata.nodes[keys[i]]);
        }
        _this.metadata_descs = _this._get_data_min_max_middle(metadata);
    }
  }

  InCHlib.prototype._set_heatmap_settings = function(){
      var i, keys, key, len, node;

      _this.header = [];
      for(i = 0; i<_this.dimensions["overall"]; i++){
          _this.header.push("");
      }

      _this.heatmap_header = false;
      _this.metadata_header = false;
      _this.current_label = null;

      _this._set_color_settings();

      if(_this.data.feature_names !== undefined){
          _this.heatmap_header = _this.data.feature_names;
          for(i=0; i<_this.dimensions["data"]; i++){
              _this.header[i] = _this.heatmap_header[i];
          }
      }

      if(_this.settings.metadata){

          if(_this.metadata.feature_names){
              _this.metadata_header = _this.metadata.feature_names;

              for(i=0; i<_this.dimensions["metadata"]; i++){
                  _this.header[_this.dimensions["data"]+i] = _this.metadata_header[i];
              }
          }
      }

      if(_this.settings.column_metadata){
        if(_this.column_metadata.feature_names !== undefined){
          _this.column_metadata_header = _this.column_metadata.feature_names;
        }
      }

      if(_this.settings.count_column){
          _this.max_item_count = 1;
          _this.min_item_count = 1;
          _this.dimensions["overall"]++;
          _this.header.push("Count");
      }
      _this.features = {};

      for(i=0; i<_this.dimensions["overall"]; i++){
          _this.features[i] = true;
      }

      _this._set_on_features();
      _this._adjust_horizontal_sizes();
      _this.top_heatmap_distance = _this.header_height + _this.column_metadata_height + _this.settings.column_metadata_row_height/2;
  }

  InCHlib.prototype._set_on_features = function(features){
    var key;
    if(features === undefined){
      var features = [];
      for(var i = 0, keys = Object.keys(_this.features), len = keys.length; i < len; i++){
        key = keys[i];
        if(_this.features[key]){
          features.push(key);
        }
      }
    }

    _this.on_features = {"data":[], "metadata":[], "count_column":[]}

    for(var i = 0, len = features.length; i < len; i++){
      key = features[i];
      if(key < _this.dimensions["data"]){
          _this.on_features["data"].push(key);
      }
      else if(key <= _this.dimensions["data"] + _this.dimensions["metadata"] - 1){
            _this.on_features["metadata"].push(key-_this.dimensions["data"]);
      }
      else{
        _this.on_features["count_column"].push(0);
      }
    }
  }

  InCHlib.prototype._draw_heatmap = function(){
      if(!_this.settings.heatmap || _this.dimensions["overall"]==0){
          return;
      }

      var heatmap_row, row_id, col_number, col_label, row_values, y;
      _this.heatmap_layer = new Kinetic.Layer();
      _this.heatmap_overlay = new Kinetic.Layer();
      _this.highlighted_rows_y = [];
      _this.current_draw_values = true;
      _this.max_value_length = _this._get_max_value_length();
      _this.value_font_size = _this._get_font_size(_this.max_value_length, _this.pixels_for_dimension, _this.pixels_for_leaf, 12);

      if(_this.value_font_size < 4){
          _this.current_draw_values = false;
      }

      var x1 = _this.heatmap_distance;
      var current_leaves_y = [];

      for(var i = 0, keys = Object.keys(_this.leaves_y_coordinates), len = keys.length; i < len; i++){
          key = keys[i];
          y = _this.leaves_y_coordinates[key];
          heatmap_row = _this._draw_heatmap_row(key, x1, y);
          _this.heatmap_layer.add(heatmap_row);
          current_leaves_y.push([key, y]);
          _this._bind_row_events(heatmap_row);
      }

      if(_this.settings.column_metadata){
          _this.column_metadata_descs = _this._get_data_min_max_middle(_this.column_metadata.features, "row");
          y1 = _this.header_height + 0.5*_this.settings.column_metadata_row_height;

          for(var i = 0, len = _this.column_metadata.features.length; i < len; i++){
              heatmap_row = _this._draw_column_metadata_row(_this.column_metadata.features[i], i, x1, y1);
              _this.heatmap_layer.add(heatmap_row);
              _this._bind_row_events(heatmap_row);
              y1 = y1 + _this.settings.column_metadata_row_height;
          }
      }

      if(_this.settings.draw_row_ids){
          _this._draw_row_ids(current_leaves_y);
      }

      _this.highlighted_rows_layer = new Kinetic.Layer();
      _this.stage.add(_this.heatmap_layer, _this.heatmap_overlay, _this.highlighted_rows_layer);

      _this.highlighted_rows_layer.moveToTop();
      _this.row_overlay = _this.objects_ref.heatmap_line.clone();
      _this.column_overlay = _this.objects_ref.heatmap_line.clone();


      _this.heatmap_layer.on("mouseleave", function(evt){
          _this.last_header = null;
          _this.heatmap_overlay.destroyChildren();
          _this.heatmap_overlay.draw();
          _this.events.heatmap_onmouseout(evt);
      });
  }

  InCHlib.prototype._draw_heatmap_row = function(node_id, x1, y1){
      var node = _this.data.nodes[node_id];
      var row = new Kinetic.Group({id:node_id});
      var x2, y2, color, line, value, text, text_value, col_index;
      
      for (var i = 0, len = _this.on_features["data"].length; i < len; i++){
          col_index = _this.on_features["data"][i];
          x2 = x1 + _this.pixels_for_dimension;
          y2 = y1;
          value = node.features[col_index];

          if(value !== null){
            color = _this._get_color_for_value(value, _this.data_descs[col_index]["min"], _this.data_descs[col_index]["max"], _this.data_descs[col_index]["middle"], _this.settings.heatmap_colors);

            line = _this.objects_ref.heatmap_line.clone({
                stroke: color,
                points: [x1, y1, x2, y2],
                value: value,
                column: ["d", col_index].join("_"),
                strokeWidth: _this.pixels_for_leaf,
            });
            row.add(line);

            if(_this.current_draw_values){
                text = _this.objects_ref.heatmap_value.clone({
                    x: _this._hack_round((x1 + x2)/2-(""+value).length*(_this.value_font_size/4)),
                    y: _this._hack_round(y1-_this.value_font_size/2),
                    fontSize: _this.value_font_size,
                    text: value,
                });
                row.add(text);
            }
          }
          x1 = x2;
      }

      if(_this.settings.metadata){
          var metadata = _this.metadata.nodes[node_id];

          if(metadata !== undefined){
            for (var i = 0, len = _this.on_features["metadata"].length; i < len; i++){
                col_index = _this.on_features["metadata"][i];
                value = metadata[col_index];
                x2 = x1 + _this.pixels_for_dimension;
                y2 = y1;

                if(value !== null && value !== undefined){
                  text_value = value;
                  
                  if(_this.metadata_descs[col_index]["str2num"] !== undefined){
                      value = _this.metadata_descs[col_index]["str2num"][value];
                  }
                  color = _this._get_color_for_value(value, _this.metadata_descs[col_index]["min"], _this.metadata_descs[col_index]["max"], _this.metadata_descs[col_index]["middle"], _this.settings.metadata_colors);
                      
                  line = _this.objects_ref.heatmap_line.clone({
                          stroke: color,
                          points: [x1, y1, x2, y2],
                          value: text_value,
                          column: ["m", col_index].join("_"),
                          strokeWidth: _this.pixels_for_leaf,
                      });
                  row.add(line);

                  if(_this.current_draw_values){
                      text = _this.objects_ref.heatmap_value.clone({
                          text: text_value,
                          fontSize: _this.value_font_size,
                      });

                      width = text.getWidth();
                      x = _this._hack_round((x1+x2)/2-width/2);
                      y = _this._hack_round(y1-_this.value_font_size/2);
                      text.position({x:x, y:y});
                      row.add(text);
                  }
                }
                x1 = x2;
            }
          }
      }

      if(_this.settings.count_column && _this.features[_this.dimensions["overall"]-1]){
          x2 = x1 + _this.pixels_for_dimension;
          var count = node.objects.length;
          color = _this._get_color_for_value(count, _this.min_item_count, _this.max_item_count, _this.middle_item_count, _this.settings.count_column_colors);

          line = _this.objects_ref.heatmap_line.clone({
                  stroke: color,
                  points: [x1, y1, x2, y2],
                  value: count,
                  column: "Count",
                  strokeWidth: _this.pixels_for_leaf,
          });
          row.add(line);

          if(_this.current_draw_values){
              text = _this.objects_ref.heatmap_value.clone({
                  text: count,
              });

              width = text.getWidth();
              x = _this._hack_round((x1+x2)/2-width/2);
              y = _this._hack_round(y1-_this.value_font_size/2);
              text.position({x:x, y:y});
              row.add(text);
          }
      }
      return row;
  }

  InCHlib.prototype._draw_column_metadata_row = function(data, row_index, x1, y1){
      var row = new Kinetic.Group({"class": "column_metadata"});
      var x2, y2, color, line, value, text, text_value, width, col_index;
      var str2num = (_this.column_metadata_descs[row_index]["str2num"] === undefined)?false:true;

      for (var i = 0, len = _this.on_features["data"].length; i < len; i++){
          col_index = _this.on_features["data"][i];
          value = data[col_index];
          text_value = value;
          
          if(str2num){
              value = _this.column_metadata_descs[row_index]["str2num"][value];
          }

          color = _this._get_color_for_value(value, _this.column_metadata_descs[row_index]["min"], _this.column_metadata_descs[row_index]["max"], _this.column_metadata_descs[row_index]["middle"], _this.settings.column_metadata_colors);
          x2 = x1 + _this.pixels_for_dimension;
          y2 = y1;
              
          line = _this.objects_ref.heatmap_line.clone({
                  strokeWidth: _this.settings.column_metadata_row_height,
                  stroke: color,
                  value: text_value,
                  points: [x1, y1, x2, y2],
                  column: ["cm", row_index].join("_"),
              });
          row.add(line);
          x1 = x2;
      }
      return row;
  }

  InCHlib.prototype._bind_row_events = function(row){
      row.on("mouseenter", function(evt){
          _this._row_mouseenter(evt);
      });

      row.on("mouseleave", function(evt){
          _this._row_mouseleave(evt);
      });

      row.on("mouseover", function(evt){
          _this._draw_col_label(evt);
      });

      row.on("mouseout", function(evt){
          _this.heatmap_overlay.find("#col_label")[0].destroy();
      });

      row.on("click", function(evt){
          var row_id = evt.target.parent.attrs.id;
          if(evt.target.parent.attrs.class !== "column_metadata"){
              var items = _this.data.nodes[row_id].objects;
              var item_ids = [];
              
              for(i = 0; i < items.length; i++){
                  item_ids.push(items[i]);
              }
              _this.events.row_onclick(item_ids, evt);
          }
      });
  }

  InCHlib.prototype._draw_row_ids = function(leaves_y){
      if(_this.pixels_for_leaf < 6){
          return;
      }
      var i, objects, object_y = [], leaf, values = [], text;
      
      for(i = 0; i < leaves_y.length; i++){
          leaf = leaves_y[i];
          objects = _this.data.nodes[leaf[0]].objects;
          if(objects.length > 1){
              return;
          }
          values.push(objects[0]);
          object_y.push([objects[0], leaf[1]]);
      }
      var max_length = _this._get_max_length(values);
      var font_size = _this._get_font_size(max_length, 85, _this.pixels_for_leaf, 10);
      var x = _this.distance + _this._get_visible_count()*_this.pixels_for_dimension + 15;
      
      if(font_size > 4){
          for(i = 0; i < object_y.length; i++){
              text = _this.objects_ref.heatmap_value.clone({
                  x: x,
                  y: _this._hack_round(object_y[i][1] - font_size/2),
                  fontSize: font_size,
                  text: object_y[i][0],
                  fontStyle: 'italic',
                  fill: "gray"
              });
              _this.heatmap_layer.add(text);
          }
      }
  }

  InCHlib.prototype._draw_heatmap_header = function(){
    if(_this.settings.heatmap_header && _this.header.length > 0){
      _this.header_layer = new Kinetic.Layer();
      var count = _this._hack_size(_this.leaves_y_coordinates);
      var y = (_this.settings.column_dendrogram && _this.heatmap_header)? _this.header_height+(_this.pixels_for_leaf*count) + 10 + _this.column_metadata_height: _this.header_height - 20;
      var rotation = (_this.settings.column_dendrogram && _this.heatmap_header) ? 45 : -45;
      var distance_step = 0;
      var x, i, column_header, key;
      var current_headers = [];
      
      for(i = 0, len = _this.on_features["data"].length; i < len; i++){
        current_headers.push(_this.header[_this.on_features["data"][i]]);
      }

      for(i = 0, len = _this.on_features["metadata"].length; i < len; i++){
        current_headers.push(_this.header[_this.on_features["metadata"][i] + _this.dimensions["data"]]);
      }
      if(_this.settings.count_column && _this.features[_this.dimensions["overall"] - 1]){
        current_headers.push(_this.header[_this.dimensions["overall"] - 1]);
      }
      var max_text_length = _this._get_max_length(current_headers);
      var font_size = _this._get_font_size(max_text_length, _this.header_height, _this.pixels_for_dimension, 16);
      if(font_size < 8){
          return;
      }
      
      for(i = 0, len = current_headers.length; i<len; i++){
        x = _this.heatmap_distance+distance_step*_this.pixels_for_dimension+_this.pixels_for_dimension/2;
        column_header = _this.objects_ref.column_header.clone({
                x: x,
                y: y,
                text: current_headers[i],
                position_index: i,
                fontSize: font_size,
                rotationDeg: rotation,
        });
        _this.header_layer.add(column_header);
        distance_step++;
      }

      _this.stage.add(_this.header_layer);

      if(!(_this.settings.dendrogram)){

        _this.header_layer.on("click", function(evt){
            var column = evt.target;
            var position_index = column.attrs.position_index;
            for(i = 0; i<_this.header_layer.getChildren().length; i++){
                _this.header_layer.getChildren()[i].setFill("black");
            }
            evt.target.setAttrs({"fill": "red"});
            _this._delete_layers([_this.heatmap_layer, _this.heatmap_overlay, _this.highlighted_rows_layer]);
            _this._reorder_heatmap(_this._translate_column_to_feature_index(position_index));
            _this._draw_heatmap();
            _this.header_layer.draw();
        });

        _this.header_layer.on("mouseover", function(evt){
            var label = evt.target;
            label.setOpacity(0.7);
            this.draw();
        });

        _this.header_layer.on("mouseout", function(evt){
            var label = evt.target;
            label.setOpacity(1);
            this.draw();
        });
      }
    }
  }

  InCHlib.prototype._translate_column_to_feature_index = function(column_index){
    var key;
    var index = -1;
    for(var i = 0, keys=Object.keys(_this.features), len=keys.length; i<len; i++){
      key = keys[i];
      if(_this.features[key]){
        index++;
        if(column_index === index){
          return key;
        }
      }
    }
  }

  InCHlib.prototype._draw_distance_scale = function(distance){
      var y1 = _this.header_height + _this.column_metadata_height + _this.settings.column_metadata_row_height/2 -10;
      var y2 = y1;
      var x1 = 0;
      var x2 = _this.distance;
      var path = new Kinetic.Line({
          points: [x1, y1, x2, y2],
          stroke: "black",
          listening: false,
      })

      var circle = new Kinetic.Circle({
          x: x2, 
          y: y2,
          radius: 3,
          fill: "black",
          listening: false,
      })

      var number = 0;
      var marker_tail = 3;
      var marker_distance = x2;
      var marker_number_distance = _this._hack_round(30/_this.distance_step*10)/10;
      var distance = Math.round(100*_this.distance/_this.distance_step)/100;
      var marker_distance_step = _this._hack_round(_this.distance_step*marker_number_distance);
      var marker_counter = 0;

      var distance_number = new Kinetic.Text({
              x: 0,
              y: y1-20,
              text: distance,
              fontSize: 12,
              fontFamily: _this.settings.font,
              fontStyle: 'bold',
              fill: 'black',
              align: 'right',
              listening: false,
      });
      _this.dendrogram_layer.add(path, circle, distance_number);

      if(marker_distance_step==0){
          marker_distance_step=0.5;
      }

      var path;
      if(marker_number_distance > 0.1){
          while(marker_distance > 0){
              path = new Kinetic.Line({
                  points: [marker_distance, (y1-marker_tail), marker_distance, (y2+marker_tail)],
                  stroke: "black",
                  listening: false,
              })
              _this.dendrogram_layer.add(path);

              number = _this._hack_round((number + marker_number_distance)*10)/10;
              if(number>10){
                  number = _this._hack_round(number);
              }
              
              marker_distance = marker_distance - marker_distance_step;
              marker_counter++;
          }
      }
  }

  InCHlib.prototype._draw_navigation = function(){
      _this.navigation_layer = new Kinetic.Layer();
      var x = 0;
      var y = 10;

      _this._draw_color_scale();
      _this._draw_help();

      if(!_this.settings.column_dendrogram){
          var filter_icon = _this.objects_ref.icon.clone({
                  data: "M26.834,6.958c0-2.094-4.852-3.791-10.834-3.791c-5.983,0-10.833,1.697-10.833,3.791c0,0.429,0.213,0.84,0.588,1.224l8.662,15.002v4.899c0,0.414,0.709,0.75,1.583,0.75c0.875,0,1.584-0.336,1.584-0.75v-4.816l8.715-15.093h-0.045C26.625,7.792,26.834,7.384,26.834,6.958zM16,9.75c-6.363,0-9.833-1.845-9.833-2.792S9.637,4.167,16,4.167c6.363,0,9.834,1.844,9.834,2.791S22.363,9.75,16,9.75z",
                  x: x,
                  y: y,
                  label: "Filter\ncolumns"
          });
      
          var filter_overlay = _this._draw_icon_overlay(x, y);
          _this.navigation_layer.add(filter_icon, filter_overlay);
          x = x + 40;    
      
          filter_overlay.on("click", function(){
              _this._filter_icon_click(this);
          });
              
          filter_overlay.on("mouseover", function(){
              _this._icon_mouseover(filter_icon, filter_overlay, _this.navigation_layer);
          });
             
          filter_overlay.on("mouseout", function(){
              _this._icon_mouseout(filter_icon, filter_overlay, _this.navigation_layer);
          });
      }

      if(_this.zoomed_clusters["row"].length > 0 || _this.zoomed_clusters["column"].length > 0){
        var refresh_icon = _this.objects_ref.icon.clone({
              data: "M24.083,15.5c-0.009,4.739-3.844,8.574-8.583,8.583c-4.741-0.009-8.577-3.844-8.585-8.583c0.008-4.741,3.844-8.577,8.585-8.585c1.913,0,3.665,0.629,5.09,1.686l-1.782,1.783l8.429,2.256l-2.26-8.427l-1.89,1.89c-2.072-1.677-4.717-2.688-7.587-2.688C8.826,3.418,3.418,8.826,3.416,15.5C3.418,22.175,8.826,27.583,15.5,27.583S27.583,22.175,27.583,15.5H24.083z",
              x: x,
              y: y,
              id: "refresh_icon",
              label: "Refresh"
        });
        var refresh_overlay = _this._draw_icon_overlay(x, y);
        _this.navigation_layer.add(refresh_icon, refresh_overlay);

        refresh_overlay.on("click", function(){
            _this._refresh_icon_click();
            _this.events.on_refresh();
        });

        refresh_overlay.on("mouseover", function(){
            _this._icon_mouseover(refresh_icon, refresh_overlay, _this.navigation_layer);
        });

        refresh_overlay.on("mouseout", function(){
            _this._icon_mouseout(refresh_icon, refresh_overlay, _this.navigation_layer);
        });
      }

      if(_this.zoomed_clusters["row"].length > 0){
        x = _this.distance - 55;
        y = _this.header_height + _this.column_metadata_height - 40;
        var unzoom_icon = _this.objects_ref.icon.clone({
            data: _this.paths_ref["unzoom_icon"],
            x: x,
            y: y,
            scale: {x: 0.7, y: 0.7},
            label: "Unzoom\nrows"
        });
        var unzoom_overlay = _this._draw_icon_overlay(x, y);
        _this.navigation_layer.add(unzoom_icon, unzoom_overlay);

        unzoom_overlay.on("click", function(){
            _this._unzoom_icon_click();
        });

        unzoom_overlay.on("mouseover", function(){
            _this._icon_mouseover(unzoom_icon, unzoom_overlay, _this.navigation_layer);
        });

        unzoom_overlay.on("mouseout", function(){
            _this._icon_mouseout(unzoom_icon, unzoom_overlay, _this.navigation_layer);
        });
      }

      if(_this.zoomed_clusters["column"].length > 0){
          x = _this.settings.width - 85;
          y = _this.header_height - 50;
          var column_unzoom_icon = _this.objects_ref.icon.clone({
              data: _this.paths_ref["unzoom_icon"],
              x: x,
              y: y-5,
              scale: {x: 0.7, y: 0.7},
              label: "Unzoom\ncolumns"
          });
          var column_unzoom_overlay = _this._draw_icon_overlay(x, y);

          _this.navigation_layer.add(column_unzoom_icon, column_unzoom_overlay);

          column_unzoom_overlay.on("click", function(){
              _this._column_unzoom_icon_click(this);
          });

          column_unzoom_overlay.on("mouseover", function(){
              _this._icon_mouseover(column_unzoom_icon, column_unzoom_overlay, _this.navigation_layer);
          });

          column_unzoom_overlay.on("mouseout", function(){
              _this._icon_mouseout(column_unzoom_icon, column_unzoom_overlay, _this.navigation_layer);
          });
      }

      if(_this.settings.show_export_button){
        var export_icon = _this.objects_ref.icon.clone({
              data: "M24.25,10.25H20.5v-1.5h-9.375v1.5h-3.75c-1.104,0-2,0.896-2,2v10.375c0,1.104,0.896,2,2,2H24.25c1.104,0,2-0.896,2-2V12.25C26.25,11.146,25.354,10.25,24.25,10.25zM15.812,23.499c-3.342,0-6.06-2.719-6.06-6.061c0-3.342,2.718-6.062,6.06-6.062s6.062,2.72,6.062,6.062C21.874,20.78,19.153,23.499,15.812,23.499zM15.812,13.375c-2.244,0-4.062,1.819-4.062,4.062c0,2.244,1.819,4.062,4.062,4.062c2.244,0,4.062-1.818,4.062-4.062C19.875,15.194,18.057,13.375,15.812,13.375z",
              x: _this.settings.width - 62,
              y: 10,
              scale: {x: 0.7, y: 0.7},
              id: "export_icon",
              label: "Export\nin png format"
        });

        var export_overlay = _this._draw_icon_overlay(_this.settings.width - 62, 10);
        _this.navigation_layer.add(export_icon, export_overlay);

        export_overlay.on("click", function(){
            _this._export_icon_click(this);
        });
            
        export_overlay.on("mouseover", function(){
            _this._icon_mouseover(export_icon, export_overlay, _this.navigation_layer);
        });
           
        export_overlay.on("mouseout", function(){
            _this._icon_mouseout(export_icon, export_overlay, _this.navigation_layer);
        });
      }

      _this.stage.add(_this.navigation_layer);
  };

  InCHlib.prototype._draw_help = function(){
    var help_icon = _this.objects_ref.icon.clone({
          data: _this.paths_ref["lightbulb"],
          x: _this.settings.width - 63,
          y: 40,
          scale: {x: 0.8, y: 0.8},
          id: "help_icon",
          label: "Tip"
    });

    var help_overlay = _this._draw_icon_overlay(_this.settings.width - 63, 40);

    _this.navigation_layer.add(help_icon, help_overlay);

    help_overlay.on("mouseover", function(){
        _this._icon_mouseover(help_icon, help_overlay, _this.navigation_layer);
        _this._help_mouseover();
    });
       
    help_overlay.on("mouseout", function(){
        _this._help_mouseout();
        _this._icon_mouseout(help_icon, help_overlay, _this.navigation_layer);
    });

  }

  InCHlib.prototype._draw_color_scale = function(){
      var color_steps = [_this.settings.min_percentile/100, _this._get_color_for_value(0, 0, 1, 0.5, _this.settings.heatmap_colors), _this.settings.middle_percentile/100, _this._get_color_for_value(0.5, 0, 1, 0.5, _this.settings.heatmap_colors), _this.settings.max_percentile/100, _this._get_color_for_value(1, 0, 1, 0.5, _this.settings.heatmap_colors)];
      var color_scale = _this.objects_ref.rect_gradient.clone({"label": "Color settings",
                                                              "fillLinearGradientColorStops": color_steps,
                                                              "id": "color_scale"});

      color_scale.on("mouseover", function(){
        _this._color_scale_mouseover(color_scale, _this.navigation_layer);
      });

      color_scale.on("mouseout", function(){
        _this._color_scale_mouseout(color_scale, _this.navigation_layer);
      });

      color_scale.on("click", function(){
        _this._color_scale_click(color_scale, _this.navigation_layer);
      });

      _this.navigation_layer.add(color_scale);
  }

  InCHlib.prototype._update_color_scale = function(){
    var color_scale = _this.navigation_layer.find("#color_scale");
    color_scale.fillLinearGradientColorStops([_this.settings.min_percentile/100, _this._get_color_for_value(0, 0, 1, 0.5, _this.settings.heatmap_colors), _this.settings.middle_percentile/100, _this._get_color_for_value(0.5, 0, 1, 0.5, _this.settings.heatmap_colors), _this.settings.max_percentile/100, _this._get_color_for_value(1, 0, 1, 0.5, _this.settings.heatmap_colors)]);
    _this.navigation_layer.draw();
  }

  InCHlib.prototype._draw_icon_overlay = function(x, y){
      return _this.objects_ref.icon_overlay.clone({x: x, y: y});
  }

  InCHlib.prototype._highlight_path = function(path_id, color){
      var node = _this.data.nodes[path_id];
      if(node.count != 1){
          _this.dendrogram_layer.get("#"+path_id)[0].stroke(color);
          _this._highlight_path(node.left_child, color);
          _this._highlight_path(node.right_child, color);
      }
      else{
          _this.highlighted_rows_y.push(_this.leaves_y_coordinates[path_id]);
          _this.current_object_ids.push.apply(_this.current_object_ids, node["objects"])
       }
   }

   InCHlib.prototype._highlight_column_path = function(path_id, color){
      var node = _this.column_dendrogram.nodes[path_id];
      if(node.count != 1){
          _this.column_dendrogram_layer.get("#col"+path_id)[0].stroke(color);
          _this._highlight_column_path(node.left_child, color);
          _this._highlight_column_path(node.right_child, color);
      }
      else{
        _this.current_column_ids.push(_this.nodes2columns[path_id]);
      }
   }

  /**
    * Unhighlight highlighted heatmap rows. 
    *
    * @example 
    * instance.unhighlight_rows();
    */
   InCHlib.prototype.unhighlight_rows = function(){
      _this.highlight_rows([]);
   }

  /**
    * Highlight heatmap rows with color defined in instance.settings.highlight_colors. 
    * When the empty array is passed it unhighlights all highlighted rows.
    * 
    * @param {object} [row_ids] The array of heatmap row (object) IDs.
    *
    * @example 
    * instance.highlight_rows(["CHEMBL7781", "CHEMBL273658", "CHEMBL415309", "CHEMBL267231", "CHEMBL8007", "CHEMBL7987", "CHEMBL7988", "CHEMBL266282", "CHEMBL7655", "CHEMBL7817", "CHEMBL8637", "CHEMBL8639", "CHEMBL8055", "CHEMBL7843", "CHEMBL266488", "CHEMBL8329"]);
    */

   InCHlib.prototype.highlight_rows = function(row_ids){
      var i, row, row_id;
      if(!_this.settings.heatmap){
        return;
      }

      _this.settings.highlighted_rows = row_ids;
      _this.highlighted_rows_layer.destroyChildren();

      var original_colors = _this.settings.heatmap_colors;
      var original_metadata_colors = _this.settings.metadata_colors;
      _this.settings.heatmap_colors = _this.settings.highlight_colors;
      _this.settings.metadata_colors = _this.settings.highlight_colors;

      var done_rows = {};
      var unique_row_ids = [];

      for(i = 0; i<row_ids.length; i++){
          if(_this.objects2leaves[row_ids[i]] !== undefined){
              row_id = _this.objects2leaves[row_ids[i]];
              if(done_rows[row_id] === undefined){
                  unique_row_ids.push(row_id);
                  done_rows[row_id] = null;
              }
          }
      }

      for(i = 0; i<unique_row_ids.length; i++){
          row = _this._draw_heatmap_row(unique_row_ids[i], _this.heatmap_distance, _this.leaves_y_coordinates[unique_row_ids[i]]);
          _this.highlighted_rows_layer.add(row);
          row.setAttr("listening", false);
      }


      _this.highlighted_rows_layer.draw();
      _this.heatmap_overlay.moveToTop();

      _this.settings.heatmap_colors = original_colors;
      _this.settings.metadata_colors = original_metadata_colors;


      _this.highlighted_rows_layer.on("click", function(evt){
          _this.heatmap_layer.fire("click");
      });
      
  }

  InCHlib.prototype._highlight_cluster = function(path_id){
    var previous_cluster = _this.last_highlighted_cluster;
    
    if(previous_cluster){
      _this.unhighlight_cluster();
    }

    if(previous_cluster !== path_id){
      _this.last_highlighted_cluster = path_id;
      _this._highlight_path(path_id, "#F5273C");
      _this._draw_cluster_layer(path_id);
      _this.events.dendrogram_node_highlight(_this.current_object_ids, _this._unprefix(path_id));
    }
    _this.dendrogram_layer.draw();
  }

  InCHlib.prototype._highlight_column_cluster = function(path_id){
      var previous_cluster = _this.last_highlighted_column_cluster;
      if(previous_cluster){
        _this.unhighlight_column_cluster()
      }
      if(previous_cluster !== path_id){
        _this.last_highlighted_column_cluster = path_id;
        _this._highlight_column_path(path_id, "#F5273C");
        _this.current_column_ids.sort(function(a,b){return a - b});
        _this._draw_column_cluster_layer(path_id);
        _this.events.column_dendrogram_node_highlight(_this.current_column_ids, _this._unprefix(path_id));
      }
      _this.column_dendrogram_layer.draw();
  }

  InCHlib.prototype.unhighlight_column_cluster = function(){
      if(_this.last_highlighted_column_cluster){
        _this._highlight_column_path(_this.last_highlighted_column_cluster, "grey");
        _this.column_dendrogram_layer.draw();
        _this.column_cluster_group.destroy();
        _this.cluster_layer.draw();
        _this.current_column_ids = [];
        _this.events.column_dendrogram_node_unhighlight(_this._unprefix(_this.last_highlighted_column_cluster));
        _this.last_highlighted_column_cluster = null;
      }
  }

  /**
    * Highlight cluster defined by the dendrogram node ID.
    * 
    * @param {string} node_id The ID of particular node in dendrogram.
    *
    * @example 
    * instance.highlight_cluster("node@715");
    */

  InCHlib.prototype.highlight_cluster = function(node_id){
      return _this._highlight_cluster(_this._prefix(node_id));
  }

  /**
    * Highlight column cluster defined by the dendrogram node ID.
    * 
    * @param {string} node_id The ID of particular node in dendrogram.
    *
    * @example 
    * instance.highlight_column_cluster("node@715");
    */

  InCHlib.prototype.highlight_column_cluster = function(node_id){
      return _this._highlight_column_cluster(_this._prefix(node_id));
  }

  /**
    * Unhighlight highlighted dendrogram node (cluster).
    *
    * @example 
    * instance.unhighlight_cluster();
    */
  InCHlib.prototype.unhighlight_cluster = function(){
    if(_this.last_highlighted_cluster){
      _this._highlight_path(_this.last_highlighted_cluster, "grey");
      _this.dendrogram_layer.draw();
      _this.row_cluster_group.destroy();
      _this.cluster_layer.draw();
      _this.events.dendrogram_node_unhighlight(_this._unprefix(_this.last_highlighted_cluster));
      _this.highlighted_rows_y = [];
      _this.current_object_ids = [];
      _this.last_highlighted_cluster = null;
    }
  }

  InCHlib.prototype._neutralize_path = function(path_id){
      var node = _this.data.nodes[path_id];

      if(node.count != 1){
          var path = _this.dendrogram_layer.get("#"+path_id)[0];
          if(path){
              path.setStroke("grey");
              _this._neutralize_path(node.right_child);
              _this._neutralize_path(node.left_child);
          }
      }
  }

  InCHlib.prototype._draw_cluster_layer = function(path_id){
      _this.row_cluster_group = new Kinetic.Group();
      var visible = _this._get_visible_count();
      var count = _this.data.nodes[path_id].count;
      var x = _this.distance - 30;
      var y = _this.header_height + _this.column_metadata_height - 40;

      var rows_desc = _this.objects_ref.count.clone({x: x + 10,
                                                    y: y - 10,
                                                    text: count,
                                                    });

      var zoom_icon = _this.objects_ref.icon.clone({
                      data: _this.paths_ref["zoom_icon"],
                      x: x,
                      y: y,
                      scale: {x: 0.7, y: 0.7},
                      label: "Zoom\nrows",
                  });


      var zoom_overlay = _this._draw_icon_overlay(x, y);

      x = _this.distance + _this.dendrogram_heatmap_distance;
      var width = visible*_this.pixels_for_dimension+_this.heatmap_distance;
      var upper_y = _this.highlighted_rows_y[0]-_this.pixels_for_leaf/2;
      var lower_y = _this.highlighted_rows_y[_this.highlighted_rows_y.length-1]+_this.pixels_for_leaf/2;
      
      var cluster_overlay_1 = _this.objects_ref.cluster_overlay.clone({
          x: x,
          y: _this.header_height + _this.column_metadata_height + 5,
          width: width,
          height: _this._hack_round(upper_y -_this.header_height - _this.column_metadata_height - 5),
      });

      var cluster_border_1 = _this.objects_ref.cluster_border.clone({
          points: [0, upper_y, width, upper_y],
      });

      var cluster_overlay_2 = _this.objects_ref.cluster_overlay.clone({
          x: x,
          y: lower_y,
          width: width,
          height: _this.settings.height-lower_y-_this.footer_height + 5,
      });

      var cluster_border_2 = _this.objects_ref.cluster_border.clone({
          points: [0, lower_y, width, lower_y],
      });

      _this.row_cluster_group.add(rows_desc, cluster_overlay_1, cluster_overlay_2, zoom_icon, zoom_overlay, cluster_border_1, cluster_border_2);
      _this.cluster_layer.add(_this.row_cluster_group);
      _this.stage.add(_this.cluster_layer);
      rows_desc.moveToTop();

      _this.cluster_layer.draw();
      _this.navigation_layer.moveToTop();

      zoom_overlay.on("mouseover", function(){
          _this._icon_mouseover(zoom_icon, zoom_overlay, _this.cluster_layer);
      });

      zoom_overlay.on("mouseout", function(){
          _this._icon_mouseout(zoom_icon, zoom_overlay, _this.cluster_layer);
      });

      zoom_overlay.on("click", function(){
        _this._zoom_cluster(_this.last_highlighted_cluster);
      });
  }

  InCHlib.prototype._draw_column_cluster_layer = function(path_id){
      _this.column_cluster_group = new Kinetic.Group();
      var count = _this.column_dendrogram.nodes[path_id].count;      
      var x = _this.settings.width - 85;
      var y = _this.header_height - 25;

      var cols_desc = _this.objects_ref.count.clone({x: x + 15,
                                                        y: y - 5,
                                                        text: count,
                                                    });

      var zoom_icon = _this.objects_ref.icon.clone({
                      data: _this.paths_ref["zoom_icon"],
                      x: x,
                      y: y,
                      scale: {x: 0.7, y: 0.7},
                      label: "Zoom\ncolumns",
                  });

      var zoom_overlay = _this._draw_icon_overlay(x, y);

      var x1 = _this._hack_round((_this.current_column_ids[0] - _this.columns_start_index)*_this.pixels_for_dimension);
      var x2 = _this._hack_round((_this.current_column_ids[0] + _this.current_column_ids.length - _this.columns_start_index)*_this.pixels_for_dimension);
      var y1 = 0;
      var y2 = _this.settings.height-_this.footer_height+5;
      var height = _this.settings.height-_this.footer_height-_this.header_height+_this.settings.column_metadata_row_height;    
      
      var cluster_border_1 = _this.objects_ref.cluster_border.clone({
          points: [_this.heatmap_distance + x1, y1, _this.heatmap_distance + x1, y2],
      });

      var cluster_overlay_1 = _this.objects_ref.cluster_overlay.clone({
          x: _this.heatmap_distance,
          y: _this.header_height,
          width: x1,
          height: height,
      });

      var cluster_border_2 = _this.objects_ref.cluster_border.clone({
          points: [_this.heatmap_distance + x2, y1, _this.heatmap_distance + x2, y2],
      });

      var cluster_overlay_2 = _this.objects_ref.cluster_overlay.clone({
          x: x2+_this.heatmap_distance,
          y: _this.header_height,
          width: _this.heatmap_width - x2 - (_this.on_features["metadata"].length + _this.on_features["count_column"].length)*_this.pixels_for_dimension,
          height: height,
      });
    

      _this.column_cluster_group.add(cluster_overlay_1, cluster_overlay_2, zoom_icon, zoom_overlay, cols_desc, cluster_border_1, cluster_border_2);
      _this.cluster_layer.add(_this.column_cluster_group);
      _this.stage.add(_this.cluster_layer);
      _this.cluster_layer.draw();
      _this.navigation_layer.moveToTop();

      zoom_overlay.on("mouseover", function(){
          _this._icon_mouseover(zoom_icon, zoom_overlay, _this.cluster_layer);
      });

      zoom_overlay.on("mouseout", function(){
          _this._icon_mouseout(zoom_icon, zoom_overlay, _this.cluster_layer);
      });

      zoom_overlay.on("click", function(){
          _this._zoom_column_cluster(_this.last_highlighted_column_cluster);
      });
  }

  InCHlib.prototype._draw_column_cluster = function(node_id){
      _this.columns_start_index = _this.current_column_ids[0];
      _this.on_features["data"] = _this.current_column_ids;
      var distance = _this.distance;
      _this._adjust_horizontal_sizes();
      _this._delete_layers([_this.column_dendrogram_layer, _this.heatmap_layer, _this.heatmap_overlay, _this.column_cluster_group, _this.navigation_layer, _this.highlighted_rows_layer], [_this.dendrogram_hover_layer]);
      if(_this.settings.heatmap_header){
        _this._delete_layers([_this.header_layer]);
      }
      _this._draw_column_dendrogram(node_id);
      _this._draw_heatmap();
      _this._draw_heatmap_header();
      _this._draw_navigation();

      if(distance !== _this.distance){
        _this._delete_layers([_this.dendrogram_layer, _this.cluster_layer]);
        var row_node = (_this.zoomed_clusters["row"].length > 0)?_this.zoomed_clusters["row"][_this.zoomed_clusters["row"].length - 1]:_this.root_id;
        _this._draw_row_dendrogram(row_node);
        if(_this.last_highlighted_cluster !== null){
          _this._highlight_path(_this.last_highlighted_cluster, "#F5273C");
          _this.dendrogram_layer.draw();
          _this._draw_cluster_layer(_this.last_highlighted_cluster);
        }
      }
      else{
        _this.cluster_layer.moveToTop();
        _this.cluster_layer.draw();
      }
  }

  InCHlib.prototype._zoom_column_cluster = function(node_id){
    if(node_id != _this.column_root_id){
      _this.zoomed_clusters["column"].push(node_id);
      _this._draw_column_cluster(node_id);
      _this.highlight_rows(_this.settings.highlighted_rows);
      _this.events.on_columns_zoom(_this.current_column_ids, _this._unprefix(node_id));
      _this.current_column_ids = [];
      _this.last_highlighted_column_cluster = null;
    }
  }

  InCHlib.prototype._unzoom_column_cluster = function(){
    var unzoomed = _this.zoomed_clusters["column"].pop();
    var zoomed_count = _this.zoomed_clusters["column"].length;
    var node_id = (zoomed_count > 0)?_this.zoomed_clusters["column"][zoomed_count-1]:_this.column_root_id;
    _this._get_column_ids(node_id);
    _this._draw_column_cluster(node_id);
    _this.events.on_columns_unzoom(_this._unprefix(unzoomed));
    _this.current_column_ids = [];
    _this._highlight_column_cluster(unzoomed);
  }

  InCHlib.prototype._draw_cluster = function(node_id){
    _this._delete_layers([_this.dendrogram_layer, _this.heatmap_layer, _this.heatmap_overlay, _this.cluster_layer, _this.navigation_layer, _this.header_layer, _this.highlighted_rows_layer], [_this.dendrogram_hover_layer]);
    _this._draw_row_dendrogram(node_id);
    _this._draw_heatmap();
    _this._draw_heatmap_header();
    _this._draw_navigation();
    if(_this.settings.column_dendrogram && _this.last_highlighted_column_cluster !== null){
      _this._draw_column_cluster_layer(_this.last_highlighted_column_cluster);
    }
  }

  InCHlib.prototype._zoom_cluster = function(node_id){
    if(node_id !== _this.root_id){
      _this.zoomed_clusters["row"].push(node_id);
      _this._draw_cluster(node_id);
      _this.highlight_rows(_this.settings.highlighted_rows);
      _this.events.on_zoom(_this.current_object_ids, _this._unprefix(node_id));
      _this.current_object_ids = [];
      _this.last_highlighted_cluster = null;
    }
  }

  InCHlib.prototype._unzoom_cluster = function(){
    var unzoomed = _this.zoomed_clusters["row"].pop();
    var zoomed_count = _this.zoomed_clusters["row"].length;
    var node_id = (zoomed_count > 0)?_this.zoomed_clusters["row"][zoomed_count-1]:_this.root_id;
    _this._draw_cluster(node_id);
    _this.events.on_unzoom(_this._unprefix(unzoomed));
    _this._highlight_cluster(unzoomed);
  }

  InCHlib.prototype._get_node_neighbourhood = function(node, nodes){
      var node_neighbourhood = {"left_node": {"left_node": {"left_count" : 0,
                                                            "right_count": 0}, 
                                              "right_node": {"left_count" : 0,
                                                             "right_count": 0},
                                              "left_count" : 0.5,
                                              "right_count": 0.5
                                             },
                                "right_node": {"left_node": {"left_count" : 0,
                                                            "right_count": 0}, 
                                              "right_node": {"left_count" : 0,
                                                             "right_count": 0},
                                              "left_count" : 0.5,
                                              "right_count": 0.5
                                             },
                                "left_count": nodes[node.left_child].count,
                                "right_count": nodes[node.right_child].count,
      };

      var left_child = nodes[node.left_child];
      var right_child = nodes[node.right_child];

      var left_child_left_child = nodes[left_child.left_child];
      var left_child_right_child = nodes[left_child.right_child];

      var right_child_left_child = nodes[right_child.left_child];
      var right_child_right_child = nodes[right_child.right_child];

      if(left_child.count != 1){
              node_neighbourhood.left_node.left_count = nodes[left_child.left_child].count;
              node_neighbourhood.left_node.right_count = nodes[left_child.right_child].count;

          if(left_child_left_child.count != 1){
              node_neighbourhood.left_node.left_node.left_count = nodes[left_child_left_child.left_child].count;
              node_neighbourhood.left_node.left_node.right_count = nodes[left_child_left_child.right_child].count;
          }
          else{
              node_neighbourhood.left_node.left_node.left_count = 0.5;
              node_neighbourhood.left_node.left_node.right_count = 0.5;
          }

          if(left_child_right_child.count != 1){
              node_neighbourhood.left_node.right_node.left_count = nodes[left_child_right_child.left_child].count;
              node_neighbourhood.left_node.right_node.right_count = nodes[left_child_right_child.right_child].count;
          }
          else{
              node_neighbourhood.left_node.right_node.left_count = 0.5;
              node_neighbourhood.left_node.right_node.right_count = 0.5;
          }
      }

      if(right_child.count != 1){
          node_neighbourhood.right_node.left_count = nodes[right_child.left_child].count;
          node_neighbourhood.right_node.right_count = nodes[right_child.right_child].count;

          if(right_child_left_child.count != 1){
              node_neighbourhood.right_node.left_node.left_count = nodes[right_child_left_child.left_child].count;
              node_neighbourhood.right_node.left_node.right_count = nodes[right_child_left_child.right_child].count;
          }
          else{
              node_neighbourhood.right_node.left_node.left_count = 0.5;
              node_neighbourhood.right_node.left_node.right_count = 0.5;
          }

          if(right_child_right_child.count != 1){
              node_neighbourhood.right_node.right_node.left_count = nodes[right_child_right_child.left_child].count;
              node_neighbourhood.right_node.right_node.right_count = nodes[right_child_right_child.right_child].count;
          }
          else{
              node_neighbourhood.right_node.right_node.left_count = 0.5;
              node_neighbourhood.right_node.right_node.right_count = 0.5;
          }
      }
      return node_neighbourhood;
  }

  InCHlib.prototype._draw_column_dendrogram_node = function(node_id, node, current_left_count, current_right_count, x, y){
      
      if(node.count > 1){
          var node_neighbourhood = _this._get_node_neighbourhood(node, _this.column_dendrogram.nodes);
          var right_child = _this.column_dendrogram.nodes[node.right_child];
          var left_child = _this.column_dendrogram.nodes[node.left_child];
          var x1 = _this._get_x1(node_neighbourhood, current_left_count, current_right_count);
          var x2 = _this._get_x2(node_neighbourhood, current_left_count, current_right_count);
          var y1 = _this._hack_round(_this.vertical_distance - _this.vertical_distance_step*node.distance);
          y1 = (y1 == 0)? 2: y1;
          var y2 = y1;

          if(right_child.count == 1){
              x2 = x2 - _this.pixels_for_dimension/2;
          }

          var left_distance = _this.vertical_distance - _this.vertical_distance_step*_this.column_dendrogram.nodes[node.left_child].distance;
          var right_distance = _this.vertical_distance - _this.vertical_distance_step*_this.column_dendrogram.nodes[node.right_child].distance;

          _this.column_dendrogram_layer.add(_this._draw_vertical_path(node_id, x1, y1, x2, y2, left_distance, right_distance));
          _this._draw_column_dendrogram_node(node.left_child, left_child, current_left_count - node_neighbourhood.left_node.right_count, current_right_count + node_neighbourhood.left_node.right_count, left_distance, y1);
          _this._draw_column_dendrogram_node(node.right_child, right_child, current_left_count + node_neighbourhood.right_node.left_count, current_right_count - node_neighbourhood.right_node.left_count, right_distance, y2);
      }
      else{
        _this.column_x_coordinates[node_id] = current_right_count*_this.pixels_for_dimension;
      }
  }

  InCHlib.prototype._get_y1 = function(node_neighbourhood, current_left_count, current_right_count){
      current_left_count = current_left_count-node_neighbourhood.left_node.right_count-node_neighbourhood.left_node.left_node.right_count;
      var y = (current_left_count+(node_neighbourhood.left_node.left_node.right_count+node_neighbourhood.left_node.right_node.left_count)/2)*_this.pixels_for_leaf;
      return y + _this.top_heatmap_distance;
  }

  InCHlib.prototype._get_y2 = function(node_neighbourhood, current_left_count, current_right_count){
      current_left_count = current_left_count+node_neighbourhood.right_node.left_node.left_count;
      var y = (current_left_count+(node_neighbourhood.right_node.left_node.right_count+node_neighbourhood.right_node.right_node.left_count)/2)*_this.pixels_for_leaf;
      return y + _this.top_heatmap_distance;
  }

  InCHlib.prototype._get_x1 = function(node_neighbourhood, current_left_count, current_right_count){
      current_left_count = current_left_count-node_neighbourhood.left_node.right_count-node_neighbourhood.left_node.left_node.right_count;
      var x = (current_left_count+(node_neighbourhood.left_node.left_node.right_count+node_neighbourhood.left_node.right_node.left_count)/2)*_this.pixels_for_dimension;
      return (_this.heatmap_distance+_this.on_features["data"].length * _this.pixels_for_dimension)-x;
  }

  InCHlib.prototype._get_x2 = function(node_neighbourhood, current_left_count, current_right_count){
      current_left_count = current_left_count+node_neighbourhood.right_node.left_node.left_count;
      var x = (current_left_count+(node_neighbourhood.right_node.left_node.right_count+node_neighbourhood.right_node.right_node.left_count)/2)*_this.pixels_for_dimension;;
      return (_this.heatmap_distance+_this.on_features["data"].length * _this.pixels_for_dimension)-x;
  }

  InCHlib.prototype._draw_vertical_path = function(path_id, x1, y1, x2, y2, left_distance, right_distance){
      var path_group = new Kinetic.Group({});
      var path = _this.objects_ref.node.clone({points: [x1, left_distance, x1, y1, x2, y2, x2, right_distance], id: "col" + path_id,})
      var path_rect = _this.objects_ref.node_rect.clone({x: x2-1,
                                                            y: y1-1,
                                                            width: x1 - x2 + 2,
                                                            height: _this.header_height - y1,
                                                            id: "col_rect" + path_id,
                                                            path: path,
                                                            path_id: path_id,
                                                          });

      path_group.add(path, path_rect);
      return path_group;
  }

  InCHlib.prototype._draw_horizontal_path = function(path_id, x1, y1, x2, y2, left_distance, right_distance){
      var path_group = new Kinetic.Group({});
      var path = _this.objects_ref.node.clone({points: [left_distance, y1, x1, y1, x2, y2, right_distance, y2],
                                                  id: path_id});

      var path_rect = _this.objects_ref.node_rect.clone({x: x1-1,
                                                            y: y1-1,
                                                            width: _this.distance - x1,
                                                            height: y2 - y1,
                                                            id: [path_id, "rect"].join("_"),
                                                            path: path,
                                                            path_id: path_id,
                                                          });
      path_group.add(path, path_rect);
      return path_group;
  }

  InCHlib.prototype._filter_icon_click = function(filter_button){
      var filter_features_element = _this.target_element.find(".filter_features");
      var symbol = "âś–";

      if(filter_features_element.length){
          filter_features_element.fadeIn("fast");
          var overlay = _this._draw_target_overlay();
      }
      else{
          filter_list = "";
          
          for(var attr in _this.header){
              if(_this.features[attr]){
                  symbol = "âś”";
              }
              if(attr < _this.dimensions){
                  var text = _this.header[attr];
                  if(text == ""){
                      text =  parseInt(attr) + 1 + ". column";
                  }
                  filter_list = filter_list + "<li class='feature_switch' data-num='" + attr + "'><span class='symbol'>" + symbol + "</span>  " + text +"</li>";
              }
          }
          
          _this.target_element.append("<div class='filter_features'><ul>" + filter_list + "</ul><hr /><div><span class='cancel_filter_list'>Cancel</span>&nbsp;&nbsp;&nbsp;<span class='update_filter_list'>Update</span></div></div>");
          filter_features_element = _this.target_element.find(".filter_features");
          
          filter_features_element.css({"display":"none",
              "top": 45,
              "left": 0,
              "border-radius":"5px",
              "text-align":"center",
              "position":"absolute",
              "background-color":"#ffffff",
              "border":"solid 2px #DEDEDE",
              "padding-top":"5px",
              "padding-left":"15px",
              "padding-bottom":"10px",
              "padding-right":"15px",
              "font-weight":"bold",
              "font-size": "14px",
              "z-index": 1000,
              "font-family": _this.settings.font
          });

          filter_features_element.find("ul").css({
              "list-style-type":"none",
              "margin-left":"0",
              "padding-left":"0",
              "text-align":"left",
          });

          filter_features_element.find("li").css({
              "color":"green",
              "margin-top":"5px",
          });

          filter_features_element.find("div").css({
              "cursor":"pointer",
              "opacity":"0.7",
          });

          var overlay = _this._draw_target_overlay();
          filter_features_element.fadeIn("fast");

          _this.target_element.find(".feature_switch").click(function(){
              var num = parseInt($(this).attr("data-num"));
              var symbol_element = $(this).find("span");
              _this.features[num] = !_this.features[num];

              if(_this.features[num]){
                  symbol_element.text("âś”");
                  $(this).css("color", "green");
              }
              else{
                  symbol_element.text("âś–");
                  $(this).css("color", "red");
              }

              _this._set_on_features();
          });

          $(function(){
              filter_features_element.click(function(){
                  return false;
              });

              filter_features_element.mousedown(function(){
                  return false;
              });

             $("#" + _this.settings.target + " .filter_features ul li," + "#" + _this.settings.target + " .filter_features div span").hover(
             function(){
                $(this).css({
                      "cursor": "pointer",
                      "opacity": "0.7",
                  });
             },
             function(){
                $(this).css({
                      "cursor": "default",
                      "opacity": "1",
                  });
             });
          });

          _this.target_element.find(".cancel_filter_list").click(function(){
              filter_features_element.fadeOut("fast");
              overlay.fadeOut("fast");
          });

          overlay.click(function(){
              filter_features_element.fadeOut("fast");
              overlay.fadeOut("fast");
          });

          _this.target_element.find(".update_filter_list").click(function(){
              filter_features_element.fadeOut("slow");
              overlay.fadeOut("slow");

              var node_id = (_this.zoomed_clusters["row"].length > 0)?_this.zoomed_clusters["row"][_this.zoomed_clusters["row"].length-1]:_this.root_id;
              var highlighted_cluster = _this.last_highlighted_cluster;
              _this.last_highlighted_cluster = null;
              _this._adjust_horizontal_sizes();              
              _this._delete_all_layers();
              _this._draw_stage_layer();
              if(_this.settings.dendrogram){
                _this._draw_dendrogram_layers();
                _this._draw_row_dendrogram(node_id);
                if(_this.settings.column_dendrogram && _this._visible_features_equal_column_dendrogram_count()){
                  _this._draw_column_dendrogram(_this.column_root_id);
                }
              }

              _this._draw_navigation();
              _this._draw_heatmap();
              _this._draw_heatmap_header();

              if(highlighted_cluster != null){
                  _this._highlight_cluster(highlighted_cluster);
              }
          });
      }
  }
  InCHlib.prototype._draw_target_overlay = function(){
    var overlay = _this.target_element.find(".target_overlay");

    if(overlay.length){
      overlay.fadeIn("fast");
    }
    else{
      overlay = $("<div class='target_overlay'></div>");
      overlay.css({"background-color": "white", 
                      "position": "absolute",
                      "top": 0,
                      "left": 0,
                      "right": 0,
                      "bottom": 0,
                      "opacity": 0.5
          });
      _this.target_element.append(overlay);
    }

    return overlay;
  }

  InCHlib.prototype._refresh_icon_click = function(){
    _this.redraw();
  }

  InCHlib.prototype._export_icon_click = function(){
    var export_menu = _this.target_element.find(".export_menu");
    var overlay = _this._draw_target_overlay();

    if(export_menu.length){
      export_menu.fadeIn("fast");
    }
    else{
      export_menu = $("<div class='export_menu'><div><button type='submit' data-action='open'>Show image</button></div><div><button type='submit' data-action='save'>Save image</button></div></div>");
      _this.target_element.append(export_menu);
      export_menu.css({"position": "absolute",
                      "top": 45,
                      "left": _this.settings.width - 125,
                      "font-size": "12px",
                      "border": "solid #D2D2D2 1px",
                      "border-radius": "5px",
                      "padding": "2px",
                      "background-color": "white"});

      var buttons = export_menu.find("button");
      buttons.css({"padding-top": "7px", "padding-bottom": "5px", "padding-right": "8px", "padding-left": "8px", "color": "white", "border": "solid #D2D2D2 1px", "width": "100%", "background-color": "#2171b5", "font-weight": "bold"});  

      buttons.hover(
        function(){$(this).css({"cursor": "pointer", "opacity": 0.7})},
        function(){$(this).css({"opacity": 1})}
      );

      overlay.click(function(){
        export_menu.fadeOut("fast");
        overlay.fadeOut("fast");
      });

      buttons.click(function(){
        var action = $(this).attr("data-action");
        var zoom = 3;
        var width = _this.stage.width();
        var height = _this.stage.height();
        var loading_div = $("<h3 style='margin-top: 100px; margin-left: 100px; width: " + width + "px; height: " + height + "px;'>Loading...</h3>");
        _this.target_element.after(loading_div);
        _this.target_element.hide();
        _this.stage.width(width*zoom);
        _this.stage.height(height*zoom);
        _this.stage.scale({x: zoom, y:zoom});
        _this.stage.draw();
        _this.navigation_layer.hide();
        _this.stage.toDataURL({
          quality: 1,
          callback: function(dataUrl){
            if(action === "open"){
              open_image(dataUrl);
            }
            else{
              download_image(dataUrl);
            }
            _this.stage.width(width);
            _this.stage.height(height);
            _this.stage.scale({x: 1, y:1});
            _this.stage.draw();
            loading_div.remove();
            _this.target_element.show();
            _this.navigation_layer.show();
            _this.navigation_layer.draw();
            overlay.trigger("click");
          }
        });
      });
    }

    function download_image(dataUrl){
      $('<a download="inchlib" href="'+ dataUrl + '"></a>')[0].click();
    };
    
    function open_image(dataUrl){
      window.open(dataUrl, "_blank");
    };
  };

  InCHlib.prototype._color_scale_click = function(icon, evt){
    var i, option, key, value;
    var color_options = {"heatmap_colors": "Heatmap data colors"};

    var value_options = {"max_percentile": "Max percentile value",
                        "middle_percentile": "Middle percentile value",
                        "min_percentile": "Min percentile value",
                      };

    if(_this.settings.metadata){
      color_options["metadata_colors"] = "Metadata colors";
    }

    if(_this.settings.column_metadata){
      color_options["column_metadata_colors"] = "Column metadata colors";
    }

    var form_id = "settings_form_" + _this.settings.target;
    var settings_form = $("#" + form_id);
    var overlay = _this._draw_target_overlay();

    if(settings_form.length){
      settings_form.fadeIn("fast");
    }
    else{
      settings_form = $("<form class='settings_form' id='" + form_id + "'></form>");
      var options = "", color_1, color_2, color_3;

      for(i = 0, keys = Object.keys(color_options), len = keys.length; i < len; i++){
        key = keys[i];
        color_1 = _this._get_color_for_value(0,0,1,0.5,_this.settings[key]);
        color_2 = _this._get_color_for_value(0.5,0,1,0.5,_this.settings[key]);
        color_3 = _this._get_color_for_value(1,0,1,0.5,_this.settings[key]);

        option = "<div><div class='form_label'>" + color_options[key] + "</div><input type='text' name='" + key +"' value='"+ _this.settings[key] + "'/> <div class='color_button' style='background: linear-gradient(to right, " + color_1 + "," + color_2 + "," + color_3 + ")'></div></div>";
        options += option;
      }

      for(i = 0, keys = Object.keys(value_options), len = keys.length; i < len; i++){
        key = keys[i];
        option = "<div><div class='form_label'>" + value_options[key] + "</div><input type='text' name='" + key +"' value='"+ _this.settings[key] + "'/></div>";
        options += option;
      }
      option = "<div><div class='form_label'>Heatmap coloring</div>\
                <select name='independent_columns'>"
      
      if(_this.settings.independent_columns){
        option += "<option value='true' selected>By columns</option>\
                  <option value='false'>Entire heatmap</option>"
      }
      else{
        option += "<option value='true'>By columns</option>\
                  <option value='false' selected>Entire heatmap</option>" 
      }
      option += "</select></div>";
      options += option;

      options = options + '<button type="submit">Redraw</button>'
      settings_form.html(options);

      _this.target_element.append(settings_form);
      settings_form.css({"z-index": 1000, "position": "absolute", "top": 110, "left": 0, "padding": "10px", "border": "solid #D2D2D2 2px", "border-radius": "5px", "background-color": "white"});
      $("#" + form_id + " .color_button").css({"border": "solid #D2D2D2 1px", "height": "15px", "width": "30px", "display": "inline-block"});  
      $("#" + form_id + " > div").css({"font-size": "12px", "margin-bottom": "10px"});  
      $("#" + form_id + " input").css({"border-radius": "5px", "width": "100px"});  
      $("#" + form_id + " .form_label").css({"color": "gray", "margin-bottom": "5px", "font-style": "italic"});  
      $("#" + form_id + " button").css({"padding-top": "7px", "padding-bottom": "5px", "padding-right": "5px", "padding-left": "5px", "color": "white", "border": "solid #D2D2D2 1px", "border-radius": "5px", "width": "100%", "background-color": "#2171b5", "font-weight": "bold"});  

      overlay.click(function(){
        settings_form.fadeOut("fast");
        overlay.fadeOut("fast");
      });

      var color_buttons = $("#" + form_id + " .color_button");
      
      color_buttons.hover(
        function(){$(this).css({"cursor": "pointer", "opacity": 0.7})},
        function(){$(this).css({"opacity": 1})}
      );

      color_buttons.click(function(evt){
        _this._draw_color_scales_select(this, evt);
      });

      settings_form.submit(function(evt){
        var settings = {};
        var settings_fieldset = $(this).find("input, select");

        settings_fieldset.each(function(){
            option = $(this);
            key = option.attr("name");
            value = option.val();
            if(value != ""){
                if(value === "true"){
                    value = true;
                }
                else if(value === "false"){
                  value = false;
                }
                settings[key] = value;
            }
        });
        _this.update_settings(settings);
        _this.redraw_heatmap();
        _this._update_color_scale();
        overlay.trigger('click');
        evt.preventDefault();
        evt.stopPropagation();
      })
    }
  }

  InCHlib.prototype._draw_color_scales_select = function(element, evt){
    var scales_div = _this.target_element.find(".color_scales");
    var scale_divs;

    if(scales_div.length){
      scales_div.fadeIn("fast");
      scale_divs = scales_div.find(".color_scale");
    }
    else{
      scales_div = $("<div class='color_scales'></div>");
      var scale, color_1, color_2, color_3, key;

      for(var i = 0, keys = Object.keys(_this.colors), len = keys.length; i < len; i++){
        key = keys[i];
        color_1 = _this._get_color_for_value(0,0,1,0.5,key);
        color_2 = _this._get_color_for_value(0.5,0,1,0.5,key);
        color_3 = _this._get_color_for_value(1,0,1,0.5,key);
        scale = "<div class='color_scale' data-scale_acronym='" + key + "' style='background: linear-gradient(to right, " + color_1 + "," + color_2 + "," + color_3 + ")'></div>";
        scales_div.append(scale);
      }
      _this.target_element.append(scales_div);
      scales_div.css({"border": "solid #D2D2D2 2px",
                     "border-radius": "5px",
                     "padding": "5px",
                     "position": "absolute",
                     "top": 110,
                     "left": 170,
                     "background-color": "white"});

      scale_divs = _this.target_element.find(".color_scale");
      scale_divs.css({"margin-top":"3px",
                      "width": "80px",
                      "height": "20px",
                      "border": "solid #D2D2D2 1px",});

      scale_divs.hover(
        function(){$(this).css({"cursor": "pointer", "opacity": 0.7})},
        function(){$(this).css({"opacity": 1})}
      );

      _this.target_element.find(".target_overlay").click(function(){
        scales_div.fadeOut("fast");
      });
    }

    scale_divs.on("click", function(){
      var color = $(this).attr("data-scale_acronym");
      var input = $(element).prev("input:first").val(color);
      $(element).css({"background": "linear-gradient(to right, " + _this._get_color_for_value(0,0,1,0.5,color) + "," + _this._get_color_for_value(0.5,0,1,0.5,color) + "," + _this._get_color_for_value(1,0,1,0.5,color) + ")"})
      scales_div.fadeOut("fast");
      scale_divs.off("click");
    });

  };

  InCHlib.prototype._color_scale_mouseover = function(color_scale, layer){
      var label = color_scale.getAttr("label");
      var x = color_scale.getAttr("x");
      var y = color_scale.getAttr("y");

      _this.icon_tooltip = _this.objects_ref.tooltip_label.clone({x: x,
          y: y + 25
      });

      _this.icon_tooltip.add(_this.objects_ref.tooltip_tag.clone());
      _this.icon_tooltip.add(_this.objects_ref.tooltip_text.clone({text: label}));

      layer.add(_this.icon_tooltip);
      _this.icon_tooltip.moveToTop();
      color_scale.setOpacity(0.7);
      layer.draw();
  }

  InCHlib.prototype._color_scale_mouseout = function(color_scale, layer){
      _this.icon_tooltip.destroy();
      color_scale.setOpacity(1);
      layer.draw();
  }

  InCHlib.prototype._unzoom_icon_click = function(){
    _this._unzoom_cluster();
  };

  InCHlib.prototype._column_unzoom_icon_click = function(){
      _this._unzoom_column_cluster();
  };

  InCHlib.prototype._icon_mouseover = function(icon, icon_overlay, layer){
    if(icon.getAttr("id") !== "help_icon"){
      var label = icon.getAttr("label");
      var x = icon_overlay.getAttr("x");
      var y = icon_overlay.getAttr("y");
      var width = icon_overlay.getWidth();
      var height = icon_overlay.getHeight();

      if(icon.getAttr("id") === "export_icon"){
        x = x - 100;
        y = y - 50;
      }

      _this.icon_tooltip = _this.objects_ref.tooltip_label.clone({x: x,
          y: y+1.2*height
      });

      _this.icon_tooltip.add(_this.objects_ref.tooltip_tag.clone());
      _this.icon_tooltip.add(_this.objects_ref.tooltip_text.clone({text: label}));
      layer.add(_this.icon_tooltip);    
    }
    icon.setFill("black");
    layer.draw();
  }

  InCHlib.prototype._icon_mouseout = function(icon, icon_overlay, layer){
      if(icon.getAttr("id") !== "help_icon"){
        _this.icon_tooltip.destroy();
      }
      icon.setFill("grey");
      layer.draw();
  }

  InCHlib.prototype._help_mouseover = function(){
    var help_element = _this.target_element.find(".inchlib_help");
    if(help_element.length){
      help_element.show();
    }
    else{
      help_element = $("<div class='inchlib_help'><ul><li>Zoom clusters by a long click on a dendrogram node.</li></ul></div>");
      help_element.css({"position": "absolute",
                        "top": 70,
                        "left": _this.settings.width - 100,
                        "font-size": 12,
                        "width": 200,
                        "background-color": "white",
                        "border-radius": 5,
                        "border": "solid #DEDEDE 2px",
                        "z-index": 1000

                      });
      _this.target_element.append(help_element);
    }
  }

  InCHlib.prototype._help_mouseout = function(){
    _this.target_element.find(".inchlib_help").hide();
  }

  InCHlib.prototype._dendrogram_layers_click=function(layer, evt){
      var path_id = evt.target.attrs.path_id;
      layer.fire("mouseout", layer, evt);
      _this._highlight_cluster(path_id);
      _this.events.dendrogram_node_onclick(_this.current_object_ids, _this._unprefix(path_id), evt);
  }

  InCHlib.prototype._column_dendrogram_layers_click=function(layer, evt){
      var path_id = evt.target.attrs.path_id;
      layer.fire("mouseout", layer, evt);
      _this._highlight_column_cluster(path_id);
      _this.events.column_dendrogram_node_onclick(_this.current_column_ids, _this._unprefix(path_id), evt);
  }

  InCHlib.prototype._dendrogram_layers_mousedown = function(layer, evt){
    var node_id = evt.target.attrs.path_id;
    clearTimeout(_this.timer);
    _this.timer = setTimeout(function() {
        _this._get_object_ids(node_id);
        _this._zoom_cluster(node_id);
    }, 500);
  }

  InCHlib.prototype._column_dendrogram_layers_mousedown = function(layer, evt){
    var node_id = evt.target.attrs.path_id;
    clearTimeout(_this.timer);
    _this.timer = setTimeout(function() {
        _this._get_column_ids(node_id);
        _this._zoom_column_cluster(node_id);
    }, 500);
  }

  InCHlib.prototype._dendrogram_layers_mouseup = function(layer, evt){
    clearTimeout(_this.timer);
  }

  InCHlib.prototype._dendrogram_layers_mouseout = function(layer, evt){
    _this.path_overlay.destroy();
    _this.dendrogram_hover_layer.draw();
  }

  InCHlib.prototype._dendrogram_layers_mouseover = function(layer, evt){
    _this.path_overlay = evt.target.attrs.path.clone({"strokeWidth": 4});
    _this.dendrogram_hover_layer.add(_this.path_overlay);
    _this.dendrogram_hover_layer.draw();
  }

  InCHlib.prototype._visible_features_equal_column_dendrogram_count = function(){
      if((_this.on_features["data"].length + _this.on_features["metadata"].length) == _this.current_column_count){
          return true;
      }
      return false;
  }

  InCHlib.prototype._get_color_for_value = function(value, min, max, middle, color_scale){
      var color = _this.colors[color_scale];
      var c1 = color["start"];
      var c2 = color["end"];

      if(value > max){
        return 'rgb('+c2.r+','+c2.g+','+c2.b+')';
      }

      if(min == max || value < min){
        return 'rgb('+c1.r+','+c1.g+','+c1.b+')';
      }

      if(color["middle"] !== undefined){
          if(value >= middle){
              min = middle;
              c1 = color["middle"];
              c2 = color["end"];
          }
          else{
              max = middle;
              c1 = color["start"];
              c2 = color["middle"];
          }
      }

      var position = (value-min)/(max-min);
      var r = _this._hack_round(c1.r+(position*(c2.r-c1.r)));
      var g = _this._hack_round(c1.g+(position*(c2.g-c1.g)));
      var b = _this._hack_round(c1.b+(position*(c2.b-c1.b)));
      return 'rgb('+r+','+g+','+b+')';
  }

  InCHlib.prototype._get_font_size = function(text_length, width, height, max_font_size){
      var max_possible_size = height - 2;
      var font_size = max_possible_size;

      if(font_size/2*text_length > width-10){
          font_size = font_size/(font_size/2*text_length/(width-10));
      };
      font_size = (font_size > max_possible_size)?max_possible_size:font_size;
      font_size = (font_size > max_font_size)?max_font_size:font_size;
      return font_size;
  }

  InCHlib.prototype._get_object_ids = function(node_id){
    _this.current_object_ids = [];
    _this._collect_object_ids(node_id);
  }

  InCHlib.prototype._collect_object_ids = function(node_id){
      if(_this.data.nodes[node_id]["left_child"] !== undefined){
        _this._collect_object_ids(_this.data.nodes[node_id]["left_child"]);
        _this._collect_object_ids(_this.data.nodes[node_id]["right_child"]);
      }
      else{
        _this.current_object_ids.push.apply(_this.current_object_ids, _this.data.nodes[node_id]["objects"])
      }
  }

  InCHlib.prototype._get_column_ids = function(node_id){
      _this.current_column_ids = [];
      _this._collect_column_ids(node_id);
      _this.current_column_ids.sort(function(a,b){return a - b});
  }

  InCHlib.prototype._collect_column_ids = function(node_id){
    if(_this.column_dendrogram.nodes[node_id]["left_child"] !== undefined){
      _this._collect_column_ids(_this.column_dendrogram.nodes[node_id]["left_child"]);
      _this._collect_column_ids(_this.column_dendrogram.nodes[node_id]["right_child"]);
    }
    else{
      _this.current_column_ids.push(_this.nodes2columns[node_id]);
    }
  }

  InCHlib.prototype._hack_size = function(obj) {
      return Object.keys(obj).length;
  };

  InCHlib.prototype._hack_round = function(value){
      return (0.5 + value) >> 0;
  }

  InCHlib.prototype._is_number = function(n){
      return !isNaN(parseFloat(n)) && isFinite(n);
  }

  InCHlib.prototype._row_mouseenter = function(evt){
      var row_id = evt.target.parent.getAttr("id");
      var visible = _this._get_visible_count();

      if(evt.target.parent.attrs.class !== "column_metadata"){
          _this.highlighted_row = row_id;
          var y = _this.leaves_y_coordinates[row_id];
          var x = _this.heatmap_distance;
          
          _this.row_overlay = _this.objects_ref.heatmap_line.clone({points: [x, y, x + _this.heatmap_width, y],
            strokeWidth: _this.pixels_for_leaf,
            stroke: "#FFFFFF",
            opacity: 0.3,
            listening: false});

          _this.heatmap_overlay.add(_this.row_overlay);
          _this.heatmap_overlay.draw();
          _this.events.row_onmouseover(_this.data.nodes[row_id].objects, evt);
      }
  }

  InCHlib.prototype._row_mouseleave = function(evt){
      _this.row_overlay.destroy();
      _this.events.row_onmouseout(evt);
  };

  InCHlib.prototype._draw_col_label = function(evt){
      var i, line;
      var attrs = evt.target.attrs;
      var points = attrs.points;
      var x = _this._hack_round((points[0] + points[2])/2);
      var y = points[1]-0.5*_this.pixels_for_leaf;
      var column = attrs.column.split("_");
      var header_type2value = {"d": _this.heatmap_header[column[1]],
                               "m": _this.metadata_header[column[1]],
                               "Count": "Count"};
      
      if(_this.column_metadata_header !== undefined){
        header_type2value["cm"] = _this.column_metadata_header[column[1]];
      }


      var row_id = evt.target.parent.attrs.id;
      var value = attrs.value;
      var header = header_type2value[column[0]];

      if(header !== _this.last_column){
        _this.column_overlay.destroy();
        _this.last_column = attrs.column;
        _this.column_overlay = _this.objects_ref.heatmap_line.clone({points: [x, _this.header_height, x, _this.header_height + _this.column_metadata_height + (_this.heatmap_array.length+0.5)*_this.pixels_for_leaf],
          strokeWidth: _this.pixels_for_dimension,
          stroke: "#FFFFFF",
          opacity: 0.3,
          listening: false});

        _this.heatmap_overlay.add(_this.column_overlay);
      }
      
      if(header !== undefined){
          value = [header, value].join("\n");
      }

      if(row_id !== undefined){
        sysName = _this.data.nodes[row_id].objects[0];
        value = [sysName, value].join("\n");
      }

      var tooltip = _this.objects_ref.tooltip_label.clone({x: x, y:y, id: "col_label",});
      tooltip.add(_this.objects_ref.tooltip_tag.clone({pointerDirection: 'down'}), _this.objects_ref.tooltip_text.clone({text: value}));
      
      _this.heatmap_overlay.add(tooltip);
      _this.heatmap_overlay.moveToTop();
      _this.heatmap_overlay.draw();
  }

  InCHlib.prototype._unprefix = function(prefixed){
      return prefixed.split(_this.settings.target+"#")[1];
  }

  InCHlib.prototype._prefix = function(nonprefixed){
      return _this.settings.target + "#" + nonprefixed;
  }

  /**
    * Returns array of features for object by its ID. When sent object ID is not present, false is returned
    */
  InCHlib.prototype.get_features_for_object = function(object_id){
      if(_this.objects2leaves[object_id] !== undefined){
        var row_id = _this.objects2leaves[object_id];
        return _this.data.nodes[row_id].features;
      }
      return false;
  }

  /**
    * Adds a user defined color scale defined by its name start color, end color and optionaly middle color
    */
  InCHlib.prototype.add_color_scale = function(color_scale_name, color_scale){
      _this.colors[color_scale_name] = color_scale;
      _this.target_element.find(".color_scales").remove();
  }

  InCHlib.prototype._get_visible_count = function(){
      return _this.on_features["data"].length + _this.on_features["metadata"].length + _this.on_features["count_column"].length;
  }

  /**
    * Update cluster heatmap settings
    */
  InCHlib.prototype.update_settings = function(settings_object){
    $.extend(_this.settings, settings_object);
  }

  /**
    * Redraw cluster heatmap
    */
  InCHlib.prototype.redraw = function(){
    _this._delete_all_layers();
    _this.draw();
  }

  /**
    * Redraw heatmap only
    */
  InCHlib.prototype.redraw_heatmap = function(){
    _this._delete_layers([_this.heatmap_layer, _this.heatmap_overlay, _this.highlighted_rows_layer, _this.header_layer]);
    _this._set_color_settings();
    _this._draw_heatmap();
    _this._draw_heatmap_header();
    _this.heatmap_layer.moveToBottom();
    _this.heatmap_layer.moveUp();
  }

}(jQuery));