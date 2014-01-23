'use strict';

angular.module('rdfeditor.directives', []).
    directive('octoFade', function($parse) {
        return {
            retrict: "A",
            scope: { status: "@" },
            link: function(scope, element, attrs) {
                scope.$watch("status", function(newValue, oldValue) {
                    if(newValue === 'success') {
                        element.fadeOut(attrs.octoFadeDuration);
                    }
                }, true);
            }
        }
    }).directive('xeditable', function($q, editNodeService) {
        return {
            link: function(scope, element, attrs) {
                
                element.editable({
                    type: 'text',
                    mode: 'inline',
                    url: function(params) {
                        var deferred = $q.defer();
                        
                        editNodeService.updateProperty(scope.property[0], scope.property[1], scope.property[2], scope.property[3], params.value).then(function(data){
                            scope.alerts.push({ type: 'success', msg: 'Congratulations, you have successfully updated a Node Property.' });
                            scope.property[2] = params.value;
                            deferred.resolve();
                        }, function(){
                            scope.alerts.push({ type: 'error', msg: 'Error:  Unfortunately you were not able to update that property.' });
                            deferred.reject();
                        });
                        return deferred.promise;
                    },
                    disabled: true,
                    value: scope.property[2]
                });
                
                
                
        }
    }
}).directive('xeditablebutton', function() {
        return {
                 
             //This is the enable/disable feature as you click the "Edit Mode" button.
             link: function(scope, element, attrs) {    
                  
                  element.on('click', function(e) {
                      if(element.hasClass('active')) {
                           element.removeClass('active');
                      } else {
                           element.addClass('active');
                      }
                      e.stopPropagation();
                      jQuery('.editable').editable('toggleDisabled');
                  });
             }
        }
    
}).directive('uriTypeahead', function(searchNodeService){
	/**
     * @name autoComplete
     * @description
     * Autocomplete for a particular list based on synchronously accessed data
     */
    var searchUris = _.debounce(function(query, process) {
        // only call search when query is not undefined and contains 3 or more letters
        if(query && query.length >= 3) {
            searchNodeService.search(query).then(function(data){
                // return array to be dispalyed for autocomplete/typeahead
                process(data);
            });
        }
    }, 100);
    
	return {
		restrict: 'A',
		link: function (scope, elem, attrs) {
            
            elem.typeahead({
                source: searchUris,
                matcher: function() { return true; },
                updater: function(item) {
                    // set typeaheadValue on scope so that the proper value is passed back to the search function
                    scope.$apply(read(item));
                    // return item that you want to have displayed in <input>
                    return item;
                }, 
                items: 10
            });
            
            var read = function(value) {
                scope.typeaheadValue = value;
            }
		}
	};
});
