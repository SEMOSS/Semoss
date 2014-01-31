'use strict';

/* Services */

// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('rdfeditor.services', []).service('nodeService', function($q, $rootScope, $http, $filter){

    // service is just a constructor function
    // that will be called with 'new'
    
    // service call to run sparql 'SELECT' statement to retrieve outbound relationships
	this.getOutboundRelationships = function(param, type) {
		var deferred = $q.defer();
		
		//var sparql = 'SELECT DISTINCT ?subject ?predicate ?object WHERE {BIND (' + param + ' as ?subject). ?subject ?predicate ?object; } LIMIT 1'
		var sparql = 'SELECT DISTINCT ?subjectType ?subject ?verb ?verbType ?objectType ?object WHERE {BIND (<' + param + '> as ?subject) BIND (<' + type + '> as ?subjectType) {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation> ;} {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?verbType ;} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subjectType ;} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?objectType;} {?subject ?verb ?object ;} FILTER NOT EXISTS { { ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?objSubtype} {?objSubtype <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?objectType} } }';
		var returnData = jQuery.parseJSON(SPARQLExecuteFilterNoBase(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;
		
		//filtering the 'param' to get the correct values for the sparql query
		//var nodeLabel = $filter('shortenValueFilter')(param);
		//nodeLabel = nodeLabel.substring(0, nodeLabel.length - 1);
		//var namespace = $filter('namespaceFilter')(param);
		//sparql query
		//var sparql = 'SELECT DISTINCT ?subject ?predicate ?object WHERE {?subject <http://www.w3.org/2000/01/rdf-schema#label> "' + nodeLabel + '". ?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' + namespace + '>. ?subject ?predicate ?object; }';

        /*$http.post('http://localhost:8080/bigdata/sparql', {query: sparql}).success(function(data) {
            var returnData = {};
            if (window.DOMParser) {
                var parser = new DOMParser();
                var xmlDoc = parser.parseFromString(data, "text/xml");
                returnData = jQuery.xmlToJSON(xmlDoc)
				if(returnData.results[0].result){
					//Filters relationships to only return true relationships
					returnData = $filter('relationshipsFilter')(returnData.results[0].result);
				}else{
					returnData = '';
				}
                deferred.resolve (returnData);
            } else {
                var xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
                xmlDoc.async = "false";
                xmlDoc.loadXML(jqXHR.responseText);
                returnData = jQuery.xmlToJSON(xmlDoc)
                if(returnData.results[0].result){
					//Filters relationships to only return true relationships
					returnData = $filter('relationshipsFilter')(returnData.results[0].result);
				}else{
					returnData = '';
				}
				deferred.resolve (returnData);
            }
        }).error(function(data) {
            console.log('error ' + data);
            deferred.reject(data);
        });
        
		return deferred.promise;*/
	};
	
	// service call to run sparql 'SELECT' statement to retrieve node properties relationships
	this.getNodeValues = function(param) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?subject ?contains ?property WHERE {BIND (<' + param + '> as ?subject) {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?subject ?contains ?property ;} }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;
		
		//var nodeLabel = $filter('shortenValueFilter')(param);
		//nodeLabel = nodeLabel.substring(0, nodeLabel.length - 1);
		
		//var sparql = 'SELECT ?system1 ?contains ?property WHERE { {?system1 <http://www.w3.org/2000/01/rdf-schema#label> "' + nodeLabel + '";} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains> ;} {?system1 ?contains ?property ;} }';
		
		
		
        /*$http.post('http://localhost:8080/bigdata/sparql', {query: sparql}).success(function(data) {
            if (window.DOMParser) {
                var parser = new DOMParser();
                var xmlDoc = parser.parseFromString(data, "text/xml");
                deferred.resolve (jQuery.xmlToJSON(xmlDoc));
            } else {
                var xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
                xmlDoc.async = "false";
                xmlDoc.loadXML(data);
                deferred.resolve (jQuery.xmlToJSON(xmlDoc));
            }
        }).error(function(data) {
            deferred.reject(data);
        });
        
		return deferred.promise;*/
	};
	
	// service call to run sparql 'SELECT' statement to retrieve inbound relationships
	this.getInboundRelationships = function(param, type) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?subjectType ?subject ?verb ?verbType ?objectType ?object WHERE {BIND (<' + param + '> as ?object) BIND (<' + type + '> as ?objectType) {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation> ;} {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?verbType ;} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subjectType ;} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?objectType;} {?subject ?verb ?object ;} FILTER NOT EXISTS { { ?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subtype} {?subtype <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?subjectType} } }';
		var returnData = jQuery.parseJSON(SPARQLExecuteFilterNoBase(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		
		return deferred.promise;
		
		//filtering the 'param' to get the correct values for the sparql query
		//var nodeLabel = $filter('shortenValueFilter')(param);
		//nodeLabel = nodeLabel.substring(0, nodeLabel.length - 1);
		//var namespace = $filter('namespaceFilter')(param);
		
		
		/*
		 * $http.post('http://localhost:8080/bigdata/sparql', {query: sparql}).success(function(data) {
			if (window.DOMParser) {
				var parser = new DOMParser();
				var xmlDoc = parser.parseFromString(data, "text/xml");
				returnData = jQuery.xmlToJSON(xmlDoc)
				if(returnData.results[0].result){
					//Filters relationships to only return true relationships
					returnData = $filter('relationshipsFilter')(returnData.results[0].result);
				}else{
					returnData = '';
				}
				deferred.resolve (returnData);
			} else {
				var xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
				xmlDoc.async = "false";
				xmlDoc.loadXML(data);
				returnData = jQuery.xmlToJSON(xmlDoc)
				if(returnData.results[0].result){
					//Filters relationships to only return true relationships
					returnData = $filter('relationshipsFilter')(returnData.results[0].result);
				}else{
					returnData = '';
				}
				deferred.resolve (returnData);
			}
		}).error(function(data) {
            deferred.reject(data);
        });
        return deferred.promise;
        */
		
	};
	

	this.getOutboundTypeOptions = function(type) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?inType WHERE { BIND (<' + type + '> as ?outType) {?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?outType ;} {?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?inType ;}{?inType <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;} {?out ?x ?in} FILTER NOT EXISTS { { ?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subtype} {?subtype <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?inType} } }';
		var returnData = jQuery.parseJSON(SPARQLExecuteFilterBase(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
	this.getInboundTypeOptions = function(type) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?outType WHERE { BIND (<' + type + '> as ?inType) {?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?outType ;} {?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?inType ;}{?inType <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> ;} {?out ?x ?in} FILTER NOT EXISTS { { ?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subtype} {?subtype <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?outType} } }';
		var returnData = jQuery.parseJSON(SPARQLExecuteFilterBase(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};

	this.getInstanceOptions = function(type) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?instance WHERE {BIND (<' + type + '> as ?type) {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept> ;} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type ;} }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	

	this.getVerbOptions = function(outType, inType) {
		var deferred = $q.defer();
		
		var sparql = 'SELECT DISTINCT ?verb WHERE {BIND (<' + outType + '> as ?outType) BIND (<' + inType + '> as ?inType) {?in <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?inType ;} {?out <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?outType ;} {?out ?relationship ?in ;} {?relationship <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?verb } }';
		var returnData = jQuery.parseJSON(SPARQLExecuteFilterBase(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
}).service('editNodeService', function($q, $rootScope, $http, $filter){

//this will send a sparql insert query and insert the values into the rdfStore
	this.createNewProperty = function(param, propName, propVal, propType) {
		var deferred = $q.defer();
		var pred = 'http://semoss.org/ontologies/Relation/Contains/' + propName;
		var sparql = 'INSERT DATA {<' + param + '> <' + pred + '> "' + propVal +'"' + propType + '. <' + pred + '> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			deferred.resolve ("success");
		} else {
			deferred.reject ("unsuccessful");
		}
        /*$http.post('http://localhost:8080/bigdata/sparql', {update: sparql}).success(function(data) {
            console.log('update successful ' + data);
			deferred.resolve (data);
        }).error(function(data) {
            console.log('update UNsuccessful ' + data);
			deferred.reject (data);
        });*/
		return deferred.promise;
	};
	
	//this will send a sparql delete query and delete the values from the rdfStore
	this.deleteProperty = function(param, propName, propVal) {
		var deferred = $q.defer();
		var sparql = 'DELETE DATA {<' + param + '> <' + propName + '> ' + propVal +'}';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		deferred.resolve (returnData.results);
		
		/*
        $http.post('http://localhost:8080/bigdata/sparql', {update: sparql}).success(function(data) {
            console.log('Delete successful ' + data);
			deferred.resolve (data);
        }).error(function(data) {
            console.log('Delete UN successful ' + data);
			deferred.reject (data);
        });*/
		return deferred.promise;
	};
	
	this.updateProperty = function(param, propName, oldPropVal, propFullURI, propVal) {
		var deferred = $q.defer();
		if (propFullURI.indexOf("^^") !== -1) {
			var propType = $filter("afterFilter")(propFullURI, '"^') + ">";
		} else {
			var propType = "";
		}
		
		
		var sparql = 'DELETE {<' + param + '> <' + propName + '> "' + oldPropVal +'"' + propType +'} INSERT {<' + param + '> <' + propName + '> "' + propVal +'"' + propType + '} WHERE {}';

		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;
		
		/*if(param.indexOf('<') != -1){
        param = param.substring(1, param.length - 1);
    }
    if(propName.indexOf('<') != -1){
        propName = propName.substring(1, param.length - 1);
    }
	var sparql = 'DELETE {<' + param + '> <' + propName + '> "' + oldPropVal +'"} INSERT {<' + param + '> <' + propName + '> "' + propVal +'"} WHERE {<' + param + '> <' + propName + '> "' + oldPropVal + '"}';
	*/
        /*$http.post('http://localhost:8080/bigdata/sparql', {update: sparql}).success(function(data) {
            console.log('Delete successful ' + data);
			deferred.resolve (data);
        }).error(function(data) {
            console.log('Delete UN successful ' + data);
			deferred.reject (data);
        });
		return deferred.promise;*/
	};
}).service('searchNodeService', function($q, $http){

    //this will send a sparql insert query and insert the values into the rdfStore
	this.search = function(param) {
		var deferred = $q.defer();
		var sparql = 'SELECT DISTINCT ?s WHERE { {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://health.mil/ontologies/dbcm/Concept> ;}{?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} FILTER ( regex (str(?s), "' + param + '", "i") ) .}';
		
        $http.post('http://localhost:8080/bigdata/sparql', {query: sparql}).success(function(data) {
            var xmlData = {};
            var returnData = [];
            if (window.DOMParser) {
                var parser = new DOMParser();
                var xmlDoc = parser.parseFromString(data, "text/xml");
                xmlData = jQuery.xmlToJSON(xmlDoc)
				if(xmlData.results[0].result){
					//Filters relationships to only return true relationships
                    for (var i=0; i<xmlData.results[0].result.length; i++){
                        returnData.push(xmlData.results[0].result[i].binding[0].uri[0].Text);
                    }
				}else{
					returnData = '';
				}
                deferred.resolve (returnData);
            } else {
                var xmlDoc = new ActiveXObject("Microsoft.XMLDOM");
                xmlDoc.async = "false";
                xmlDoc.loadXML(data);
                returnData = jQuery.xmlToJSON(xmlDoc)
                /*if(returnData.results[0].result){
					//Filters relationships to only return true relationships
					returnData = $filter('relationshipsFilter')(returnData.results[0].result);
				}else{
					returnData = '';
				}*/
                deferred.resolve (returnData);
            }
        }).error(function(data) {
			deferred.reject (data);
        });
		return deferred.promise;
	};
}).service('editRelationshipService', function($q, $http, $filter){

	//making the java call to create a new inbound relationship
	this.createInboundRelationship = function(outType, outInst, verb, inType, inInst) {
		var deferred = $q.defer();
		
		var shortOutInst = $filter("shortenValueFilter")(String(outInst));
		var shortVerb = $filter("shortenValueFilter")(verb);
		var shortInInst = $filter("shortenValueFilter")(String(inInst));
		var baseURI = $filter("beforeFilter")(String(outInst), "/Concept/");
		var newRel = baseURI + "/Relation/" + shortVerb + "/" + shortOutInst + ":" + shortInInst;
		
		var sparql = 'INSERT {?newVerb ?subProp ?relation. ?newVerb ?subProp ?verbType. ?subject ?newVerb ?object. ?subject ?verbType ?object} WHERE {BIND(<' + outType + '> as ?subjectType) BIND(<' + outInst + '> as ?subject) BIND(<' + verb + '> as ?verbType) BIND(<' + inType + '> as ?objectType) BIND(<' + inInst + '> AS ?object) BIND(<' + newRel + '> AS ?newVerb) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			//InferFunction();
			RefreshFunction();
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
	//making the java call to create a new outbound relationship
	this.createOutboundRelationship = function(outType, outInst, verb, inType, inInst) {
		var deferred = $q.defer();
		
		var shortOutInst = $filter("shortenValueFilter")(String(outInst));
		var shortVerb = $filter("shortenValueFilter")(String(verb));
		var shortInInst = $filter("shortenValueFilter")(String(inInst));
		var baseURI = $filter("beforeFilter")(String(outInst), "/Concept/");
		var newRel = baseURI + "/Relation/" + shortVerb + "/" + shortOutInst + ":" + shortInInst;
		
		var sparql = 'INSERT {?newVerb ?subProp ?relation. ?newVerb ?subProp ?verbType. ?subject ?newVerb ?object. ?subject ?verbType ?object} WHERE {BIND(<' + outType + '> as ?subjectType) BIND(<' + outInst + '> as ?subject) BIND(<' + verb + '> as ?verbType) BIND(<' + inType + '> as ?objectType) BIND(<' + inInst + '> AS ?object) BIND(<' + newRel + '> AS ?newVerb) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			//InferFunction();
			RefreshFunction();
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
	//making the java call to edit a new outbound relationship
	this.editOutboundRelationship = function(outType, outInst, verb, inType, inInst, oldInInst) {
		var deferred = $q.defer();
		
		var shortOutInst = $filter("shortenValueFilter")(String(outInst));
		var shortVerb = $filter("shortenValueFilter")(verb);
		var shortInInst = $filter("shortenValueFilter")(String(inInst));
		var baseURI = $filter("beforeFilter")(String(outInst), "/Concept/");
		var newRel = baseURI + "/Relation/" + shortVerb + "/" + shortOutInst + ":" + shortInInst;

		var sparql = 'DELETE {?verb ?subProp ?verbType. ?subject ?verb ?object. ?subject ?verbType ?object. ?verb ?verb1 ?verb2. ?subject ?relation ?object} INSERT {?newVerb ?subProp ?relation. ?newVerb ?subProp ?verbType. ?subject ?newVerb ?newObject. ?subject ?verbType ?newObject} WHERE {BIND(<' + outType + '> as ?subjectType) BIND(<' + outInst + '> as ?subject) BIND(<' + verb + '> as ?verbType) BIND(<' + inType + '> as ?objectType) BIND(<' + oldInInst + '> as ?object) BIND(<' + inInst + '> AS ?newObject) BIND(<' + newRel + '> AS ?newVerb) BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) {?verb ?subProp ?relation ;} {?verb ?subProp ?verbType ;} {?subject ?type <http://semoss.org/ontologies/Concept> ;} {?subject ?type ?subjectType ;} {?object ?type <http://semoss.org/ontologies/Concept> ;} {?object ?type ?objectType;} {?subject ?verb ?object ;} {?verb ?verb1 ?verb2} }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			//InferFunction();
			RefreshFunction();
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
	//making the java call to edit a new outbound relationship
	this.editInboundRelationship = function(outType, outInst, verb, inType, inInst, oldOutInst) {
		var deferred = $q.defer();
		
		var shortOutInst = $filter("shortenValueFilter")(String(outInst));
		var shortVerb = $filter("shortenValueFilter")(verb);
		var shortInInst = $filter("shortenValueFilter")(String(inInst));
		var baseURI = $filter("beforeFilter")(String(outInst), "/Concept/");
		var newRel = baseURI + "/Relation/" + shortVerb + "/" + shortOutInst + ":" + shortInInst;
		
		var sparql = 'DELETE {?newVerb ?subProp ?relation. ?verb ?subProp ?verbType. ?subject ?verb ?object. ?subject ?verbType ?object. ?verb ?verb1 ?verb2. ?subject ?relation ?object} INSERT {?newVerb ?subProp ?relation. ?newVerb ?subProp ?verbType. ?newSubject ?newVerb ?object. ?newSubject ?verbType ?object} WHERE {BIND(<' + outType + '> as ?subjectType) BIND(<' + outInst + '> as ?newSubject) BIND(<' + verb + '> as ?verbType) BIND(<' + inType + '> as ?objectType) BIND(<' + oldOutInst + '> as ?subject) BIND(<' + inInst + '> AS ?object) BIND(<' + newRel + '> AS ?newVerb) BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) {?verb ?subProp ?relation ;} {?verb ?subProp ?verbType ;} {?subject ?type <http://semoss.org/ontologies/Concept> ;} {?subject ?type ?subjectType ;} {?object ?type <http://semoss.org/ontologies/Concept> ;} {?object ?type ?objectType;} {?subject ?verb ?object ;} {?verb ?verb1 ?verb2} }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			//InferFunction();
			RefreshFunction();
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
	//making the java call to delete relationship
	this.deleteRelationship = function(outType, outInst, verb, inType, inInst) {
		var deferred = $q.defer();
		
		var sparql = 'DELETE {?verb ?subProp ?verbType. ?subject ?verb ?object. ?subject ?verbType ?object. ?verb ?verb1 ?verb2. ?subject ?relation ?object} WHERE {BIND(<' + outType + '> as ?subjectType) BIND(<' + outInst + '> as ?subject) BIND(<' + verb + '> as ?verbType) BIND(<' + inType + '> as ?objectType) BIND(<' + inInst + '> as ?object) BIND(<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> AS ?type) BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND(<http://semoss.org/ontologies/Relation> AS ?relation) {?verb ?subProp ?relation ;} {?verb ?subProp ?verbType ;} {?subject ?type <http://semoss.org/ontologies/Concept> ;} {?subject ?type ?subjectType ;} {?object ?type <http://semoss.org/ontologies/Concept> ;} {?object ?type ?objectType;} {?subject ?verb ?object ;} {?verb ?verb1 ?verb2} }';
		var returnData = jQuery.parseJSON(SPARQLExecute(sparql));
		if (returnData.success == true) {
			RefreshFunction();
			deferred.resolve (returnData.results);
		} else {
			deferred.reject ("unsuccessful");
		}
		return deferred.promise;	
	};
	
});