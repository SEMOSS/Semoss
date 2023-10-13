package prerna.util.git.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AssetReactor extends AbstractReactor {
	
	// Asset for a given user is kept in some kind of a CSV repository
	// it maintains
	// respository type - git vs. google drive vs. drop box etc. 
	// repository name - who is the user?
	// Name of the resource
	// every resource is typicaly just a widget file i.e. json
	// Any resource can refer to another resource
	// Using the same description as the nodejs
	
	// https://docs.npmjs.com/files/package.json
	
	// name
	// description
	// version - version of this package
	// homepage
	// license
	// repository - git vs. svn vs. google drive
	// config 
	// dependencies - array with specific engine and the dependent library and the version
	// semoss: version
	// dependencies :[
	// {
	// r version  - version[['version.string']]
	// 		System.out.println("installed... " + re.eval("'moron' %in% rownames(installed.packages()) == TRUE").asBool().isTRUE()); - checks to see if it is installed
	// need something on the rdf map which says go ahead and install packages if not there
	//   "R":"3.3.2"
	// 	 "libraries": {
	//   "name of library" : "version / url"
	// 	  }
	// },
	// {
	//   "python":"2.x"
	// 	 "libraries": {
	// 	 "name of library": "version / url"
	//    }
	// }, Java
	// {
	//  name of the jar : version / url
	// }
	// ]
	// widget-file-list - {
	// fileName: URL,
	// fileName2: URL2
	// }
	
	
	
	// every file is listed as a folder
	// every folder 
	// has a meta information
	// the meta information needs to parsed to get the following information
	// 		Name of the asset
	// 		Description
	// 		Language
	// .Widget file - which is a JSON
	// Other assets - which we do not care about right now
	// 
	

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
