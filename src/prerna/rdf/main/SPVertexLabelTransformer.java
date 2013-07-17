package prerna.rdf.main;

import org.apache.commons.collections15.Transformer;

import prerna.om.DBCMVertex;
import prerna.util.Constants;


public class SPVertexLabelTransformer implements Transformer<DBCMVertex, String> {

	@Override
	public String transform(DBCMVertex arg0) {
		// TODO Auto-generated method stub
		return (String)arg0.getProperty(Constants.VERTEX_NAME);
		//return null;
	}

}