package prerna.algorithm.api;

import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.algorithm.api.IMetaData.NAME_TYPE;
import prerna.engine.api.IEngine;

public interface IMetaData {

	
	enum NAME_TYPE {USER_DEFINED, DB_PHYSICAL_NAME, DB_PHYSICAL_URI, DB_LOGICAL}

	Vertex upsertVertex(String type, String uniqueName, String logicalName, String instancesType, String physicalUri,
			String engineName, String parentIfProperty); // TODO: Rishi needs to remove this

	Map<String, String> getProperties();

	String getPhysicalUriForNode(String string, String engineName);

}
