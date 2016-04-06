package prerna.algorithm.api;

import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface IMetaData {

	
	enum NAME_TYPE {USER_DEFINED, DB_PHYSICAL_NAME, DB_PHYSICAL_URI, DB_LOGICAL}

	Vertex upsertVertex(String type, String uniqueName, String logicalName, String instancesType, String physicalUri,
			String engineName, String dataType, String parentIfProperty);
	
	Map<String, String> getProperties();

	String getPhysicalUriForNode(String string, String engineName);

	Set<String> getEnginesForUniqueName(String uniqueName);

}
