package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.parsers.SqlParser;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseQueryTranslator extends AbstractReactor {
	public DatabaseQueryTranslator() {
		this.keysToGet = new String[] { "query", "sourceDB", "targetDB" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		String sourceDB = this.keyValue.get(this.keysToGet[1]);
		String targetDB = this.keyValue.get(this.keysToGet[2]);
		
		SqlParser sourceQueryParser = new SqlParser();
		try {
			sourceQueryParser.processQuery(query);
			Map tableAlias = sourceQueryParser.getTableAlias();
			Map columnAlias = sourceQueryParser.getColumnAlias();
			// get all the tables and columns used in query string
			Map<String, Set<String>> schema = sourceQueryParser.getSchema();
		
			
			// get all physical names from query schema
			List<String> allPhysicalNames = new Vector<>();
			for(String table: schema.keySet()) {
				// add physical table names
				allPhysicalNames.add(table);
				Set<String> columns = schema.get(table);
				// add physical col names
				allPhysicalNames.addAll(columns);
				System.out.println("table:" + table);
				System.out.println("columns:" + columns);
			}
			
			// get map of physical to conceptual names
			Map<String, String> conceptPhysical = MasterDatabaseUtility.getConceptualFromPhysical(allPhysicalNames, "film");
			// map source conceptual names to target conceptual names
			Map<String, Object> conceptMap = MasterDatabaseUtility.getConceptMapping(sourceDB,
					new Vector<String>(conceptPhysical.values()), targetDB);

			// start translation here
			String targetQuery = query;
			for(String sourceConcept : conceptPhysical.keySet()) {
				if(conceptMap.containsKey(sourceConcept)) {
					List<String> targetConcepts = (List<String>) conceptMap.get(sourceConcept); 
					if(!targetConcepts.isEmpty()) {
						String targetConcept = targetConcepts.get(0);
						targetQuery=targetQuery.replaceAll(sourceConcept, targetConcept);
					}
				}
			}

			SqlParser targetQueryParser = new SqlParser();
			SelectQueryStruct targetQS = targetQueryParser.processQuery(targetQuery);
			targetQS.setEngineName(targetDB);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return null;
	}
}
