package prerna.reactor.database.metaeditor;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.semarglproject.vocab.RDF;

import com.hp.hpl.jena.vocabulary.OWL;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetOwlDictionaryReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(GetOwlDictionaryReactor.class);

	public GetOwlDictionaryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have an alias
		databaseId = testDatabaseId(databaseId, false);
		
		// we have some ordering requirements
		// so can't just flush these results straight to the FE
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		String query = "SELECT DISTINCT "
				+ "?URI "
				+ "(COALESCE(?DESCRIPTION, '') AS ?desc) "
				+ "(COALESCE(?LOGICAL_NAME, '') AS ?logical) "
				+ "(COALESCE(?TYPE, 'STRING') AS ?type) "
				+ "WHERE {"
				+ "{"
					+ "{?URI <" + RDFS.SUBCLASSOF + "> <http://semoss.org/ontologies/Concept> } "
					+ "OPTIONAL{?URI <" + RDFS.COMMENT.toString() + "> ?DESCRIPTION} "
					+ "OPTIONAL{?URI <" + OWL.sameAs.toString() + "> ?LOGICAL_NAME} "
					+ "OPTIONAL{?URI <" + RDFS.CLASS.toString() + "> ?TYPE} "
					+ "Filter(?URI != <http://semoss.org/ontologies/Concept>)"
					// currently not returning the table names in RDBMS/R/etc. data source structures
					+ "MINUS{?URI <" + RDFS.DOMAIN.toString() + "> \"noData\" }"
				+ "}"
				+ "UNION "
				+ "{"
					+ "{?URI <" + RDF.TYPE + "> <http://semoss.org/ontologies/Relation/Contains> } "
					+ "OPTIONAL{?URI <" + RDFS.COMMENT.toString() + "> ?DESCRIPTION} "
					+ "OPTIONAL{?URI <" + OWL.sameAs.toString() + "> ?LOGICAL_NAME} "
					+ "OPTIONAL{?URI <" + RDFS.CLASS.toString() + "> ?TYPE} "
				+ "}"
				+ "} ORDER BY ?CONCEPT ?LOGICAL_NAME";
		
		Map<String, Map<String, Object>> results = new HashMap<String, Map<String, Object>>();
				
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = database.getOWLEngineFactory().getReadOWL().query(query);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] raw = row.getRawValues();
				Object[] clean = row.getValues();
				
				String uri = raw[0].toString();
				String tableName = Utility.getInstanceName(uri);
				String columnName = Utility.getClassName(uri);
				
				// get the other values
				String description = clean[1].toString();
				String logical = clean[2].toString();
				String type = clean[3].toString();
				
				if(results.containsKey(uri)) {
					// table, column, primkey, dataType will not change
					// just appending to the description and logical names
					Map<String, Object> record = results.get(uri);
					
					List<String> descArr = (List<String>) record.get("description");
					if(description != null && !description.isEmpty() && !descArr.contains(description)) {
						descArr.add(description);
					}
					
					List<String> logicalArr = (List<String>) record.get("logical");
					if(logical != null && !logical.isEmpty() && !logicalArr.contains(logical)) {
						logicalArr.add(logical);
					}
				} else {
					Map<String, Object> record = new HashMap<String, Object>();
					record.put("table", tableName);
					record.put("column", columnName);
					if(uri.startsWith("http://semoss.org/ontologies/Concept")) {
						record.put("isPrimKey", true);
					} else {
						record.put("isPrimKey", false);
					}
					record.put("dataType", type.replace("TYPE:", ""));

					List<String> descArr = new Vector<String>();
					if(description != null && !description.isEmpty()) {
						descArr.add(description);
					}
					record.put("description", descArr);
					
					List<String> logicalArr = new Vector<String>();
					if(logical != null && !logical.isEmpty()) {
						logicalArr.add(logical);
					}
					record.put("logical", logicalArr);
					
					// store
					results.put(uri, record);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// need to order the information
		List<Map<String, Object>> values = new Vector<Map<String, Object>>(results.values());
		Collections.sort(values, new Comparator<Map<String, Object>>(){
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				// going to first order by table
				// then put prim key first
				// then by column
				
				String o1Table = (String) o1.get("table");
				String o2Table = (String) o2.get("table");
				
				int tableCompare = o1Table.compareTo(o2Table);
				if(tableCompare == 0) {
					// is one a prim key?
					boolean o1Key = (boolean) o1.get("isPrimKey");
					boolean o2Key = (boolean) o2.get("isPrimKey");
					
					if(o1Key) {
						return -1;
					} else if(o2Key) {
						return 1;
					}
					
					// okay, lets try the column now
					String o1Colum = (String) o1.get("column");
					String o2Colum = (String) o2.get("column");

					int columnCompare = o1Colum.compareTo(o2Colum);
					return columnCompare;
				}
				
				return tableCompare;
			}});
		
		return new NounMetadata(values, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_DICTIONARY);
	}

}
