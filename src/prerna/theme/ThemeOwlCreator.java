package prerna.theme;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ThemeOwlCreator {
	
	// each column name paired to its type in a var
	private List<Pair<String, String>> adminThemeColumns = null;

	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;
	
	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new ArrayList<>();
	static {
		conceptsRequired.add("ADMIN_THEME");
	}
	
	private IRDBMSEngine themesDb;
	
	public ThemeOwlCreator(IRDBMSEngine modelInferenceDb) {
		this.themesDb = modelInferenceDb;
		createColumnsAndTypes(this.themesDb.getQueryUtil());
	}
	
	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();

		this.adminThemeColumns = Arrays.asList(
				Pair.with("ID", "VARCHAR(255)"),
				Pair.with("THEME_NAME", "VARCHAR(255)"),
				Pair.with("THEME_MAP", CLOB_DATATYPE_NAME),
				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME)
			);
		
		this.allSchemas = Arrays.asList(
				Pair.with("ADMIN_THEME", this.adminThemeColumns)
			);
	}
	
	/**
	 * Determine if we need to remake the OWL
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check
		 * Just looking at the tables
		 * Not doing anything with columns but should eventually do that
		 */
		
		List<String> cleanConcepts = new ArrayList<>();
		List<String> concepts = themesDb.getPhysicalConcepts();
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}
		
		if (!cleanConcepts.containsAll(conceptsRequired)) {
			return true;
		}
		
		// check all columns
		for (Pair<String, List<Pair<String, String>>> tableWithColumns : allSchemas) {
			String tableName = tableWithColumns.getValue0();
			String[] columnNames = tableWithColumns.getValue1().stream()
					.map(Pair::getValue0).toArray(String[]::new);

			for (String columnName : columnNames) {
				if (columnChecks(tableName, columnName)) {
					return true;
				}
			}
		}
		
		// does not need to be remade
		return false;
	}
	
	
	private boolean columnChecks(String tableName, String columnName) {
		String propsURI = "http://semoss.org/ontologies/Concept/" + tableName;
		String relationURI = "http://semoss.org/ontologies/Relation/Contains/" 
				+ columnName + "/" + tableName; 

		List<String> props = themesDb.getPropertyUris4PhysicalUri(propsURI);	
		if(!props.contains(relationURI)) {
			return true;
		}
		
		return false;
	}
		
	
	/**
	 * Remake the OWL 
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		try(WriteOWLEngine owlEngine = themesDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			// write the new OWL
			writeNewOwl(owlEngine);
		}
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * @param owlLocation
	 * @throws Exception 
	 */
	private void writeNewOwl(WriteOWLEngine owler) throws Exception {
		for (Pair<String, List<Pair<String, String>>> columns : allSchemas) {
			String tableName = columns.getValue0();
			owler.addConcept(tableName, null, null);
			for (Pair<String, String> x : columns.getValue1()) {	
				owler.addProp(tableName, x.getValue0(), x.getValue1());
			}
		}
		
		owler.commit();
		owler.export();
	}
	
	public List<Pair<String, List<Pair<String, String>>>> getDBSchema() {
		return this.allSchemas;
	}
}
