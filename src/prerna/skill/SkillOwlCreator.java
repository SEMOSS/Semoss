package prerna.skill;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;

public class SkillOwlCreator {

	private static List<String> conceptsRequired = new ArrayList<String>();
	static {
		conceptsRequired.add("SKILL");
		conceptsRequired.add("SKILL_VERSION");
	}

	private IDatabaseEngine skillDb;

	public SkillOwlCreator(IDatabaseEngine skillDb) {
		this.skillDb = skillDb;
	}

	public boolean needsRemake() {
		List<String> cleanConcepts = new ArrayList<>();
		List<String> concepts = skillDb.getPhysicalConcepts();
		if (concepts.isEmpty()) {
			return true;
		}

		for (String concept : concepts) {
			if (concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			cleanConcepts.add(Utility.getInstanceName(concept));
		}

		if (!cleanConcepts.containsAll(conceptsRequired)) {
			return true;
		}

		List<String> skillProps = skillDb.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/SKILL");
		if (!skillProps.contains("http://semoss.org/ontologies/Relation/Contains/SKILL/IS_ACTIVE")) {
			return true;
		}

		List<String> skillVersionProps = skillDb
				.getPropertyUris4PhysicalUri("http://semoss.org/ontologies/Concept/SKILL_VERSION");
		if (!skillVersionProps.contains("http://semoss.org/ontologies/Relation/Contains/SKILL_VERSION/CHANGE_NOTES")) {
			return true;
		}

		return false;
	}

	public void remakeOwl() throws Exception {
		try (WriteOWLEngine owlEngine = skillDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			writeNewOwl(owlEngine);
		}
	}

	private void writeNewOwl(WriteOWLEngine owler) throws Exception {
		owler.addConcept("SKILL", null, null);
		owler.addProp("SKILL", "SKILL_ID", "VARCHAR(255)");
		owler.addProp("SKILL", "SKILL_NAME", "VARCHAR(255)");
		owler.addProp("SKILL", "DESCRIPTION", "CLOB");
		owler.addProp("SKILL", "CONTENT", "CLOB");
		owler.addProp("SKILL", "VERSION", "INT");
		owler.addProp("SKILL", "TAGS", "CLOB");
		owler.addProp("SKILL", "OWNER_ID", "VARCHAR(255)");
		owler.addProp("SKILL", "PROJECT_ID", "VARCHAR(255)");
		owler.addProp("SKILL", "CREATED_AT", "TIMESTAMP");
		owler.addProp("SKILL", "UPDATED_AT", "TIMESTAMP");
		owler.addProp("SKILL", "IS_ACTIVE", "BOOLEAN");

		owler.addConcept("SKILL_VERSION", null, null);
		owler.addProp("SKILL_VERSION", "SKILL_ID", "VARCHAR(255)");
		owler.addProp("SKILL_VERSION", "VERSION", "INT");
		owler.addProp("SKILL_VERSION", "CONTENT", "CLOB");
		owler.addProp("SKILL_VERSION", "CHANGE_NOTES", "CLOB");
		owler.addProp("SKILL_VERSION", "CREATED_AT", "TIMESTAMP");

		owler.commit();
		owler.export();
	}

}
