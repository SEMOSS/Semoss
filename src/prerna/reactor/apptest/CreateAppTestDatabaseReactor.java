package prerna.reactor.apptest;

import java.util.Map;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.database.upload.rdbms.internal.CreateEmptyRdbmsDatabaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Reuse normal database provisioning permissions; never clone a production connection or copy its rows. */
public class CreateAppTestDatabaseReactor extends AbstractAppTestReactor {
	public CreateAppTestDatabaseReactor() {
		keysToGet = new String[] { "project", "name", "rdbmsType" }; keyRequired = new int[] { 1, 1, 0 };
	}
	@Override public NounMetadata execute() {
		organizeKeys(); String project = targetProject();
		String type = getString("rdbmsType");
		if (type == null || type.isBlank()) type = "H2_DB";
		if (!type.equals("H2_DB") && !type.equals("SQLITE")) throw new IllegalArgumentException("Choose H2_DB or SQLITE for a dedicated test database");
		var creator = new CreateEmptyRdbmsDatabaseReactor();
		creator.setInsight(this.insight);
		creator.getNounStore().makeNoun("database").addLiteral(getString("name"));
		creator.getNounStore().makeNoun("rdbmsType").addLiteral(type);
		Object value = creator.execute().getValue();
		if (!(value instanceof Map<?, ?> engine) || !(engine.get("engine_id") instanceof String id))
			throw new IllegalArgumentException("Database creation did not return an engine id");
		return new NounMetadata(Map.of("target", project, "engineId", id, "engine", value), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Create a new private, empty file-backed H2/SQLite test database for an app. Normal publisher/admin provisioning policies apply. Does not copy production data, create application tables or change app connections. Seed its schema/fixtures before recording.")
				.key("project", "Target app project id").key("name", "New test database name")
				.key("rdbmsType", "H2_DB (default) or SQLITE; match the application's supported SQL dialect")
				.output(Map.class, "Target, new engineId and engine metadata");
	}
}
