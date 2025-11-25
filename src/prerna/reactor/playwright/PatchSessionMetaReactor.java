package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PatchSessionMetaReactor extends AbstractReactor {

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public PatchSessionMetaReactor() {
		this.keysToGet = new String[] { "sessionId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> paramValues = getMap(this.keysToGet[1]);

		MetaPatch patch = json.convertValue(paramValues, MetaPatch.class);

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		RecordingMeta old = playwrightSession.history.meta();
		long now = System.currentTimeMillis();

		String id = old != null && old.id() != null ? old.id() : java.util.UUID.randomUUID().toString();
		String title = patch.title() != null ? patch.title() : (old != null ? old.title() : null);
		String desc = patch.description() != null ? patch.description() : (old != null ? old.description() : null);
		Long created = old != null ? old.createdAt() : null; // keep null during recording
		Long updated = now; // bump updatedAt on edit

		RecordingMeta meta = new RecordingMeta(id, title, desc, created, updated);
		playwrightSession.history = new StepsEnvelope(playwrightSession.history.version(), meta,
				playwrightSession.history.steps());

		return new NounMetadata(meta, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that allow the Recorder app to update the title and the description for a recorded file";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		}

		return super.getDescriptionForKey(key);
	}

}
