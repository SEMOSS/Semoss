package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PatchSessionMetaReactor extends AbstractReactor{
	
	public PatchSessionMetaReactor() {
		this.keysToGet = new String[] {
				"sessionId",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
	    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
		
		MetaPatch patch = json.convertValue(paramValues, MetaPatch.class);
		
		return new NounMetadata(updateSessionMeta(sessionId, patch), PixelDataType.MAP);
	}
	
    public RecordingMeta updateSessionMeta(String sessionId, MetaPatch patch) {
        Session s = SessionReactor.get(sessionId);
        RecordingMeta old = s.history.meta();
        long now = System.currentTimeMillis();

        String id = old != null && old.id() != null ? old.id() : java.util.UUID.randomUUID().toString();
        String title = patch.title() != null ? patch.title() : (old != null ? old.title() : null);
        String desc  = patch.description() != null ? patch.description() : (old != null ? old.description() : null);
        Long created = old != null ? old.createdAt() : null; // keep null during recording
        Long updated = now;                                   // bump updatedAt on edit

        RecordingMeta meta = new RecordingMeta(id, title, desc, created, updated);
        s.history = new StepsEnvelope(s.history.version(), meta, s.history.steps());
        return meta;
    }

}
