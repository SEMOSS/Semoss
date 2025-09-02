package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PatchFileMetaReactor extends AbstractReactor{
	
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	
	public PatchFileMetaReactor() {
		this.keysToGet = new String[] {
				"sessionId",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1, 1 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
		
		MetaPatch patch = json.convertValue(paramValues, MetaPatch.class);
		
		return new NounMetadata(updateFileMeta(sessionId, patch), PixelDataType.MAP);
	}
	
    public RecordingMeta updateFileMeta(String nameOrPath, MetaPatch patch) {
        StepsEnvelope env = ReplayFromFileReactor.loadStepsFromFile(nameOrPath);
        RecordingMeta old = env.meta();
        long now = System.currentTimeMillis();

        String id = old != null && old.id() != null ? old.id() : java.util.UUID.randomUUID().toString();
        String title = patch.title() != null ? patch.title() : (old != null ? old.title() : null);
        String desc  = patch.description() != null ? patch.description() : (old != null ? old.description() : null);
        Long created = (old != null && old.createdAt() != null) ? old.createdAt() : now; // set if missing
        Long updated = now;

        StepsEnvelope updatedEnv = new StepsEnvelope(
                env.version(),
                new RecordingMeta(id, title, desc, created, updated),
                env.steps()
        );

        java.nio.file.Path file = nameOrPath.contains(java.nio.file.FileSystems.getDefault().getSeparator())
                ? java.nio.file.Paths.get(nameOrPath)
                : ReplayFromFileReactor.recordingsDir.resolve(nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json");

        try {
            json.writeValue(file.toFile(), updatedEnv);
            return updatedEnv.meta();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write: " + file, e);
        }
    }
}
