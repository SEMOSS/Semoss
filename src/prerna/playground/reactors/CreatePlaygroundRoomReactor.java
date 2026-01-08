package prerna.playground.reactors;

import prerna.engine.impl.model.inferencetracking.reactors.CreateRoomReactor;
import prerna.playground.PlaygroundUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CreatePlaygroundRoomReactor extends CreateRoomReactor {

	
	@Override
	public NounMetadata execute() {
		GenRowStruct projectGRS = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey());
		if(projectGRS != null) {
			projectGRS.clear();
		} else {
			projectGRS = new GenRowStruct();
		}
		projectGRS.add(new NounMetadata(PlaygroundUtils.PLAYGROUND_PROJECT_ID, PixelDataType.CONST_STRING));
		this.store.addNoun(ReactorKeysEnum.PROJECT.getKey(), projectGRS);
		return super.execute();
	}
	
	
}
