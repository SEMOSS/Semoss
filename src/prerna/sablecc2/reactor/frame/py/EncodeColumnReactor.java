package prerna.sablecc2.reactor.frame.py;

import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EncodeColumnReactor extends AbstractPyFrameReactor {

    @Override
    public NounMetadata execute() {

        organizeKeys();
        PandasFrame frame = (PandasFrame) getFrame();
        String frameName = frame.getName();
        List<String> scripts = new ArrayList<>();

        scripts.add("import hashlib");
        scripts.add("def encode(value) : return hashlib.sha256(str(value).encode()).hexdigest()");
        List<String> columns = this.store.nounRow.get("columns").getVector().stream().map(noun -> noun.getValue().toString()).collect(Collectors.toList());
        for (String col : columns) {
            scripts.add(frameName + "['" + col + "'] = " + frameName + "['" + col + "'].apply(encode)");
        }

        scripts.add("print(" + frameName + ")");
        String[] scriptArray = new String[scripts.size()];
        scripts.toArray(scriptArray);
        insight.getPyTranslator().runPyAndReturnOutput(scriptArray);

        // NEW TRACKING
        UserTrackerFactory.getInstance().trackAnalyticsWidget(
                this.insight,
                frame,
                "EncodeColumnReactor",
                AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

        return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
    }
}
