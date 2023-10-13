package prerna.reactor.frame.rdbms;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class EncodeColumnReactor extends AbstractFrameReactor {

    @Override
    public NounMetadata execute() {

        organizeKeys();

        AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
        Set<String> columnHeaders = Stream.of(frame.getColumnHeaders()).collect(Collectors.toSet());
        List<String> columns = this.store.nounRow.get("columns").getVector().stream().map(noun -> noun.getValue().toString()).collect(Collectors.toList());
        if (!columnHeaders.containsAll(columns)) {
            throw new IllegalArgumentException("One or more columns could not be found: " + columnHeaders.removeAll(columns));
        }

        String[] columnsToUpdate = new String[columns.size()];
        columns.toArray(columnsToUpdate);

        String frameName = frame.getName();
        PreparedStatement statement = frame.getBuilder().hashColumn(frameName, columnsToUpdate);
        try {
            for (int i = 0; i < columns.size(); i++) {
                String col = columns.get(i);
                String query = frame.getQueryUtil().modColumnType(frameName, col, "VARCHAR");
                frame.getBuilder().runQuery(query);
            }
            statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
		// upon successful execution
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		for(String col : columns) {
			// set the type for all the columns to be string
			metadata.modifyDataTypeToProperty(frameName + "__" + col, frameName, SemossDataType.STRING.toString());
		}

        // NEW TRACKING
        UserTrackerFactory.getInstance().trackAnalyticsWidget(
                this.insight,
                frame,
                "EncodeColumnReactor",
                AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

        NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
        return noun;

    }
}
