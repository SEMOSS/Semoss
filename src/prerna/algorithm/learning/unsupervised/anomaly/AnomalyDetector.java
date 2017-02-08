package prerna.algorithm.learning.unsupervised.anomaly;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutine;
import prerna.algorithm.learning.r.RRoutineException;
import prerna.algorithm.learning.r.RRoutine.Builder.RRoutineType;

public class AnomalyDetector {

	private RRoutine rRoutine;

	public enum AnomDirection {
		POSITIVE, NEGATIVE, BOTH
	}

	private static final String SCRIPT_NAME = "AnomalyDetection.R";
	private static final String R_FRAME_NAME = "df";

	// TODO enforce restrictions on parameters
	public AnomalyDetector(ITableDataFrame dataFrame, String seriesColumn, String timeColumn, double maxAnoms,
			AnomDirection direction, double alpha, int period) {

		String stringDirection;
		switch (direction) {
		case POSITIVE:
			stringDirection = "pos";
			break;
		case NEGATIVE:
			stringDirection = "neg";
			break;
		case BOTH:
			stringDirection = "both";
			break;
		default:
			stringDirection = "both";
			break;
		}

		String selectedColumns = timeColumn + ";" + seriesColumn;

		String arguments = "'" + seriesColumn + "';" + maxAnoms + ";'" + stringDirection + "';" + alpha + ";" + period
				+ ";FALSE";

		rRoutine = new RRoutine.Builder(dataFrame, SCRIPT_NAME, R_FRAME_NAME).selectedColumns(selectedColumns)
				.arguments(arguments).routineType(RRoutineType.ANALYTICS).build();
	}

	public ITableDataFrame detectAnomalies() throws RRoutineException {
		return rRoutine.returnDataFrame();
	}
}
