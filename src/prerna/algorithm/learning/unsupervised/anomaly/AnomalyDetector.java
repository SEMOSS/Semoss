package prerna.algorithm.learning.unsupervised.anomaly;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutine;
import prerna.algorithm.learning.r.RRoutineException;
import prerna.algorithm.learning.r.RRoutine.Builder.RRoutineType;

public class AnomalyDetector {

	private RRoutine rRoutine;

	/**
	 * The direction in which to detect anomalies
	 * 
	 * @author tbanach
	 *
	 */
	public enum AnomDirection {
		POSITIVE, NEGATIVE, BOTH
	}

	private static final String SCRIPT_NAME = "AnomalyDetection.R";
	private static final String CATEGORICAL_SCRIPT_NAME = "CategoricalAnomalyDetection.R";
	private static final String R_SYNC_FRAME_NAME = "dt";

	// If the user chooses to keep existing columns, then the data frame with
	// the same name as R_SYNC_FRAME_NAME will be returned
	// If the user chooses not to keep existing columns,
	// a simplified frame is returned, given by the following name
	private static final String R_SIMPLIFIED_FRAME_NAME = "unique.times";

	/**
	 * 
	 * This constructor creates an anomaly detector that detects anomalies in
	 * numeric data.
	 * 
	 * @param dataFrame
	 *            The ITableDataFrame containing a numeric series
	 * @param timeColumn
	 *            The column containing time stamps; can be date, string, or
	 *            numeric representation
	 * @param seriesColumn
	 *            The column containing a numeric series with potential
	 *            anomalies
	 * @param aggregateFunction
	 *            The function used to aggregate the series when there are
	 *            duplicated time stamps
	 * @param maxAnoms
	 *            The maximum proportion of the series of counts that can be
	 *            considered an anomaly, must be between 0 and 1
	 * @param direction
	 *            The direction in which anomalies can occur, includes POSITIVE,
	 *            NEGATIVE, and BOTH
	 * @param alpha
	 *            The level of statistical significance, must be between 0 and
	 *            1, but should generally be less than 0.1
	 * @param period
	 *            The number of time stamps per natural cycle; anomalies are
	 *            sensitive to this input
	 * @param keepExistingColumns
	 *            Whether to keep the existing column structure and add to it,
	 *            or return a simplified data frame
	 */
	public AnomalyDetector(ITableDataFrame dataFrame, String timeColumn, String seriesColumn, String aggregateFunction,
			double maxAnoms, AnomDirection direction, double alpha, int period, boolean keepExistingColumns) {
		String arguments = "'" + timeColumn + "';'" + seriesColumn + "';'" + aggregateFunction + "';" + maxAnoms + ";'"
				+ determineDirectionString(direction) + "';" + alpha + ";" + period;
		if (keepExistingColumns) {
			rRoutine = new RRoutine.Builder(dataFrame, SCRIPT_NAME, R_SYNC_FRAME_NAME).arguments(arguments)
					.routineType(RRoutineType.ANALYTICS).build();
		} else {
			rRoutine = new RRoutine.Builder(dataFrame, SCRIPT_NAME, R_SYNC_FRAME_NAME)
					.rReturnFrameName(R_SIMPLIFIED_FRAME_NAME).arguments(arguments).routineType(RRoutineType.ANALYTICS)
					.build();
		}
	}

	/**
	 * This constructor creates an anomaly detector that detects anomalies in
	 * categorical data. This is done by counting the number of events that
	 * occur per unit time for each group.
	 * 
	 * @param dataFrame
	 *            The ITableDataFrame containing categorical data
	 * @param timeColumn
	 *            The column containing time stamps; can be date, string, or
	 *            numeric representation
	 * @param eventColumn
	 *            The column containing events to count, usually the primary key
	 *            when counting records
	 * @param groupColumn
	 *            The column to group by; the count of events is reported for
	 *            each level of this group
	 * @param aggregateFunction
	 *            The function used to aggregate events, count or count distinct
	 * @param maxAnoms
	 *            The maximum proportion of the series of counts that can be
	 *            considered an anomaly, must be between 0 and 1
	 * @param direction
	 *            The direction in which anomalies can occur, includes POSITIVE,
	 *            NEGATIVE, and BOTH
	 * @param alpha
	 *            The level of statistical significance, must be between 0 and
	 *            1, but should generally be less than 0.1
	 * @param period
	 *            The number of time stamps per natural cycle; anomalies are
	 *            sensitive to this input
	 */
	public AnomalyDetector(ITableDataFrame dataFrame, String timeColumn, String eventColumn, String groupColumn,
			String aggregateFunction, double maxAnoms, AnomDirection direction, double alpha, int period) {
		String arguments = "'" + timeColumn + "';'" + eventColumn + "';'" + groupColumn + "';'" + aggregateFunction
				+ "';" + maxAnoms + ";'" + determineDirectionString(direction) + "';" + alpha + ";" + period;
		rRoutine = new RRoutine.Builder(dataFrame, CATEGORICAL_SCRIPT_NAME, R_SYNC_FRAME_NAME)
				.rReturnFrameName(R_SIMPLIFIED_FRAME_NAME).arguments(arguments).routineType(RRoutineType.ANALYTICS)
				.build();
	}

	/**
	 * Detects anomalies using this detector.
	 * 
	 * @return ITableDataFrame - The data frame after running anomaly detection
	 * @throws RRoutineException
	 */
	public ITableDataFrame detectAnomalies() throws RRoutineException {
		return rRoutine.returnDataFrame();
	}

	// Determines the proper string to represent the direction in which to
	// detect anomalies
	// This string is what is passed into the arguments list that is
	// synchronized to R
	private String determineDirectionString(AnomDirection direction) {
		switch (direction) {
		case POSITIVE:
			return ("pos");
		case NEGATIVE:
			return ("neg");
		case BOTH:
			return ("both");
		default:
			return ("both");
		}
	}
}
