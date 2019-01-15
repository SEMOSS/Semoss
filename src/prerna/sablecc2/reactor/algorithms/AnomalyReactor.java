package prerna.sablecc2.reactor.algorithms;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;

public class AnomalyReactor extends AbstractRFrameReactor {

	public static final String TIME_COLUMN = "timeColumn";
	public static final String SERIES_COLUMN = "seriesColumn";
	public static final String AGG_FUNC = "aggregateFunction";
	public static final String MAX_ANOMS = "maxAnoms";
	public static final String DIRECTION = "direction";
	public static final String ALPHA = "alpha";
	public static final String PERIOD = "period";

	public AnomalyReactor() {
		this.keysToGet = new String[] { TIME_COLUMN, SERIES_COLUMN, AGG_FUNC, MAX_ANOMS, DIRECTION, ALPHA, PERIOD };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// check if packages are installed
		String[] packages = { "AnomalyDetection", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// inputs
		String timeColumn = this.keyValue.get(this.keysToGet[0]);
		String seriesColumn = this.keyValue.get(this.keysToGet[1]);
		String aggregateFunction = this.keyValue.get(this.keysToGet[2]);
		double maxAnoms = getDouble(MAX_ANOMS);
		String direction = this.keyValue.get(this.keysToGet[4]);
		double alpha = getDouble(ALPHA);
		int period = getPeriod();

		// get frame
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		if (frame == null) {
			new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		}

		// throw error if frame isnt r frame
		// TODO: we should sync to R in the pixel
		if (!(frame instanceof RDataTable)) {
			throw new IllegalArgumentException("Frame must be an R Datatable to generate anomalies.");
		}
		
		String table = frame.getName();
		// Convert string direction to AnomDirection
		// Default to both
		AnomDirection anomDirection;
		switch (direction) {
		case "positive":
			anomDirection = AnomDirection.POSITIVE;
			break;
		case "negative":
			anomDirection = AnomDirection.NEGATIVE;
			break;
		default:
			anomDirection = AnomDirection.BOTH;
			break;
		}

		String argsScript = "args <- list('" + timeColumn + "', '" + seriesColumn + "', '" + aggregateFunction + "', "
				+ maxAnoms + ", '" + anomDirection.toString().toLowerCase() + "', " + alpha + ", " + period + ")";
		String createFrame = "this.dt.is.reserved.for.anomaly.detection <- " + table;
		this.rJavaTranslator.executeR(argsScript);
		this.rJavaTranslator.executeR(createFrame);

		String script = "source(\"" + getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\AnomalyDetection.R" + "\");";
		script = script.replace("\\", "\\\\");

		this.rJavaTranslator.executeR(script);

		// do something with the results df
		String frameUpdate = table + " <- " + "this.dt.is.reserved.for.anomaly.detection";
		this.rJavaTranslator.executeR(frameUpdate);

		// garbage cleanup
		String gc = "rm( args , this.dt.is.reserved.for.anomaly.detection); gc();";
		this.rJavaTranslator.executeR(gc);

		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + aggregateFunction + "_" + seriesColumn);
		metaData.setAliasToProperty(table + "__" + aggregateFunction + "_" + seriesColumn,
				aggregateFunction + "_" + seriesColumn);
		metaData.setDataTypeToProperty(table + "__" + aggregateFunction + "_" + seriesColumn, "NUMBER");

		metaData.addProperty(table, table + "__" + "anom_" + aggregateFunction + "_" + seriesColumn);
		metaData.setAliasToProperty(table + "__" + "anom_" + aggregateFunction + "_" + seriesColumn,
				"anom_" + aggregateFunction + "_" + seriesColumn);
		metaData.setDataTypeToProperty(table + "__" + "anom_" + aggregateFunction + "_" + seriesColumn, "NUMBER");

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TIME_COLUMN)) {
			return "The column containing time stamps; can be date, string, or numeric representation";
		} else if (key.equals(SERIES_COLUMN)) {
			return "The column containing a numeric series with potential anomalies.";
		} else if (key.equals(AGG_FUNC)) {
			return "The function used to aggregate the series when there are duplicated time stamps";
		} else if (key.equals(MAX_ANOMS)) {
			return "The maximum proportion of the series of counts that can be considered an anomaly, must be between 0 and 1";
		} else if (key.equals(DIRECTION)) {
			return "The direction in which anomalies can occur, includes POSITIVE, NEGATIVE, and BOTH";
		} else if (key.equals(ALPHA)) {
			return "The level of statistical significance, must be between 0 and 1, but should generally be less than 0.1";
		} else if (key.equals(PERIOD)) {
			return "The number of time stamps per natural cycle; anomalies are sensitive to this input";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

	private double getDouble(String key) {
		// see if defined as individual key
		GenRowStruct grs = this.store.getNoun(key);
		if (grs != null) {
			List<Object> value = grs.getAllNumericColumns();
			if (value.size() > 0) {
				return ((Number) value.get(0)).doubleValue();
			}
		}
		// else, we assume it is column values in the curRow
		List<Object> value = this.curRow.getAllNumericColumns();
		if (value.size() > 0) {
			return ((Number) value.get(0)).doubleValue();
		}
		// return 0 by default;
		return 0.0;
	}

	private int getPeriod() {
		GenRowStruct grs = this.store.getNoun(PERIOD);
		if (grs != null) {
			List<Object> value = grs.getAllNumericColumns();
			if (value.size() > 0) {
				return ((Number) value.get(0)).intValue();
			}
		}
		return 0;
	}
}
