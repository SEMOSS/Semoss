package prerna.sablecc2.reactor.algorithms;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.RImporter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RatioAlgorithmReactor extends AbstractReactor {

	private static final String CLASS_NAME = RatioAlgorithmReactor.class.getName();

	private static final String WEIGHTS_KEY = "weight";

	//keys for resulting frame
	private static final String SCORE_LABEL = "Score_";

	private String[] ratioFrameHeaders;

	public RatioAlgorithmReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// get pixel inputs
		String instanceColumn = getInstanceColumn();
		List<String> attributeColumns = getAttributes();
		ITableDataFrame frame = getData();
		String origTableName = frame.getName();
		frame.setLogger(logger);
		optimizeFrame(frame, instanceColumn);

		// generate the new frame to hold the ratio information
		RDataTable newFrame = new RDataTable(this.insight.getRJavaTranslator(logger), origTableName);
		newFrame.setLogger(logger);

		// set ratio frame headers
		// instanceCol_1, instanceCol_2, ratio, score_attributeCol1, score_attributeCol2....
		this.ratioFrameHeaders = new String[3 + attributeColumns.size()];
		String[] dataTypes = new String[3 + attributeColumns.size()];
		String instanceColumnHeader = "";
		if (instanceColumn.contains("__")) {
			String[] split = instanceColumn.split("__");
			instanceColumnHeader = split[1];
		} else {
			instanceColumnHeader = instanceColumn;
		}
		this.ratioFrameHeaders[0] = instanceColumnHeader + "_1";
		this.ratioFrameHeaders[1] = instanceColumnHeader + "_2";
		this.ratioFrameHeaders[2] = "Ratio";

		dataTypes[0] = SemossDataType.STRING.toString();
		dataTypes[1] = SemossDataType.STRING.toString();
		dataTypes[2] = SemossDataType.DOUBLE.toString();

		int headersIndex = 3;
		for (int i = 0; i < attributeColumns.size(); i++) {
			String attribute = attributeColumns.get(i).toString();
			if (attribute.contains("__")) {
				String[] split = attribute.split("__");
				this.ratioFrameHeaders[headersIndex] = SCORE_LABEL+ split[1];
			} else {
				this.ratioFrameHeaders[headersIndex] = SCORE_LABEL + attribute;
			}
			dataTypes[headersIndex] = SemossDataType.DOUBLE.toString();
			headersIndex++;
		}
		Map<String, Double> weights = getWeights();

		// set up csv path
		FileWriter writer = null;
		BufferedWriter bufferedWriter = null;

		String insightCacheDir = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		final String FILE_SEPARATOR = System.getProperty("file.separator");
		final String LINE_SEPARATOR = "\n";
		String csvCache = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String path = insightCacheDir + FILE_SEPARATOR + csvCache + FILE_SEPARATOR + Utility.getRandomString(10) + ".csv";
		StringBuilder sb = new StringBuilder();
		boolean fileError = false;
		// write headers to csv file
		try {
			writer = new FileWriter(path);
			bufferedWriter = new BufferedWriter(writer);
			for (int i = 0; i < this.ratioFrameHeaders.length; i++) {
				sb.append("\"").append(this.ratioFrameHeaders[i].toString()).append("\"");
				if (i < this.ratioFrameHeaders.length - 1) {
					sb.append(",");
				}
			}
			bufferedWriter.write(sb.append(LINE_SEPARATOR).toString());
		} catch (IOException ex) {
			fileError = true;
			throw new IllegalArgumentException("Unable to write to file");
		} finally {
			if(fileError) {
				try {
					if(bufferedWriter != null) {
						bufferedWriter.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					if(writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		int counter = 0;

		// grab all unique instance values for instance column
		List<Object> instanceValuesList = getInstanceValues(frame, instanceColumn);

		//iterate through combinations
		logger.setLevel(Level.OFF);
		try {
			for (int sourceIndex = 0; sourceIndex < instanceValuesList.size(); sourceIndex++) {
				Object sourceInstance = instanceValuesList.get(sourceIndex);

				// so we do not need to calculate this so many times
				// we will grab and store all the values for the source
				// and just grab for each target
				List<List<String>> sourceAttributesStore = new Vector<List<String>>();
				for (int attributeIndex = 0; attributeIndex < attributeColumns.size(); attributeIndex++) {
					String attribute = attributeColumns.get(attributeIndex);
					List<String> sourceAttributes = getAttributeValuesForInstance(frame, instanceColumn, sourceInstance, attribute);
					sourceAttributesStore.add(sourceAttributes);
				}

				for (int targetIndex = sourceIndex + 1; targetIndex < instanceValuesList.size(); targetIndex++) {
					Object targetInstance = instanceValuesList.get(targetIndex);

					Object[] cells = new Object[this.ratioFrameHeaders.length];
					cells[0] = sourceInstance;
					cells[1] = targetInstance;

					int cellsIndex = 3;
					// get instance values for first attribute
					double ratio = 0;
					for (int attributeIndex = 0; attributeIndex < attributeColumns.size(); attributeIndex++) {
						String attribute = attributeColumns.get(attributeIndex);
						// we have the source and target ... lets calculate the similarity
						List<String> sourceAttributes = sourceAttributesStore.get(attributeIndex);
						List<String> targetAttributes = getAttributeValuesForInstance(frame, instanceColumn, targetInstance, attribute);

						// get the union size
						Set<String> union = new HashSet<String>(sourceAttributes);
						union.addAll(targetAttributes);
						int unionSize = union.size();
						double score = 0.0;
						// if the union size is 0
						// then there is no similarity
						if(unionSize != 0) {
							// now determine the intersect
							union.retainAll(sourceAttributes);
							union.retainAll(targetAttributes);
							int intersectSize = union.size();

							score = (double) intersectSize / (double) unionSize;
						}
						//clean Attribute
						String cleanAttribute = attribute;
						if(attribute.contains("__")){
							String[] split = attribute.split("__");
							cleanAttribute = split[1];
						}
						//multiply by weight
						cells[cellsIndex] = score;
						ratio += score * weights.get(cleanAttribute);
						cellsIndex++;
					}

					cells[2] = ratio;

					// write data in CSV format
					// A - B, score
					sb = new StringBuilder();
					sb.append("\"").append(cells[0].toString()).append("\",");
					sb.append("\"").append(cells[1].toString()).append("\",");
					for (int i = 2; i < cells.length; i++) {
						sb.append(cells[i].toString());
						if (i < cells.length - 1) {
							sb.append(",");
						}
					}
					counter++;
					sb.append(LINE_SEPARATOR);
					bufferedWriter.write(sb.toString());

					// B - A, score
					sb = new StringBuilder();
					sb.append("\"").append(cells[1].toString()).append("\",");
					sb.append("\"").append(cells[0].toString()).append("\",");
					for (int i = 2; i < cells.length; i++) {
						sb.append(cells[i].toString());
						if (i < cells.length - 1) {
							sb.append(",");
						}
					}
					sb.append(LINE_SEPARATOR);
					bufferedWriter.write(sb.toString());

					counter++;
					if(counter % 5000 == 0) {
						logger.setLevel(Level.INFO);
						logger.info("Added row #" + counter);
						logger.setLevel(Level.OFF);
					}
				}
				//write ratio match to itself A-A 
				Object targetInstance = sourceInstance;
				Object[] cells = new Object[this.ratioFrameHeaders.length];
				cells[0] = sourceInstance;
				cells[1] = targetInstance;

				int cellsIndex = 3;
				double ratio = 0;
				for (int i = 0; i < attributeColumns.size(); i++) {
					String attribute = attributeColumns.get(0);
					int unionSize = 1;
					int intersectSize = 1;
					double score = (double) intersectSize / (double) unionSize;
					//clean Attribute
					String cleanAttribute = attribute;
					if(attribute.contains("__")){
						String[] split = attribute.split("__");
						cleanAttribute = split[1];
					}
					cells[cellsIndex] = score;
					//multiply by weight
					ratio += score * weights.get(cleanAttribute);
					cellsIndex++;
				}
				cells[2] = ratio;

				// write data in CSV format
				// A - A, ratio, attribute score
				sb = new StringBuilder();
				sb.append("\"").append(cells[0].toString()).append("\",");
				sb.append("\"").append(cells[1].toString()).append("\",");
				for (int i = 2; i < cells.length; i++) {
					sb.append(cells[i].toString());
					if (i < cells.length - 1) {
						sb.append(",");
					}
				}
				counter++;
				sb.append(LINE_SEPARATOR);
				bufferedWriter.write(sb.toString());
			}
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		logger.setLevel(Level.INFO);

		// add data to R frame
		CsvQueryStruct csvQS = new CsvQueryStruct();
		csvQS.setDelimiter(',');
		csvQS.setFilePath(path);
		// add headers to qs
		Map<String, String> csvHeaders = new HashMap<String, String>();
		for (int i = 0; i < this.ratioFrameHeaders.length; i++) {
			QueryColumnSelector csvColSelector = new QueryColumnSelector();
			csvColSelector.setTable(origTableName);
			csvColSelector.setColumn(this.ratioFrameHeaders[i]);
			csvColSelector.setAlias(this.ratioFrameHeaders[i]);
			csvQS.addSelector(csvColSelector);
			csvHeaders.put(this.ratioFrameHeaders[i], dataTypes[i]);
		}
		csvQS.setColumnTypes(csvHeaders);
		csvQS.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE);
		RImporter importer = new RImporter(newFrame, csvQS);
		importer.insertData();

		//		//testing
		//		it = newFrame.iterator();
		//		counter = 0;
		//		while(it.hasNext()) {
		//			it.next();
		//			counter++;
		//		}
		//		System.out.println(counter);
		//		System.out.println(counter);
		//		System.out.println(counter);
		//		System.out.println(counter);
		//		return null;

//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "Ratio");

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Ratio", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		this.insight.setDataMaker(newFrame);
		return new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<Object> getInstanceValues( ITableDataFrame frame, String instanceColumn) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryColumnSelector colSelector = new QueryColumnSelector();
		if (instanceColumn.contains("__")) {
			String[] split = instanceColumn.split("__");
			colSelector.setTable(split[0]);
			colSelector.setColumn(split[1]);
		} else {
			colSelector.setTable(instanceColumn);
			colSelector.setColumn(null);
		}
		qs.addSelector(colSelector);
		// execute query to get all the unique values
		Iterator<IHeadersDataRow> it = frame.query(qs);
		Set<Object> instancValues = new HashSet<Object>();
		// flush out the unique values
		while (it.hasNext()) {
			instancValues.add(it.next().getRawValues()[0]);
		}
		List<Object> instanceValuesList = new Vector<Object>();
		instanceValuesList.addAll(instancValues);		

		return instanceValuesList;
	}

	private List<String> getAttributeValuesForInstance(ITableDataFrame frame, String instanceColumn, Object sourceInstance, String attributeCol) {
		List<String> uniqueAttributes = new ArrayList<String>();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryColumnSelector colSelector = new QueryColumnSelector();
		if (attributeCol.contains("__")) {
			String[] split = attributeCol.split("__");
			colSelector.setTable(split[0]);
			colSelector.setColumn(split[1]);
		} else {
			colSelector.setTable(attributeCol);
			colSelector.setColumn(null);
		}
		qs.addSelector(colSelector);
		SimpleQueryFilter instanceFilter = new SimpleQueryFilter(new NounMetadata(new QueryColumnSelector(instanceColumn), PixelDataType.COLUMN), "==", new NounMetadata(sourceInstance, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(instanceFilter);
		Iterator<IHeadersDataRow> it = frame.query(qs);
		while (it.hasNext()) {
			Object val = it.next().getValues()[0];
			if(val != null) {
				uniqueAttributes.add(val.toString());
			}
		}
		return uniqueAttributes;
	}

	/**
	 * Optimize the frame for querying
	 * If H2 -> create an index
	 * @param dataframe
	 * @param instanceColumn
	 */
	private void optimizeFrame(ITableDataFrame dataframe, String instanceColumn) {
		if (dataframe instanceof H2Frame) {
			H2Frame hFrame = (H2Frame) dataframe;
			String uniqueName = hFrame.getMetaData().getUniqueNameFromAlias(instanceColumn);
			if(uniqueName == null) {
				uniqueName = instanceColumn;
			}
			hFrame.addColumnIndex(uniqueName.split("__")[1]);
		}
	}

	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////

	/*
	 * Get input values from store
	 */

	private String getInstanceColumn() {
		//check if instance column was input with the key
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[0]);
		if(instanceGrs != null && !instanceGrs.isEmpty()) {
			return (String) instanceGrs.get(0);
		}

		throw new IllegalArgumentException("Need to define the instance for the Ratio reactor");
	}

	private List<String> getAttributes() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null && !columnGrs.isEmpty()) {
			List<String> attributes = new Vector<String>();
			for(int i = 0; i < columnGrs.size(); i++) {
				String attribute = columnGrs.get(i).toString();
				//                      if(!attribute.equals(instanceColumn)) {
				attributes.add(attribute);
				//                      }
			}
			return attributes;
		}

		throw new IllegalArgumentException("Need to define the attributes for the Ratio reactor");
	}

	private ITableDataFrame getData() {

		if(this.insight.getDataMaker() != null) {
			return (ITableDataFrame) this.insight.getDataMaker();
		}

		throw new IllegalArgumentException("Need to define the data for the Ratio reactor");
	}

	/**
	 * @return weights Map 
	 * 		   {"attributeCol" : weight value, ...}
	 */
	private Map<String, Double> getWeights() {
		//TODO get weights from user
		GenRowStruct columnGrs = this.store.getNoun(WEIGHTS_KEY);
		HashMap<String, Double> weightMap = null;
		if (columnGrs != null && !columnGrs.isEmpty()) {
			for (int i = 0; i < columnGrs.size(); i++) {
				System.out.println(columnGrs.get(i));
			}
		}
		//calculate weights
		if(weightMap == null) {
			weightMap = new HashMap<String, Double>();
			double attributeCount = this.ratioFrameHeaders.length - 3;
			double weight = 1 / attributeCount;
			for(int weightMapIndex = 3; weightMapIndex < this.ratioFrameHeaders.length; weightMapIndex++) {
				//attribute column in ratioFrameHeaders has Score_ need to clean this up
				String attributeCol = this.ratioFrameHeaders[weightMapIndex];
				attributeCol = attributeCol.substring(SCORE_LABEL.length());
				weightMap.put(attributeCol, weight);
			}
		}
		return weightMap;
	}
}