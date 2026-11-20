/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.export;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.convert.ConvertReactor;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.util.Utility;

public class CollectSeabornReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	// sns.relplot(data=plotterframe, x='height', y='weight', kind='scatter')

	private static final Logger classLogger = LogManager.getLogger(CollectSeabornReactor.class);

	private int limit = 0;

	public CollectSeabornReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SPLOT.getKey(), ReactorKeysEnum.FORMAT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		PyTranslator pyt = this.insight.getPyTranslator();

		String command = keyValue.get(keysToGet[0]) + "";
		String format = "png";
		if (keyValue.containsKey(keysToGet[1])) {
			format = keyValue.get(keysToGet[1]);
		}

		this.task = getTask();

		String loadDT = "";
		String adjustTypes = "";

		// I need to get the basic iterator and then get types from there
		// this is typically what we do on seaborn

		// import seaborn as sns
		// daplot = <Whatever the user enters>
		// daplot.savefig(location)
		// del daplot
		// del plotterframe
		// return output

		SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
		ITableDataFrame thisFrame = qs.getFrame();
		String type = thisFrame.getFrameType().getTypeAsString();

		// need to also check if it is already there
		// obviously the issue of synchronization comes but for now

		if (!type.equalsIgnoreCase("py")) {
			ConvertReactor cr = new ConvertReactor();
			GenRowStruct grs = new GenRowStruct();
			grs.add(new NounMetadata(thisFrame, PixelDataType.FRAME));
			this.getNounStore().addNoun(ReactorKeysEnum.FRAME.getKey(), grs);
			grs = new GenRowStruct();
			grs.add(new NounMetadata("PY", PixelDataType.CONST_STRING));
			this.getNounStore().addNoun(ReactorKeysEnum.FRAME_TYPE.getKey(), grs);
			grs = new GenRowStruct();
			grs.add(new NounMetadata(thisFrame.getName(), PixelDataType.CONST_STRING));
			this.getNounStore().addNoun(ReactorKeysEnum.ALIAS.getKey(), grs);
			cr.setNounStore(getNounStore());
			cr.setInsight(this.insight);
			cr.execute();

			insight.getVarStore().put("PY_SYNCHRONIZED", new NounMetadata(true, PixelDataType.BOOLEAN));
			// need replace to the frame back
			insight.getVarStore().put(Insight.CUR_FRAME_KEY, new NounMetadata(thisFrame, PixelDataType.FRAME));
		}

		qs.getRelations().clear();
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, thisFrame.getMetaData());
		PandasInterpreter interp = new PandasInterpreter();
		interp.setDataTableName(thisFrame.getName(), thisFrame.getName() + "w" + ".cache['data']");
		interp.setDataTypeMap(thisFrame.getMetaData().getHeaderToTypeMap());
		interp.setQueryStruct(qs);

		StringBuffer columns = new StringBuffer("columns=[");
		// compose the columns string
		try {
			IRawSelectWrapper taskItearator = (((BasicIteratorTask) (task)).getIterator());
			String[] headers = taskItearator.getHeaders();
			for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				if (headerIndex != 0) {
					columns.append(",");
				}
				columns.append("'").append(headers[headerIndex]).append("'");
			}
			columns.append("]");
		} catch (Exception ex) {
		}
		// pd.DataFrame.from_dict(mv.loc[(mv['Genre'].isin(['Family-Animation']))
		// ].iloc[0:][['MovieBudget', 'Genre', 'Nominated',
		// 'RevenueDomestic']].to_dict('split'), orient='index')
		// get the composed string and turn it into a data frame
		String subDataTable = "pd.DataFrame(" + interp.composeQuery() + "['data'], " + columns + ")";

		command = command.replaceAll("\\s", ""); // remove all spaces
		String assigner = "plotterframe = " + subDataTable;
		if (command.contains("data=" + thisFrame.getName() + "")) {
			command = command.replace("data=" + thisFrame.getName() + "", "data=plotterframe");
		}

		String ROOT = insight.getInsightFolder();
		ROOT = ROOT.replace("\\", "/");

		// this reactor saves the figure itself and returns it as the task output,
		// so turn off the automatic inline rendering that would otherwise also
		// push a copy of the image onto the console stream
		String disableInlineDisplay = "smss_inline_display(False)";
		String importSeaborn = "import seaborn as sns";
		String importMatPlot = "import matplotlib.pyplot as plt";
		String clearPlot = "plt.clf()";
		String runPlot = command;
		String seabornFile = Utility.getRandomString(6);
		String printFile = "print(saveFile)";
		String saveFileName = "saveFile = '" + ROOT + "/" + seabornFile + "." + format + "'";
		String savePlot = "plt.savefig(saveFile)";
		String removeFrame = "";
		String removeSeaborn = "del(sns)";
		String removeMatPlot = "del(plt)";
		String removeSaveFile = "del(saveFile)";

		seabornFile = (String) pyt.runDirectPy(disableInlineDisplay, loadDT, adjustTypes, importSeaborn, importMatPlot,
				clearPlot, assigner, saveFileName, runPlot, savePlot, removeFrame, removeSeaborn, removeMatPlot,
				printFile, removeSaveFile);

		String IF = insight.getInsightFolder();
		seabornFile = Utility.normalizePath(seabornFile.replace("$IF", IF));

		StringWriter sw = new StringWriter();
		try {
			// read the file and populate it
			byte[] bytes = FileUtils.readFileToByteArray(new File(seabornFile));
			String encodedString = Base64.getEncoder().encodeToString(bytes);
			String mimeType = "image/png";
			mimeType = Files.probeContentType(new File(seabornFile).toPath());
			sw.write("<img src='data:" + mimeType + ";base64," + encodedString + "'>");
		} catch (IOException e) {
			classLogger.error("Failed to read and Base64-encode the generated Seaborn plot image file {}", seabornFile,
					e);
		}

		new File(seabornFile).delete();

		ConstantDataTask cdt = new ConstantDataTask();

		Map<String, Object> outputMap = new HashMap<String, Object>();

		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);

		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[] { sw.toString() });
		outputMap.put("splot", command);
		outputMap.put("format", format);

		cdt.setOutputData(outputMap);

		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA,
				PixelOperationType.FILE);
	}

	@Override
	protected void buildTask() throws Exception {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if (this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if (outputs != null && !outputs.isEmpty()) {
			return outputs;
		}

		outputs = new ArrayList<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET,
				PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
