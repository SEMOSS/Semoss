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
package prerna.reactor.frame.r;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class SynchronizeToRReactor extends AbstractRFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(SynchronizeToRReactor.class);

	/**
	 * This reactor takes a frame and synchronizes it to an r frame inputs are: 1)
	 * table name for the synchronized frame 2) working directory, which is optional
	 * and only used for tinker frame
	 */

	// keys used to retrieve user input
	private static final String R_DATA_TABLE_NAME = "rDataTable";
	private static final String WORKING_DIRECTORY = "Wd";

	// this variable is used for synchronizing from tinker
	public static final String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";

	// counter variable will be used for assigning default r data table names
	private static long counter = 0;

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();

		// get frame - we dont know what type of frame this is yet
		ITableDataFrame frame = getFrame();
		// get input
		// get the desired table name for the r data table
		String rDataTableName = getSyncedTableName();
		// need to determine the type of frame
		// synchronization method will depend on the frame type
		if (frame instanceof H2Frame) {
			synchronizeGridToR(frame, rDataTableName);
		} else if (frame instanceof TinkerFrame) {
			String wd = getWd();
			synchronizeGraphToR(frame, rDataTableName, wd);
		} else {
			throw new IllegalArgumentException("Current frame type not supported");
		}

		return new NounMetadata(rDataTableName, PixelDataType.CONST_STRING);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////// GET PIXEL INPUT /////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getSyncedTableName() {
		// see if defined as individual key
		GenRowStruct tableNameGrs = this.store.getGenRowStruct(R_DATA_TABLE_NAME);
		if (tableNameGrs != null) {
			if (tableNameGrs.size() > 0) {
				return tableNameGrs.get(0).toString();
			}
		}
		return getDefaultName();
	}

	private String getDefaultName() {
		// TODO: need to check variable names
		// make sure default name won't override
		return "df_" + counter++;
	}

	// wd needed to synchronize from tinker
	private String getWd() {
		// see if working directory has been defined
		GenRowStruct WdGrs = this.store.getGenRowStruct(WORKING_DIRECTORY);
		if (WdGrs != null) {
			if (WdGrs.size() > 0) {
				return WdGrs.get(0).toString();
			}
		}
		return getDefaultWd();
	}

	// get default wd is none is defined and original frame is tinker
	private String getDefaultWd() {
		String baseFolder = getBaseFolder();
		String randomDir = Utility.getRandomString(22);
		String wd = baseFolder + "/" + randomDir;
		return wd;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////// SYNCHRONIZATION METHODS /////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	// this method used to go from h2 to r
	private void synchronizeGridToR(ITableDataFrame frame, String rDataTableName) {
		long start = java.lang.System.currentTimeMillis();
		// logger.info("Synchronizing H2Frame to R data.table...");
		// cast frame to an h2 frame
		H2Frame gridFrame = (H2Frame) frame;

		// note : do not use * since R will not preserve the column order
		// use the string[] of selectors to build a string with selectors
		// separated by commas
		StringBuilder selectors = new StringBuilder();
		String[] colSelectors = gridFrame.getColumnHeaders();
		for (int selectIndex = 0; selectIndex < colSelectors.length; selectIndex++) {
			// TODO: lots of assumptions around a single table
			// TODO: lots of assumptions around a single table
			// TODO: lots of assumptions around a single table
			String colSelector = colSelectors[selectIndex];
			if (colSelector.contains("__")) {
				colSelector = colSelector.split("__")[1];
				selectors.append(colSelector);
				colSelectors[selectIndex] = colSelector;
			} else {
				selectors.append(colSelector);
			}
			if (selectIndex + 1 < colSelectors.length) {
				selectors.append(", ");
			}
		}

		// we'll write to TSV and load into data.table to avoid rJava setup
		String random = Utility.getRandomString(10);
		String outputLocation = Utility.getBaseFolder().replace("\\", "/") + DIR_SEPARATOR + "R" + DIR_SEPARATOR
				+ "Temp" + DIR_SEPARATOR + "output" + random + ".tsv";
		try {
			gridFrame.getBuilder().runQuery("CALL CSVWRITE('" + outputLocation + "', 'SELECT " + selectors + " FROM "
					+ gridFrame.getName() + "', 'charset=UTF-8 fieldDelimiter= fieldSeparator=' || CHAR(9));");
		} catch (Exception e) {
			classLogger.error("Failed to export grid frame {} to temporary TSV {} for R synchronization.",
					gridFrame.getName(), outputLocation, e);
		}
		this.rJavaTranslator.executeEmptyR("library(data.table);");
		this.rJavaTranslator.executeEmptyR(rDataTableName + " <- fread(\"" + outputLocation + "\", sep=\"\t\");");
		File f = new File(Utility.normalizePath(outputLocation));
		f.delete();
		this.rJavaTranslator.executeEmptyR("setDT(" + rDataTableName + ")");

		// modify the headers to be what they used to be because the query
		// return everything in
		// all upper case which may not be accurate
		String[] currHeaders = this.rJavaTranslator.getColumns(rDataTableName);
		renameColumn(rDataTableName, currHeaders, colSelectors, false);
		storeVariable("GRID_NAME", new NounMetadata(rDataTableName, PixelDataType.CONST_STRING));

		long end = java.lang.System.currentTimeMillis();
		// logger.info("Done synchronizing to R data.table...");
		// logger.debug("Time to finish synchronizing to R data.table " +
		// (end-start) + "ms");

	}

	// this method used to go from Tinker to r
	private void synchronizeGraphToR(ITableDataFrame frame, String rDataTableName, String wd) {
		java.io.File file = new File(wd);
		String curWd = null;
		try {
			// logger.info("Trying to start R.. ");
			// logger.info("Successfully started R");

			// get the current directory
			// we need to switch out of this to write the graph file
			// but want to go back to this original one
			curWd = this.rJavaTranslator.getString("getwd()");

			// create this directory
			file.mkdir();
			String fileName = writeGraph(frame, wd);

			wd = wd.replace("\\", "/");

			// set the working directory
			this.rJavaTranslator.executeEmptyR("setwd(\"" + wd + "\")");
			// load the library
			Object ret = this.rJavaTranslator.executeR("library(\"igraph\");");
			if (ret == null) {
				FileSystemUtil.deleteFolderIfExists(wd);
				throw new ClassNotFoundException("Package igraph could not be found!");
			}
			String loadGraphScript = rDataTableName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			this.rJavaTranslator.executeEmptyR(loadGraphScript);
			this.rJavaTranslator.executeEmptyR(rDataTableName);

			System.out.println("Successfully synchronized, your graph is now available as " + rDataTableName);
			// store the graph name for future use
			storeVariable("GRAPH_NAME", new NounMetadata(rDataTableName, PixelDataType.CONST_STRING));

			// store the directories used for the iGraph
			List<String> graphLocs = new ArrayList<String>();
			if (retrieveVariable(R_GRAQH_FOLDERS) != null) {
				graphLocs = (List<String>) retrieveVariable(R_GRAQH_FOLDERS);
			}
			graphLocs.add(wd);
			storeVariable(R_GRAQH_FOLDERS, new NounMetadata(graphLocs, PixelDataType.CONST_STRING));
		} catch (Exception ex) {
			classLogger.error("Failed to synchronize graph frame {} to R in working directory {}.", frame.getName(), wd,
					ex);
		} finally {
			// reset back to the original wd
			if (curWd != null) {
				this.rJavaTranslator.executeEmptyR("setwd(\"" + curWd + "\")");
			}
		}
		// java.lang.System.setSecurityManager(reactorManager);
	}

	/**
	 * Serialize the TinkerGraph in GraphML format
	 * 
	 * @param directory
	 * @return
	 */
	public String writeGraph(ITableDataFrame frame, String directory) {
		String absoluteFileName = null;
		if (frame instanceof TinkerFrame) {
			final Graph graph = ((TinkerFrame) frame).g;
			absoluteFileName = "output" + java.lang.System.currentTimeMillis() + ".xml";
			String fileName = directory + "/" + absoluteFileName;
			OutputStream os = null;
			try {
				os = new FileOutputStream(fileName);
				graph.io(IoCore.graphml()).writer().normalize(true).create().writeGraph(os, graph);
			} catch (Exception ex) {
				classLogger.error("Failed to serialize graph frame {} to GraphML file {}.", frame.getName(), fileName,
						ex);
			} finally {
				try {
					if (os != null) {
						os.close();
					}
				} catch (IOException e) {
					classLogger.error("Failed to close GraphML output stream for file {}.", fileName, e);
				}
			}
		}
		return absoluteFileName;
	}
}
