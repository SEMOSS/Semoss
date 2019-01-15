package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.ICache;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SynchronizeToRReactor extends AbstractRFrameReactor {

	/**
	 * This reactor takes a frame and synchronizes it to an r frame inputs are:
	 * 1) table name for the synchronized frame 2) working directory, which is
	 * optional and only used for tinker frame
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
		GenRowStruct tableNameGrs = this.store.getNoun(R_DATA_TABLE_NAME);
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
		GenRowStruct WdGrs = this.store.getNoun(WORKING_DIRECTORY);
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
		final String sep = java.lang.System.getProperty("file.separator");
		String random = Utility.getRandomString(10);
		String outputLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/") + sep + "R"
				+ sep + "Temp" + sep + "output" + random + ".tsv";
		gridFrame.execQuery("CALL CSVWRITE('" + outputLocation + "', 'SELECT " + selectors + " FROM "
				+ gridFrame.getName() + "', 'charset=UTF-8 fieldDelimiter= fieldSeparator=' || CHAR(9));");
		this.rJavaTranslator.executeR("library(data.table);");
		this.rJavaTranslator.executeR(rDataTableName + " <- fread(\"" + outputLocation + "\", sep=\"\t\");");
		File f = new File(outputLocation);
		f.delete();
		this.rJavaTranslator.executeR("setDT(" + rDataTableName + ")");

		// modify the headers to be what they used to be because the query
		// return everything in
		// all upper case which may not be accurate
		String[] currHeaders = this.rJavaTranslator.getColumns(rDataTableName);
		renameColumn(rDataTableName, currHeaders, colSelectors, false);
		storeVariable("GRID_NAME", new NounMetadata(rDataTableName, PixelDataType.CONST_STRING));
		System.out.println("Completed synchronization as " + rDataTableName);

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
			this.rJavaTranslator.executeR("setwd(\"" + wd + "\")");
			// load the library
			Object ret = this.rJavaTranslator.executeR("library(\"igraph\");");
			if (ret == null) {
				ICache.deleteFolder(wd);
				throw new ClassNotFoundException("Package igraph could not be found!");
			}
			String loadGraphScript = rDataTableName + "<- read_graph(\"" + fileName + "\", \"graphml\");";
			java.lang.System.out.println(" Load !! " + loadGraphScript);
			// load the graph
			this.rJavaTranslator.executeR(loadGraphScript);
			this.rJavaTranslator.executeR(rDataTableName);

			System.out.println("Successfully synchronized, your graph is now available as " + rDataTableName);
			// store the graph name for future use
			storeVariable("GRAPH_NAME", new NounMetadata(rDataTableName, PixelDataType.CONST_STRING));

			// store the directories used for the iGraph
			List<String> graphLocs = new Vector<String>();
			if (retrieveVariable(R_GRAQH_FOLDERS) != null) {
				graphLocs = (List<String>) retrieveVariable(R_GRAQH_FOLDERS);
			}
			graphLocs.add(wd);
			storeVariable(R_GRAQH_FOLDERS, new NounMetadata(graphLocs, PixelDataType.CONST_STRING));
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(
					"ERROR ::: Could not convert TinkerFrame into igraph.\nPlease make sure iGraph package is installed.");
		} finally {
			// reset back to the original wd
			if (curWd != null) {
				this.rJavaTranslator.executeR("setwd(\"" + curWd + "\")");
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
				ex.printStackTrace();
			} finally {
				try {
					if (os != null) {
						os.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return absoluteFileName;
	}
}
