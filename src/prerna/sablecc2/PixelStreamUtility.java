package prerna.sablecc2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.ThreadStore;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.export.GraphFormatter;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.util.gson.GsonUtility;
import prerna.util.gson.InsightPanelAdapter;
import prerna.util.gson.InsightSheetAdapter;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;
import prerna.util.insight.InsightUtility;

public class PixelStreamUtility {

	private static final String CLASS_NAME = PixelStreamUtility.class.getName();
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	private static Gson getDefaultGson() {
		return GsonUtility.getDefaultGson();
	}
	
	private static Gson getPanelGson() {
		 return new GsonBuilder()
			.disableHtmlEscaping()
			.excludeFieldsWithModifiers(Modifier.STATIC)
			.registerTypeAdapter(Double.class, new NumberAdapter())
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.registerTypeAdapter(InsightPanel.class, new InsightPanelAdapter(true))
			.registerTypeAdapter(InsightSheet.class, new InsightSheetAdapter())
			.create();
	}
	
	/**
	 * Collect pixel data from the runner
	 * @param runner
	 * @return
	 */
	public static StreamingOutput collectPixelData(PixelRunner runner) {
		// get the default gson object
		Gson gson = getDefaultGson();

		// now process everything
		try {
			return new StreamingOutput() {
				PrintStream ps = null;
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try {
						ps = new PrintStream(outputStream, true, "UTF-8");
						// we want to ignore the first index since it will be a job
						LOGGER.debug("Starting to generate response");
						long start = System.currentTimeMillis();
						processPixelRunner(ps, gson, runner);
						long end = System.currentTimeMillis();
						LOGGER.debug("Time to generate json response = " + (end-start) + "ms");
					} catch(Exception e) {
						e.printStackTrace();
					} finally {
						if(ps != null) {
							ps.close();
						}
						ThreadStore.remove();
					}
				}};
		} catch (Exception e) {
			LOGGER.error("Failed to write object to stream");
		}
		return null;
	}
	
	/**
	 * Collect pixel data from the runner
	 * @param runner
	 * @return
	 */
	public static File writePixelData(PixelRunner runner, File fileToWrite) {
		// get the default gson object
		Gson gson = getDefaultGson();

		// now process everything
		FileOutputStream fos = null;
		try {
			StreamingOutput output = new StreamingOutput() {
				PrintStream ps = null;
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try {
						ps = new PrintStream(outputStream, false, "UTF-8");
						// we want to ignore the first index since it will be a job
						processPixelRunner(ps, gson, runner);
					} catch(Exception e) {
						e.printStackTrace();
						// ugh... this is unfortunate
					} finally {
						if(ps != null) {
							ps.close();
						}
					}
				}};
			fos = new FileOutputStream(fileToWrite);
			output.write(fos);
		} catch (Exception e) {
			LOGGER.error("Failed to write object to stream");
		} finally {
			try {
				fos.flush();
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return fileToWrite;
	}
	
	/** 
	 * Made for testing generation purposes, uncomment if you'd like to generate test output
	 */
	public static File writePixelDataForTest(PixelRunner runner, File fileToWrite) {
		Gson gson = getDefaultGson();
		FileOutputStream fos = null;
		try {
			StreamingOutput output = new StreamingOutput() {
				PrintStream ps = null;
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try {
						ps = new PrintStream(outputStream, true, "UTF-8");
						processPixelRunnerForTest(ps,gson,runner);
					} catch(Exception e) {
						e.printStackTrace();
					} finally {
						if(ps != null) {
							ps.close();
						}
					}
				}};
			fos = new FileOutputStream(fileToWrite);
			output.write(fos);
		} catch (Exception e) {
			LOGGER.error("Failed to write object to stream");
		} finally {
			try {
				fos.flush();
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return fileToWrite;
	}

	private static void processPixelRunner(PrintStream ps, Gson gson, PixelRunner runner) {
		// get the values we need from the runner
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<String> pixelStrings = runner.getPixelExpressions();
		List<Boolean> isMeta = runner.isMeta();

		// start of the map
		// and the insight id
		ps.print("{\"insightID\":\"" + in.getInsightId() + "\",");
		ps.flush();

		// now flush array of pixel returns
		ps.print("\"pixelReturn\":[");
		int size = pixelStrings.size();
		// this can be empty when we open an empty insight
		// from an insight
		if(size > 0) {
			// THIS IS BECAUSE WE APPEND THE JOB PIXEL
			// BUT FE DOESN'T RESPOND TO IT AND NEED TO REMOVE IT
			// HOWEVER, IF THE SIZE IS JUST 1, IT MEANS THAT THERE WAS
			// AN ERROR THAT OCCURED
			// but when we run a saved insight within a pixel
			// we do not want to shift the index
			int startIndex = 0;
			boolean firstIsJob = resultList.get(0).getOpType().contains(PixelOperationType.JOB_ID);
			if(firstIsJob) {
				startIndex = 1;
			}
			if(size == 1 && !firstIsJob) {
				startIndex = 0;
			}
			for (int i = startIndex; i < size; i++) {
				NounMetadata noun = resultList.get(i);
				String expression = pixelStrings.get(i);
				boolean meta = isMeta.get(i);
				processNounMetadata(in, ps, gson, noun, expression, meta);

				// add a comma for the next item in the list
				if( (i+1) != size) {
					ps.print(",");
					ps.flush();
				}
			}
			
			List<NounMetadata> delayedMessages = in.getDelayedMessages();
			for(int i = 0; i < delayedMessages.size(); i++) {
				ps.print(",");
				ps.flush();
				// we want to display these messages
				// so meta is always false
				processNounMetadata(in, ps, gson, delayedMessages.get(i), "\"delayed message\";", false);
			}
		}

		// now close of the array and the map
		ps.print("]}");
		ps.flush();
		
		// help java do garbage cleaning
		resultList.clear();
		pixelStrings.clear();
		resultList = null;
		pixelStrings = null;
		runner = null;
	}
	/** 
	 * Made for testing generation purposes, uncomment if you'd like to generate test output
	 */
	private static void processPixelRunnerForTest(PrintStream ps, Gson gson, PixelRunner runner) {
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		
		List<String> pixelStrings = runner.getPixelExpressions();
		List<Boolean> isMeta = runner.isMeta();

		int size = pixelStrings.size();
		
		if(size > 0) {
			int lastItem = size-1;
			NounMetadata noun = resultList.get(lastItem);
			String expression = pixelStrings.get(lastItem);
			boolean meta = isMeta.get(lastItem);
			processNounMetadata(in, ps, gson, noun, expression, meta);
		}

		resultList.clear();
		pixelStrings.clear();
		resultList = null;
		pixelStrings = null;
		runner = null;
	}
	/**
	 * Process the noun metadata for consumption on the FE
	 * @param noun
	 * @return
	 */
	private static void processNounMetadata(Insight in, PrintStream ps, Gson gson, NounMetadata noun, String expression, Boolean isMeta) {
		PixelDataType nounT = noun.getNounType();

		// returning a cached insight
		if(nounT == PixelDataType.CACHED_PIXEL_RUNNER) {
			List<Object> pixelReturn = (List<Object>) noun.getValue();
			for(int i = 0; i < pixelReturn.size(); i++) {
				if(i > 0) {
					ps.print(",");
				}
				ps.print(gson.toJson(pixelReturn.get(i)));
			}
			return;
		}
		
		ps.print("{");

		// add expression if there
		if(expression != null) {
			expression = expression.trim();
			while(expression.endsWith("; ;")) {
				expression = expression.substring(0, expression.length()-2);
			}
			ps.print("\"pixelExpression\":" + gson.toJson(expression) + ",");
		}
		// add is meta if there
		if(isMeta != null) {
			ps.print("\"isMeta\":" + isMeta + ",");
		}

		if(nounT == PixelDataType.FRAME) {
			// if we have a frame
			// return the table name of the frame
			// FE needs this to create proper QS
			// this has no meaning for graphs
			Map<String, String> frameData = new HashMap<String, String>();
			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
			frameData.put("type", FrameFactory.getFrameType(frame));
			String name = frame.getName();
			if(name != null) {
				frameData.put("name", name);
			}

			ps.print("\"output\":");
			ps.print(gson.toJson(frameData));
			ps.print(",\"operationType\":");
			List<PixelOperationType> opTypes = noun.getOpType();
			ps.print(gson.toJson(noun.getOpType()));

			// adding logic to auto send back headers if PixelOperationsType.FRAME_HEADERS_CHANGE is passed back
			if(opTypes.contains(PixelOperationType.FRAME_HEADERS_CHANGE)) {
				// since we may be running through a long recipe
				// need to make sure this exists
				if(frame.getMetaData().isOpen()) {
					noun.addAdditionalReturn(
							new NounMetadata(frame.getFrameHeadersObject(), 
									PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS));
				}
			}
			
		} else if(nounT == PixelDataType.CODE || nounT == PixelDataType.TASK_LIST) {
			// code is a tough one to process
			// since many operations could have been performed
			// we need to loop through a set of noun meta datas to output
			List<NounMetadata> codeOutputs = (List<NounMetadata>) noun.getValue();
			int numOutputs = codeOutputs.size();
			ps.print("\"output\":[");
			for(int i = 0; i < numOutputs; i++) {
				if(i > 0) {
					ps.print(",");
				}
				processNounMetadata(in, ps, gson, codeOutputs.get(i), null, null);
			}
			ps.print("]");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));

		} else if(nounT == PixelDataType.VECTOR) {
			List<Object> codeOutputs = (List<Object>) noun.getValue();
			int numOutputs = codeOutputs.size();
			ps.print("\"output\":[");
			for(int i = 0; i < numOutputs; i++) {
				if(i > 0) {
					ps.print(",");
				}
				if(codeOutputs.get(i) instanceof NounMetadata) {
					processNounMetadata(in, ps, gson, (NounMetadata) codeOutputs.get(i), null, null);
				} else {
					ps.print(gson.toJson(codeOutputs.get(i)));
				}
			}
			ps.print("]");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
		} else if(nounT == PixelDataType.TASK) {
			// if we have a task
			// we gotta iterate through it to return the data
			ITask task = (ITask) noun.getValue();
			ps.print("\"output\":{");
			ps.print("\"taskId\":\"" + task.getId() + "\"");
			ps.print("}");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
	
		} else if(nounT == PixelDataType.FORMATTED_DATA_SET) {
			Object value = noun.getValue();
			if(value instanceof ITask) {
				// if we have a task
				// we gotta iterate through it to return the data
				ITask task = (ITask) noun.getValue();
				int numCollect = task.getNumCollect();
				boolean collectAll = numCollect == -1;
				String formatType = task.getFormatter().getFormatType();

				if(task instanceof ConstantDataTask) {
					ps.print("\"output\":{");
					ps.print("\"data\":" + gson.toJson( ((ConstantDataTask) task).getOutputData()));
					ps.flush();
				} else if(formatType.equals("TABLE")) {
					// right now, only grid will work
					boolean first = true;
					boolean flushable = false;
					String[] headers = null;
					String[] rawHeaders = null;
					int count = 0;

					// try to see if extreme cache is enabled
					// this can come by the way of pragma as well
					String X_CACHE = "False";
					String qsPragma = task.getPragma("xCache");
					if(qsPragma != null) {// try to see if the query is telling you to - obviously the server setting overrides ?
						X_CACHE = qsPragma;
					}
					
					// we need to use a try catch
					// in case there is an issue
					// since we can get to this point 
					// without trying to execute or anything
					try {
						// recall, a task is also an iterator!
						// I do this  so I dont need to check everytime
						
						// need to see how to get to this for R eventually
						// all processes are done. we need to host it into RInterpreter
						
						// if no xcache
						// just flush out as normal
						boolean noCache = false;
						if(X_CACHE.equalsIgnoreCase("False")) {
							noCache = true;
						}
						if(!noCache && task instanceof BasicIteratorTask) {
							SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
							if(qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
								noCache = !FrameFactory.canCacheFrameQueries(qs.getFrame());
							} else {
								// you are not a frame
								// i cannot cache you
								noCache = true;
							}
						}
						
						if(noCache) {
							if(task instanceof BasicIteratorTask) {
								IRawSelectWrapper iterator = ((BasicIteratorTask) task).getIterator();
								if( (flushable = iterator.flushable()) ) {
									ps.print("\"output\":{");
									ps.print("\"data\":{" );
									ps.print("\"values\":");
									ps.print(iterator.flush());
									ps.flush();
									// logic around sending an empty data map in proper structure
									first = false;
									headers = iterator.getHeaders();
									rawHeaders = headers;
								}
							}
							
							while(!flushable && task.hasNext() && (collectAll || count < numCollect)) {
								IHeadersDataRow row = task.next();
								// need to set the headers
								if(headers == null) {
									headers = row.getHeaders();
									rawHeaders = row.getRawHeaders();
									ps.print("\"output\":{");
									ps.print("\"data\":{" );
									ps.print("\"values\":[");
									ps.flush();
								}
	
								if(!first) {
									ps.print(",");
								}
								ps.print(gson.toJson(row.getValues()));
								ps.flush();
	
								first = false;
								count++;
							}
						} else {
							RawCachedWrapper cw = task.createCache();
							CachedIterator cit = cw.getIterator();
							// caching for the first time
							if(cw.first()) {
								// some more processing can be saved by not having set query every time and doing it up front
								while(task.hasNext() && (collectAll || count < numCollect)) {
									IHeadersDataRow row = task.next();
									// add it to cit
									cit.addNext(row);
									Object[] dataValues = row.getValues();

									// need to set the headers
									if(headers == null) {
										headers = row.getHeaders();
										rawHeaders = row.getRawHeaders();
										ps.print("\"output\":{");
										ps.print("\"data\":{" );
										ps.print("\"values\":[");
										ps.flush();
									}

									if(!first) {
										ps.print(",");
									}
									String output = gson.toJson(dataValues);
									ps.print(output);
									cit.addJson(output);
									ps.flush();

									first = false;
									count++;		
								}
							} else {
								if(task.hasNext() && (collectAll || count < numCollect)) {
									IHeadersDataRow row = task.next();
									// need to set the headers
									headers = row.getHeaders();
									rawHeaders = row.getRawHeaders();
									ps.print("\"output\":{");
									ps.print("\"data\":{" );
									ps.print("\"values\":[");
									ps.print(cit.getAllJson());
									ps.flush();
									first = false;
								}
							}
							// persist it into the cache
							cit.processCache();
						}
					} catch(Exception e) {
						// on no, this is not good
						e.printStackTrace();

						// let us send back an error
						ps.print("\"output\":");
						ps.print(gson.toJson(e.getMessage()));
						ps.print(",\"operationType\":");
						ps.print(gson.toJson(new PixelOperationType[]{PixelOperationType.ERROR}));
						// close the map
						ps.print("}");
						ps.flush();
						return;
					}

					// this happens if there is no data to return
					if(first == true) {
						ps.print("\"output\":{");
						ps.print("\"data\":{" );
						ps.print("\"values\":[");
						// try to at least provide the headers
						List<Map<String, Object>> headerInfo = task.getHeaderInfo();
						if(headerInfo != null) {
							headers = new String[headerInfo.size()];
							rawHeaders = new String[headerInfo.size()];
							for(int i = 0; i < headers.length; i++) {
								headers[i] = headerInfo.get(i).get("alias") + "";
								rawHeaders[i] = headerInfo.get(i).get("header") + "";
							}
						}
					}
					// close the data values
					if(!flushable) {
						ps.print("]");
					}
					// end the values and add the headers
					ps.print(",\"headers\":" + gson.toJson(headers));
					ps.print(",\"rawHeaders\":" + gson.toJson(rawHeaders));
					ps.print("}" );
					ps.flush();

				} else if(formatType.equals("GRAPH")){
//					// format type is probably graph
//					ps.print("\"output\":{");
//					ps.print("\"data\":" );
//					// this is a map return
//					ps.print(gson.toJson( ((AbstractTask) task).getData()));
					
					// format type is graph
					ps.print("\"output\":{");
					printMapData(ps, (Map<String, Object>) ((AbstractTask) task).getData(), gson);
				
				} else {
					// just let the formatter handle the output of this data
					ps.print("\"output\":{");
					ps.print("\"data\":" );
					ps.print(gson.toJson( ((AbstractTask) task).getData()));
					ps.flush();
				}
				
				// grab the meta and output as well
				Map<String, Object> taskMeta = task.getMetaMap();
				for(String taskMetaKey : taskMeta.keySet()) {
					ps.print(",\"" + taskMetaKey + "\":" + gson.toJson(taskMeta.get(taskMetaKey)));
					ps.flush();
				}
				
				ps.print(",\"taskId\":\"" + task.getId() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
				ps.flush();
			}
			// if we do not have a task
			// we just have data to send
			else {
				// sometimes there is just data to send
				// dont need to do anything special
				ps.print("\"output\":");
				Object obj = noun.getValue();
				if(obj instanceof Map && ((Map) obj).containsKey("type") && ((Map) obj).get("type").equals("GRAPH")) {
					ps.print("{");
					Map mapObj = (Map) obj;
					Map<String, Object> retData = (Map<String, Object>) mapObj.remove("data");
					printMapData(ps, retData, gson);
					// print the rest of the stuff
					for(Object key : mapObj.keySet()) {
						ps.println(",\"" + key + ":" + gson.toJson(mapObj.get(key)));
					}
					ps.print("}");
				} else {
					LOGGER.info("Starting time to convert data to json");
					long start = System.currentTimeMillis();
					ps.print(gson.toJson(noun.getValue()));
					ps.flush();
					long end = System.currentTimeMillis();
					LOGGER.info("Total time to convert to json = " + (end-start) + "ms");
				}
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
			}
		}

		// running a saved insight
		else if(nounT == PixelDataType.PIXEL_RUNNER) {
			Map<String, Object> runnerWraper = (Map<String, Object>) noun.getValue();
			PixelRunner runner = (PixelRunner) runnerWraper.get("runner");
			Object params = runnerWraper.get("params");
			List<String> additionalPixels = (List<String>) runnerWraper.get("additionalPixels");

			Insight innerInsight = runner.getInsight();
			ps.print("\"output\":{");
			ps.print("\"name\":" + gson.toJson(innerInsight.getInsightName()));
			ps.print(",\"core_engine\":" + gson.toJson(innerInsight.getEngineId()));
			ps.print(",\"core_engine_id\":" + gson.toJson(innerInsight.getRdbmsId()));
			ps.print(",\"recipe\":" + gson.toJson(innerInsight.getPixelRecipe()));
			ps.print(",\"params\":" + gson.toJson(params));
			ps.print(",\"additionalPixels\":" + gson.toJson(additionalPixels));
			ps.flush();
			ps.print(",\"insightData\":");
			// process the inner recipe
			processPixelRunner(ps, gson, runner);
			ps.print("}");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
			ps.flush();
		}
		
		// remove variable
		else if(nounT == PixelDataType.REMOVE_VARIABLE) {
			// we only remove variables at the end
			// because the user may want to get the task and then
			// remove the frame right after
			// so we need to remove only at the end
			NounMetadata newNoun = null;
			if(noun.getOpType().contains(PixelOperationType.REMOVE_FRAME)) {
				// if it is specifically the remove frame reactor 
				// we will only remove the variable if it is pointing to a frame
				newNoun = InsightUtility.removeFrameVaraible(in.getVarStore(), noun.getValue().toString());
			} else {
				newNoun = InsightUtility.removeVaraible(in.getVarStore(), noun.getValue().toString());
			}
			ps.print("\"output\":");
			ps.print(gson.toJson(newNoun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(newNoun.getOpType()));
			ps.flush();
		}
		
		// remove insight 
		else if(nounT == PixelDataType.DROP_INSIGHT) {
			NounMetadata newNoun = InsightUtility.dropInsight(in);
			ps.print("\"output\":");
			ps.print(gson.toJson(newNoun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(newNoun.getOpType()));
			ps.flush();
		}
		
		else if(nounT == PixelDataType.REMOVE_TASK) {
			// we only remove variables at the end
			// because the user may want to get the task and then
			// remove the frame right after
			// so we need to remove only at the end
			ITask task = InsightUtility.removeTask(in, noun.getValue().toString());
			ps.print("\"output\":{");
			if(task == null) {
				ps.print("\"taskId\":\"Could not find task id = " + noun.getValue().toString() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(new PixelOperationType[]{PixelOperationType.ERROR}));
				ps.flush();
			} else {
				ps.print("\"taskId\":\"" + task.getId() + "\"");
				ps.print("}");
				ps.print(",\"operationType\":");
				ps.print(gson.toJson(noun.getOpType()));
				ps.flush();
			}
		}

		// panel information
		else if(nounT == PixelDataType.PANEL) {
			Gson panelGson = getPanelGson();
			ps.print("\"output\":");
			ps.print(panelGson.toJson(noun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(panelGson.toJson(noun.getOpType()));
			ps.flush();
		}
		
		// everything else is simple
		else {
			ps.print("\"output\":");
			ps.print(gson.toJson(noun.getValue()));
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));
			ps.flush();
		}
		
		// add additional outputs
		List<NounMetadata> addReturns = noun.getAdditionalReturn();
		int numOutputs = addReturns.size();
		if(numOutputs > 0) {
			ps.print(",\"additionalOutput\":[");
			for(int i = 0; i < numOutputs; i++) {
				if(i > 0) {
					ps.print(",");
				}
				processNounMetadata(in, ps, gson, addReturns.get(i), null, null);
			}
			ps.print("]");
		}

		// close the map
		ps.print("}");
		ps.flush();
	}
	
	/**
	 * Logic to more efficiently print out the map formatted data 
	 * @param ps
	 * @param retData
	 * @param gson
	 */
	private static void printMapData(PrintStream ps, Map<String, Object> retData, Gson gson) {
		ps.print("\"data\":" );
		// this is a map return
		ps.print("{\"" + GraphFormatter.GRAPH_META + "\":" + gson.toJson(retData.get(GraphFormatter.GRAPH_META)));
		ps.print(", \"" + GraphFormatter.NODES + "\":[");
		ps.flush();

		List<Object> nodeList = (List<Object>) retData.get(GraphFormatter.NODES);
		// print first node
		if(!nodeList.isEmpty()) {
			ps.print(gson.toJson(nodeList.remove(0)));
			ps.flush();
		}
		// print rest of nodes
		nodeList.stream().forEach(node -> ps.print("," + gson.toJson(node)));
		ps.print("], \"" + GraphFormatter.EDGES + "\":[");
		List<Object> edgeList = (List<Object>) retData.get(GraphFormatter.EDGES);
		// print first node
		if(!edgeList.isEmpty()) {
			ps.print(gson.toJson(edgeList.remove(0)));
			ps.flush();
		}
		// print rest of nodes
		edgeList.stream().forEach(edge -> ps.print("," + gson.toJson(edge)));
		ps.print("]}");
		ps.flush();
	}
	
}
