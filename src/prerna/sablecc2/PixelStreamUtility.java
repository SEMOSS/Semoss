package prerna.sablecc2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.date.SemossDate;
import prerna.ds.shared.CachedIterator;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IStorageEngine;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.Pixel;
import prerna.om.ThreadStore;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.export.GraphFormatter;
import prerna.reactor.frame.FrameFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.AbstractTask;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;
import prerna.util.gson.InsightPanelAdapter;
import prerna.util.gson.InsightSheetAdapter;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;
import prerna.util.insight.InsightUtility;

public class PixelStreamUtility {

	private static final Logger classLogger = LogManager.getLogger(PixelStreamUtility.class);

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
	public static StreamingOutput collectPixelData(PixelRunner runner, Long sessionTimeRemaining) {
		// get the default gson object
		Gson gson = GsonUtility.getDefaultGson();

		// now process everything
		try {
			return new StreamingOutput() {
				PrintStream ps = null;
				@Override
				public void write(OutputStream outputStream) throws IOException, WebApplicationException {
					try {
						ps = new PrintStream(outputStream, true, "UTF-8");
						// we want to ignore the first index since it will be a job
						classLogger.debug("Starting to generate response");
						long start = System.currentTimeMillis();
						processPixelRunner(ps, gson, runner, sessionTimeRemaining);
						long end = System.currentTimeMillis();
						classLogger.debug("Time to generate json response = " + (end-start) + "ms");
					} catch(Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						ThreadStore.remove();
					}
				}};
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}
	
	/**
	 * Collect pixel data from the runner
	 * @param runner
	 * @return
	 */
	public static File writePixelData(PixelRunner runner, File fileToWrite, Cipher cipher) {
		// get the default gson object
		Gson gson = GsonUtility.getDefaultGson();

		// now process everything
		PrintStream ps = null;
		try {
			if(cipher != null) {
				ps = new PrintStream(new BufferedOutputStream(new CipherOutputStream(new FileOutputStream(fileToWrite), cipher)), false, "UTF-8");
			} else {
				ps = new PrintStream(new FileOutputStream(fileToWrite), false, "UTF-8");
			}
			processPixelRunner(ps, gson, runner, null);
		} catch (Exception e) {
			classLogger.error("Failed to write object to stream");
		} finally {
			if (ps != null) {
				ps.flush();
				ps.close();
			}
		}

		return fileToWrite;
	}
	
	/** 
	 * Made for testing generation purposes, uncomment if you'd like to generate test output
	 */
	public static File writePixelDataForTest(PixelRunner runner, File fileToWrite) {
		Gson gson = GsonUtility.getDefaultGson();
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
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
					}
				}};
			fos = new FileOutputStream(fileToWrite);
			output.write(fos);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if (fos != null) {
					fos.flush();
					fos.close();
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		return fileToWrite;
	}

	private static void processPixelRunner(PrintStream ps, Gson gson, PixelRunner runner, Long sessionTimeRemaining) {
		// get the values we need from the runner
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		// get the expression which created the return
		// this matches with the above by index
		List<Pixel> pixelList = runner.getReturnPixelList();

		// start of the map
		// and the insight id
		ps.print("{\"insightID\":\"" + in.getInsightId() + "\",");
		if(sessionTimeRemaining != null) {
			ps.print("\"sessionTimeRemaining\":\"" + sessionTimeRemaining + "\",");
		}
		ps.flush();

		// now flush array of pixel returns
		ps.print("\"pixelReturn\":[");
		int size = pixelList.size();
		// this can be empty when we open an empty insight
		// from an insight
		if(size > 0) {
			// THIS IS BECAUSE WE APPEND THE JOB PIXEL
			// BUT FE DOESN'T RESPOND TO IT AND NEED TO REMOVE IT
			// HOWEVER, IF THE SIZE IS JUST 1, IT MEANS THAT THERE WAS
			// AN ERROR THAT occurred
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
				Pixel pixelObj = pixelList.get(i);
				processNounMetadata(in, ps, gson, noun, pixelObj);

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
				Pixel pixelObj = new Pixel("thread_execution_" + Utility.getRandomString(6), "\"delayed message\";");
				pixelObj.setMeta(false);
				processNounMetadata(in, ps, gson, delayedMessages.get(i), pixelObj);
			}
		}

		// now close of the array and the map
		ps.print("]}");
		ps.flush();
		
		// help java do garbage cleaning
		resultList.clear();
		pixelList.clear();
		resultList = null;
		pixelList = null;
		runner = null;
	}
	
	/** 
	 * Made for testing generation purposes, uncomment if you'd like to generate test output
	 */
	private static void processPixelRunnerForTest(PrintStream ps, Gson gson, PixelRunner runner) {
		Insight in = runner.getInsight();
		List<NounMetadata> resultList = runner.getResults();
		List<Pixel> pixelList = runner.getReturnPixelList();

		int size = pixelList.size();
		
		if(size > 0) {
			int lastItem = size-1;
			NounMetadata noun = resultList.get(lastItem);
			Pixel pixelObj = pixelList.get(lastItem);
			processNounMetadata(in, ps, gson, noun, pixelObj);
		}

		resultList.clear();
		pixelList.clear();
		resultList = null;
		pixelList = null;
		runner = null;
	}

	/**
	 * Process the noun metadata for consumption on the FE
	 * @param in 			Insight 
	 * @param ps			PrintStream to write output json
	 * @param gson			Gson utility instance
	 * @param noun			Nounmetadata to process
	 * @param pixelObj		Pixel object for the step
	 */
	private static void processNounMetadata(Insight in, PrintStream ps, Gson gson, NounMetadata noun, Pixel pixelObj) {
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

		if(pixelObj != null) {
			ps.print("\"pixelId\":\"" + pixelObj.getId() + "\",");
			String expression = pixelObj.getPixelString();
			// add expression if there
			if(expression != null) {
				expression = expression.trim();
				while(expression.endsWith("; ;")) {
					expression = expression.substring(0, expression.length()-2);
				}
				ps.print("\"pixelExpression\":" + gson.toJson(expression) + ",");
			}
			// add is meta 
			ps.print("\"isMeta\":" + pixelObj.isMeta() + ",");
			ps.print("\"timeToRun\":" + pixelObj.getTimeToRun() + ",");
		}

		if(nounT == PixelDataType.FRAME) {
			// if we have a frame
			// return the table name of the frame
			// FE needs this to create proper QS
			// this has no meaning for graphs
			Map<String, String> frameData = new HashMap<>();
			ITableDataFrame frame = (ITableDataFrame) noun.getValue();
			frameData.put("type", frame.getFrameType().getTypeAsString());
			String name = frame.getOriginalName();
			if(name != null) {
				frameData.put("name", name);
				if(!name.equals(frame.getName())) {
					frameData.put("queryName", frame.getName());
				}
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
			
		} else if(nounT == PixelDataType.STORAGE) {
			// if we have a frame
			// return the table name of the frame
			// FE needs this to create proper QS
			// this has no meaning for graphs
			Map<String, String> storageData = new HashMap<>();
			IStorageEngine storage = (IStorageEngine) noun.getValue();
			storageData.put("storageId", storage.getEngineId());
			storageData.put("storageName", storage.getEngineName());

			ps.print("\"output\":");
			ps.print(gson.toJson(storageData));
			ps.print(",\"operationType\":");
			List<PixelOperationType> opTypes = noun.getOpType();
			ps.print(gson.toJson(noun.getOpType()));
			
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
				processNounMetadata(in, ps, gson, codeOutputs.get(i), null);
			}
			ps.print("]");
			ps.print(",\"operationType\":");
			ps.print(gson.toJson(noun.getOpType()));

		} else if(nounT == PixelDataType.VECTOR) {
			ps.print("\"output\":[");
			Object multiValue = noun.getValue();
			if(multiValue instanceof List) {
				List<Object> listOutputs = (List<Object>) multiValue;
				int numOutputs = listOutputs.size();
				for(int i = 0; i < numOutputs; i++) {
					if(i > 0) {
						ps.print(",");
					}
					if(listOutputs.get(i) instanceof NounMetadata) {
						processNounMetadata(in, ps, gson, (NounMetadata) listOutputs.get(i), null);
					} else {
						ps.print(gson.toJson(listOutputs.get(i)));
					}
				}
			} else if(multiValue instanceof Object[]) {
				Object[] listOutputs = (Object[]) multiValue;
				int numOutputs = listOutputs.length;
				for(int i = 0; i < numOutputs; i++) {
					if(i > 0) {
						ps.print(",");
					}
					if(listOutputs[i] instanceof NounMetadata) {
						processNounMetadata(in, ps, gson, (NounMetadata) listOutputs[i], null);
					} else {
						ps.print(gson.toJson(listOutputs[i]));
					}
				}
			} else {
				ps.print(gson.toJson(multiValue));
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
				classLogger.debug("Start flushing task = " + task.getId());
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
					String xCache = "False";
					String qsPragma = task.getPragma("xCache");
					if(qsPragma != null) {
						// try to see if the query is telling you to - obviously the server setting overrides ?
						xCache = qsPragma;
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
						if(xCache.equalsIgnoreCase("False")) {
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
									String flushedOutput = iterator.flush();
									ps.print("\"output\":{");
									ps.print("\"data\":{" );
									ps.print("\"values\":");
									ps.print(flushedOutput);
									ps.flush();
									// logic around sending an empty data map in proper structure
									first = false;
									headers = iterator.getHeaders();
									rawHeaders = headers;
									// update the internal offset
									long curOffset = ((BasicIteratorTask) task).getInternalOffset();
									((BasicIteratorTask) task).setInternalOffset(curOffset + task.getNumCollect());
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
						classLogger.error(Constants.STACKTRACE, e);
						// let us send back an error
						ps.print("\"output\":");
						ps.print(gson.toJson(e.getMessage()));
						ps.print(",\"operationType\":");
						ps.print(gson.toJson(new PixelOperationType[]{PixelOperationType.ERROR}));
						// close the map
						ps.print("}");
						ps.flush();
						
						// set this as an error in the pixel object
						pixelObj.setReturnedError(true);
						
						// close resources before returning
						try {
							task.close();
						} catch(IOException e2) {
							classLogger.error(Constants.STACKTRACE, e2);
						}
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

					classLogger.debug("Done flushing sending task = " + task.getId());
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
				
				// done with the task data
				if(!task.hasNext()) {
					try {
						task.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
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
					classLogger.info("Starting time to convert data to json");
					long start = System.currentTimeMillis();
					ps.print(gson.toJson(noun.getValue()));
					ps.flush();
					long end = System.currentTimeMillis();
					classLogger.info("Total time to convert to json = " + (end-start) + "ms");
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
			ps.print(",\"core_engine\":" + gson.toJson(innerInsight.getProjectId()));
			ps.print(",\"core_engine_id\":" + gson.toJson(innerInsight.getRdbmsId()));
			ps.print(",\"recipe\":" + gson.toJson(innerInsight.getPixelList().getPixelRecipe()));
			ps.print(",\"params\":" + gson.toJson(params));
			ps.print(",\"additionalPixels\":" + gson.toJson(additionalPixels));
			ps.flush();
			ps.print(",\"insightData\":");
			// process the inner recipe
			processPixelRunner(ps, gson, runner, null);
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
				processNounMetadata(in, ps, gson, addReturns.get(i), null);
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
