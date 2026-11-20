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
package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.PixelPlanner;
import prerna.sablecc2.lexer.IPushbackReader;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.util.Constants;
import prerna.util.SandboxedJavaExecution;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class PixelRunner {

	private static final Logger classLogger = LogManager.getLogger(PixelRunner.class);

	private static final String CANCELLED_MESSAGE = "The request was cancelled by the user";
	private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

	private static List<PixelOperationType> errorOpTypes = new ArrayList<>();
	static {
		errorOpTypes.add(PixelOperationType.ERROR);
		errorOpTypes.add(PixelOperationType.UNEXECUTED_PIXELS);
		errorOpTypes.add(PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED);
		errorOpTypes.add(PixelOperationType.USER_INPUT_REQUIRED);
		errorOpTypes.add(PixelOperationType.LOGGIN_REQUIRED_ERROR);
		errorOpTypes.add(PixelOperationType.ANONYMOUS_USER_ERROR);
		errorOpTypes.add(PixelOperationType.INVALID_SYNTAX);
	}

	/**
	 * Runs a given pixel expression (can be multiple if semicolon delimited) on a
	 * provided data maker
	 * 
	 * @param expression The sequence of semicolon delimited pixel expressions. If
	 *                   just one expression, still must end with a semicolon
	 * @param frame      The data maker to run the pixel expression on
	 */

	protected transient GreedyTranslation translation = null;
	protected Insight insight = null;
	protected boolean maintainErrors = false;

	protected List<NounMetadata> results = new ArrayList<>();
	protected List<Pixel> returnPixelList = new ArrayList<>();

	protected List<String> encodingList = new ArrayList<>();
	protected Map<String, String> encodedTextToOriginal = new HashMap<>();

	/**
	 * Wrapper class to interrupt pushback reader
	 */
	private static final class InterruptiblePushbackReader implements IPushbackReader {
		private final PushbackReader pushbackReader;
		private final PixelRunner runner;

		private InterruptiblePushbackReader(PushbackReader pushbackReader, PixelRunner runner) {
			this.pushbackReader = pushbackReader;
			this.runner = runner;
		}

		@Override
		public int read() throws IOException {
			if (runner.isCancelRequested()) {
				throw new InterruptedIOException(CANCELLED_MESSAGE);
			}
			return this.pushbackReader.read();
		}

		@Override
		public void unread(int c) throws IOException {
			if (runner.isCancelRequested()) {
				throw new InterruptedIOException(CANCELLED_MESSAGE);
			}
			this.pushbackReader.unread(c);
		}
	}

	/**
	 * This is the main method for parsing and executing a pixel expression
	 * 
	 * @param expression
	 * @param insight
	 */
	public void runPixel(String expression, Insight insight) {
		this.insight = insight;
		throwIfCancelRequested();
		try {
			expression = PixelPreProcessor.preProcessPixel(expression.trim(), this.encodingList,
					this.encodedTextToOriginal);
		} catch (SemossPixelException e) {
			// treat this as a META so that FE doesn't record it
			addInvalidSyntaxResult(expression, new NounMetadata(e.getMessage(), PixelDataType.INVALID_SYNTAX,
					PixelOperationType.ERROR, PixelOperationType.INVALID_SYNTAX), false);
			throw e;
		}
		throwIfCancelRequested();

		final boolean USER_CHROOT = insight.getUser() != null
				&& Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE));
		try {
			if (USER_CHROOT) {
				SandboxedJavaExecution
						.setSandboxRootForCurrentThread(insight.getUser().getUserSymlinkHelper().getUserChrootFolder());
			} else {
				SandboxedJavaExecution.setSandboxRootForCurrentThread(null);
			}
			Parser p = new Parser(new Lexer(new InterruptiblePushbackReader(new PushbackReader(
					new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
							StandardCharsets.UTF_8),
					expression.length()), this)));
			translation = new GreedyTranslation(this, insight);

			throwIfCancelRequested();
			// parsing the pixel - this process also determines if expression is
			// syntactically correct
			Start tree = p.parse();
			throwIfCancelRequested();
			// apply the translation.
			tree.apply(translation);
		} catch (SemossPixelException e) {
			if (isCancellationException(e)) {
				classLogger.info("Pixel execution canceled while running expression");
			} else {
				classLogger.error("Error occurred running pixel: {} with detailed error: {}", expression,
						e.getMessage(), e);
			}
			if (!e.isContinueThreadOfExecution()) {
				throw e;
			}
		} catch (InterruptedIOException e) {
			throwCancellationException();
		} catch (ParserException | LexerException | IOException e) {
			// we only need to catch invalid syntax here
			// other exceptions are caught in lazy translation
			trackInvalidSyntaxError(expression, e);
			classLogger.error("Invalid pixel command: {}", expression, e);
			String eMessage = e.getMessage();
			if (eMessage.startsWith("[")) {
				Pattern pattern = Pattern.compile("\\[\\d+,\\d+\\]");
				Matcher matcher = pattern.matcher(eMessage);
				if (matcher.find()) {
					String location = matcher.group(0);
					location = location.substring(1, location.length() - 1);
					int findIndex = Integer.parseInt(location.split(",")[1]);
					eMessage += ". Error in syntax around " + expression
							.substring(Math.max(findIndex - 10, 0), Math.min(findIndex + 10, expression.length()))
							.trim();
				}
			}
			// treat this as a META so that FE doesn't record it
			addInvalidSyntaxResult(expression, new NounMetadata(eMessage, PixelDataType.INVALID_SYNTAX,
					PixelOperationType.ERROR, PixelOperationType.INVALID_SYNTAX), false);
		} finally {
			// help clean up
			if (translation != null) {
				PixelPlanner planner = translation.getPlanner();
				planner.dropGraph();
				planner.getVarStore().remove("$RESULT");
			}
			this.encodingList.clear();
			this.encodedTextToOriginal.clear();

			// always clear the sandbox
			SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		}
	}

	/**
	 * Track the error
	 * 
	 * @param pixel
	 * @param ex
	 */
	private void trackInvalidSyntaxError(String pixel, Exception ex) {
	}

	/**
	 * Store the terminal output of the pixel statement
	 * 
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	public void addResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList,
				this.encodedTextToOriginal);
		this.results.add(result);

		// we will start to add to the insight recipe
		// when we have an expression that is returned
		// create the pixel via the correct id
		// or if meta - assign random one
		Pixel pixel = null;
		if (!isMeta) {
			PixelList pixelList = this.insight.getPixelList();
			pixel = pixelList.addPixel(origExpression);
			// add if there is an error or warning
			determineErrorOrWarning(pixel, result);
			pixel.setEndingFrameHeaders(InsightUtility.getAllFrameHeaders(this.insight.getVarStore()));
			if (this.translation != null) {
				Pixel.translationMerge(pixel, this.translation.getPixelObj());
			}
			if (pixel.isReturnedError() && !this.maintainErrors) {
				// we actually need to remove this from the pixel list
				// there is also no sync required
				List<String> removeId = new ArrayList<String>();
				removeId.add(pixel.getId());
				pixelList.removeIds(removeId, false);
			} else {
				pixelList.syncLastPixel();
			}
			// store this pixel
			// in the return pixel list
			this.returnPixelList.add(pixel);
		} else {
			// store this pixel
			// in the return pixel list
			pixel = new Pixel("meta_unstored", origExpression);
			// make sure the pixel is set to meta
			pixel.setMeta(true);
			// also set the time to run
			if (this.translation != null && this.translation.getPixelObj() != null) {
				pixel.setTimeToRun(this.translation.getPixelObj().getTimeToRun());
			}
			this.returnPixelList.add(pixel);
			// add if there is an error or warning
			determineErrorOrWarning(pixel, result);
		}
	}

	/**
	 * Same as addResult but since this is an error we do not want to store it in
	 * the pixel recipe
	 * 
	 * @param pixelExpression
	 * @param result
	 * @param isMeta
	 */
	private void addInvalidSyntaxResult(String pixelExpression, NounMetadata result, boolean isMeta) {
		String origExpression = PixelUtility.recreateOriginalPixelExpression(pixelExpression, this.encodingList,
				this.encodedTextToOriginal);
		Pixel pixel = new Pixel("meta_unstored", origExpression);
		pixel.setReturnedError(true);
		pixel.setMeta(true);
		// also set the time to run
		if (this.translation != null && this.translation.getPixelObj() != null) {
			pixel.setTimeToRun(this.translation.getPixelObj().getTimeToRun());
		}
		this.returnPixelList.add(pixel);
		this.results.add(result);
	}

	private void determineErrorOrWarning(Pixel pixel, NounMetadata result) {
		// if the result is a direct error
		if (!Collections.disjoint(errorOpTypes, result.getOpType())) {
			pixel.setReturnedError(true);
			pixel.addErrorMessage(result.getValue() + "");
			return;
		}

		// if the result is a direct warning
		if (result.getOpType().contains(PixelOperationType.WARNING)) {
			pixel.setReturnedWarning(true);
			pixel.addWarningMessage(result.getValue() + "");
			return;
		}

		// if the result has an additional type
		if (result.getAdditionalReturn() != null) {
			for (NounMetadata addReturn : result.getAdditionalReturn()) {
				// check if add return is an error
				if (!Collections.disjoint(errorOpTypes, addReturn.getOpType())) {
					pixel.setReturnedError(true);
					pixel.addErrorMessage(result.getValue() + "");
					return;
				}

				// check if add return is a warning
				if (addReturn.getOpType().contains(PixelOperationType.WARNING)) {
					pixel.setReturnedWarning(true);
					pixel.addWarningMessage(result.getValue() + "");
					return;
				}
			}
		}
	}

	private void throwIfCancelRequested() {
		if (isCancelRequested()) {
			throwCancellationException();
		}
	}

	public void cancelRequest() {
		this.cancelRequested.set(true);
	}

	public boolean isCancelRequested() {
		return this.cancelRequested.get() || Thread.currentThread().isInterrupted();
	}

	private void throwCancellationException() {
		throw new SemossPixelException(CANCELLED_MESSAGE, false);
	}

	private boolean isCancellationException(SemossPixelException ex) {
		return ex != null && !ex.isContinueThreadOfExecution() && CANCELLED_MESSAGE.equals(ex.getMessage());
	}

	public List<NounMetadata> getResults() {
		return this.results;
	}

	public List<Pixel> getReturnPixelList() {
		return this.returnPixelList;
	}

	public Insight getInsight() {
		return this.insight;
	}

	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	public boolean isMaintainErrors() {
		return maintainErrors;
	}

	public void setMaintainErrors(boolean maintainErrors) {
		this.maintainErrors = maintainErrors;
	}

	public void clear() {
		this.results.clear();
		this.returnPixelList.clear();
		this.encodingList.clear();
		this.encodedTextToOriginal.clear();
	}

	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

	/**
	 * 
	 * @param expression
	 * @return
	 * 
	 *         Method to take a string and return the parsed value of the pixel
	 */
	public static List<String> parsePixel(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(
				new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
						StandardCharsets.UTF_8),
				expression.length())));
		Start tree = p.parse();

		ARoutineConfiguration configNode = (ARoutineConfiguration) tree.getPConfiguration();

		List<String> pixelList = new ArrayList<>();
		for (PRoutine script : configNode.getRoutine()) {
			pixelList.add(script.toString());
		}
		return pixelList;
	}

	/**
	 * 
	 * @param expression
	 * @return returns set of reactors that are not implemented throws exception if
	 *         pixel cannot be parsed
	 */
	public static Set<String> validatePixel(String expression) throws ParserException, LexerException, IOException {
		Parser p = new Parser(new Lexer(new PushbackReader(
				new InputStreamReader(new ByteArrayInputStream(expression.getBytes(StandardCharsets.UTF_8)),
						StandardCharsets.UTF_8),
				expression.length())));
		ValidatorTranslation translation = new ValidatorTranslation();
		// parsing the pixel - this process also determines if expression is
		// syntactically correct
		Start tree = p.parse();
		// apply the translation.
		tree.apply(translation);
		return translation.getUnimplementedReactors();
	}
}
