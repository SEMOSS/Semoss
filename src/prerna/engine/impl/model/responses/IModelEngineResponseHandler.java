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
package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;

/**
 * The {@code IModelEngineResponseHandler} interface should be implemented for any {@code
 * IModelEngine} that makes inference calls using REST directly. It defines how responses from a
 * REST call should be built or consolidated so that a standard model engine response can be
 * created.
 *
 * <p>This interface defines the structure for handling responses from a model engine, which
 * potentially involves streaming data.
 */
public interface IModelEngineResponseHandler {

  /**
   * This method is intended to append or add a stream (or partial response) to the handler.
   *
   * <p>It takes an instance of IModelEngineResponseStreamHandler as its parameter, which represents
   * a part of the overall response from the model engine.
   *
   * @param partial
   */
  void appendStream(IModelEngineResponseStreamHandler partial);

  /**
   * This method is used to created the full response after it has been assimilated from partial
   * responses
   */
  void setResponse(Object response);

  /**
   * This method returns a list of partial responses. Each item in the list is an instance of {@code
   * IModelEngineResponseStreamHandler}, representing a segment or part of the full response from
   * the model engine.
   */
  List<IModelEngineResponseStreamHandler> getPartialResponses();

  /**
   * This method returns the Class type of the stream handler. It ensures that the returned class is
   * a subclass of IModelEngineResponseStreamHandler.
   *
   * <p>This is used to instantiate a {@code IModelEngineResponseStreamHandler} when processing the
   * request response.
   */
  Class<? extends IModelEngineResponseStreamHandler> getStreamHandlerClass();

  /**
   * This method returns a map which should contain the following keys:
   * <li>response
   * <li>numberOfTokensInPrompt
   * <li>numberOfTokensInResponse
   *
   *     <p>Ideally the values for these keys come from {@code getResponse}, {@code
   *     getPromptTokens}, and {@code getResponseTokens} respectively.
   */
  Map<String, Object> getModelEngineResponse();

  /**
   * This method retrieves the complete response from the model engine. The return type is generic
   * (Object), since the response could be anything.
   */
  Object getResponse();

  /**
   * This method returns the number of tokens in the prompt that was sent to the {@code
   * IModelEngine} class. Tokens are typically units of text, like words or sentences, depending on
   * the model's tokenization scheme.
   */
  Integer getPromptTokens();

  /**
   * This method returns the number of tokens in the text generation response from the {@code
   * IModelEngine} class. Tokens are typically units of text, like words or sentences, depending on
   * the model's tokenization scheme.
   */
  Integer getResponseTokens();
}
