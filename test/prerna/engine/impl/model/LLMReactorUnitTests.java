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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.reactor.model.LLMReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class LLMReactorUnitTests {
  private User user;
  private NounStore ns;
  private Insight insight;
  private GenRowStruct grs;

  private LLMReactor reactor;
  private Map<String, String> keyValues;

  @BeforeEach
  void setUp() {
    reactor = new LLMReactor();
    keyValues = reactor.keyValue;
    insight = mock(Insight.class);
    user = mock(User.class);
    ns = mock(NounStore.class);
    grs = mock(GenRowStruct.class);

    reactor.setInsight(insight);
    reactor.setNounStore(ns);

    when(insight.getUser()).thenReturn(user);
  }

  @Test
  void engineNoExist() {
    String engineId = "engine";
    String command = "command";

    keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine");
    keyValues.put(ReactorKeysEnum.COMMAND.getKey(), "command");

    try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class)) {
      seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(false);

      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
      assertEquals(
          "Model " + engineId + " does not exist or user does not have access to this model",
          e.getMessage());
    }
  }

  @Test
  void executeRequired() {
    String engineId = "engine";
    String command = "command";

    keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
    keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);

    try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
        MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
      seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
      utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);

      List<NounMetadata> list = new ArrayList<>();
      NounMetadata meta = new NounMetadata(new HashMap<>(), PixelDataType.MAP);
      list.add(meta);

      when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
      when(grs.getNounsOfType(PixelDataType.MAP)).thenReturn(list);

      IModelEngine modelEngine = mock(IModelEngine.class);
      AskModelEngineResponse ask = mock(AskModelEngineResponse.class);
      utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
      when(modelEngine.ask(eq(command), isNull(), eq(insight), anyMap())).thenReturn(ask);

      Map<String, Object> output = new HashMap<>();
      when(ask.toMap()).thenReturn(output);

      NounMetadata nm = reactor.execute();

      assertNotNull(nm);
      assertEquals(PixelDataType.MAP, nm.getNounType());
      assertEquals(PixelOperationType.OPERATION, nm.getOpType().get(0));
      assertEquals(output, nm.getValue());
    }
  }

  @Test
  void executeWithContext() {
    String engineId = "engine";
    String command = "command";
    String context = "context";

    keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
    keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
    keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), context);

    try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
        MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {

      seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
      utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
      utility.when(() -> Utility.decodeURIComponent(context)).thenReturn(context);

      List<NounMetadata> list = new ArrayList<>();
      NounMetadata meta = new NounMetadata(new HashMap<>(), PixelDataType.MAP);
      list.add(meta);

      when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
      when(grs.getNounsOfType(PixelDataType.MAP)).thenReturn(list);

      IModelEngine modelEngine = mock(IModelEngine.class);
      AskModelEngineResponse ask = mock(AskModelEngineResponse.class);
      utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
      when(modelEngine.ask(eq(command), eq(context), eq(insight), anyMap())).thenReturn(ask);

      Map<String, Object> output = new HashMap<>();
      when(ask.toMap()).thenReturn(output);

      NounMetadata nm = reactor.execute();

      assertNotNull(nm);
      assertEquals(PixelDataType.MAP, nm.getNounType());
      assertEquals(PixelOperationType.OPERATION, nm.getOpType().get(0));
      assertEquals(output, nm.getValue());
    }
  }

  @Test
  void executeNullParamMap() {
    String engineId = "engine";
    String command = "command";
    String context = "context";

    keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
    keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
    keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), context);

    try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
        MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
      seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
      utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
      utility.when(() -> Utility.decodeURIComponent(context)).thenReturn(context);

      String noun = "[]";
      when(ns.makeNoun(noun)).thenReturn(grs);
      when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(null);
      when(grs.getNounsOfType(PixelDataType.MAP)).thenReturn(null);

      reactor.curNoun(new GenRowStruct().toString());

      IModelEngine modelEngine = mock(IModelEngine.class);
      AskModelEngineResponse ask = mock(AskModelEngineResponse.class);
      utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
      when(modelEngine.ask(eq(command), eq(context), eq(insight), anyMap())).thenReturn(ask);

      Map<String, Object> output = new HashMap<>();
      when(ask.toMap()).thenReturn(output);

      NounMetadata nm = reactor.execute();

      assertNotNull(nm);
      assertEquals(PixelDataType.MAP, nm.getNounType());
      assertEquals(PixelOperationType.OPERATION, nm.getOpType().get(0));
      assertEquals(output, nm.getValue());
    }
  }

  @Test
  void executeMapInputs() {
    String engineId = "engine";
    String command = "command";
    String context = "context";

    HashMap<Object, Object> paramVals = new HashMap<>();
    paramVals.put("test", "test");

    keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
    keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
    keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), context);
    keyValues.put(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), paramVals.toString());

    try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class);
        MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
      seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
      utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
      utility.when(() -> Utility.decodeURIComponent(context)).thenReturn(context);

      List<NounMetadata> list = new ArrayList<>();
      NounMetadata meta = new NounMetadata(new HashMap<>(), PixelDataType.MAP);
      list.add(meta);

      String noun = "[]";
      when(ns.makeNoun(noun)).thenReturn(grs);
      when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(null);
      when(grs.getNounsOfType(PixelDataType.MAP)).thenReturn(list);

      reactor.curNoun(new GenRowStruct().toString());

      IModelEngine modelEngine = mock(IModelEngine.class);
      AskModelEngineResponse ask = mock(AskModelEngineResponse.class);
      utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);

      when(modelEngine.ask(eq(command), eq(context), eq(insight), anyMap())).thenReturn(ask);

      Map<String, Object> output = new HashMap<>();
      when(ask.toMap()).thenReturn(output);

      NounMetadata nm = reactor.execute();

      assertNotNull(nm);
      assertEquals(PixelDataType.MAP, nm.getNounType());
      assertEquals(PixelOperationType.OPERATION, nm.getOpType().get(0));
      assertEquals(output, nm.getValue());
    }
  }

  @Test
  void executeGetReactorDescription() {
    assertEquals(
        "This method is used to run an LLM text-generation call", reactor.getReactorDescription());
  }
}
