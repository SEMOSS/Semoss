package prerna.unit.engine.impl.model;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import prerna.engine.impl.model.LLMReactor;
import prerna.om.Insight;
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
		reactor.setInsight(insight);
        ns = mock(NounStore.class);
		grs = mock(GenRowStruct.class);

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
			assertEquals("Model " + engineId + " does not exist or user does not have access to this model", e.getMessage());
        }
    }

    @Test
    void executeNoContext () {
        String engineId = "engine";
        String command = "command";

        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
        keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);

        try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class); 
            MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
                seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
                utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
                
                GenRowStruct grs = new GenRowStruct();
                Map<Object, Object> map = new HashMap<>();
                map.put("test", "test");
                grs.addMap(map, PixelOperationType.OPERATION);
                when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
                
                IModelEngine modelEngine = mock(IModelEngine.class);
                utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
                
                NounMetadata nm = reactor.execute();
        }
    }

    @Test
    void executeWithContext () {
        String engineId = "engine";
        String command = "command";

        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
        keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
        keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), "context");

        try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class); 
            MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
                seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
                utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
                
                GenRowStruct grs = new GenRowStruct();
                Map<Object, Object> map = new HashMap<>();
                map.put("test", "test");
                grs.addMap(map, PixelOperationType.OPERATION);
                when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
                
                IModelEngine modelEngine = mock(IModelEngine.class);
                utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
                
                NounMetadata nm = reactor.execute();
        }
    }

    @Test
    void executeNoParamMap () {
        String engineId = "engine";
        String command = "command";

        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
        keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
        keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), "context");

        try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class); 
            MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
                seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
                utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
                
                GenRowStruct grs = new GenRowStruct();
                Map<Object, Object> map = new HashMap<>();
                map.put("test", "test");
                grs.addMap(map, PixelOperationType.OPERATION);
                when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
                
                IModelEngine modelEngine = mock(IModelEngine.class);
                utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
                
                NounMetadata nm = reactor.execute();
        }
    }

    @Test
    void executeWithParamMap () {
        String engineId = "engine";
        String command = "command";

        HashMap<Object, Object> paramVals = new HashMap<>();
        paramVals.put("test", "test");

        keyValues.put(ReactorKeysEnum.ENGINE.getKey(), engineId);
        keyValues.put(ReactorKeysEnum.COMMAND.getKey(), command);
        keyValues.put(ReactorKeysEnum.CONTEXT.getKey(), "context");
        keyValues.put(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), paramVals.toString());

        try (MockedStatic<SecurityEngineUtils> seu = Mockito.mockStatic(SecurityEngineUtils.class); 
            MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
                seu.when(() -> SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
                utility.when(() -> Utility.decodeURIComponent(command)).thenReturn(command);
                
                // GenRowStruct grs = new GenRowStruct();
                // Map<Object, Object> map = new HashMap<>();
                // map.put("test", "test");
                // grs.addMap(map, PixelOperationType.OPERATION);
                List<NounMetadata> list = new ArrayList<>();
                NounMetadata meta = new NounMetadata("test", PixelDataType.CONST_STRING);
                list.add(meta);

                grs = mock(GenRowStruct.class);
                when(ns.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())).thenReturn(grs);
                when(grs.getNounsOfType(PixelDataType.MAP)).thenReturn(list);
                
                IModelEngine modelEngine = mock(IModelEngine.class);
                utility.when(() -> Utility.getModel(engineId)).thenReturn(modelEngine);
                
                NounMetadata nm = reactor.execute();

                assertNotNull(nm);
        }
    }

    @Test
    void executeGetReactorDescription () {
        assertEquals("This method is used to run an LLM text-generation call", reactor.getReactorDescription());
    }
}
