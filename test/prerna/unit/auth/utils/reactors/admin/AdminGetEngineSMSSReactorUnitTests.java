package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminGetEngineSMSSReactor;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetEngineSMSSReactorUnitTests {

	private FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
	
	private AdminGetEngineSMSSReactor reactor;
	private Insight insight;
	private User user;
	private SecurityAdminUtils securityAdminUtils;
	private IEngine engine;
	private NounStore ns;
	private GenRowStruct engineGrs;
	
	
	@BeforeEach
	void setup() {
		reactor = new AdminGetEngineSMSSReactor();
		reactor.setFileSystem(fs);
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
		
		ns = mock(NounStore.class);

		engineGrs = mock(GenRowStruct.class);
		reactor.setNounStore(ns);
	}
	
	@Test
	void test_AdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}
	
	@Test
	void test_EngineIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Need to define the engine", e.getMessage());
		}
	}
	
	@Test
	void test_SmssFileNotExist() {
		
		when(ns.size()).thenReturn(2);
		when(ns.getNoun(ReactorKeysEnum.ENGINE.getKey())).thenReturn(engineGrs);
		
		when(engineGrs.isEmpty()).thenReturn(false);
		when(engineGrs.get(0)).thenReturn("id");
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class); 	
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
	
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			IEngine engine = mock(IEngine.class);
			utility.when(() -> Utility.getEngine("id")).thenReturn(engine);
			
			when(engine.getSmssFilePath()).thenReturn("Semoss.txt");
			

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find smss file for engine id. Please reach out to an administrator for assistance", e.getMessage());
		}
	}
	
	@Test
	void test_SmssFileReturnDirectory() throws IOException {
		
		Path dir = Files.createDirectories(fs.getPath("dir"));
		
		when(ns.size()).thenReturn(2);
		when(ns.getNoun(ReactorKeysEnum.ENGINE.getKey())).thenReturn(engineGrs);
		
		when(engineGrs.isEmpty()).thenReturn(false);
		when(engineGrs.get(0)).thenReturn("id");
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class); 	
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class)) {
	
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			IEngine engine = mock(IEngine.class);
			utility.when(() -> Utility.getEngine("id")).thenReturn(engine);
			
			when(engine.getSmssFilePath()).thenReturn(dir.toString());
			

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find smss file for engine id. Please reach out to an administrator for assistance", e.getMessage());
		}
	}
	
	@Test
	void test_SmssFileException() throws IOException {
		
		FileSystem mockFs = mock(FileSystem.class);
		reactor.setFileSystem(mockFs);
		Path mockPath = mock(Path.class);
		when(mockFs.getPath("Semoss.txt")).thenReturn(mockPath);
		
		Path dir = Files.createDirectories(fs.getPath("dir"));
		
		when(ns.size()).thenReturn(2);
		when(ns.getNoun(ReactorKeysEnum.ENGINE.getKey())).thenReturn(engineGrs);
		
		when(engineGrs.isEmpty()).thenReturn(false);
		when(engineGrs.get(0)).thenReturn("id");
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class); 	
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class); 
				MockedStatic<Files> files = Mockito.mockStatic(Files.class);
				MockedStatic<Paths> paths = Mockito.mockStatic(Paths.class)) {
	
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			IEngine engine = mock(IEngine.class);
			utility.when(() -> Utility.getEngine("id")).thenReturn(engine);
			when(engine.getSmssFilePath()).thenReturn("Semoss.txt");
			
			files.when(() -> Files.exists(mockPath)).thenReturn(true);
			files.when(() -> Files.isRegularFile(mockPath)).thenReturn(true);
			
			URI mockUri = mock(URI.class);
			when(mockPath.toUri()).thenReturn(mockUri);
			
			paths.when(() -> Paths.get(mockUri)).thenReturn(mockPath);
			
			files.when(() -> Files.readAllBytes(mockPath)).thenThrow(new IOException("error"));

			
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("An error occurred reading the current engine smss details. Detailed message = error", e.getMessage());
		}
	}
	
	@Test
	void test_SmssFileNoException() throws IOException {
		
		FileSystem mockFs = mock(FileSystem.class);
		reactor.setFileSystem(mockFs);
		Path mockPath = mock(Path.class);
		when(mockFs.getPath("Semoss.txt")).thenReturn(mockPath);
		
		Path dir = Files.createDirectories(fs.getPath("dir"));
		
		when(ns.size()).thenReturn(2);
		when(ns.getNoun(ReactorKeysEnum.ENGINE.getKey())).thenReturn(engineGrs);
		
		when(engineGrs.isEmpty()).thenReturn(false);
		when(engineGrs.get(0)).thenReturn("id");
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class); 	
				MockedStatic<Utility> utility = Mockito.mockStatic(Utility.class); 
				MockedStatic<Files> files = Mockito.mockStatic(Files.class);
				MockedStatic<Paths> paths = Mockito.mockStatic(Paths.class);
				MockedStatic<SmssUtilities> smssUtil = Mockito.mockStatic(SmssUtilities.class)) {
	
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			IEngine engine = mock(IEngine.class);
			utility.when(() -> Utility.getEngine("id")).thenReturn(engine);
			when(engine.getSmssFilePath()).thenReturn("Semoss.txt");
			
//			Files file = mock(Files.class);
			files.when(() -> Files.exists(mockPath)).thenReturn(true);
			files.when(() -> Files.isRegularFile(mockPath)).thenReturn(true);
			
			URI mockUri = mock(URI.class);
			when(mockPath.toUri()).thenReturn(mockUri);
			
			paths.when(() -> Paths.get(mockUri)).thenReturn(mockPath);
			
			String test = "test";
			files.when(() -> Files.readAllBytes(mockPath)).thenReturn(test.getBytes());

			smssUtil.when(() -> SmssUtilities.concealSmssSensitiveInfo(test)).thenReturn("test2");
			NounMetadata nm = reactor.execute();
			assertEquals("test2", nm.getValue().toString());
			assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
			
		}
	}

}