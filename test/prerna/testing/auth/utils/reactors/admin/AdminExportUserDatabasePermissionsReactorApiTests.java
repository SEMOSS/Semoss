package prerna.testing.auth.utils.reactors.admin;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminExportUserDatabasePermissionsReactor;
import prerna.om.Insight;

import prerna.sablecc2.om.NounStore;


public class AdminExportUserDatabasePermissionsReactorApiTests  extends AbstractBaseSemossApiTests {

	private FileSystem fs = Jimfs.newFileSystem(Configuration.unix());

	private AdminExportUserDatabasePermissionsReactor reactor;
	private Insight insight;
	private User user;
	private NounStore ns;
	private Path testFilePath;
	private String engine;

 
	@BeforeEach
	void setup() throws IOException {
		reactor = new AdminExportUserDatabasePermissionsReactor();
		reactor.setFileSystem(fs);
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);

		ns = mock(NounStore.class);
		reactor.setNounStore(ns); 

		Path p = fs.getPath("work", "insight1");
		Files.createDirectories(p);

	}

    @Test
    public void testAdminUtilsNullThrowsException() {
 
    	//testFilePath = Files.createTempFile("test-file", ".txt");
    	String engine = ApiSemossTestEngineUtils.createBasicEngine();
        when(SecurityAdminUtils.getInstance(user)).thenReturn(null); // Simulate adminUtils being null
        String pixel = ApiSemossTestUtils.buildPixelCall(AdminExportUserDatabasePermissionsReactor.class, ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    }
    
    
   }