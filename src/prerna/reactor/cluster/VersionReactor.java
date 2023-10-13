package prerna.reactor.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.validator.GenericValidator;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VersionReactor extends AbstractReactor {

	private static Map<String, String> versionMap;
	private static final String VER_PATH = DIHelper.getInstance().getProperty("BaseFolder") + java.nio.file.FileSystems.getDefault().getSeparator() + "ver.txt";
	public static String VERSION_KEY = "version";
	public static String DATETIME_KEY = "datetime";
	public static String OS = System.getProperty("os.name");


	public VersionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RELOAD.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String reloadStr = this.keyValue.get(this.keysToGet[0]);
		boolean reload = reloadStr != null && Boolean.parseBoolean(reloadStr);
		return new NounMetadata(getVersionMap(reload), PixelDataType.MAP, PixelOperationType.VERSION);
	}

	public static Map<String, String> getVersionMap(boolean reload) {
		if(reload || VersionReactor.versionMap == null) {
			Map<String, String> tempMap = new HashMap<>();

			if(!inContainer()) {
				synchronized(VersionReactor.class) {
					if(VersionReactor.versionMap != null) {
						return VersionReactor.versionMap;
					}

					Properties props = Utility.loadProperties(VER_PATH);
					if(props == null) {
						NounMetadata noun = new NounMetadata("Failed to parse the version", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
						SemossPixelException err = new SemossPixelException(noun);
						err.setContinueThreadOfExecution(false);
						throw err;
					}

					tempMap.put(VERSION_KEY, props.getProperty(VERSION_KEY));
					tempMap.put(DATETIME_KEY, props.getProperty(DATETIME_KEY));
					VersionReactor.versionMap = new HashedMap<>(tempMap);
				}
			} else {
				InputStream versionStream = null;
				InputStream dateStream =null;
				BufferedReader br =null;
				try {
					Properties props = new Properties();
					Enumeration<URL> resources = VersionReactor.class.getClassLoader().getResources("META-INF/maven/org.semoss/semoss/pom.properties");
					while(resources.hasMoreElements()) {
						URL url = resources.nextElement();
						versionStream = url.openStream();
						dateStream = url.openStream();
						br = new BufferedReader(new InputStreamReader(dateStream));
						String line;

						while((line = br.readLine()) != null) {
							if(line.contains("#")) {
								line = line.replaceAll("#", "");
								if(GenericValidator.isDate(line, "EEE MMM d HH:mm:ss z yyyy", false)) {
									String date = line;
									DateFormat originalFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy", Locale.ENGLISH);
									DateFormat targetFormat = new SimpleDateFormat("yyyy-MM-d HH:mm:ss", Locale.ENGLISH);
									Date d = originalFormat.parse(date);
									String formattedDate = targetFormat.format(d); 
									tempMap.put(DATETIME_KEY, formattedDate);
								}
							}
						}
						props.load(versionStream);
						tempMap.put(VERSION_KEY, props.getProperty(VERSION_KEY));
						VersionReactor.versionMap = new HashedMap<>(tempMap);
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				} finally {

					try {
						if(versionStream != null) {
							versionStream.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

					try {
						if(dateStream != null) {
							dateStream.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						if(br != null) {
							br.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
			}
		}

		return VersionReactor.versionMap;
	}

	public static boolean inContainer() {
		
		//check the os
		if(!SystemUtils.IS_OS_LINUX) {
			return false;
		}
		
		//checking if its in kubernetes
		File kubernetesPath = new File("/var/run/secrets/kubernetes.io");
		if(kubernetesPath.exists()) {
			return true;
		}
		
		//check docker or generic lxc container
		try (Stream<String> stream = Files.lines(Paths.get("/proc/1/cgroup"))){
			return stream.anyMatch(line -> line.contains("/docker") || line.contains("/lxc") );
		} catch(IOException e) {
			return false;
		}
	}


}
