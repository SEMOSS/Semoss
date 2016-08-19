package prerna.ds.nativeframe;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class NativeFrameBuilder {
	private static final Logger LOGGER = LogManager
			.getLogger(NativeFrameBuilder.class.getName());
	Connection conn = null;
	protected String engineName = null;
	private String propFile = null;
	protected Properties prop = null;
	private String userName = null;
	private String password = null;
	private String url = null;

	static final String NATIVEFRAME = "NATIVEFRAME";
	
	public NativeFrameBuilder() {
		//    	//initialize a connection
		///getConnection();

	}

	public void setConnection(String proFile) {
		Properties prop= getEginge(proFile);
		this.url = prop.getProperty(Constants.CONNECTION_URL);
		this.userName = prop.getProperty(Constants.USERNAME);
		this.password = prop.getProperty(Constants.PASSWORD);
		
		if (this.conn == null) {
			try {
				// working with Mariadb
				Class.forName("org.mariadb.jdbc.Driver");
				conn = DriverManager
						.getConnection(url + "?user=" + userName + "&password=" + new String(password));

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Connection getConnection() {
		return this.conn;
	}


	/*************************** TEST **********************************************/
	public static void main(String[] a) throws Exception {
		Connection conn = null;

		//testDB();
		Properties prop = new NativeFrameBuilder().loadProp("C:/Users/phok/workspace/SEMOSS/db/MariaDb.smss");
//		System.out.print(prop.getProperty(Constants.USERNAME));
		String url = prop.getProperty(Constants.CONNECTION_URL);
		String userName = prop.getProperty(Constants.USERNAME);
		String password = prop.getProperty(Constants.PASSWORD);
		Class.forName("org.mariadb.jdbc.Driver");
		conn = DriverManager
				.getConnection(url + "?user=" + userName + "&password=" + new String(password));
		Statement stmt = conn.createStatement();
		String query = "select * from director";
		ResultSet rs = stmt.executeQuery(query);
		while (rs.next()) {
		 System.out.print(rs.toString());
		}

	}

	// Test method
	public static void testDB() throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
		Connection conn = DriverManager
				.getConnection("jdbc:mariadb://localhost:3306/moviedb?user=root&password=password");

		Statement stmt = conn.createStatement();
		String query = "select * from Movie_Data";
		// String query =
		// "select t.title, s.studio from title t, studio s where t.title = s.title_fk";
		// query =
		// "select t.title, concat(t.title,':', s.studio), s.studio from title t, studio s where t.title = s.title_fk";
		// ResultSet rs = stmt.executeQuery("SELECT * FROM TITLE");
		ResultSet rs = stmt.executeQuery(query);
		// stmt.execute("CREATE TABLE MOVIES AS SELECT * From CSVREAD('../Movie.csv')");

		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();

		ArrayList records = new ArrayList();
		System.err.println("Number of columns " + columnCount);

		while (rs.next()) {
			Object[] data = new Object[columnCount];
			for (int colIndex = 0; colIndex < columnCount; colIndex++) {
				data[colIndex] = rs.getObject(colIndex + 1);
				System.out.print(rsmd.getColumnType(colIndex + 1));
				System.out.print(rsmd.getColumnName(colIndex + 1) + ":");
				System.out.print(rs.getString(colIndex + 1));
				System.out.print(">>>>" + rsmd.getTableName(colIndex + 1)
						+ "<<<<");
			}
			System.out.println();
		}

		// add application code here
		conn.close();
	}
	
	/**
	 * Read the engine and properties from an exist file
	 */
	
	public Properties getEginge (String engineName){ 
		try{
			String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
			if(engineName != null){
				this.propFile = engineName;
				LOGGER.info("Opening DB - " + engineName);
				prop = loadProp(dbBaseFolder + "/db/" + engineName +".smss");
			}			
		}catch (RuntimeException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		return this.prop;
		
		
	}
	

	/*************************** END TEST **********************************************/

	/**
	 * Method loadProp. Loads the database properties from a specifed properties
	 * file.
	 * @param fileName			String of the name of the properties file to be loaded.
	 * @return Properties		The properties imported from the prop file.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public Properties loadProp(String fileName) throws FileNotFoundException, IOException {
		Properties retProp = new Properties();
		if(fileName != null) {
			FileInputStream fis = new FileInputStream(fileName);
			retProp.load(fis);
			fis.close();
		}
		LOGGER.debug("Properties >>>>>>>>" + fileName);
		return retProp;
	}

}
