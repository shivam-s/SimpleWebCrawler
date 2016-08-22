package crawler.resource.manager;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import oracle.jdbc.OracleDriver;

public class DBManager {

	/* Variables to hold database details */
	private String dbHost = null;
	private String dbPort = null;
	private String dbSID = null;
	private String dbUsername = null;
	private String dbPassword = null;
	private String dbString = null;

	public Connection conn = null;

	public DBManager(String configFileName) throws FileNotFoundException,
			IOException, SQLException {
		try {
			String configFile = configFileName;
			readConfigFile(configFile);
			DriverManager.registerDriver(new OracleDriver());
			dbString = "jdbc:oracle:thin:@" + dbHost + ":" + dbPort + ":"
					+ dbSID;
			System.out.println(dbString);
			conn = DriverManager
					.getConnection(dbString, dbUsername, dbPassword);
			System.out.println("conn built");
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
	}

	// Read Configuration file crawler.conf
	private void readConfigFile(String fileName) throws FileNotFoundException,
			IOException {
		Properties configFile = new Properties();
		configFile.load(new FileInputStream(fileName));
		dbHost = configFile.getProperty("host").trim();
		dbPort = configFile.getProperty("port").trim();
		dbSID = configFile.getProperty("dbid").trim();
		dbUsername = configFile.getProperty("uid").trim();
		dbPassword = configFile.getProperty("pwd").trim();
		System.out.println(dbHost + " " + dbPort + " " + dbSID + " "
				+ dbUsername + " " + dbPassword);
	}

	public void truncateTable() throws SQLException {
		Statement statement = null;
		try {
			if ((conn == null) || conn.isClosed()) {
				// Connect to the database.
				conn = DriverManager.getConnection(dbString, dbUsername,
						dbPassword);
			}
			System.out.println("connection established " + conn);
			statement = conn.createStatement();
			statement.executeUpdate("TRUNCATE TABLE T_CRAWLER_DATA");
			System.out.println("T_CRAWLER_DATA Table has been Truncated");
		} catch (Exception e) {
			System.out.println("Exception while truncating " + e.toString());
		} finally {
			if (statement != null)
				statement.close();
			if (conn != null)
				conn.close();
		}
	}

	// Store to DB
	public void storeToDB(Map<Integer, String> urlsCacheMap) throws Exception {

		// Create the database connection, if it is closed.
		if ((conn == null) || conn.isClosed()) {
			conn = DriverManager
					.getConnection(dbString, dbUsername, dbPassword);
		}
		int sequenceNumber = 0;
		String URL;
		// Create a PreparedStatement object.
		PreparedStatement pstmt = null;
		// Create SQL query to insert data into table
		String sql = "INSERT INTO T_CRAWLER_DATA VALUES(?,?)";
		// Create the OraclePreparedStatement object
		pstmt = conn.prepareStatement(sql);
		Iterator<Integer> iterator = urlsCacheMap.keySet().iterator();
		try {
			while (iterator.hasNext()) {
				sequenceNumber = (int) iterator.next();
				URL = urlsCacheMap.get(sequenceNumber);
				pstmt.setInt(1, sequenceNumber);
				pstmt.setString(2, URL);
				// Execute the PreparedStatement
				pstmt.executeUpdate();
			}
			System.out.println("Data has been stored in DB successfully");
		} catch (SQLException sqlex) {
			// Catch Exceptions and display messages accordingly.
			System.out
					.println("SQLException while connecting and inserting into "
							+ "the database table: " + sqlex.toString());
		} catch (Exception ex) {
			System.out
					.println("Exception while connecting and inserting into the"
							+ " database table: " + ex.toString());
		} finally {
			// Close the Statement and the connection objects.
			if (pstmt != null)
				pstmt.close();
			if (conn != null)
				conn.close();
		}
	}
}