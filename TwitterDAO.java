import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class TwitterDAO {
	
	private String insertSQL = "INSERT INTO Statuses(UserId, StatusId, ScreenName, StatusText, Latitude, Longitude, Keyword) " +
								"VALUES (?,?,?,?,?,?,?)";
	private String deleteSQL = "DELETE FROM Statuses WHERE UserId = ";
	private String deleteStatusSQL = " AND StatusId = ";
	private String selectSQL = "SELECT * FROM Statuses";
	private String selectFilterSQL = " WHERE Keyword='";
	private String updateSQL = "UPDATE Statuses SET Latitude=0, Longitude=0 WHERE UserId = ";
	private String updateStatusSQL = " AND StatusId <= ";
	private String endSQL = ")";
	
	public TwitterDAO() {
		try {
    		Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
	}
	
	public void insertStatus(Tweet tweet, String keyword) {
		Connection conn = null;
		PreparedStatement stmt = null;
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=cloudcomputing&password=Assignment1");
    		stmt = conn.prepareStatement(insertSQL);
			stmt.setLong(1,  tweet.getUserId());
			stmt.setLong(2, tweet.getStatusId());
			stmt.setString(3, tweet.getScreenName());
			stmt.setString(4, tweet.getText());
			stmt.setDouble(5, tweet.getLatitude());
			stmt.setDouble(6, tweet.getLongitude());
			stmt.setString(7, keyword);
			stmt.executeUpdate();
    	} catch (SQLException e) {
    		System.out.println("SQLException: " + e.getMessage());
    	    System.out.println("SQLState: " + e.getSQLState());
    	    System.out.println("VendorError: " + e.getErrorCode());
    	} finally {
    		try {
		         if(stmt!=null) {
		        	 conn.close();
		         }
    		} catch(SQLException se) {
    			
    		}
    		try{
    			if(conn!=null) {
    				conn.close();
    			}
    		} catch(SQLException se) {
    			se.printStackTrace();
    		}
    	}
	}
	
	public void deleteStatus(long userId, long statusId) {
		Connection conn = null;
		Statement stmt = null;
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=cloudcomputing&password=Assignment1");
    		stmt = conn.createStatement();
    		String deleteString = deleteSQL;
    		deleteString += userId;
    		deleteString += deleteStatusSQL;
    		deleteString += statusId;
    		stmt.executeUpdate(deleteString);
    	} catch (SQLException e) {
    		System.out.println("SQLException: " + e.getMessage());
    	    System.out.println("SQLState: " + e.getSQLState());
    	    System.out.println("VendorError: " + e.getErrorCode());
    	} finally {
    		try {
		         if(stmt!=null) {
		        	 conn.close();
		         }
    		} catch(SQLException se) {
    			
    		}
    		try{
    			if(conn!=null) {
    				conn.close();
    			}
    		} catch(SQLException se) {
    			se.printStackTrace();
    		}
    	}
	}
	/**
	 * Removes geo location from all records for a user up until a certain status id
	 * @param userId user to be scrubbed
	 * @param statusId statusid to scrub until
	 */
	public void scrubGeo(long userId, long statusId) {
		Connection conn = null;
		Statement stmt = null;
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=cloudcomputing&password=Assignment1");
    		stmt = conn.createStatement();
    		String updateString = updateSQL;
    		updateString += userId;
    		updateString += updateStatusSQL;
    		updateString += statusId;
    		stmt.executeUpdate(updateString);
    	} catch (SQLException e) {
    		System.out.println("SQLException: " + e.getMessage());
    	    System.out.println("SQLState: " + e.getSQLState());
    	    System.out.println("VendorError: " + e.getErrorCode());
    	} finally {
    		try {
		         if(stmt!=null) {
		        	 conn.close();
		         }
    		} catch(SQLException se) {
    			
    		}
    		try{
    			if(conn!=null) {
    				conn.close();
    			}
    		} catch(SQLException se) {
    			se.printStackTrace();
    		}
    	}
	}
	
	public List<Tweet> getAllTweets() {
		Connection conn = null;
		Statement stmt = null;
		List<Tweet> tweets = new ArrayList<Tweet>();
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=cloudcomputing&password=Assignment1");
    		stmt = conn.createStatement();
    		String selectString = selectSQL;
    		ResultSet rs = stmt.executeQuery(selectString);
    		while (rs.next()) {
    			long userId = rs.getLong("UserId");
    			long statusId = rs.getLong("StatusId");
    			String screenName = rs.getString("ScreenName");
    			String text = rs.getString("StatusText");
    			double latitude = rs.getDouble("Latitude");
    			double longitude = rs.getDouble("Longitude");
    			Tweet tweet = new Tweet(userId, statusId, screenName, text, latitude, longitude);
    			tweets.add(tweet);
    		}
    	} catch (SQLException e) {
    		System.out.println("SQLException: " + e.getMessage());
    	    System.out.println("SQLState: " + e.getSQLState());
    	    System.out.println("VendorError: " + e.getErrorCode());
    	} finally {
    		try {
		         if(stmt!=null) {
		        	 conn.close();
		         }
    		} catch(SQLException se) {
    			
    		}
    		try{
    			if(conn!=null) {
    				conn.close();
    			}
    		} catch(SQLException se) {
    			se.printStackTrace();
    		}
    	}
    	return tweets;
	}
	
	public List<Tweet> getFilteredTweets(String filter) {
		Connection conn = null;
		Statement stmt = null;
		List<Tweet> tweets = new ArrayList<Tweet>();
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?user=cloudcomputing&password=Assignment1");
    		stmt = conn.createStatement();
    		String selectString = selectSQL;
    		selectString += selectFilterSQL;
    		selectString += filter + "'";
    		ResultSet rs = stmt.executeQuery(selectString);
    		while (rs.next()) {
    			long userId = rs.getLong("UserId");
    			long statusId = rs.getLong("StatusId");
    			String screenName = rs.getString("ScreenName");
    			String text = rs.getString("StatusText");
    			double latitude = rs.getDouble("Latitude");
    			double longitude = rs.getDouble("Longitude");
    			Tweet tweet = new Tweet(userId, statusId, screenName, text, latitude, longitude);
    			tweets.add(tweet);
    		}
    	} catch (SQLException e) {
    		System.out.println("SQLException: " + e.getMessage());
    	    System.out.println("SQLState: " + e.getSQLState());
    	    System.out.println("VendorError: " + e.getErrorCode());
    	} finally {
    		try {
		         if(stmt!=null) {
		        	 conn.close();
		         }
    		} catch(SQLException se) {
    			
    		}
    		try{
    			if(conn!=null) {
    				conn.close();
    			}
    		} catch(SQLException se) {
    			se.printStackTrace();
    		}
    	}
    	return tweets;
	}
}
