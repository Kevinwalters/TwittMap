import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


public class TwitterDAO {
	
	private String insertSQL = "INSERT INTO Statuses(StatusId, ScreenName, Test, Latitude, Longitude, Keyword) " +
								"VALUES (";
	private String deleteSQL = "DELETE FROM Statuses WHERE StatusId = ";
	private String endSQL = ")";
	
	public TwitterDAO() {
		try {
    		Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
	}
	
	public void insertStatus(long statusId, String screenName, String text, double latitude, double longitude, List<String> keywords) {
		Connection conn = null;
		Statement stmt = null;
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost/test?user=admin&password=assignment1rootpassword");
    		stmt = conn.createStatement();
    		String insertString;
    		for (String keyword : keywords) {
    			insertString = insertSQL;
    			insertString += "'" + statusId + "'";
    			insertString += "'" + screenName + "'";
    			insertString += "'" + text + "'";
    			insertString += "'" + latitude + "'";
    			insertString += "'" + longitude + "'";
    			insertString += "'" + keyword + "'";
    			insertString += endSQL;
    			stmt.executeUpdate(insertString);
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
	}
	
	public void deleteStatus(String statusId) {
		Connection conn = null;
		Statement stmt = null;
    	try {
    		conn = DriverManager.getConnection("jdbc:mysql://localhost/test?user=admin&password=assignment1rootpassword");
    		stmt = conn.createStatement();
    		String deleteString = deleteSQL;
    		deleteString += "'" + statusId + "'";
    		deleteString += endSQL;
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
}
