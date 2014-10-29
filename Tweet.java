import java.util.List;


public class Tweet {
	
	private long userId;
	private long statusId;
	private String screenName;
	private String text;
	private double latitude;
	private double longitude;
	
	public Tweet() {
		
	}
	
	public Tweet(long userId, long statusId, String screenName, String text, double latitude, double longitude) {
		this.setUserId(userId);
		this.setStatusId(statusId);
		this.setScreenName(screenName);
		this.setText(text);
		this.setLatitude(latitude);
		this.setLongitude(longitude);
	}

	public long getStatusId() {
		return statusId;
	}

	public void setStatusId(long statusId) {
		this.statusId = statusId;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

}
