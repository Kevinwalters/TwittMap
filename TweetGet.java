
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>This is a code example of Twitter4J Streaming API - sample method support.<br>
 * Usage: java twitter4j.examples.PrintSampleStream<br>
 * </p>
 *
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
public final class TweetGet {

	private static String oAuthConsumerKey = "oQ7aCFiY7cjfmLUutJiouvzw5";
	private static String oAuthConsumerSecret = "mturYGKTi7CXhRlK9gkSJWF8XKyV1pTRLX7n2OBBydYKBTL9e6";
	private static String oAuthAccessToken = "1952751391-ISOlpkQMwv79EOtUQRwdOhlmY5eZMCWM3TePX50";
	private static String oAuthAccessTokenSecret = "lKbVowBHd7xogIsmdiuHB6WBGO0GyR8RoddDmS0XW0TGl";
	private static String[] keywords = {"ISIS", "NFL", "Ebola","Interstellar","Thanksgiving","Halloween","Winter","NYC","Obama"};
	
	
    /**
     * Main entry of this application.
     *
     * @param args
     */
    public static void main(String[] args) throws TwitterException {
    	final TwitterDAO dao = new TwitterDAO();
    	
    	 ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.setDebugEnabled(true)
           .setOAuthConsumerKey(oAuthConsumerKey)
           .setOAuthConsumerSecret(oAuthConsumerSecret)
           .setOAuthAccessToken(oAuthAccessToken)
           .setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
         
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                long statusId = status.getId();
                long userId = status.getUser().getId();
                String screenName = status.getUser().getScreenName();
                String text = status.getText();
                GeoLocation location = status.getGeoLocation();
                String userProfileLocation = status.getUser().getLocation();  
                Date createdTime = status.getCreatedAt();
                double latitude = 0;
                double longitude = 0;
                if (location != null) {
	                latitude = location.getLatitude();
	                longitude = location.getLongitude();
                } else if (userProfileLocation != null) {
                	//TODO query google for coords
                }
                
                Tweet tweet = new Tweet(userId, statusId, screenName, text, latitude, longitude, createdTime);
                boolean hasKeyword = false;
                for (String keyword : keywords) {
                	if (text.toUpperCase().contains(keyword.toUpperCase())) {
                		hasKeyword = true;
                		dao.insertStatus(tweet, keyword);
                	}
                }
                if (!hasKeyword) {
                	dao.insertStatus(tweet, "none");
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                long userId = statusDeletionNotice.getUserId();
                long statusId = statusDeletionNotice.getStatusId();
                dao.deleteStatus(userId, statusId);
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                dao.scrubGeo(userId, upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        //FilterQuery fq = new FilterQuery();
        //String keywords[] = {"ISIS", "NFL", "Ebola","Interstellar","Thanksgiving","Christopher Nolan","Winter","NYC","Obama"};
        //String lang[] = {"en","es"};
        //fq.track(keywords).language(lang);

        twitterStream.addListener(listener);
        twitterStream.sample();
        //twitterStream.filter(fq); 
    }
}