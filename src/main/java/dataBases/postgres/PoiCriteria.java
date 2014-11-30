package dataBases.postgres;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.sql.Connection;
import java.sql.Timestamp;

//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;

public class PoiCriteria {
    int user_id = -1;
    Timestamp start_time = null;
    Timestamp end_time = null;
    double x1Region = -1;
    double y1Region = -1;
    double x2Region = -1;
    double y2Region = -1;
    ArrayList<Integer> friendsIdList = null;
    String orderBy = null;
    int noOfResults = -1;
    String keywords = null;
    ArrayList<String> keywordsList = null;
    
    
    public PoiCriteria ( int user_id, Timestamp start_time, Timestamp end_time, double x1Region, double y1Region, double x2Region, double y2Region, ArrayList<Integer> friendsIdList, String orderBy, int noOfResults, String keywords){
        this.user_id = user_id;
    	this.start_time = start_time;
        this.end_time = end_time;
        this.x1Region = x1Region;
        this.y1Region = y1Region;
        this.x2Region = x2Region;
        this.y2Region = y2Region;
        this.friendsIdList = friendsIdList;
        this.orderBy = orderBy;
        this.noOfResults = noOfResults;
        this.keywords = keywords;
        if ( keywords != null ){
            keywordsList = new ArrayList<String>();
            if (keywords.contains(",")){
                String []temp;
                temp = keywords.split(",");
                if ( temp.length != 0 ){
                    for (int i = 0; i < temp.length ; i++ ){
                        keywordsList.add(temp[i]);
                    }
                }
            }
            else{
                keywordsList.add(keywords);
            }
        }
        else{
            keywordsList = null;
        }
    }
    
    public String toString(){
    	String str = null;
    	str = user_id + "," + start_time + "," + end_time + "," + x1Region + "," + y1Region + "," + x2Region +"," + y2Region + "," + orderBy + "," + noOfResults + "," + keywords; 
    	return str;
    }
    
    
    //////////////////////////get///////////////////////


    public int getUserId() {
        return user_id;
    }

    public Timestamp getStart_time() {
        return start_time;
    }

    public Timestamp getEnd_time() {
        return end_time;
    }

    public Double getX1Region() {
        return x1Region;
    }

    public Double getY1Region() {
        return y1Region;
    }

    public Double getX2Region() {
        return x2Region;
    }

    public Double getY2Region() {
        return y2Region;
    }

    public ArrayList<Integer> getFriendsList() {
        return friendsIdList;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public int getNoOfResults() {
        return noOfResults;
    }

    public String getKeywords() {
        return keywords;
    }

    public ArrayList<String> getKeywordsList() {
        return keywordsList;
    }
    
    ////////////////////////////set////////////////////////////////

    public void setUserId(int user_id) {
        this.user_id = user_id;
    }

    public void setStart_time(Timestamp start_time) {
        this.start_time = start_time;
    }

    public void setEnd_time(Timestamp end_time) {
        this.end_time = end_time;
    }

    public void setX1Region(double x1Region) {
        this.x1Region = x1Region;
    }

    public void setY1Region(double y1Region) {
        this.y1Region = y1Region;
    }

    public void setX2Region(double x2Region) {
        this.x2Region = x2Region;
    }

    public void setY2Region(double y2Region) {
        this.y2Region = y2Region;
    }

    public void setFriendsList(ArrayList<Integer> friendsIdList) {
        this.friendsIdList = friendsIdList;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public void setNoOfResults(int noOfResults) {
        this.noOfResults = noOfResults;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public void setKeywordsList(ArrayList<String> keywordsList) {
        this.keywordsList = keywordsList;
    }
    
}

