package dataBases.postgres;

import java.sql.Timestamp;
import java.util.ArrayList;

//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;

public class PoiCharacteristics {
    private int poi_id = -1;
    private int user_id= -1;
    private String name = null;
    private double x = -1;
    private double y = -1;
    private int interest = -1;
    private int hotness = -1;
    private boolean publicity = false;
    private String keywords = null;
    private String description = null;
    private Timestamp tmstamp = null;
    private ArrayList <String> keywordsList = null;
    private boolean ismine = false;
    private String pictureURL;

    public PoiCharacteristics() {
    }
   
    public PoiCharacteristics(int poi_id,int user_id,String name, double x , double y, int interest, int hotness, boolean publicity, String keywords, String description, Timestamp tmstamp, boolean ismine){
        this.poi_id = poi_id;
    	this.user_id = user_id;
    	this.name = name;
        this.x = x;
        this.y = y;
        this.interest = interest;
        this.hotness = hotness;
        this.publicity = publicity;
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
        this.description = description;
        this.tmstamp = tmstamp;
        this.ismine = ismine;
    }
    
    
    public String toJson(){
    	String msgJson = null;
    	String strKeywords = null;
    	
    	if ( keywords != null  && !keywords.isEmpty()){
    		strKeywords = "[\"" + keywordsList.get(0) + "\"";
    		for ( int i = 1 ; i < keywordsList.size() ; i ++ ){
    			strKeywords = strKeywords + ",\"" + keywordsList.get(i) + "\"";
    		}
    		strKeywords = strKeywords + "]";
    	} 
    	msgJson = "{\"poi_id\":" + poi_id + ",\"name\":\"" + name + "\",\"x\":" + x + ", \"y\":" + y + ",\"interest\":" + interest + ",\"hotness\":" + hotness + ",\"publicity\":" + publicity + ",\"keywords\":" + strKeywords + ",\"description\" : \"" + description  + "\",\"ismine\": " + ismine + "}";	
    	return msgJson;
    }
    
    
    public String toString(){
        String str = null;
       
        str = poi_id + "," +user_id + "," + name + "," + x + "," + y + "," + interest + "," + hotness + "," + publicity + "," + keywords + "," + description + "," + ismine ;
        
        return str;
    }
    
    /////////////////////set/////////////////////////
    
    public void setPoiId(int poi_id){
    	this.poi_id = poi_id;
    }
    
    public void setUserId(int user_id){
    	this.user_id = user_id;
    }
    
    public void setName(String poi_id) {
        this.name = poi_id;
    }

    
    public void setX(double x) {
        this.x = x;
    }

    
    public void setY(double y) {
        this.y = y;
    }

    
    public void setInterest(int interest) {
        this.interest = interest;
    }

    
    public void setHotness(int hotness) {
        this.hotness = hotness;
    }

    
    public void setPublicity(boolean publicity) {
        this.publicity = publicity;
    }
    
    public void setKeywords (String keywords){
        this.keywords = keywords;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }

    public void setTmstamp(Timestamp tmstamp) {
        this.tmstamp = tmstamp;
    }
    
    public void setIsMine(boolean ismine){
    	this.ismine = ismine;
    }


    ////////////////////////////get//////////////////////
    
    public int getPoiId(){
    	return poi_id;
    }
    
    public int getUserId(){
    	return user_id;
    }
    
    public String getName() {
        return name;
    }

    
    public double getX() {
        return x;
    }

    
    public double getY() {
        return y;
    }

    
    public int getInterest() {
        return interest;
    }

    
    public int getHotness() {
        return hotness;
    }

    
    public boolean getPublicity() {
        return publicity;
    }

    
    public String getKeywords(){
        return keywords;
    }
        
    public String getDescription() {
        return description;
    }

    public Timestamp getTmstamp() {
        return tmstamp;
    }
    
    public boolean getIsMine(){
    	return ismine;
    }

    public ArrayList<String> getKeywordsList() {
        return keywordsList;
    }

    public void setKeywordsList(ArrayList<String> keywordsList) {
        this.keywordsList = keywordsList;
        this.keywords = new String();
        for(String s : this.keywordsList) {
            this.keywords+=s+",";
        }
        
        if(this.keywords.length()>0)
            this.keywords = this.keywords.substring(0,this.keywords.length()-1);
    }

    public String getPictureURL() {
        return pictureURL;
    }

    public void setPictureURL(String pictureURL) {
        this.pictureURL = pictureURL;
    }
    
    
    
    
}
