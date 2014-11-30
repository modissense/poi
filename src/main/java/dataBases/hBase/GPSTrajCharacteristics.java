package dataBases.hBase;

import java.sql.Timestamp;

public class GPSTrajCharacteristics {
	int user_id;
	String tmstamp;
	double lat;
	double lon;
	String date = null;
	String del = " ";
	String []temp;
	
	public GPSTrajCharacteristics(){
		
	}
	
	public GPSTrajCharacteristics (int user_id, String tmstamp, double lat,double lon){
		this.user_id = user_id;
		this.tmstamp = tmstamp;
		this.lat = lat;
		this.lon = lon;
		temp = tmstamp.split(del);
		this.date = temp[0];
	}
        
        public String toJson(){
            String msgJson = null;
            msgJson = "{\"user_id\":" + this.getUser_id() + ",\"x\":\"" + this.getLat() + "\",\"y\":\"" + this.getLon() + "\", \"tmstmp\":" + this.getTimestamp() + "}";	
            return msgJson;
        }
	
	public String toString(){
		String str = "user_id = " + user_id + ",tmstamp = " + tmstamp + ",x = " + lat + ",y = " + lon + "\n"; 
		return str;
	}
	
	//////////////////////////set///////////////////////////////
	public void setUser_id(int user_id){
		this.user_id = user_id;
	}
	
	public void setTimestamp(String tmstamp){
		this.tmstamp = tmstamp;
	} 
	
	public void setLat(double lat){
		this.lat = lat;
	}
	
	public void setLon(double lon){
		this.lon = lon;
	}
	
	public void setDate(String date){
		this.date = date;
		
	}
	
	//////////////////////////get///////////////////////////////
	
	public int getUser_id(){
		return user_id;
	}
	
	public String getTimestamp(){
		return tmstamp;
	}
	
	public double getLat(){
		return lat;
	}
	
	public double getLon(){
		return lon;
	}
	
	public String getDate(){
		return date;
	}
}
