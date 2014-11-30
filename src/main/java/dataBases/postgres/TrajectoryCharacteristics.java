package dataBases.postgres;

import java.sql.Date;
import java.sql.Timestamp;

public class TrajectoryCharacteristics {
	private int user_id = -1;
	private Date date = null;
	private int seq_number = -1;
	private String comment = null;
	private Timestamp arrived = null;
	private Timestamp off = null;
	private boolean publicity = true;
	private int poi_id = -1;
	
	public TrajectoryCharacteristics(){
		
	}
	
	public TrajectoryCharacteristics(int user_id, Date date, int seq_number, String comment, Timestamp arrived, Timestamp off,boolean publicity,int poi_id){
		this.user_id = user_id;
		this.date = date;
		this.seq_number = seq_number;
		this.comment = comment;
		this.arrived = arrived;
		this.off = off;
		this.publicity = publicity;
		this.poi_id = poi_id;
	}
	
	public String toString(){
		String str = null;
		str = user_id + "," + date + "," + poi_id + "," + seq_number + "," + publicity + "," + comment + "," + arrived + "," + off;
		return str;
	}

	public int getUserId() {
		return user_id;
	}

	public Date getDate() {
		return date;
	}
	
	public int getSeq_number() {
		return seq_number;
	}
	
	
	public String getComment() {
		return comment;
	}
	
	public Timestamp getArrived() {
		return arrived;
	}
	
	public Timestamp getOff() {
		return off;
	}	
	
	public boolean getPublicity(){
		return publicity;
	}
	
	public int getPoiId(){
		return poi_id;
	}
	
	
	
	public void setUserId(int user_id) {
		this.user_id = user_id;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public void setSeq_number(int seq_number) {
		this.seq_number = seq_number;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
	
	public void setArrived(Timestamp arrived) {
		this.arrived = arrived;
	}

	public void setOff(Timestamp off) {
		this.off = off;
	}

	public void setPublicity(boolean publicity){
		this.publicity = publicity;
	}
		
	public void setPoiId(int poi_id){
		this.poi_id = poi_id;
	}
}
