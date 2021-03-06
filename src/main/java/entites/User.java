package entites;

import java.util.HashMap;
import java.util.List;

/**
 * Class used to implement all user's transaction with the database. This class implements method
 * in which the user can fetch a user from the database, update him, delete him, etc. In addition, with this class,
 * a user's friends can be specified, (fetched, pushed, deleted, etc.). Finally, the social network specific details
 * can be retrieved (and set) using this class.
 * 
 * <br/>
 * <b>Tables managed by this class</b>
 * <ul>
 * <li>users </li>
 * <li>friends</li>
 * <li>sn_list</li>
 * </ul>
 * @author Giannis Giannakopoulos
 *
 */
public class User extends DatabaseTransaction {

	// fields of the actual table
	private Integer id=-1;
	private String username;
	private int coordX=-1;
	private int coordY=-1;
	
	public User() {
		super();
	}

	public User(int id){
		super();
		this.id=id;
		this.read();
	}
	
	public User(String username){
		super();
		this.username=username;
		this.read();
	}
	
	@Override
	public String getTableName() {
		return "users";
	}
	
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
		this.isSynced=false;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
		this.isSynced=false;
	}
	
	/**
	 * @return the coordX
	 */
	public int getCoordX() {
		return coordX;
	}

	/**
	 * @param coordX the coordX to set
	 */
	public void setCoordX(int coordX) {
		this.coordX = coordX;
	}

	/**
	 * @return the coordY
	 */
	public int getCoordY() {
		return coordY;
	}

	/**
	 * @param coordY the coordY to set
	 */
	public void setCoordY(int coordY) {
		this.coordY = coordY;
	}

	/**
	 * Put this user object to database.
	 */
	public boolean create(){
		HashMap<String, String> foo = new HashMap<String,String>();
		if(this.id!=-1)
			foo.put("id", this.id.toString());
		if(this.username!=null)
			foo.put("username", "'"+this.username+"'");
		if(this.coordX!=-1 && this.coordY!=-1)
			foo.put("map_center", "POINT("+this.coordX+","+this.coordY+")");
		return this.create(foo);
	}
	
	/**
	 * Delete object from database.
	 */
	public boolean delete(){
		HashMap<String, String> foo = this.serialize();
		foo.remove("map_center");
		return this.delete(foo);
	}
	
	public void close(){
		this.closeConnection();
	}
	/**
	 * 
	 * @return
	 */
	public List<HashMap<String, String>> read(){
		HashMap<String, String> t = this.serialize();
		t.remove("map_center");
		List<HashMap<String, String>> res =this.read(t);
		if(res.size()==1){
			HashMap<String, String> info = res.get(0);
			this.id=new Integer(info.get("id"));
			this.username = info.get("username");
			if(info.get("map_center")!=null){
				String temp[] = info.get("map_center").split(",");
				this.coordX=new Integer(temp[0].substring(1));
				this.coordY=new Integer(temp[1].substring(0,temp[1].length()-1));
			}
			this.isSynced=true;
		}
		return res;
	}
	
	public boolean update(){
		HashMap<String, String> id = new HashMap<String, String>();
		id.put("id", this.id.toString());
		this.update(this.serialize(),id);
		return true;
	}
	
	/**
	 * Returns a HashMap of the object. Used for database transactions.
	 * @return
	 */
	private HashMap<String, String> serialize(){
		HashMap<String, String> foo = new HashMap<String, String>();
		if(this.id!=-1)
			foo.put("id", this.id.toString());
		if(this.username!=null)
			foo.put("username", "'"+this.username+"'");
		if(this.coordX!=-1 && this.coordY!=-1)
			foo.put("map_center", "POINT("+this.coordX+","+this.coordY+")");
		return foo;
	}
	
	public boolean isSynced(){
		return this.isSynced;
	}
	
	@Override
	public String toString() {
		return "("+this.id.toString()+","+this.username+", ["+this.coordX+","+this.coordY+"])";
	}
	
	public String toJson(){
		return "{\"id\":\""+this.id+"\", \"username\":\""+this.username+"\",\"map_center\":\"("+this.coordX+","+this.coordY+")\"}";
	}
	
	public static void main(String[] args) {
		User u = new User();
		u.setUsername("ggian");
		u.read();
		System.out.println(u.toJson());
	}
}
