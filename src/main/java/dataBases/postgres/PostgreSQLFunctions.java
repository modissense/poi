package dataBases.postgres;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import dataBases.hBase.DBScanCharacteristics;

import entites.Configuration;



public class PostgreSQLFunctions {
	Connection con = null;
    Statement st = null;
    ResultSet rs = null;
    ResultSet rs2 = null;
    String query = null;
	
	private Configuration conf;
    
	
    public PostgreSQLFunctions(){
    }
    
    
    public Connection OpenConnection() throws SQLException{
        
    	
    	conf = new Configuration();
        
    	try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(PostgreSQLFunctions.class.getName()).log(Level.SEVERE, null, ex);
        }
    	
    	con = DriverManager.getConnection("jdbc:postgresql://dbserver/new_modissense","root","root");
    	
        if ( con == null){
            System.err.println("Connection Failed");
        }
        else{
            System.out.println("Connection Success");
        }
        return con;
    }
    
    
    public void CloseConnection( Connection con ) throws SQLException{
        if ( con != null ){
            con.close();
            System.out.println("Connection Close");
        }
        else{
            System.err.println("Connection does not exist");
        }
    }
    

    
/////////////////////////////////////////POI Functions////////////////////////////////////////////////////

    
    public PoiCharacteristics addNewPOIs(Connection con, PoiCharacteristics poi) throws SQLException{
		PoiCharacteristics returnPoi = null;
		java.util.Date tmstamp= new java.util.Date();
	
		st = con.createStatement();  
		if ( poi.getUserId() != -1 ){
			query = "INSERT INTO poi(user_id,name,geo,interest,hotness,publicity,keywords,description,tmstamp) VALUES ('" + poi.getUserId() + "','" + poi.getName() + "',GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326),'" + poi.getInterest() + "','" + poi.getHotness() + "','" + poi.getPublicity() + "','" + poi.getKeywords() + "','" + poi.getDescription()  + "','" + tmstamp + "')";
	    	System.out.println(query);
		}
		else{
			query = "INSERT INTO poi(name,geo,interest,hotness,publicity,keywords,description,tmstamp) VALUES ('" + poi.getName() + "',GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326),'" + poi.getInterest() + "','" + poi.getHotness() + "','" + poi.getPublicity() + "','" + poi.getKeywords() + "','" + poi.getDescription()  + "','" + tmstamp + "')";
		}
	    st.executeUpdate(query);
	    query = "SELECT poi_id FROM poi WHERE geo = GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326)";
	    rs = st.executeQuery(query);
	    while(rs.next()){
	    	poi.setPoiId(rs.getInt(1));
	    }
	    return poi;
	}
	    
	    
	    
	public boolean addNewPOIsList(Connection con, ArrayList<PoiCharacteristics> listOfPOIs) throws SQLException{
		PoiCharacteristics poi;
		java.util.Date tmstamp= new java.util.Date();
	
		st = con.createStatement();
		for ( int i = 0 ; i < listOfPOIs.size() ; i ++ ){
			poi = listOfPOIs.get(i);                                          
			if ( poi.getUserId() != -1 ){
				query = "INSERT INTO poi(user_id,name,geo,interest,hotness,publicity,keywords,description,tmstamp) VALUES ('" + poi.getUserId() + "','" + poi.getName() + "',GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326),'" + poi.getInterest() + "','" + poi.getHotness() + "','" + poi.getPublicity() + "','" + poi.getKeywords() + "','" + poi.getDescription()  + "','" + tmstamp + "')";
		    	System.out.println(query);
			}
			else{
				query = "INSERT INTO poi(name,geo,interest,hotness,publicity,keywords,description,tmstamp) VALUES ('" + poi.getName() + "',GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326),'" + poi.getInterest() + "','" + poi.getHotness() + "','" + poi.getPublicity() + "','" + poi.getKeywords() + "','" + poi.getDescription()  + "','" + tmstamp + "')";
				System.out.println(query);
			}
			
			st.executeUpdate(query);
		}
		return true;
	}
	

	public ArrayList<PoiCharacteristics> findDuplicates( Connection con, double x1, double y1, int r , int userID) throws SQLException{
		ArrayList<PoiCharacteristics> listOfPOIs = null;
		PoiCharacteristics poi;
		String name = null;
		int poi_id = -1;
		double x;
		double y;
		int interest;
		int hotness;
		boolean publicity;
		String keywords = null;
		String description = null;
		Timestamp tmstamp = null;
		Timestamp deltmstamp = null;
		int user_id;
		boolean FLAG = false;
	
		st = con.createStatement();
		query = "SELECT p.poi_id,p.name,ST_X(geo) AS lon, ST_Y(geo) AS lat,p.hotness,p.publicity,p.interest,p.keywords,p.description,p.tmstamp,p.deltmstamp,p.user_id,ST_Distance_Sphere(geo,GeomFromText('POINT(" + x1 +" " + y1 + ")',4326)) AS dist FROM poi p WHERE p.deltmstamp is Null and (ST_Distance_Sphere(p.geo,GeomFromText('POINT(" + x1 +" " + y1 + ")',4326)) <= " + r +" )" ;
		System.out.println("query = " + query);
		rs = st.executeQuery(query);
		if (rs.next()){
			poi_id = rs.getInt(1);
			name = rs.getString(2);
			x = rs.getDouble(3);
			y = rs.getDouble(4);
			hotness = rs.getInt(5);
			publicity = rs.getBoolean(6);
			interest = rs.getInt(7);
			keywords = rs.getString(8);
			description = rs.getString(9);
			tmstamp = rs.getTimestamp(10);
			deltmstamp = rs.getTimestamp(11);
			user_id = rs.getInt(12);
			
			if (publicity == false ){
				if ( user_id == userID){
					listOfPOIs = new ArrayList<PoiCharacteristics>();
					poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
					listOfPOIs.add(poi);
					FLAG = true;
				}
				else {
					st = con.createStatement();
					query = "SELECT userb FROM friends WHERE usera = " + userID + " AND userb = " + user_id + ";";
					rs2 = st.executeQuery(query);
					if (rs2.next()){
						listOfPOIs = new ArrayList<PoiCharacteristics>();
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
						listOfPOIs.add(poi);
						FLAG = true;
					}
				}
			}
			else{
				listOfPOIs = new ArrayList<PoiCharacteristics>();
				if (user_id == userID){
					poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
				}
				else{
					poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
				}
				listOfPOIs.add(poi);
				FLAG = true;
			}
	
			
			while(rs.next()){
				poi_id = rs.getInt(1);
				name = rs.getString(2);
				x = rs.getDouble(3);
				y = rs.getDouble(4);
				hotness = rs.getInt(5);
				publicity = rs.getBoolean(6);
				interest = rs.getInt(7);
				keywords = rs.getString(8);
				description = rs.getString(9);
				tmstamp = rs.getTimestamp(10);
				deltmstamp = rs.getTimestamp(11);
				user_id = rs.getInt(12);
				if (publicity == false ){
					if ( user_id == userID){
						if (FLAG == true){
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
							listOfPOIs.add(poi);
						}
						else{
							listOfPOIs = new ArrayList<PoiCharacteristics>();
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
							listOfPOIs.add(poi);
							FLAG = true;
						}
					}
					else{
						st = con.createStatement();
						query = "SELECT userb FROM friends WHERE usera = " + userID + " AND userb = " + user_id + ";";
						rs2 = st.executeQuery(query);
						if (rs2.next()){
							if ( FLAG == true){
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
								listOfPOIs.add(poi);
							}
							else{
								listOfPOIs = new ArrayList<PoiCharacteristics>();
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
								listOfPOIs.add(poi);
								FLAG = true;
							}
						}
					}
				}
				else{
					if (FLAG == true){
						if ( user_id == userID){
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
						}
						else{
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
						}
						listOfPOIs.add(poi);
					}
					else{
						listOfPOIs = new ArrayList<PoiCharacteristics>();
						if ( user_id == userID){
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
						}
						else{
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
						}
						listOfPOIs.add(poi);
						FLAG = true;
					}
				}
			}
		}
		else{
			return null;
		}
		
		return listOfPOIs;
	}
	
	public ArrayList<PoiCharacteristics> getPOIs(Connection con,PoiCriteria poiCrit) throws SQLException{
		String name;
		double x = 0,y = 0;
		int interest;
		int hotness;
		int poi_id = -1;
		boolean publicity;
		String keywords = null;
		String description = null;
		Timestamp tmstamp = null;
		Timestamp deltmstamp = null;
		ArrayList<PoiCharacteristics>listOfPOIs = null;
		PoiCharacteristics poi = null ;
		int user_id= -1;
		boolean FLAG = false;
		boolean FLAGLIST = false;
	
		st = con.createStatement();
		
	
		/*search in his own POIs*/
		if ( poiCrit.getFriendsList() == null ){
	
			query = "SELECT p.poi_id,p.name,ST_X(geo) AS lon, ST_Y(geo) AS lat,p.hotness,p.publicity,p.interest,p.keywords,p.description,p.tmstamp,p.deltmstamp,p.user_id  FROM poi p WHERE p.deltmstamp is Null and ";
	
			/*rectangle*/
			if ( poiCrit.getX1Region() != -1 && poiCrit.getY1Region() != -1 && poiCrit.getX2Region() != -1 && poiCrit.getY2Region() != -1 ){
				FLAG = true; //GeomFromText('POINT(" + x1 +" " + y1 + ")',4326)
				query = query +"  ST_Contains(ST_SetSRID(ST_MakeBox2D('Point(" + poiCrit.getX1Region()+ " " + poiCrit.getY1Region() + ")','Point(" + poiCrit.getX2Region() + " " + poiCrit.getY2Region() + ")'),4326),p.geo) ";
			}
		
			/*start time, end time*/
			if ( poiCrit.getStart_time() != null && poiCrit.getEnd_time() != null ){
				if (FLAG == true){
					query = query + " and ";
				}
				FLAG = true;
				query = query + "p.poi_id IN ( SELECT poi_id FROM poi_list WHERE tmstamp >= '" + poiCrit.getStart_time() + "' and tmstamp <= '" + poiCrit.getEnd_time() + "')" ;
			}
			else if ( poiCrit.getStart_time() != null ){
				if (FLAG == true){
					query = query + " and ";
				}
				FLAG = true;
				query = query + "p.poi_id IN ( SELECT poi_id FROM poi_list WHERE tmstamp >= '" + poiCrit.getStart_time() + "')" ;
			}
			else if ( poiCrit.getEnd_time() != null ){
				if (FLAG == true){
					query = query + " and ";
				}
				FLAG = true;
				query = query + " p.poi_id IN (  SELECT poi_id FROM poi_list WHERE tmstamp <= '" + poiCrit.getEnd_time() + "') " ;
			}
                        
                        /*name*/
			if ( poiCrit.getKeywordsList() != null ){
		
				if (FLAG == true){
					query = query + " and ";
				}
				query = query + "(p.name ILIKE '%" + poiCrit.getKeywordsList().get(0) + "%'";
				for ( int i = 1 ; i < poiCrit.getKeywordsList().size() ; i ++ ){
					query = query +  " or p.name ILIKE '%" + poiCrit.getKeywordsList().get(i) + "%'";
				}
                                
                                query = query + ")";
				FLAG = true;
			}
		
			/*keywords*/
			if ( poiCrit.getKeywordsList() != null ){
		
				if (FLAG == true){
					query = query + " or ";
				}
				query = query + "(p.keywords ILIKE '%" + poiCrit.getKeywordsList().get(0) + "%'";
				for ( int i = 1 ; i < poiCrit.getKeywordsList().size() ; i ++ ){
					query = query +  " or p.keywords ILIKE '%" + poiCrit.getKeywordsList().get(i) + "%'";
				}
                                
                                query = query + ")";
				FLAG = true;
			}
		
			/*Order by*/
			if (poiCrit.getOrderBy() != null ){
				if (FLAG == true){
					query = query + " ORDER BY " + poiCrit.getOrderBy() + " DESC";
				}
				FLAG = true;
			}
		
			/*number of resutls*/
			if (poiCrit.getNoOfResults() != -1 ){
				if (FLAG == true){
					query = query + " LIMIT " + poiCrit.getNoOfResults();
				}
				FLAG = true;
			}
		}
		/*search his freinds POIs*/
		else{
			query = "SELECT DISTINCT(p.poi_id),p.name,ST_X(geo) AS lon, ST_Y(geo) AS lat,p.hotness,p.publicity,p.interest,p.keywords,p.description,p.tmstamp,p.deltmstamp, p.user_id FROM poi p WHERE ";
	
			/*find all POIs of his friends*/
			if ( poiCrit.getFriendsList() != null && poiCrit.getEnd_time() == null && poiCrit.getStart_time() == null ){
				query = query + " ( p.poi_id IN  ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(0) +"' and f.userb = pl.user_id )" ;
				for (int i = 1 ; i < poiCrit.getFriendsList().size() ; i ++ ){
					System.out.println("friends = " + poiCrit.getFriendsList().get(i));
					query = query + " or p.poi_id IN ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(i) +"' and f.userb = pl.user_id )" ;
				}
				query = query + ")";
				FLAG = true;
			}
	
			else{
				/*start time, end time*/
				if ( poiCrit.getStart_time() != null && poiCrit.getEnd_time() != null ){
					if (FLAG == true){
						query = query + " and ";
					}
					FLAG = true;
					query = query + " ( p.poi_id IN  ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(0) + "' and f.userb = pl.user_id and pl.tmstamp >= '" + poiCrit.getStart_time() + "' and pl.tmstamp <= '" + poiCrit.getEnd_time() + "')";
					for (int i = 1 ; i < poiCrit.getFriendsList().size() ; i ++ ){
						System.out.println("friends = " + poiCrit.getFriendsList().get(i));
						query = query + " or p.poi_id IN ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(i) +"' and f.userb = pl.user_id and pl.tmstamp >= '" + poiCrit.getStart_time() + "' and pl.tmstamp <= '" + poiCrit.getEnd_time() + "')";
					}
					query = query + ")";
				}
				else if ( poiCrit.getStart_time() != null ){
					if (FLAG == true){
						query = query + " and ";
					}
					FLAG = true;
					query = query + " ( p.poi_id IN  ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(0) + "' and f.userb = pl.user_id and pl.tmstamp >= '" + poiCrit.getStart_time() + "')";
					for (int i = 1 ; i < poiCrit.getFriendsList().size() ; i ++ ){
						System.out.println("friends = " + poiCrit.getFriendsList().get(i));
						query = query + " or p.poi_id IN ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(i) +"' and f.userb = pl.user_id and pl.tmstamp >= '" + poiCrit.getStart_time() + "')";
					}
					query = query + ")";
				}
				else if ( poiCrit.getEnd_time() != null ){
					if (FLAG == true){
						query = query + " and ";
					}
					FLAG = true;
					query = query + " ( p.poi_id IN  ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(0) + "' and f.userb = pl.user_id and pl.tmstamp <= '" + poiCrit.getEnd_time() + "') ";
					for (int i = 1 ; i < poiCrit.getFriendsList().size() ; i ++ ){
						System.out.println("friends = " + poiCrit.getFriendsList().get(i));
						query = query + " or p.poi_id IN ( SELECT pl.poi_id FROM poi_list pl , friends f WHERE f.usera = '" + poiCrit.getUserId() + "' and f.userb = '" + poiCrit.getFriendsList().get(i) +"' and f.userb = pl.user_id and pl.tmstamp <= '" + poiCrit.getEnd_time() + "')" ;
					}
					query = query + ")";
				}
			}
	
			/*rectangle*/
			if ( poiCrit.getX1Region() != -1 && poiCrit.getY1Region() != -1 && poiCrit.getX2Region() != -1 && poiCrit.getY2Region() != -1 ){
				if (FLAG == true){
					query = query + " and ";
				}
				FLAG = true;
				query = query +"  ST_Contains(ST_SetSRID(ST_MakeBox2D('Point(" + poiCrit.getX1Region()+ " " + poiCrit.getY1Region() + ")','Point(" + poiCrit.getX2Region() + " " + poiCrit.getY2Region() + ")'),4326),p.geo) ";
			}
	
			/*keywords*/
			if ( poiCrit.getKeywordsList() != null ){
	
				if (FLAG == true){
					query = query + " and ";
				}
				query = query + "p.keywords LIKE '%" + poiCrit.getKeywordsList().get(0) + "%'";
				for ( int i = 1 ; i < poiCrit.getKeywordsList().size() ; i ++ ){
					query = query +  " AND p.keywords LIKE '%" + poiCrit.getKeywordsList().get(i) + "%'";
				}
				FLAG = true;
			}
	
	
			/*Order by*/
			if (poiCrit.getOrderBy() != null ){
				if (FLAG == true){
					query = query + " ORDER BY " + poiCrit.getOrderBy() + " DESC";
				}
				FLAG = true;
			}
	
			/*number of resutls*/
			if (poiCrit.getNoOfResults() != -1 ){
				if (FLAG == true){
					query = query + " LIMIT " + poiCrit.getNoOfResults();
				}
				FLAG = true;
			}
		}
	
		query = query + ";";
	
		System.out.println("query = " + query);
	
		rs = st.executeQuery(query);
		if (rs.next()){
			poi_id = interest = rs.getInt(1);
			name = rs.getString(2);
			x = rs.getDouble(3);
			y = rs.getDouble(4);
			hotness = rs.getInt(5);
			publicity = rs.getBoolean(6);
			interest = rs.getInt(7);
			keywords = rs.getString(8);
			description = rs.getString(9);
			tmstamp = rs.getTimestamp(10);
			deltmstamp = rs.getTimestamp(11);
			user_id = rs.getInt(12);
			if (deltmstamp == null){ 
				if (publicity == false ){
					if ( user_id == poiCrit.getUserId()){
						listOfPOIs = new ArrayList<PoiCharacteristics>();
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
						listOfPOIs.add(poi);
						FLAGLIST = true;
					}
					else {
						st = con.createStatement();
						query = "SELECT userb FROM friends WHERE usera = " + poiCrit.getUserId() + " AND userb = " + user_id + ";";
						rs2 = st.executeQuery(query);
						if (rs2.next()){
							listOfPOIs = new ArrayList<PoiCharacteristics>();
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
							listOfPOIs.add(poi);
							FLAGLIST = true;
						}
					}
					
				}
				else{
					listOfPOIs = new ArrayList<PoiCharacteristics>();
					if ( user_id == poiCrit.getUserId()){
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
					}
					else{
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
					}
					listOfPOIs.add(poi);
					FLAGLIST = true;
				}
			 }

			while(rs.next()){
				poi_id = rs.getInt(1);
				name = rs.getString(2);
				x = rs.getDouble(3);
				y = rs.getDouble(4);
				hotness = rs.getInt(5);
				publicity = rs.getBoolean(6);
				interest = rs.getInt(7);
				keywords = rs.getString(8);
				description = rs.getString(9);
				tmstamp = rs.getTimestamp(10);
				deltmstamp = rs.getTimestamp(11);
				user_id = rs.getInt(12);
				if (deltmstamp == null){
					if (publicity == false ){
						if ( user_id == poiCrit.getUserId()){
							if (FLAGLIST == true){
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
								listOfPOIs.add(poi);
							}
							else{
								listOfPOIs = new ArrayList<PoiCharacteristics>();
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
								listOfPOIs.add(poi);
								FLAGLIST = true;
							}
						}
						else{
							st = con.createStatement();
							query = "SELECT userb FROM friends WHERE usera = " + poiCrit.getUserId() + " AND userb = " + user_id + ";";
							rs2 = st.executeQuery(query);
							if (rs2.next()){
								if ( FLAGLIST == true){
									poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
									listOfPOIs.add(poi);
								}
								else{
									listOfPOIs = new ArrayList<PoiCharacteristics>();
									poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
									listOfPOIs.add(poi);
									FLAGLIST = true;
								}
							}
						}
					}
					else{
						if (FLAGLIST == true){
							if ( user_id == poiCrit.getUserId()){
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
							}
							else{
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
							}
							listOfPOIs.add(poi);
						}
						else{
							listOfPOIs = new ArrayList<PoiCharacteristics>();
							if ( user_id == poiCrit.getUserId()){
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
							}
							else{
								poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
							}
							listOfPOIs.add(poi);
							FLAGLIST = true;
						}
					}
				}
			}
		}
		else{
			return null;
		}
	
		return listOfPOIs;
	}
	
	
	public ArrayList<PoiCharacteristics> filterUnique(Connection con,ArrayList<PoiCharacteristics> POIList) throws SQLException{
		int i,j;
		PoiCharacteristics poi;
		ArrayList <PoiCharacteristics> PoiNotExistList = null;
		String del = ",";
		String[] temp;
		boolean FLAG = false;  
	
		st = con.createStatement();
		for ( i = 0 ; i < POIList.size() ; i ++ ){
			poi = POIList.get(i);
			query = "SELECT * FROM poi p WHERE geo = GeomFromText('point(" + poi.getX() + " " + poi.getY() + ")',4326)";
			if ( poi.getKeywords() != null ){
				temp = poi.getKeywords().split(del);
				query = query + " and  p.poi_id in (SELECT pl.poi_id FROM poi_other_characteristics pl WHERE pl.property_name = '" + temp[0] +"'" ;
				for ( j = 1 ; j < temp.length ; j ++ ){
					query = query + " and pl.poi_id in( SELECT pl.poi_id FROM poi_other_characteristics pl WHERE pl.property_name = '" + temp[j] + "')";
				}
				query = query + ")";
			}      
			query = query + ";";
			System.out.println("query = " + query);
			rs = st.executeQuery(query);
			if(rs.next()){
	
			}
			else{
				if (FLAG == false){
					FLAG = true;
					PoiNotExistList = new ArrayList<PoiCharacteristics>();
				}
				PoiNotExistList.add(poi);
			}          
		}
	
		return PoiNotExistList;
	
	}
	
	
	public ArrayList<PoiCharacteristics> getTrendingEvents(Connection con, double xCenter, double yCenter,double x1, double y1, double x2, double y2) throws SQLException{
		ArrayList <PoiCharacteristics> eventPois = null;
		Boolean FLAG = false;
		PoiCharacteristics poi ;
		String name;
		double x = 0,y = 0;
		int interest;
		int hotness;
		int poi_id= -1;
		boolean publicity;
		String keywords = null;
		String description = null;
		Timestamp tmstamp = null;
		Timestamp deltmstamp = null;
		
		
		st = con.createStatement();
		query = "SELECT p.poi_id,p.name,ST_X(geo) AS lon, ST_Y(geo) AS lat, p.interest, p.hotness, p.publicity, p.keywords, p.description, p.tmstamp, p.deltmstamp FROM poi p where p.deltmstamp is Null and user_id = -1 and ST_Contains(ST_SetSRID(ST_MakeBox2D('Point(" + x1 + " " + y1 + ")','Point(" + x2 + " " + y2 + ")'),4326),p.geo) ";
		
		System.out.println("query = " + query);
		rs = st.executeQuery(query);
		
		while(rs.next()){
			poi_id = rs.getInt(1);
			name = rs.getString(2);
			x = rs.getDouble(3);
			y = rs.getDouble(4);
			interest = rs.getInt(5);
			hotness = rs.getInt(6);
			publicity = rs.getBoolean(7);
			keywords = rs.getString(8);
			description = rs.getString(9);
			tmstamp = rs.getTimestamp(10);
			deltmstamp = rs.getTimestamp(11);
			if (FLAG == false){
				FLAG = true;
				eventPois = new ArrayList<PoiCharacteristics>();
				poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
				eventPois.add(poi);
			}
			else{
				poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
				eventPois.add(poi);
			}
		}
		
		return eventPois;
	}
	
	
	public ArrayList<PoiCharacteristics> getNN(Connection con,PoiCharacteristics centralPOI, int k) throws SQLException{
		ArrayList <PoiCharacteristics> neighboorhoodPOIs = null;
		PoiCharacteristics poi ;
		Boolean FLAG = false;
		String name;
		double x = 0,y = 0;
		int interest;
		int hotness;
		int poi_id= -1;
		boolean publicity;
		String keywords = null;
		String description = null;
		Timestamp tmstamp = null;
		Timestamp deltmstamp = null;
		int user_id = -1;
		
		st = con.createStatement();
		query = "SELECT p.poi_id,p.name,ST_X(geo) AS lon, ST_Y(geo) AS lat, p.interest, p.hotness, p.publicity, p.keywords, p.description, p.tmstamp, p.deltmstamp, p.user_id,ST_Distance_Sphere(p.geo,GeomFromText('POINT(" + centralPOI.getX() +" " + centralPOI.getY() + ")',4326)) FROM poi p where p.deltmstamp is Null and (ST_Distance_Sphere(p.geo,GeomFromText('POINT(" + centralPOI.getX() +" " + centralPOI.getY() + ")',4326)) <= 20000 ) ORDER BY ST_Distance_Sphere(p.geo,GeomFromText('POINT(" + centralPOI.getX() +" " + centralPOI.getY() + ")',4326)) LIMIT " + k ;
		
		System.out.println("query = " + query);
		rs = st.executeQuery(query);
	
		while(rs.next()){
			poi_id = rs.getInt(1);
			name = rs.getString(2);
			x = rs.getDouble(3);
			y = rs.getDouble(4);
			interest = rs.getInt(5);
			hotness = rs.getInt(6);
			publicity = rs.getBoolean(7);
			keywords = rs.getString(8);
			description = rs.getString(9);
			tmstamp = rs.getTimestamp(10);
			deltmstamp = rs.getTimestamp(11);
			user_id = rs.getInt(12);
			if (publicity == false ){
				if ( user_id == centralPOI.getUserId()){
					if (FLAG == false){
						FLAG = true;
						neighboorhoodPOIs = new ArrayList<PoiCharacteristics>();
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
						neighboorhoodPOIs.add(poi);
					}
					else{
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
						neighboorhoodPOIs.add(poi);
					}
				}
				else{
					st = con.createStatement();
					query = "SELECT userb FROM friends WHERE usera = " + centralPOI.getUserId() + " AND userb = " + user_id + ";";
					rs2 = st.executeQuery(query);
					if (rs2.next()){
						if ( FLAG == true){
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
							neighboorhoodPOIs.add(poi);
						}
						else{
							neighboorhoodPOIs = new ArrayList<PoiCharacteristics>();
							poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
							neighboorhoodPOIs.add(poi);
							FLAG = true;
						}
					}
				}
			}
			else{
				if (FLAG == false){
					FLAG = true;
					neighboorhoodPOIs = new ArrayList<PoiCharacteristics>();
					if ( user_id == centralPOI.getUserId()){
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
					}
					else{
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
					}
					neighboorhoodPOIs.add(poi);
				}
				else{
					if ( user_id == centralPOI.getUserId()){
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,true);
					}
					else{
						poi = new PoiCharacteristics(poi_id,-1,name,x,y,interest,hotness,publicity,keywords,description,tmstamp,false);
					}
					neighboorhoodPOIs.add(poi);
				}
			}
		}
	
		return neighboorhoodPOIs;
	}
	
	
	public boolean updatePOIInterest(Connection con,PoiCharacteristics poi) throws SQLException{
	
		if ( poi == null){
		//kanoume update ta simeia olis tis vasis
		}
		else{
			st = con.createStatement();
			query = "UPDATE poi SET interest = '" + poi.getInterest() + "' WHERE poi_id = " + poi.getPoiId();
			System.out.println("query = " + query);
			st.executeUpdate(query);
		}
	
	
		return true;
	}
	
	
	public boolean updatePOI(Connection con,int user_id, PoiCharacteristics poi) throws SQLException{
		boolean FLAG = false;
		int result = 0;
		boolean publicity = false;
		st = con.createStatement();
	
		if ( poi != null ){
			if ( user_id == -1){
				query = "UPDATE poi SET ";
				if ( poi.getName() != null ){
					FLAG = true;
					query = query + "name = '" + poi.getName() + "'";
				}
				if ( poi.getKeywords() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "keywords = '" + poi.getKeywords() + "'";
					FLAG = true;
				}
				if ( poi.getDescription() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "description = '" + poi.getDescription() + "'";
					FLAG = true;
				}
				query =  query + " WHERE poi_id = " + poi.getPoiId();
			}
			else if( user_id == -2){ 
				query = "UPDATE poi SET ";
				if ( poi.getKeywords() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "keywords = '" + poi.getKeywords() + "'";
					FLAG = true;
				}
				if ( poi.getDescription() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "description = '" + poi.getDescription() + "'";
					FLAG = true;
				}
				query =  query + " WHERE poi_id = " + poi.getPoiId();
			}
			else{
				query = "SELECT publicity FROM poi WHERE poi_id = " + poi.getPoiId();
				rs = st.executeQuery(query);
				if (rs.next()){
					publicity = rs.getBoolean(1);
				}
		
				query = "UPDATE poi SET ";
				if ( poi.getName() != null ){
					FLAG = true;
					query = query + "name = '" + poi.getName() + "'";
				}
				/*if ( poi.getInterest() != -1 ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "interest = '" + poi.getInterest() + "'";
					FLAG = true;
				}
				if ( poi.getHotness() != -1 ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "hotness = '" + poi.getHotness() + "'";
					FLAG = true;
				}*/
				if ( poi.getPublicity() != true ){
					publicity = false;
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "publicity = '" + poi.getPublicity() + "'";
					FLAG = true;
				}
				else{
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "publicity = '" + poi.getPublicity() + "'";
					FLAG = true;
				}
				if ( poi.getKeywords() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "keywords = '" + poi.getKeywords() + "'";
					FLAG = true;
				}
				if ( poi.getDescription() != null ){
					if ( FLAG == true){
						query = query + ",";
					}
					query = query + "description = '" + poi.getDescription() + "'";
					FLAG = true;
				}
		
				if( publicity == true){
					query =  query + " WHERE poi_id = " + poi.getPoiId();
				}
				else{
					query =  query + " WHERE poi_id = " + poi.getPoiId() + " AND user_id = " + user_id + "";
				}
			}
			
			System.out.println("query = " + query);
			st = con.createStatement();
			result = st.executeUpdate(query);
			if ( result == 0){
				return false;
			}
			else{
				return true;
			}
		}
		else{
			return false;
		}
	}
	
	public boolean deletePOI(Connection con, int user_id, int poi_id) throws SQLException{
		java.util.Date tmstamp= new java.util.Date();
		int result = 0;
		if ( poi_id != -1 ){
			st = con.createStatement();
			query = "UPDATE poi SET deltmstamp = '" + tmstamp +"' WHERE poi_id = " + poi_id + " AND user_id = '" + user_id + "'"; 
			System.out.println("query = " + query);
			result = st.executeUpdate(query);
			if ( result == 0 ){
				return false;
			}
			else{
				return true;
			}
		}
		else{
			return false;
		}
	}

        public PoiCharacteristics getPOI(Connection con, int poi_id) throws SQLException {
            st = con.createStatement();
            query = "SELECT * FROM poi WHERE poi_id = "+poi_id;
            System.out.println("Query = "+query);
            ResultSet result = st.executeQuery(query);
            result.next();
            
            
            PoiCharacteristics returnPOI = new PoiCharacteristics();
            returnPOI.setPoiId(result.getInt("poi_id"));
            returnPOI.setHotness(result.getInt("hotness"));
            returnPOI.setInterest(result.getInt("interest"));
            returnPOI.setName(result.getString("name"));
            returnPOI.setPictureURL(result.getString("picture_url"));
            return returnPOI;
        }

/////////////////////////////////////////Sem Trajectories Functions////////////////////////////////////////////////////


	public Boolean addNewVisit(Connection con,TrajectoryCharacteristics trajChar) throws SQLException{
		String query1 = null;
		String query2 = null;
		int sumOfRows = -1;
		int result = -1;
		int sequenceNumber= -1;
		int counter = -1;
		int nextSeqNumber = -1;
		
		st = con.createStatement();
		query = "SELECT count(*) FROM sem_trajectory WHERE user_id = " + trajChar.getUserId() + " and date = '" + trajChar.getDate() + "';";
		rs = st.executeQuery(query);
		if(rs.next()){
			sumOfRows = rs.getInt(1);
		}
		
		if ( sumOfRows != 0){
			counter = sumOfRows;
			query = "SELECT seq_number FROM sem_trajectory WHERE user_id = '" + trajChar.getUserId() + "' AND date = '" + trajChar.getDate() + "' ORDER BY seq_number DESC" ;
			rs = st.executeQuery(query);
			
			while(rs.next()){
				if ( rs.getInt(1) == trajChar.getSeq_number()){
					if ( counter != 1){
						rs.next();
						nextSeqNumber = rs.getInt(1);
					}
					break;
				}
				counter --;
			}
			
			if ( counter == 1 ){
				sequenceNumber = trajChar.getSeq_number()/2;
			}
			else if ( counter > 0 && counter <= sumOfRows){
				sequenceNumber = (trajChar.getSeq_number() + nextSeqNumber) /2;
			}
			else{
				query = "SELECT MAX(seq_number) FROM sem_trajectory WHERE user_id = " + trajChar.getUserId();
				rs = st.executeQuery(query);
				if (rs.next()){
					sequenceNumber = rs.getInt(1) + 1000;
				}
			}
		}
		
		else { 
			sequenceNumber = 1000;
		}
			
		query1 = "INSERT INTO sem_trajectory (user_id,date,seq_number,poi_id,public";
		query2 = " VALUES('" + trajChar.getUserId() + "','" + trajChar.getDate() + "','" + sequenceNumber + "','" + trajChar.getPoiId() + "','" + trajChar.getPublicity() + "'";
			
		if ( trajChar.getArrived() != null ){
			query1 = query1 + ",arrived";
			query2 = query2 + ",'" + trajChar.getArrived() + "'";
		}
		if( trajChar.getOff() != null ){
			query1 = query1 + ",off";
			query2 = query2 + ",'" + trajChar.getOff() + "'";
		}
		if ( trajChar.getComment() != null ){
			query1 = query1 + ",comment";
			query2 = query2 + ",'" + trajChar.getComment() + "'";
		}
			
		query = query1 + ")" + query2 + ")";
		result = st.executeUpdate(query);
		if ( result == 0 ){
			return false;
		}
		return true;
	}
	
	
	public boolean addSemTrajectory(){
		boolean result = false;
	
	
		return result;
	}
	
	
	public boolean updateSemTrajectory(){
		boolean result = false;
	
	
		return result;
	}
	
	
	public boolean refreshPOIs( Connection con, ArrayList<DBScanCharacteristics> poiList ,int r ) throws SQLException{
		DBScanCharacteristics dbscanChar = null;
		boolean FLAG = false;
		ArrayList<PoiCharacteristics> listOfPOIs = null;
		PoiCharacteristics poiChar = null;
		double x,y;
		
		
		for ( int i = 0 ; i < poiList.size() ; i ++ ){
			dbscanChar = poiList.get(i);
			st = con.createStatement();                                        
		    query = "INSERT INTO temp_poi (geo) VALUES (GeomFromText('point(" + dbscanChar.getX() + " " + dbscanChar.getY() + ")',4326))" ;
		    System.out.println(query);
		    st.executeUpdate(query);
		}
		
		st = con.createStatement();                                        
	    query = "SELECT ST_X(geo),ST_Y(geo) FROM temp_poi tp EXCEPT SELECT DISTINCT ST_X(tp1.geo),ST_Y(tp1.geo) FROM temp_poi tp1, poi p1 WHERE (ST_Distance_Sphere(tp1.geo,p1.geo) <= " + r + ")" ;
	    System.out.println(query);
	    rs = st.executeQuery(query);
	    while (rs.next()){
	    	x = rs.getDouble(1);
	    	y = rs.getDouble(2);
	    	poiChar = new PoiCharacteristics(-1,-1,null,x,y,-1,-1,true,null,null,null,false);				
	    	if (FLAG == false){
	    		listOfPOIs =  new ArrayList<PoiCharacteristics>();
	    		FLAG = true;
	    	}
	    	listOfPOIs.add(poiChar);
	    }
	    
	    if (listOfPOIs != null){
	    	addNewPOIsList(con,listOfPOIs);
	    }
	    
	    st = con.createStatement();                                        
	    query = "DELETE FROM temp_poi" ;
	    System.out.println(query);
	    st.executeUpdate(query);

	    
		return true;
	}
	
	
	public PoiCharacteristicsTraj findPOI(PoiCharacteristicsTraj poiChar, int r, int user_id) throws SQLException{
		String str = null;
        PoiCharacteristicsTraj poiReturn = null;
        boolean publicity = false;
        
        
        st = con.createStatement();
        query = "SELECT p.poi_id,p.publicity,p.user_id FROM poi p WHERE p.deltmstamp is Null and (ST_Distance_Sphere(p.geo,GeomFromText('POINT(" + poiChar.getX() +" " + poiChar.getY() + ")',4326)) <= " + r +" ) LIMIT 1" ;
        rs = st.executeQuery(query);
        if ( rs.next()){
            publicity = rs.getBoolean(2);
            if ( publicity == false) {
                if ( user_id == rs.getInt(3)){
                    poiReturn = new PoiCharacteristicsTraj();
                    poiReturn.setPoi_id(rs.getInt(1));
                    poiReturn.setPublicity(publicity);
               }
            }
            else{
                poiReturn = new PoiCharacteristicsTraj();
                poiReturn.setPoi_id(rs.getInt(1));
                poiReturn.setPublicity(rs.getBoolean(2));
           
            }
        }
        return poiReturn;
    }
    
	
    public boolean addTraj(PoiCharacteristicsTraj poiChar, int user_id ,Date date,int seq_number) throws SQLException{
        boolean FLAG = false;
        
        st = con.createStatement();
        if ( poiChar.getOff() != null ){
            query = "INSERT INTO sem_trajectory(user_id,date,seq_number,poi_id,arrived,off,public) VALUES('" + user_id + "','" + date + "','" + seq_number + "','" + poiChar.getPoi_id() + "','" + poiChar.getArrived() + "','" + poiChar.getOff() + "','" +poiChar.getPublicity() + "')";
        }
        else{
            query = "INSERT INTO sem_trajectory(user_id,date,seq_number,poi_id,arrived,public) VALUES('" + user_id + "','" + date + "','" + seq_number + "','" + poiChar.getPoi_id() + "','" + poiChar.getArrived() + "','" + poiChar.getPublicity() + "')";
        }
        st.executeUpdate(query);//insert
        
        return FLAG;
    }
}

