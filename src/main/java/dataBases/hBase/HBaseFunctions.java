package dataBases.hBase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseFunctions {
	private static Configuration config = null;
	
	
	public void HBaseFunctions(){
		
	}
	
	/*Configuration*/
	public static void Configuration(){
            config = HBaseConfiguration.create();
            //config.set("hbase.nameserver.address","192.168.0.7");
            config.set("hbase.zookeeper.quorum", "192.168.0.7");
            config.set("hbase.zookeeper.property.clientPort","2222");
            config.set("hbase.master", "192.168.0.7:60000");       
	}
	
	/*Create a new Table in Hbase*/
	public static void createTable(String tableName, String []columnsNames) throws IOException{
		HBaseAdmin hAdmin = null;
		HColumnDescriptor[] descColumns = new HColumnDescriptor[columnsNames.length];
		
		hAdmin = new HBaseAdmin(config);
		if (hAdmin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} 
		else {
			/*Put name on the table and the columns*/
			HTableDescriptor desc = new HTableDescriptor(tableName);
			for ( int i = 0 ; i < columnsNames.length ; i ++ ){
				descColumns[i] = new HColumnDescriptor(columnsNames[i].getBytes());
			}
			for ( int i = 0 ; i < columnsNames.length ; i ++ ){
				desc.addFamily(descColumns[i]);
			}

			/*put the table in the Hbase*/
			hAdmin.createTable(desc);

			System.out.println("Table : " + tableName + " created" );
		}
	}
	
	
	/*Delete table*/
	public static void deleteTable(String tableName) throws IOException{
		HBaseAdmin hAdmin = new HBaseAdmin(config);
		hAdmin.disableTable(tableName);
		hAdmin.deleteTable(tableName);
		System.out.println("Table : " + tableName + " deleted" );
	}
	
	
	public static void insertIdTmstmpRecords(String tableName,String columnName,ArrayList<GPSTrajCharacteristics> list,String key, int choice) throws IOException, ParseException{
		int i;
		GPSTrajCharacteristics gpsTraj;
		HTable table = null;
		int counter = 1;
		
		
		if ( !list.isEmpty() ){
			table = new HTable(config, tableName);
		
			for (i = 0 ; i < list.size() ; i ++ ){
				gpsTraj = list.get(i);
				Get g = new Get(Bytes.toBytes(gpsTraj.getUser_id() + gpsTraj.getDate() ));
				Result r = table.get(g);
				if ( !r.isEmpty()){
					int numberOfColumns = r.size();
					counter = numberOfColumns/3 + 1;	
				}
				else{
					counter = 1;
				}
                                
                                CompositeKey ckey = new CompositeKey(choice);
                                ckey.getBytes(key);
                                
				byte[] keyByte = ckey.get(null);
			
				//insert name of row
				Put p = new Put(keyByte);
				
				//add data : first:name of column , second : id of column , third : value of column 
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("x_" + gpsTraj.getTimestamp()),Bytes.toBytes(gpsTraj.getLat()));
	
				//put data in the table
				table.put(p);
				
				//add data : first:name of column , second : id of column , third : value of column 
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("y_" + gpsTraj.getTimestamp()),Bytes.toBytes(gpsTraj.getLon()));
	
				//put data in the table
				table.put(p);
				
				//add data : first:name of column , second : id of column , third : value of column 
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                java.util.Date parsedDate = dateFormat.parse(gpsTraj.getTimestamp());
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("timestamp_" + gpsTraj.getTimestamp()),Bytes.toBytes(parsedDate.getTime()));
	
				//put data in the table//
				table.put(p);
				//counter ++;
			}
			table.close();
		}
		
	}
        
        public static void insertTmstmpIdRecords(String tableName,String columnName,ArrayList<GPSTrajCharacteristics> list,String key, int choice) throws IOException, ParseException{
		int i;
		GPSTrajCharacteristics gpsTraj;
		HTable table = null;
		int counter = 1;
		
		
		if ( !list.isEmpty() ){
			table = new HTable(config, tableName);
		
			for (i = 0 ; i < list.size() ; i ++ ){
				gpsTraj = list.get(i);
				Get g = new Get(Bytes.toBytes(gpsTraj.getDate() + gpsTraj.getUser_id() ));
				Result r = table.get(g);
				if ( !r.isEmpty()){
					int numberOfColumns = r.size();
					counter = numberOfColumns/3 + 1;	
				}
				else{
					counter = 1;
				}
                                
                                CompositeKey ckey = new CompositeKey(choice);
                                ckey.getBytes(key);
                                
				byte[] keyByte = ckey.get(null);
			
				//insert name of row
				Put p = new Put(keyByte);
				
				//add data : first:name of column , second : id of column , third : value of column 
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("x_" + gpsTraj.getTimestamp()),Bytes.toBytes(gpsTraj.getLat()));
	
				//put data in the table
				table.put(p);
				
				//add data : first:name of column , second : id of column , third : value of column 
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("y_" + gpsTraj.getTimestamp()),Bytes.toBytes(gpsTraj.getLon()));
	
				//put data in the table
				table.put(p);
				
				//add data : first:name of column , second : id of column , third : value of column 
                                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                                java.util.Date parsedDate = dateFormat.parse(gpsTraj.getTimestamp());
				p.add(Bytes.toBytes(columnName), Bytes.toBytes("timestamp_" + gpsTraj.getTimestamp()),Bytes.toBytes(parsedDate.getTime()));
	
				//put data in the table//
				table.put(p);
				//counter ++;
			}
			table.close();
		}
		
	}
        
	
	/*Delete record*/
	public static void deleteRecord(String tableName, String record) throws IOException{
		HTable table = new HTable(config, tableName);
		Delete delRecord = new Delete(Bytes.toBytes(record));
		table.delete(delRecord);
		System.out.println("Record deleted");
	}
	
	
	public  ArrayList<GPSTrajCharacteristics> getWithCompositeKey(String tableName, String row, int choice) throws IOException, ParseException{
		ArrayList<GPSTrajCharacteristics> list = new ArrayList<GPSTrajCharacteristics>();
		HTable table = null;
                String str;
                byte[] startBuffer = null;
                byte[] stopBuffer = null;
                ByteBuffer buffer = null;
                GPSTrajCharacteristics gpsTraces = null;
                String []temp;
		String del = "_";
		int user_id;
		String value = null;
                int counter1 = 0;
                int counter2 = 0;
                boolean FLAG = false;
                boolean FLAG2 = false;
                
		
		/*Connect to the table*/
		table = new HTable(config, tableName);
                CompositeKey ckey = new CompositeKey(choice);
                
                ckey.getBytes(row);
                startBuffer = ckey.get(Long.parseLong("0"));
                stopBuffer = ckey.get(Long.MAX_VALUE);
                Scan scan = new Scan(startBuffer,stopBuffer); //creating a scan object with start and stop row keys

                //scan.setFilter(columnName);//set the Column filters you have to this scan object.

                //And then you can get a scanner object and iterate through your results
                ResultScanner scanner = table.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next())
                {
                    FLAG2 = true;
                    counter1 = counter2 ;
                    FLAG =false; 
                    for(KeyValue kv : result.raw()){
                        String qualifier = new String(kv.getQualifier());
                        buffer = ByteBuffer.wrap(kv.getValue());

                        if ( qualifier.contains("x")){
                            list.get(counter2).setLat(buffer.getDouble());
                            counter2 ++;
                        }
                        else if (qualifier.contains("y")){
                            if ( FLAG == false ){
                                counter2 = counter1;
                                FLAG = true;
                            }
                            list.get(counter2).setLon(buffer.getDouble());
                            counter2++;
                        }
                        else{
                            gpsTraces = new GPSTrajCharacteristics();
                            String rowKey = new String(kv.getRow());
                            temp = rowKey.split(del);
                            user_id = Integer.parseInt(temp[0]);
                            Timestamp tm = new Timestamp(buffer.getLong());
                            gpsTraces.setTimestamp(tm.toString());
                            gpsTraces.setUser_id(user_id);
                            list.add(gpsTraces);
                            counter2 = counter1;
                        }
                    }
                }
                if (FLAG2 == false){
                    return null;
                }
                else {
                    return list;
                }
	}
	
	
	public static ArrayList<GPSTrajCharacteristics> getOneRecord( String tableName , String row , int choice) throws IOException, ParseException{
		String valueStr = null;
		HTable table = null;
		ArrayList<GPSTrajCharacteristics> list = new ArrayList<GPSTrajCharacteristics>();
		GPSTrajCharacteristics gpsTraces = null;
		String []temp;
		String del = "_";
		int user_id;
		String value = null;
                int counter1 = 0;
                int counter2 = 0;
                boolean FLAG = false;
                boolean FLAG2 = false;
		
		/*Connect to the table*/
		table = new HTable(config, tableName);
                CompositeKey ckey = new CompositeKey(choice);
                ckey.getBytes(row);
                byte[] keyByte = ckey.get(null);
                ByteBuffer buffer = ByteBuffer.wrap(keyByte);
		Get g = new Get(Bytes.toBytes(buffer));
		Result r = table.get(g);

                counter1 = counter2 ;
                FLAG = false; 
		for(KeyValue kv : r.raw()){
                    FLAG2 = true;
                    String qualifier = new String(kv.getQualifier());
                    buffer = ByteBuffer.wrap(kv.getValue());

                    if ( qualifier.contains("x")){
                        list.get(counter2).setLat(buffer.getDouble());
                        counter2++;
                    }
                    else if (qualifier.contains("y")){
                        if ( FLAG == false ){
                            counter2 = counter1;
                            FLAG = true;
                        }
                        list.get(counter2).setLon(buffer.getDouble());
                        counter2++;
                    }
                    else{
                        gpsTraces = new GPSTrajCharacteristics();
                        ckey.parseBytes(kv.getRow());
                        user_id = ckey.getId();
                        gpsTraces.setUser_id(user_id);
                        Timestamp tm = new Timestamp(buffer.getLong());
                        gpsTraces.setTimestamp(tm.toString());
                        list.add(gpsTraces);
                        counter2 = counter1;
                    }
                }
                
                if(FLAG2 == false){
                    return null;
                }
                else {
                    return list;
                }
	}
	
        /*read all records*/
	public static ArrayList <DBScanCharacteristics> getAllRecords(String tableName, int choice) throws IOException{
		HTable table = null;
		ArrayList <DBScanCharacteristics> dbscanList = new ArrayList <DBScanCharacteristics>();
		DBScanCharacteristics dbscanChar = null;
		/*Connect to the table*/
		table = new HTable(config, tableName);
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		for(Result r:ss){
			int i= 0;
			dbscanChar = new DBScanCharacteristics();
			for(KeyValue kv : r.raw()){
                            CompositeKey ckey = new CompositeKey(choice);
                            ckey.parseBytes(kv.getRow());
                            //System.out.println(ckey.getId());
			/*	if ( i==0 ){
					dbscanChar.setLon(Double.parseDouble(new String(kv.getValue())));
				}
				else{
					dbscanChar.setLat(Double.parseDouble(new String(kv.getValue())));
				}
				i ++ ;*/
			}
			dbscanList.add(dbscanChar);
		}
		return dbscanList;
	}
	
}
