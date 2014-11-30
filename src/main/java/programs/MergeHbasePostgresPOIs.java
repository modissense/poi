package programs;


import dataBases.hBase.DBScanCharacteristics;
import dataBases.hBase.HBaseFunctions;
import dataBases.postgres.PostgreSQLFunctions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;



public class MergeHbasePostgresPOIs {
	private HBaseFunctions hbase = null;
	private PostgreSQLFunctions postgres = null;
	private String hbaseTableName = "DBScanResults";
	private int r;
	private Connection con = null;
	
	
	public MergeHbasePostgresPOIs (){
	}
	
	public MergeHbasePostgresPOIs (int r){
		this.r = r;
	}
	
	public boolean compute() throws IOException, SQLException{
		boolean result = false;
		ArrayList<DBScanCharacteristics> dbscanList = null;
		HBaseFunctions hbase = new HBaseFunctions();
		PostgreSQLFunctions postgres = new PostgreSQLFunctions();
		
		hbase.Configuration();
		con = postgres.OpenConnection();
		int choice =0;
		dbscanList = hbase.getAllRecords(hbaseTableName,choice);
		result = postgres.refreshPOIs(con,dbscanList,r);
		
		hbase.deleteTable("DBScanResults");
		String []columnNames = new String[]{"points"};		
		hbase.createTable("DBScanResults", columnNames);
		postgres.CloseConnection(con);
		
		return result;
	}
}
