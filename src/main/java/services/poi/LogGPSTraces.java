package services.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import dataBases.hBase.GPSTrajCharacteristics;
import dataBases.hBase.HBaseFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet implementation class LogGPSTraces
 */
@WebServlet("/LogGPSTraces")
public class LogGPSTraces extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public LogGPSTraces() {
        super();
        this.description = new HTMLDescription("Log GPS Traces");
        this.description.addParameter("List Of GPSTraces", "GPSTraces");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service saves a list of GPS traces into repository of traces");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} 
		
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
               
            System.out.println("GPS Log received");
                String token = null;
		double lat = -1;
		double lon = -1;
		String strTmstamp = null;
		int user_id = 0 ;
		String msgResponse = null;
		boolean result = true;
		ArrayList<GPSTrajCharacteristics> list = new ArrayList<GPSTrajCharacteristics>();
		GPSTrajCharacteristics gpsTraj = null;
		StringBuffer jb = new StringBuffer();
		String line = null;
                String key = null;
                int choice = 0;
                String date = null;
                
		try {
			BufferedReader reader = request.getReader();
			while ((line = reader.readLine()) != null){
				jb.append(line);
			}
		} catch (Exception e) {  }
		
		String str = jb.toString();
		
		JSONParser parser = new JSONParser();
		
		Object obj = null;
		try {
			obj = parser.parse(str);
		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		JSONObject jsonObj = (JSONObject)obj;
		JSONArray array = (JSONArray) jsonObj.get("traces");
		
		
		PersistentHashMapClient user = new PersistentHashMapClient();	
		
		for ( int i = 0 ; i < array.size() ; i++ ){
			jsonObj = (JSONObject) array.get(i);
			token= (String) jsonObj.get("token");
			user_id = user.getUserId(token);
                        
			if ( user_id == -1 ){
				list = null;
				result = false;
				break;
			}
			lat = (Double) jsonObj.get("lat");
			lon = (Double) jsonObj.get("lon");
			strTmstamp = (String) jsonObj.get("timestamp");		
			gpsTraj = new GPSTrajCharacteristics(user_id,strTmstamp,lat,lon);
			date = gpsTraj.getDate();
                        list.add(gpsTraj);
                        
		}
			
		if ( list != null){
                    //String tableName = "GPSIdTmstmp";
                    String tableName = "testGPS";
                    String columnName = "info";
                    HBaseFunctions hBase = new HBaseFunctions();
                    hBase.Configuration();
                        
                    key = user_id + "_" +date;                  
                    choice = 0;
                    try {
                        hBase.insertIdTmstmpRecords(tableName, columnName, list,key,choice);
                    } catch (ParseException ex) {
                        Logger.getLogger(LogGPSTraces.class.getName()).log(Level.SEVERE, null, ex);
                    }
                        
                    //tableName = "GPSTmstmpId";
                    tableName = "testGPS2";
                    key = date + "_" +user_id;
                    choice = 1;
                    try {
                        hBase.insertTmstmpIdRecords(tableName, columnName, list,key,choice);
                    } catch (ParseException ex) {
                        Logger.getLogger(LogGPSTraces.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                    result = true;
                         
                }
                
		msgResponse = "{\"result\":\"" + result + "\"}";
		response.setContentType("application/json;charset=UTF-8");
		response.getOutputStream().print(msgResponse);
			
			/*response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-Type", "application/json");
			response.setHeader("charset", "utf-8");
			
			PrintWriter out = response.getWriter();
			out.write(msgResponse);*/
            
	}

}
