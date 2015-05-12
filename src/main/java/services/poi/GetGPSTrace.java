/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package services.poi;

import dataBases.hBase.GPSTrajCharacteristics;
import dataBases.hBase.HBaseFunctions;
import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;
import javax.servlet.http.HttpServlet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author jimakos
 */
@WebServlet("/GetGPSTrace")
public class GetGPSTrace extends HttpServlet  {
    private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetGPSTrace() {
        super();
        this.description = new HTMLDescription("Get Gps Trace");
        this.description.addParameter("List Of POI objects", "POIList");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service used to get a list of traces");
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} 
		else {
                       ArrayList<GPSTrajCharacteristics> GPSresults = new ArrayList<GPSTrajCharacteristics>();
			String msgResponse = null;
                        HBaseFunctions hBase = new HBaseFunctions();
                        
                        String token = request.getParameter("token");
                        String date = request.getParameter("date");
                        String msgChoice = request.getParameter("format");
                        
                        int user_id = -1;
			PersistentHashMapClient user = new PersistentHashMapClient();
			user_id = user.getUserId(token);
                        
                        //ser_id = 2581;
                        //date = "2014-10-17";
                        
                        
                        hBase.Configuration();
                        
                        if ( date.equals("")){
                            date = null;
                        }
                        
                        int choice = 0;
                        
                        if (date == null){
                            try {
                                ///////hbase code///////
                                
                                //GPSresults = hBase.getWithCompositeKey("GPSIdTmstmp", user_id +"",choice);
                                GPSresults = hBase.getWithCompositeKey("testGPS", user_id +"",choice);
                            } catch (ParseException ex) {
                                Logger.getLogger(GetGPSTrace.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        else{
                            try {
                                //GPSresults = hBase.getOneRecord("GPSIdTmstmp",user_id + "_" + date,choice);
                                GPSresults = hBase.getOneRecord("testGPS",user_id + "_" + date,choice);
                            } catch (ParseException ex) {
                                Logger.getLogger(GetGPSTrace.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                        
                        
                        
                        if (GPSresults != null) {
                            if (msgChoice.equals("json")) {
                                msgResponse = "{\"poiList\":[" + GPSresults.get(0).toJson();
                                for (int i = 1; i < GPSresults.size(); i++) {
                                    msgResponse = msgResponse + "," + GPSresults.get(i).toJson();
                                }
                                msgResponse = msgResponse + "]}";
                            } else {
                                String callback = request.getParameter("callback");
                                msgResponse = callback + "({\"poiList\":[" + GPSresults.get(0).toJson();
                                for (int i = 1; i < GPSresults.size(); i++) {
                                    msgResponse = msgResponse + "," + GPSresults.get(i).toJson();
                                }
                                msgResponse = msgResponse + "]})";
                            }
                        } else {
                            if (msgChoice.equals("json")) {
                                msgResponse = "{\"poiList\":[]}";
                            } else {
                                String callback = request.getParameter("callback");
                                msgResponse = callback + "({\"poiList\":[]})";

                            }

                        }
                        
                        
			response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-Type", "application/json");
			response.setHeader("charset", "utf-8");
			
			PrintWriter out = response.getWriter();
			out.write(msgResponse);
			
			/*response.setContentType("application/json;charset=UTF-8");
			response.getOutputStream().print(msgResponse);*/
		}
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}
}
