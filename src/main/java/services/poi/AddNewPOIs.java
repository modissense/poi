package services.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;

import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class AddNewPOIs
 */
@WebServlet("/AddNewPOIs")
public class AddNewPOIs extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AddNewPOIs() {
        super();
        this.description = new HTMLDescription("Add new POIs");
        this.description.addParameter("List Of POI objects", "POIList");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service used to add a list of POIs");
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
			System.out.println();
			PostgreSQLFunctions postgres = new PostgreSQLFunctions();
			ArrayList<PoiCharacteristics> listOfPOIs = new ArrayList<PoiCharacteristics>();
			Connection con = null;
			PoiCharacteristics resultPoi = null;
			
			String msgResponse = null;
			String queryString = URLDecoder.decode(request.getQueryString(), "UTF-8");
			String name = request.getParameter("name");
			String x = request.getParameter("x");
			String y = request.getParameter("y");
			int interest = -1; 
			int hotness =  -1;
			String keywords = request.getParameter("keywords");
			String pub = request.getParameter("publicity");
			String description = request.getParameter("description");
			String msgChoice = request.getParameter("format");
			String token = (String) request.getParameter("token");
			boolean publicity ;
			
			if (name.contains("'")){
				name = name.replaceAll("'", "''");
			}
			
			if (keywords.equals("")){
				keywords = null;
			}
			else{
				keywords = keywords.toLowerCase();
				keywords = keywords.replaceAll(" ","");
				if (keywords.contains("'")){
					keywords.replace("'", "''");
				}
			}
			
			if (description.equals("")){
				description = null;
			}
			else{
				description = description.toLowerCase();
				if (description.contains("'")){
					description.replace("'", "''");
				}
			}
			
			if (!token.equals("")){
				PersistentHashMapClient user = new PersistentHashMapClient();
				int user_id = user.getUserId(token);
				
				if ( pub.equals("true")){
					publicity = true;
				}
				else{
					publicity = false;
				}
				
				
				PoiCharacteristics poiChar = new PoiCharacteristics(-1,user_id,name,Double.parseDouble(x),Double.parseDouble(y),interest,hotness,publicity,keywords,description,null,false);				
				
				try {
					con = postgres.OpenConnection();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					resultPoi = postgres.addNewPOIs(con, poiChar);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					postgres.CloseConnection(con);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				resultPoi = null;
			}
		
			if (msgChoice.equals("json")){
				if (resultPoi == null ){
					msgResponse = "{\"result\":\"false\"}";
				} 
				else{
					msgResponse = resultPoi.toJson();
				}
				
			}
			else{
				String callback = request.getParameter("callback");
				if (resultPoi == null){
					msgResponse = callback + "({\"result\":\"false\"})";
				}
				else{
					msgResponse = callback + "(" + resultPoi.toJson() + ")";
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
