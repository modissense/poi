package services.poi;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import dataBases.postgres.TrajectoryCharacteristics;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;

public class ShowTrendingEvents extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ShowTrendingEvents() {
    	// TODO Auto-generated constructor stub
    	super();
        this.description = new HTMLDescription("Show Trending Events");
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
			PostgreSQLFunctions postgres = new PostgreSQLFunctions();
			Connection con = null;
			ArrayList<PoiCharacteristics> listOfPOIs = null;
			boolean result = false;
			
			String msgResponse = null;
			String msgChoice = (String) request.getParameter("format");
			String token = request.getParameter("token");
			String xpos = request.getParameter("xpos");
			String ypos = request.getParameter("ypos");
                        String x1 = request.getParameter("x1");
                        String y1 = request.getParameter("y1");
                        String x2 = request.getParameter("x2");
                        String y2 = request.getParameter("y2");
                        
			int user_id = -1;
			
			PersistentHashMapClient user = new PersistentHashMapClient();
			user_id = user.getUserId(token);
			

			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				listOfPOIs = postgres.getTrendingEvents(con, Double.parseDouble(xpos),Double.parseDouble(ypos),Double.parseDouble(x1),Double.parseDouble(y1),Double.parseDouble(x2),Double.parseDouble(y2));
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			try {
				postgres.CloseConnection(con);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
                        
			if ( listOfPOIs != null ){
				if (msgChoice.equals("json")){
					msgResponse = "{\"poiList\":[" +  listOfPOIs.get(0).toJson();
			        for (int i = 1 ; i < listOfPOIs.size() ; i ++ ){
			            msgResponse = msgResponse + "," + listOfPOIs.get(i).toJson();
			        }
			        msgResponse = msgResponse + "]}";
				}
				else{
					String callback = request.getParameter("callback");
					msgResponse = callback + "({\"poiList\":[" + listOfPOIs.get(0).toJson();
			        for (int i = 1 ; i < listOfPOIs.size() ; i ++ ){
			            msgResponse = msgResponse + "," + listOfPOIs.get(i).toJson();
			        }
			        msgResponse = msgResponse + "]})";
				}
			}
			else{
				if (msgChoice.equals("json")){
					msgResponse = "{\"poiList\":[]}";
				}
				else{
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
