package services.poi;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import dataBases.postgres.TrajectoryCharacteristics;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class AddNewVisit
 */
@WebServlet("/AddNewVisit")
public class AddNewVisit extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public AddNewVisit() {
    	// TODO Auto-generated constructor stub
    	super();
        this.description = new HTMLDescription("Add new Visit");
        this.description.addParameter("integer","poiId");
        this.description.addParameter("integer","seq_num");
        this.description.addParameter("string","token");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service used to add a visit");
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
			boolean result = false;
			String msgResponse = null;
			TrajectoryCharacteristics trajChar;
			
			String poi_id = request.getParameter("poi_id");
			String token = request.getParameter("token");
			String seq_num = request.getParameter("seq_num");
			String dateStr = request.getParameter("date");
			String arrivedStr = request.getParameter("arrived");
			String offStr = request.getParameter("off");
			String comment = request.getParameter("comments");
			String msgChoice = request.getParameter("format");
			String publicity = request.getParameter("public");
			
			Date date = null;
			java.sql.Timestamp arrived = null;
			java.sql.Timestamp off = null;
			
			//response.getOutputStream().print(token);
			
			PersistentHashMapClient user = new PersistentHashMapClient();
			int user_id = user.getUserId(token);
			
			//response.getOutputStream().print(user_id);
			
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	        java.util.Date parsedDate = null;
			
	        if ( !dateStr.equals("")){
				try {
					parsedDate = dateFormat.parse(dateStr);
				} catch (ParseException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
		        date = new java.sql.Date(parsedDate.getTime());
	        }
	        else {
	        	date = null;
	        }
	        
	        
	        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        parsedDate = null;
	        
			if ( !arrivedStr.equals("") ){
				try {
					parsedDate = dateFormat.parse(arrivedStr);
				} catch (ParseException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
		        arrived = new java.sql.Timestamp(parsedDate.getTime());
			}
			else {
				arrived = null;
			}
			
			if ( !offStr.equals("") ){
				try {
					parsedDate = dateFormat.parse(offStr);
				} catch (ParseException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				off = new java.sql.Timestamp(parsedDate.getTime());
			}
			else{
				off = null;
			}
			
			
			if (comment.equals("")){
				comment = null;
			}
			else {
				if (comment.contains("'")){
					comment = comment.replaceAll("'","''");
				}
			}
			
			if(publicity.equals("")){
				 publicity = "true";
			}
			
			
			trajChar = new TrajectoryCharacteristics(user_id,date,Integer.parseInt(seq_num),comment,arrived,off,Boolean.parseBoolean(publicity),Integer.parseInt(poi_id));
			
			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			try {
				result = postgres.addNewVisit(con,trajChar);
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
			
			if (msgChoice.equals("json")){		
				msgResponse = "{\"result\":\"" + result + "\"}";
			}
			else{
				String callback = request.getParameter("callback");
				msgResponse = callback + "({\"result\":\"" + result + "\"})";
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
