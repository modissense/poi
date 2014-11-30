package services.poi;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class FindDuplicates
 */
@WebServlet("/FindDuplicates")
public class FindDuplicates extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FindDuplicates() {
        super();
        // TODO Auto-generated constructor stub
        this.description = new HTMLDescription("Find Duplicates of POIs");
        this.description.addParameter("POI", "InPOI");
        this.description.setReturnValue("List of POI objects");
        this.description.setDescription("Web service used to find and return duplicates of a POI");
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
			ArrayList<PoiCharacteristics> listOfPOIs = null;
			Connection con = null;
			
			String x = request.getParameter("lat");
			String y = request.getParameter("lon");
			String msgChoice = request.getParameter("format");
			String token = (String) request.getParameter("token");
			
			PersistentHashMapClient user = new PersistentHashMapClient();
			int user_id = user.getUserId(token);
			
			String msgResponse = null;
			int r = 100;
			
			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			try {
				listOfPOIs = postgres.findDuplicates(con, Double.parseDouble(x), Double.parseDouble(y),r,user_id);
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
					msgResponse = "{\"poiList\":[" + listOfPOIs.get(0).toJson();
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
