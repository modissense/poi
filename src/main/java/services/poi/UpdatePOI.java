package services.poi;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

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
 * Servlet implementation class UpdatePOI
 */
@WebServlet("/UpdatePOI")
public class UpdatePOI extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public UpdatePOI() {
        super();
        this.description = new HTMLDescription("Update POI");
        this.description.addParameter("POI", "UpdatePOI");
        this.description.addParameter("String", "UserID");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service updates a POI");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		}  
		else {
			PostgreSQLFunctions postgres = new PostgreSQLFunctions();
			String msgResponse = null;
			Connection con = null;
			boolean result = false;
			
			String name = request.getParameter("name");
			String poi_id = request.getParameter("poi_id");
			int interest = -1; 
			int hotness =  -1;
			String pub = request.getParameter("publicity");
			String keywords = request.getParameter("keywords");
			String description = request.getParameter("description");
			String msgChoice = request.getParameter("format");
			String token = request.getParameter("token");
			
			boolean publicity= false;
			int user_id = -1;
			
			
			
			PersistentHashMapClient user = new PersistentHashMapClient();
			user_id = user.getUserId(token);
			if ( name.equals("")){
				name = null;
			}
			else{
				if ( name.contains("'")){
					name = name.replaceAll("'", "''");
				}
			}
			
			if ( pub.equals("true")){
				publicity = true;
			}
			else{
				publicity = false;
			}
			
			if (keywords.equals("")){
				keywords = null;
			}
			else{
				if (keywords.contains("'")){
					keywords = keywords.replaceAll("'", "''");
				}
			}
			
			if (description.equals("")){
				description = null;
			}
			else{
				if (description.contains("'")){
					description = description.replaceAll("'","''");
				}
			}
			
			PoiCharacteristics poiChar = new PoiCharacteristics(Integer.parseInt(poi_id),user_id,name,-1,-1,interest,hotness,publicity,keywords,description,null,false);
			
			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				result = postgres.updatePOI(con, user_id, poiChar);
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
