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

@WebServlet("/DeletePOI")
public class DeletePOI extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DeletePOI() {
        super();
        this.description = new HTMLDescription("DeletePOI");
        this.description.addParameter("POIobject","POI");
        this.description.setReturnValue("true/false");
        this.description.setDescription("Delete a poi");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} 
		else{
			PostgreSQLFunctions postgres = new PostgreSQLFunctions();
			Connection con = null;
			boolean result = false;
			String msgResponse = null;
			
		
			String poi_id = request.getParameter("poi_id");
			String msgChoice = request.getParameter("format");
			String token = request.getParameter("token");
			
			PersistentHashMapClient user = new PersistentHashMapClient();
			int user_id = user.getUserId(token);
			
			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				result = postgres.deletePOI(con,user_id,Integer.parseInt(poi_id));
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

