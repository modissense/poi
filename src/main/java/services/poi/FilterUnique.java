package services.poi;

import java.io.IOException;
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
import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class FilterUnique
 */
@WebServlet("/FilterUnique")
public class FilterUnique extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FilterUnique() {
        super();
        this.description = new HTMLDescription("Filter Unique");
        this.description.addParameter("List Of POI objects", "POIList");
        this.description.setReturnValue("List of POI Objects");
        this.description.setDescription("Web service filters a list of POIs and find duplicates. Returns POIs that do not exist in the database");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} else if(request.getParameter("POILIst")==null){
			response.sendError(HttpServletResponse.SC_BAD_REQUEST);
		} else {
			PostgreSQLFunctions postgres = new PostgreSQLFunctions();
			ArrayList<PoiCharacteristics> listOfPOIs = new ArrayList<PoiCharacteristics>();
			Connection con = null;
			boolean result = false;
			
			String POIs = request.getParameter("POIList");
			
			try {
				con = postgres.OpenConnection();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			if ( listOfPOIs != null ){
				try {
					listOfPOIs = postgres.filterUnique(con, listOfPOIs);
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			
			try {
				postgres.CloseConnection(con);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			if ( listOfPOIs != null ){
				//return
			}
			else{
				
			}
			response.getOutputStream().print(" ");
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
