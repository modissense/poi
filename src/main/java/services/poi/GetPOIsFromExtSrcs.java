package services.poi;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class GetPOIsFromExtSrcs
 */
@WebServlet("/GetPOIsFromExtSrcs")
public class GetPOIsFromExtSrcs extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetPOIsFromExtSrcs() {
        super();
        this.description = new HTMLDescription("Get POIs From External Sources");
        this.description.setReturnValue("List of POI objects");
        this.description.setDescription("Web service detects new POIs by default external sources");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} else {
			response.getOutputStream().print(true);
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
