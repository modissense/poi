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
 * Servlet implementation class RefreshUsers
 */
@WebServlet("/RefreshUsers")
public class RefreshUsers extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public RefreshUsers() {
        super();
        this.description = new HTMLDescription("Refresh Users");
        this.description.setReturnValue("boolean");
        this.description.setDescription("Web service processes the raw data from social networks and updates user profiles with all the new information");
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
