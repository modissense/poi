package services.poi;

import hadoopPrograms.computeSemanticTrajectories.ComputeSemanticTrajectories;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import description.HTMLDescription;
import description.ServiceDescription;

/**
 * Servlet implementation class GetPOIsFromTrajectories
 */
@WebServlet("/GetPOIsFromTrajectories")
public class GetPOIsFromTrajectories extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServiceDescription description;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetPOIsFromTrajectories() {
        super();
        this.description = new HTMLDescription("Get POIs From Trajectories");
        this.description.setReturnValue("List of POI objects");
        this.description.setDescription("Web service have access to the repository of the points and verify the results with those found in the repository of POIs");
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if(request.getParameter("info")!=null){
			response.getOutputStream().print(this.description.serialize());
		} else {
			String []args = {"input","output"};
			
			ComputeSemanticTrajectories compSemTraj = new  ComputeSemanticTrajectories();
			try {
				compSemTraj.main(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			response.getOutputStream().print("OK");
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
