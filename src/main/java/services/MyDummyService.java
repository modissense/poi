package services;

import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Demo usage only!!!
 */
@WebServlet("/MyDummyService")
public class MyDummyService extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public MyDummyService() { 
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		ServletContext cont=request.getSession().getServletContext();
//		HashMap<String, Integer> hash ;
////		= new HashMap<String, Integer>();
//		hash = (HashMap<String, Integer>)cont.getAttribute("hashMap");
		if(cont.getAttribute("hashMap")==null){
			response.getOutputStream().print("Null");
		} else {
			HashMap<String, Integer> hash = (HashMap<String, Integer>)cont.getAttribute("hashMap");
			response.getOutputStream().print(hash.toString());	
		}
//		if(hash==null){
//			response.getOutputStream().print("nothing to read");
//		} else {
//			response.getOutputStream().print(hash.toString());
//		}
//		cont.setAttribute("hashMap", hash);
//		response.getOutputStream().print("Hello world");


	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
	}

}
