package services.poi;

import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PostgreSQLFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//import org.json.simple.JSONException;
import org.json.simple.JSONObject;

public class GetPOI extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private ServiceDescription description;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetPOI() {
        // TODO Auto-generated constructor stub
        super();
        this.description = new HTMLDescription("Get poi details");
        this.description.addParameter("integer","POIid");
        this.description.setReturnValue("Json");
        this.description.setDescription("Web service returns information about the poi");
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("info") != null) {
            response.getOutputStream().print(this.description.serialize());
            return;
        }

        String token = request.getParameter("token");
        Integer poi_id = new Integer(request.getParameter("poi_id"));
        boolean jsonP = (request.getParameter("format")!=null && request.getParameter("format").equals("jsonp"));
        String callback = null;
        if(jsonP){
            callback = request.getParameter("callback");
        }
        if (poi_id == null) {
            response.sendError(
                    HttpServletResponse.SC_BAD_REQUEST,
                    "You must add the poi_id in your query");
        } else if (token == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "You must add the user token in your query");
        }
        ////////////////////////////code//////////////////////////////////////

        response.setCharacterEncoding("UTF-8");
        response.setHeader("Content-Type", "application/json");
        response.setHeader("charset", "utf-8");

        PostgreSQLFunctions postgres = new PostgreSQLFunctions();
        Connection con = null;

        PersistentHashMapClient user = new PersistentHashMapClient();
//        int user_id = user.getUserId(token);

        try {
            con = postgres.OpenConnection();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        PoiCharacteristics poi = null;
        try {
            poi = postgres.getPOI(con, poi_id);
            
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

        PrintWriter out = response.getWriter();
        
        try {
            if(!jsonP)
                out.print(this.createJSONObject(poi).toJSONString());
            else
                out.print(callback+"("+this.createJSONObject(poi).toJSONString()+")");
        } 
        catch (Exception ex) {
            Logger.getLogger(GetPOI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // TODO Auto-generated method stub
    }

    private JSONObject createJSONObject(PoiCharacteristics poi) {
        JSONObject poiObject = new JSONObject();
        if(poi == null){
            return poiObject;
        }
        try {
            poiObject.put("id", poi.getPoiId());
            poiObject.put("name", poi.getName());
            List<String> keys = new LinkedList<>();
            
//            keys.add("beer");
//            keys.add("wine");
            poiObject.put("keywords", keys);
            poiObject.put("hotness", poi.getHotness());
            poiObject.put("interest", poi.getInterest());
            poiObject.put("number_of_comments", 1000);
            if(poi.getPoiId()==95){
                poiObject.put("image", "https://scontent-a-vie.xx.fbcdn.net/hphotos-xfp1/t1.0-9/300025_308281582516175_432171612_n.jpg");   
                keys.add("cafe");
                keys.add("bar");
                poiObject.put("keywords", keys);
                poiObject.put("hotness", 120);
                poiObject.put("interest", 0.8);
                poiObject.put("number_of_comments", 30);
                
            } else {
                poiObject.put("image", poi.getPictureURL());   
                poiObject.put("keywords", keys);
                poiObject.put("hotness", poi.getHotness());
                poiObject.put("interest", poi.getInterest());
                poiObject.put("number_of_comments", 1000);
            }
            
            
            // they must be filled from another query here...
            JSONObject personalizedInfo = new JSONObject();
            personalizedInfo.put("hotness", 5);
            personalizedInfo.put("interest", 0.87);
            
            JSONObject comment = new JSONObject();
            comment.put("text", "Good coffee, good beer, good wine :)");
            comment.put("user", "Nikos");
            comment.put("user_picture", "https://www.gnu.org/graphics/babies/BabyGnuTux-Big.png");
            
            personalizedInfo.put("comment", comment);

            poiObject.put("personalized", personalizedInfo);
        } catch (Exception ex) {
            Logger.getLogger(GetPOI.class.getName()).log(Level.SEVERE, null, ex);
        }
        return poiObject;
    }
}
