package services.poi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
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

//import org.json.JSONArray;
//import org.json.JSONException;
//import org.json.JSONObject;
import dataBases.postgres.PoiCharacteristics;
import dataBases.postgres.PoiCriteria;
import dataBases.postgres.PostgreSQLFunctions;
import datastore.client.PersistentHashMapClient;
import description.HTMLDescription;
import description.ServiceDescription;
import gr.ntua.ece.cslab.modissense.queries.clients.UserCheckinsQueryClient;
import gr.ntua.ece.cslab.modissense.queries.containers.POI;
import gr.ntua.ece.cslab.modissense.queries.containers.POIList;
import gr.ntua.ece.cslab.modissense.queries.containers.UserCheckinsQueryArguments;
import gr.ntua.ece.cslab.modissense.queries.containers.UserIdStruct;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.ColumnIndexEndpoint;
import gr.ntua.ece.cslab.modissense.queries.coprocessors.ColumnIndexProtocol;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servlet implementation class GetPOIs
 */
@WebServlet("/GetPOIs")
public class GetPOIs extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private ServiceDescription description;

    /**
     * @see HttpServlet#HttpServlet()
     */
    public GetPOIs() {
        super();
        this.description = new HTMLDescription("Get POIs");
        this.description.addParameter("TimeStamp", "Start");
        this.description.addParameter("TimeStamp", "End");
        this.description.addParameter("Rectangle", "Region");
        this.description.addParameter("List Of Strings", "FriendsList");
        this.description.addParameter("String", "OrderBy");
        this.description.addParameter("Integer", "NoOfResults");
        this.description.addParameter("List of Strings", "Keywords");
        this.description.setReturnValue("List Of POI objects");
        this.description.setDescription("Web service used to return a list of objects satisfying certain criteria");
    }

    /**
     * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getParameter("info") != null) {
            response.getOutputStream().print(this.description.serialize());
        } else {
            PostgreSQLFunctions postgres = new PostgreSQLFunctions();
            ArrayList<PoiCharacteristics> listOfPOIs = null;
            ArrayList<Integer> friendsIdList = null;
            Connection con = null;
            int user_id;

            String startTime = request.getParameter("stime");
            String endTime = request.getParameter("etime");
            String x1Region = request.getParameter("x1");
            String y1Region = request.getParameter("y1");
            String x2Region = request.getParameter("x2");
            String y2Region = request.getParameter("y2");
            String friends = request.getParameter("friends");
            String keywords = request.getParameter("keywords");
            String orderBy = request.getParameter("orderby");
            String numberOfResults = request.getParameter("nresults");
            String msgChoice = request.getParameter("format");
            String token = request.getParameter("token");

            double x1;
            double y1;
            double x2;
            double y2;
            int nOfResults;
            String del = ",";
            String[] temp = null;

            PersistentHashMapClient user = new PersistentHashMapClient();
            user_id = user.getUserId(token);

            if (x1Region.equals("") && y1Region.equals("") && x2Region.equals("") && y2Region.equals("")) {
                x1 = -1;
                y1 = -1;
                x2 = -1;
                y2 = -1;
            } else {
                x1 = Double.parseDouble(x1Region);
                y1 = Double.parseDouble(y1Region);
                x2 = Double.parseDouble(x2Region);
                y2 = Double.parseDouble(y2Region);
            }

            if (numberOfResults.equals("")) {
                nOfResults = -1;
            } else {
                nOfResults = Integer.parseInt(numberOfResults);
            }

            if (orderBy.equals("")) {
                orderBy = null;
            }

            if (friends.equals("")) {
                friendsIdList = null;
            } else {
                friendsIdList = new ArrayList<Integer>();
                if (!friends.contains(",")) {
                    friendsIdList.add(user.getUserId(friends));
                } else {
                    temp = friends.split(del);
                    for (int i = 0; i < temp.length; i++) {
                        friendsIdList.add(user.getUserId(temp[i]));
                    }
                }
            }

            if (keywords.equals("")) {
                keywords = null;
            } else {
                keywords = keywords.toLowerCase();
                keywords = keywords.replaceAll(" ", "");
                if (keywords.contains("'")) {
                    keywords = keywords.replaceAll("'", "''");
                }
            }

            java.sql.Timestamp start_time = null;
            java.sql.Timestamp end_time = null;

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            java.util.Date parsedDate = null;

            if (!startTime.equals("")) {
                parsedDate = null;
                try {
                    parsedDate = dateFormat.parse(startTime);
                } catch (ParseException e2) {
                    // TODO Auto-generated catch block
                    e2.printStackTrace();
                }
                start_time = new java.sql.Timestamp(parsedDate.getTime());
            } else {
                start_time = null;
            }

            if (!endTime.equals("")) {
                try {
                    parsedDate = dateFormat.parse(endTime);
                } catch (ParseException e2) {
                    // TODO Auto-generated catch block
                    e2.printStackTrace();
                }
                end_time = new java.sql.Timestamp(parsedDate.getTime());
            } else {
                end_time = null;
            }

            String msgResponse = null;

            if (friends.equals("") && startTime.equals("") && endTime.equals("")) {     //non personalized, not time related query
                PoiCriteria poiCrit = new PoiCriteria(user_id, start_time, end_time, x1, y1, x2, y2, friendsIdList, orderBy, nOfResults, keywords);
                try {
                    con = postgres.OpenConnection();
                    listOfPOIs = postgres.getPOIs(con, poiCrit);
                    postgres.CloseConnection(con);
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {    //personalized, time related query

            	
                UserCheckinsQueryClient client  = new UserCheckinsQueryClient();
                UserCheckinsQueryArguments args = new UserCheckinsQueryArguments();
                
                // reverse indexing
//                args.setyFrom((x1>x2?x2:x1));
//                args.setyTo((x1>x2?x1:x2));
//                args.setxFrom(y1>y2?y2:y1);
//                args.setxTo(y1>y2?y1:y2);

                args.setxFrom((x1>x2?x2:x1));
                args.setxTo((x1>x2?x1:x2));
                args.setyFrom(y1>y2?y2:y1);
                args.setyTo(y1>y2?y1:y2);
                
                if(start_time!=null)
                	args.setStartTimestamp(start_time.getTime());
                if(end_time!=null)
                	args.setEndTimestamp(end_time.getTime());

                List<UserIdStruct> friendsList = new LinkedList<>();
                for(String s:friends.split(",")){
                    Long id = new Long(s);
                    friendsList.add(new UserIdStruct('F', id));
                    friendsList.add(new UserIdStruct('f', id));
                    friendsList.add(new UserIdStruct('t', id));

                    //friendsList.add(new UserIdStruct('F', id%21));
                    //friendsList.add(new UserIdStruct('f', id%21));
                    //friendsList.add(new UserIdStruct('t', id%21));
                }
                
                args.setUserIds(friendsList);
                if(keywords!=null && !keywords.equals(""))
                    args.setKeywords(Arrays.asList(keywords.split(",")));
                
                // setting ordering method
                System.out.println("POIs orderedBy:\t"+orderBy);
                if(orderBy == null || orderBy.toLowerCase().equals("hotness")) {
                    client.setOrderByInterest(false);
                } else {
                    client.setOrderByInterest(true);
                }
                
                System.out.format("Friends: %d, Keywords: %d, "
                        + "From: (%.5f,%.5f), to: (%.5f,%.5f), "
                        + "Start: %d, End: %d\n", 
                        args.getUserIds().size(), args.getKeywords().size(), 
                        args.getxFrom(), args.getyFrom(), 
                        args.getxTo(), args.getyTo(),
                        args.getStartTimestamp(), args.getEndTimestamp());
                
                
                client.setProtocol(ColumnIndexProtocol.class);
                client.setArguments(args);
                client.openConnection("UserCheckins50k");
                client.executeQuery();
//                client.executeSerializedQuery();
                POIList rs = client.getResults();
                
                System.out.format("Exec time: %d ms, POIs returned: %d\n", 
                        client.getExecutionTime(), rs.getPOIs().size());
                
                
                listOfPOIs = new ArrayList<>();
                for(POI p : rs.getPOIs()) {
                    PoiCharacteristics oneOfTheMany = new PoiCharacteristics();
                    oneOfTheMany.setName(p.getName());
                    //reverse indexing
                    oneOfTheMany.setX(p.getX());
                    oneOfTheMany.setY(p.getY());
//                    oneOfTheMany.setDescription();
                    oneOfTheMany.setKeywordsList(new ArrayList<>(p.getKeywords()));
                    oneOfTheMany.setTmstamp(new Timestamp(p.getTimestamp()));
                    oneOfTheMany.setPoiId((int)Math.ceil(p.getId()));
                    oneOfTheMany.setIsMine(false);
                    oneOfTheMany.setHotness((int)p.getHotness());
                    oneOfTheMany.setInterest((int)(p.getInterest()*100));
                    
                    listOfPOIs.add(oneOfTheMany);
                    if(new Integer(numberOfResults)==listOfPOIs.size())
                        break;
                }
                if(listOfPOIs.isEmpty())
                    listOfPOIs=null;
            }

            if (listOfPOIs != null) {
                if (msgChoice.equals("json")) {
                    msgResponse = "{\"poiList\":[" + listOfPOIs.get(0).toJson();
                    for (int i = 1; i < listOfPOIs.size(); i++) {
                        msgResponse = msgResponse + "," + listOfPOIs.get(i).toJson();
                    }
                    msgResponse = msgResponse + "]}";
                } else {
                    String callback = request.getParameter("callback");
                    msgResponse = callback + "({\"poiList\":[" + listOfPOIs.get(0).toJson();
                    for (int i = 1; i < listOfPOIs.size(); i++) {
                        msgResponse = msgResponse + "," + listOfPOIs.get(i).toJson();
                    }
                    msgResponse = msgResponse + "]})";
                }
            } else {
                if (msgChoice.equals("json")) {
                    msgResponse = "{\"poiList\":[]}";
                } else {
                    String callback = request.getParameter("callback");
                    msgResponse = callback + "({\"poiList\":[]})";

                }

            }
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            response.setHeader("charset", "utf-8");

            PrintWriter out = response.getWriter();
            out.write(msgResponse);
			//response.setContentType("application/json;charset=utf-8");
            //response.getOutputStream().print(msgResponse);
        }
    }

    /**
     * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
     * response)
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

    }
    //}

}
