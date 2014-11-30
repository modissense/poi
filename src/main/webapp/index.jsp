<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Web services</title>
</head>
<body>

	<h2>Available web services</h2>
	<p>Hello again</p>
<table>
<tr><td><h3>User Management</h3></td></tr>
<tr><td> <a href='users/register/?info'>User Registration</a></td><td>Done</td></tr>
<tr><td><a href='users/loggedin/?info'>User LoggedIn</a></td><td><b>Done</b></td></tr>
<tr><td> <del><a href='users/savetoken/?info'>Save Auth Token</a></del></td></tr>
<tr><td> <a href='users/checkname/?info'>Check user name</a></td><td><b>Done</b></td></tr>
<tr><td> <a href='users/logout/?info'>Log out</a></td><td><b>Done</b></td></tr>
<tr><td> <a href='users/update/?info'>User update</a></td><td>not critical</td></tr>
<tr><td> <a href='users/delete/?info'>User delete</a></td><td><b>Done</b></td></tr>
<tr><td> <a href='users/addpoi/?info'>Add user poi</a></td><td>not critical</td></tr>
<tr><td> <a href='users/refresh/?info'>Refresh user</a></td><td>not critical</td></tr>
<tr><td> <a href='blog/create/?info'>Create blog</a></td><td>not critical</td></tr>
<tr><td> <a href='users/getnetworks/?info'>Get user networks</a></td><td><b>Done</b></td></tr>
</table>
<table>
<tr><td><h3>POIs Management</h3></td></tr>
<tr><td> <a href='addnewpois/?info'>Add new POIs</a></td></tr>
<tr><td> <a href='findduplicates/?info'>Find dupicate POIs</a></td></tr>
<tr><td> <a href='getpois/?info'>Get POIs</a></td></tr>
<tr><td> <a href='getpoisfromtrajectories/?info'>Get POIs From Trajectories</a></td></tr>
<tr><td> <a href='getpoisfromextsrcs/?info'>Get POIs From External Sources</a></td></tr>
<tr><td> <a href='getpoisfromsn/?info'>Get POIs From Social Networks</a></td></tr>
<tr><td> <a href='filterunique/?info'>Filter Unique</a></td></tr>
<tr><td> <a href='createsemantictrajectories/?info'>Create Semantic Trajectories</a></td></tr>
<tr><td> <a href='updatesemantictrajectories/?info'>Update Semantic Trajectories</a></td></tr>
<tr><td> <a href='updatepoiinterest/?info'>Update POI Interest</a></td></tr>
<tr><td> <a href='updatepoi/?info'>Update POI</a></td></tr>
<tr><td> <a href='refreshusers/?info'>Refresh Users</a></td></tr>
<tr><td> <a href='loggpstraces/?info'>Log GPS Traces</a></td></tr>
<tr><td> <a href='getnn/?info'>Get Nearest Neighbors</a></td></tr>
<tr><td> <a href='deletepoi/?info'>Delete POI</a></td></tr>
<tr><td> <a href='addnewvisit/?info'>Add New Visit</a></td></tr>
<tr><td> <a href='showtrendingevents/?info'>Show Trending Events</a></td></tr> 
<tr><td> <a href='getpoi/?info'>Get POI</a></td></tr> 
<tr><td> <a href='getgpstrace/?info'>Get GPS Trace</a></td></tr> 
</table>
</body>
</html>
