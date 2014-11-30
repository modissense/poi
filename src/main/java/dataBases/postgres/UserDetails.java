package dataBases.postgres;

public class UserDetails {
	String user_id = null;
    String name = null;
    String surname = null;
    int xMapCenter;
    int yMapCenter;
    
    public UserDetails(String user_id, String name, String surname){
        this.user_id = user_id;
        this.name = name;
        this.surname = surname;
    }
    
    //////////////////////get/////////////////////////////

    public String getUser_id() {
        return user_id;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public int getxMapCenter() {
        return xMapCenter;
    }

    public int getyMapCenter() {
        return yMapCenter;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }
    
    
    /////////////////////////set/////////////////////////

    public void setName(String name) {
        this.name = name;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public void setxMapCenter(int xMapCenter) {
        this.xMapCenter = xMapCenter;
    }

    public void setyMapCenter(int yMapCenter) {
        this.yMapCenter = yMapCenter;
    }
}

