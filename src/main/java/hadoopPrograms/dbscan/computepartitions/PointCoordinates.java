package hadoopPrograms.dbscan.computepartitions;

public class PointCoordinates {
    private double x;
    private double y;
    private int id;
    private String partitions;
    
    PointCoordinates( int id, double x, double y){
        this.x = x;
        this.y = y;
        this.id = id;
        this.partitions = null;
    }
    
    public int getId(){
        return id;
    }
    
    public Double getX(){
        return x;
    }
    
    public Double getY(){
        return y;
    }
    
    public String getPartitions(){
        return partitions;
    } 
    
    
    
    public void setId( int id ){
        this.id = id;
    }
    
    public void setX(Double x){
        this.x = x;
    }
    
    public void setY(Double y){
        this.y = y;
    }
    
    public void setPartitions(String partition){
        this.partitions = partition;
    }
            
}
