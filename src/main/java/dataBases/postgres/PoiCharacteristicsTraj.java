package dataBases.postgres;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Timestamp;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jimakos
 */
public class PoiCharacteristicsTraj implements Writable {
    double x = -1;
    double y = -1;
    String arrived = null;
    String off = null;
    boolean publicity = true;
    int poi_id = -1;
    
    public PoiCharacteristicsTraj(){
        this(0.0,0.0,null);
    }
    
    public PoiCharacteristicsTraj( double x, double y, String arrived ){
        this.x = x;
        this.y = y;
        this.arrived = arrived;
    }
    
    public void setX( double x ){
        this.x = x;
    }
            
    public void setY( double y ){
        this.y = y;
    }
    
    public void setArrived( String arrived ){
        this.arrived = arrived;
    }
    
    public void setOff( String off ){
        this.off = off;
    }
    
    public void setPublicity( boolean publicity){
        this.publicity = publicity;
    }
    
    public void setPoi_id( int poi_id){
        this.poi_id = poi_id;
    }
    
    public double getX(){
        return x;
    }
    
    public double getY(){
        return y;
    }
    
    public String getArrived(){
        return arrived;
    }
    
    public String getOff(){
        return off;
    }
    
    public boolean getPublicity(){
        return publicity;
    }
    
    public int getPoi_id(){
        return poi_id;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
        out.writeUTF(arrived);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readDouble();
        y = in.readDouble();
        arrived = in.readUTF();
    }
    
    public String toString() {
        return Double.toString(x) + ", " + Double.toString(y) + ", " + arrived + "," + off ;
    }
}

