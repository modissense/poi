package dataBases.hBase;

public class DBScanCharacteristics {
	private double x = -1;
	private double y = -1;
	
	
	public DBScanCharacteristics(){
	}
	
	
	public DBScanCharacteristics(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	
	public String toString(){
		String str = null;
		
		str = "x = " + x + "\ty = " + y;
		
		return str;
	}
	
	public void setX(double x){
		this.x = x;
	}
	
	
	public void setY(double y){
		this.y = y;
	}
	
	
	public double getX(){
		return x;
	}
	
	
	public double getY(){
		return y;
	}
	
}
