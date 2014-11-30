/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dataBases.hBase;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 *
 * @author jimakos
 */
public class CompositeKey {
    long id ;
    long tmstmp ;
    int choice;
    
    
    public CompositeKey(int choice){
        this.id = 0;
        this.tmstmp = 0;
        this.choice = choice;
    }
   
    public int getId(){
        return (int) this.id;
    }
    
    public byte[] get(Long l){
        ByteBuffer buffer = null;
         
        if (this.id !=0 && this.tmstmp != 0){
            buffer = ByteBuffer.allocate(2*Long.SIZE/8);
            if ( choice == 0){
                buffer.putLong(this.id);
                buffer.putLong(this.tmstmp);
            }
            else{
                buffer.putLong(this.tmstmp);
                buffer.putLong(this.id);
            }
        }
        else if ( this.id !=0 ){
            buffer = ByteBuffer.allocate(2*Long.SIZE/8);
            buffer.putLong(this.id);
            buffer.putLong(l);
        }
        else if (this.tmstmp != 0){
            buffer = ByteBuffer.allocate(2*Long.SIZE/8);
            buffer.putLong(this.tmstmp);
            buffer.putLong(l);
        }
         
         
         return buffer.array();
    }
    
    
    
    public void parseBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if ( choice == 0 ){
            this.id = buffer.getLong();
            this.tmstmp = buffer.getLong();
            //System.out.println("choice = " + choice);
            //System.out.println("Timestamp = " + tmstmp);
            //System.out.println("id = " + id);
        }
        else{
            this.tmstmp = buffer.getLong();
            this.id = buffer.getLong();
            //System.out.println("choice = " + choice);
            //System.out.println("Timestamp = " + tmstmp);
            //System.out.println("id = " + id);
        }
    }
    
    
    
    public void getBytes(String str) throws ParseException{
        String[] temp = null;
        
        ByteBuffer buffer = null;
        temp = str.split("_");
        if (choice == 0 ){
            if (temp.length > 1){
               // buffer = ByteBuffer.allocate(2*Long.SIZE/8);           
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date parsedDate = dateFormat.parse(temp[1]);
                this.id = Long.parseLong(temp[0]);
                this.tmstmp = parsedDate.getTime();
            }
            else{
                if(!temp[0].contains("-")){
                //buffer = ByteBuffer.allocate(1*Long.SIZE);           
                    this.id = Long.parseLong(str);
                }
                else{
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    java.util.Date parsedDate = dateFormat.parse(temp[0]);
                    this.tmstmp = parsedDate.getTime();
                    System.out.println("timestamp = " + this.tmstmp);
                }
            }
        }
        else{
            if (temp.length > 1){
               // buffer = ByteBuffer.allocate(2*Long.SIZE/8);           
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                java.util.Date parsedDate = dateFormat.parse(temp[0]);
                this.id = Long.parseLong(temp[1]);
                this.tmstmp = parsedDate.getTime();
            }
            else{
                if(!temp[0].contains("-")){
                //buffer = ByteBuffer.allocate(1*Long.SIZE);           
                    this.id = Long.parseLong(str);
                }
                else{
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    java.util.Date parsedDate = dateFormat.parse(temp[0]);
                    this.tmstmp = parsedDate.getTime();
                    System.out.println("timestamp = " + this.tmstmp);
                }
            }
        }

    }
    
}
