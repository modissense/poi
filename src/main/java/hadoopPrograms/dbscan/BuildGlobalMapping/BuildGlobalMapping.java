package hadoopPrograms.dbscan.BuildGlobalMapping;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author jimakos
 */
public class BuildGlobalMapping {
    
    private String inputFile = null;
    private String outputFile = null;
    private HashMap<String,HashSet<String>> hm = null; 
    private HashSet<String> newSet = null;
    private HashSet<String> tempSet = null;
    FindCommonGroups comGroups = null;
    Configuration conf;
    
    public BuildGlobalMapping( String inputFile, String outputFile, Configuration conf){
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        hm = new HashMap<String,HashSet<String>>();
        this.conf = conf;
    }
    
    public void Compute() throws FileNotFoundException, IOException{
        FileInputStream fstream = null;
        DataInputStream in = null;
        FileWriter fstreamOut = null;
        String strLine;
        String del = "<->";
        String []temp;
        String partClusId1 = null;
        String partClusId2 = null;
        boolean FLAG = false;
        boolean FLAGSet1 = false;
        boolean FLAGSet2 = false;
        int glClusterId = 1;
        comGroups = new FindCommonGroups(null,null,null);
        
        Path ptInput =new Path(inputFile);
        FileSystem fsInput = FileSystem.get(conf);
        BufferedReader br=new BufferedReader(new InputStreamReader(fsInput.open(ptInput)));
        
        Path ptOutput=new Path(outputFile);
        FileSystem fsOutput = FileSystem.get(conf);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fsOutput.create(ptOutput,true)));
        
        
        while ((strLine = br.readLine()) != null) {
            FLAGSet1 = false;
            FLAGSet2 = false;
            if ( strLine.contains(del)){ 
                temp = strLine.split(del);
                partClusId1 = temp[0];
                partClusId2 = temp[1];
                comGroups.setHm(hm);
                comGroups.setPartClusId1(partClusId1);
                comGroups.setPartClusId2(partClusId2);
                comGroups.find();
            }
            else{
                partClusId1 = strLine;
                comGroups.setHm(hm);
                comGroups.setPartClusId1(partClusId1);
                comGroups.setPartClusId2(null);
                comGroups.find();
            }
        }
        
        for (String key : hm.keySet()){
            tempSet = hm.get(key);
            out.write(key + "," + glClusterId + "\n");
            if ( tempSet != null ){
                Iterator iterator = tempSet.iterator(); 
                while (iterator.hasNext()){
                   out.write(iterator.next() + "," + glClusterId + "\n");
                }
            }
            glClusterId++;
        }
        
        out.close();
    }
}

