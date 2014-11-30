package hadoopPrograms.dbscan.BuildGlobalMapping;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author jimakos
 */
public class FindCommonGroups {
    private HashMap<String,HashSet<String>> hm = null; 
    private String partClusId1 = null;
    private String partClusId2 = null;
    private HashSet<String> newSet = null;
    
    
    public FindCommonGroups(HashMap<String,HashSet<String>> hm, String partClusId1, String partClusId2){
        this.hm = hm;
        this.partClusId1 = partClusId1;
        this.partClusId2 = partClusId2;
    }
    
    
    ///////////////////Set/////////////////////////////
    public void setHm(HashMap<String, HashSet<String>> hm) {
        this.hm = hm;
    }

    public void setPartClusId1(String partClusId1) {
        this.partClusId1 = partClusId1;
    }

    public void setPartClusId2(String partClusId2) {
        this.partClusId2 = partClusId2;
    }
    
    
    /////////////////Get/////////////////////////////
    public HashMap<String, HashSet<String>> getHm() {
        return hm;
    }

    public String getPartClusId1() {
        return partClusId1;
    }

    public String getPartClusId2() {
        return partClusId2;
    }
    private HashSet<String> tempSet = null;
    
    public void find(){
        HashSet<String> set1 = null;
        HashSet<String> set2 = null;
        boolean FLAG = false;
        String delString = null;
        
        if ( partClusId1 != null && partClusId2 != null ){
            if ( hm.containsKey(partClusId1)){
                tempSet = hm.get(partClusId1);
                if ( tempSet == null ){
                    hm.remove(partClusId1);
                }
                else {
                    set1 = (HashSet<String>) tempSet.clone();
                    hm.remove(partClusId1);
                    FLAG = true;
                }
                for (String key : hm.keySet()){
                    tempSet = (HashSet<String>) hm.get(key);
                    if (tempSet != null){
                        if (tempSet.contains(partClusId1)){
                            if (FLAG == true){
                                set1.addAll(tempSet);
                                set1.add(key);
                            }
                            else{
                                set1 = new HashSet<String>();
                                set1.addAll(tempSet);
                                set1.add(key);
                            }
                            delString = key;
                            break;
                        }
                    }
                }
                if (delString != null){
                    hm.remove(delString);
                }
            }
            else{
                for (String key : hm.keySet()){
                    tempSet = (HashSet<String>) hm.get(key);
                    if (tempSet != null){
                        if (tempSet.contains(partClusId1)){
                            set1 = new HashSet<String>();
                            set1.addAll(tempSet);
                            set1.add(key);
                            delString = key;
                            break;
                        }
                    }
                }
                if (delString != null){
                    hm.remove(delString);
                }
            }


            if ( hm.containsKey(partClusId2)){
                tempSet = hm.get(partClusId2);
                if ( tempSet == null ){
                    hm.remove(partClusId2);
                }
                else {
                    set2 = (HashSet<String>) tempSet.clone();
                    hm.remove(partClusId2);
                    FLAG = true;
                }
                for (String key : hm.keySet()){
                    tempSet = (HashSet<String>) hm.get(key);
                    if (tempSet != null){
                        if (tempSet.contains(partClusId2)){
                            if (FLAG == true){
                                set2.addAll(tempSet);
                                set2.add(key);
                            }
                            else{
                                set2 = new HashSet<String>();
                                set2.addAll(tempSet);
                                set2.add(key);
                            }
                            delString = key;
                            break;
                        }
                    }
                }
                if (delString != null){
                    hm.remove(delString);
                }
            }
            else{
                for (String key : hm.keySet()){
                    tempSet = (HashSet<String>) hm.get(key);
                    if (tempSet != null){
                        if (tempSet.contains(partClusId2)){
                            set2 = new HashSet<String>();
                            set2.addAll(tempSet);
                            set2.add(key);
                            delString = key;
                            break;
                        }
                    }
                }
                if (delString != null){
                    hm.remove(delString);
                }
            }

            if ( set1 != null && set2!= null){
                set1.addAll(set2);
                set1.remove(partClusId1);
                hm.put(partClusId1, set1);
            }
            else if ( set1 != null ){
                set1.add(partClusId2);
                set1.remove(partClusId1);
                hm.put(partClusId1, set1);
            }
            else if ( set2 != null ){
                set2.add(partClusId1);
                set2.remove(partClusId2);
                hm.put(partClusId2, set2);
            }
            else{
                newSet = new HashSet<String>();
                newSet.add(partClusId2);
                hm.put(partClusId1,newSet);
            }
        }
        else{
            FLAG = false;
            if( !hm.containsKey(partClusId1) ){
                for (String key : hm.keySet()){
                    tempSet = (HashSet<String>) hm.get(key);
                    if (tempSet != null){
                        if ( tempSet.contains(partClusId1)){
                            FLAG = true;
                            break;
                        }
                    }
                }
                if ( FLAG == false ){
                    hm.put(partClusId1,null);
                }
            }
        }
        
    }
    
}

