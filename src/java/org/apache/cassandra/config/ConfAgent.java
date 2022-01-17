package org.apache.cassandra.config;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class ConfAgent {
    private static boolean msxConfEnable = false;
    private static final String reconf_systemRootDir = System.getenv("ZEBRACONF_HOME") + "/runner/shared/";
    private static String reconf_vvmode = "";
    private static List<HTask> h_list = new ArrayList<HTask>();

    // load just once
    static {
	loadSharedVariables();
    }
    
    public static class HTask {
        public String reconf_parameter = "";
        public String reconf_component = "";
        public String reconf_point = "";
        public int reconf_point_int = 0;
        public String reconf_v1 = "";
        public String reconf_v2 = "";

        public HTask(String para, String comp, String po, String v1, String v2) {
            this.reconf_parameter = para;
            this.reconf_component = comp;
            this.reconf_point = po;
            this.reconf_point_int = Integer.valueOf(this.reconf_point);
            this.reconf_v1 = v1.equals("null") ? null : v1;
            this.reconf_v2 = v2.equals("null") ? null : v2;
        }

        @Override
        public String toString() {
            return reconf_parameter + "," + reconf_component + "," + reconf_point +
                "," + reconf_v1 + "," + reconf_v2;
        }
    }

    private static void loadSharedVariables() {
        try {
            BufferedReader reader;
            reader = new BufferedReader(new FileReader(new File(reconf_systemRootDir + "reconf_vvmode")));
            ConfAgent.reconf_vvmode = reader.readLine();
            reader.close();
            if (!reconf_vvmode.equals("v1v1") && !reconf_vvmode.equals("v2v2") && !reconf_vvmode.equals("v1v2") && 
                !reconf_vvmode.equals("none")) {
                myErrorPrint("ERROR : wrong value of reconf_vvmode " + reconf_vvmode);
                System.exit(1);
            }
                
            reader = new BufferedReader(new FileReader(System.getenv("ZEBRACONF_HOME") + "/app_meta/lib/enable"));
            String buffer = reader.readLine();
            reader.close();
            if (buffer.equals("true")) { 
                msxConfEnable = true;
            } else {
                msxConfEnable = false;
            }

            if (!ConfAgent.reconf_vvmode.equals("none")) {
                reader = new BufferedReader(new FileReader(new File(reconf_systemRootDir + "reconf_h_list")));
                String h_list_str = reader.readLine();
                reader.close();
        	String[] htasks = h_list_str.split("%%%");
        	//System.out.println("htasks size = " + htasks.length);
        	for (String task : htasks) {
            	    String[] fields = task.split("@@@");
            	    if (fields.length != 5) {
                        myErrorPrint("ERROR: wrong fields length");
                    }
                    HTask tt = new HTask(fields[0], fields[1], fields[2], fields[3], fields[4]);
                    h_list.add(tt);
                }
            }
	    myInfoPrint("v17.0 reconf_vvmode=" + reconf_vvmode + ", reconf_h_list=" + h_list); 
        } catch (Exception e) {
            myErrorPrint("ERROR : loadSharedVariables");
            e.printStackTrace();
        }
    }

    public static synchronized String whichV(String para, String v) {
        /*if (reconf_vvmode.equals("none")) {
            if (msxConfEnable == true) {
                ComponentConf entry_found = findEntryByConf(conf);
                if (v != null) {
                    if (entry_found != null) {
                        System.out.println("msx-get " + para + " " + entry_found.componentWithIndex() + " "
                            + v + " getter");
                    } else {
                        System.out.println("msx-get " + para + " " + "unit_test" + " "
                            + v + " getter");
                    }
                }
            }
            return v;
        }*/

        // check if this para matches any tasks in h_list
        boolean para_concerned = false;
        for (HTask task : h_list) {
            if (task.reconf_parameter.equals(para)) {
                para_concerned = true;
            }
        }
        if (para_concerned == false) {
            return v;
        }

        // h_list is used after this point
        for (HTask task : h_list) {
            // if I'm not the concerned para, continue to the next task
            if (!para.equals(task.reconf_parameter)) {
                continue;
            }

            // decide value for reconf_parameter with mode v1v1/v2v2/v1v2
            if (ConfAgent.reconf_vvmode.equals("v1v1")) {
                myPrint("get-return para " + para + " " +
                    task.reconf_v1 + " for v1v1");
                return task.reconf_v1;
            }
            
            if (ConfAgent.reconf_vvmode.equals("v2v2")) {
                myPrint("get-return para " + para + " " +
                    task.reconf_v2 + " for v2v2");
                return task.reconf_v2;
            }

            // 1. external, 2. component internal
            if (ConfAgent.reconf_vvmode.equals("v1v2")) {
                // check if component group match; if not, set with v1
                /*if (!entry.group.equals(task.reconf_component)) {
                    myPrint("get-return para " + para + " " +
                        task.reconf_v1 + " for other components " + entry.componentWithIndex());
                    return task.reconf_v1;
                } else { // group match
                    // decide v1 or v2 for our interested component object based on index
                    if (task.reconf_point_int == -1) { // set odd components with v2
                        if ((entry.index % 2) == 1) {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v2 + " for " + entry.componentWithIndex());
                            return task.reconf_v2;
                        } else {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v1 + " for " + entry.componentWithIndex());
                            return task.reconf_v1;
                        }
                    } else if (task.reconf_point_int == -2) { // set even components with v2
                        if ((entry.index % 2) == 0) {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v2 + " for " + entry.componentWithIndex());
                            return task.reconf_v2;
                        } else {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v1 + " for " + entry.componentWithIndex());
                            return task.reconf_v1;
                        }
                    } else if (task.reconf_point_int == -3) { // set v2 for all components in this group
                        myPrint("get-return para " + para + " " +
                            task.reconf_v2 + " for " + entry.componentWithIndex());
                        return task.reconf_v2;
                    } else { // set v2 to component whose index is reconf_point_int
                        if (entry.index == task.reconf_point_int) {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v2 + " for " + entry.componentWithIndex());
                            return task.reconf_v2;
                        } else {
                            myPrint("get-return para " + para + " " +
                                task.reconf_v1 + " for " + entry.componentWithIndex());
                            return task.reconf_v1;
                        }
                    }
                }*/
            } 
        }
        
        // should not reach here
        myErrorPrint("ERROR: should not reach here in whichV");
        return v;
    }

    public static void myPrint(String str) { 
        if (msxConfEnable) {
            System.out.println("msx-confcontroller " + str);
        } 
    }
    
    public static void myErrorPrint(String str) { if (msxConfEnable) { System.out.println("msx-confcontroller " + str);}}
    
    public static void myInfoPrint(String str) { if (msxConfEnable) {System.out.println("msx-confcontroller " + str);}}
}
