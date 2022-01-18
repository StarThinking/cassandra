/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.config;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfAgent {
    private static final Logger logger = LoggerFactory.getLogger(ConfAgent.class);
   
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
                myPrint("ERROR : wrong value of reconf_vvmode " + reconf_vvmode);
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
                        myPrint("ERROR: wrong fields length");
                    }
                    HTask tt = new HTask(fields[0], fields[1], fields[2], fields[3], fields[4]);
                    h_list.add(tt);
                }
            }
	    myPrint("v17.0 reconf_vvmode=" + reconf_vvmode + ", reconf_h_list=" + h_list); 
        } catch (Exception e) {
            myPrint("ERROR : loadSharedVariables");
            e.printStackTrace();
        }
    }

    private static String returnHelper(String para, String value, String componentType, int componentId) {
        myPrint("parameter " + para + " returns " + value + " as considered component " + 
            componentType + "." + componentId);
        return value;
    }

    public static synchronized String whichV(String para, String v, String componentType, int componentId) {
        // check each task/rule in the htask list
        for (HTask task : h_list) {
            if (para.equals(task.reconf_parameter)) {
                if (ConfAgent.reconf_vvmode.equals("v1v1")) {
                    myPrint("parameter " + para + " returns " + task.reconf_v1 + " with v1v1");
                    return task.reconf_v1;
                }
                
                if (ConfAgent.reconf_vvmode.equals("v2v2")) {
                    myPrint("parameter " + para + " returns " + task.reconf_v2 + " with v2v2");
                    return task.reconf_v2;
                }

                if (ConfAgent.reconf_vvmode.equals("v1v2")) {
                    // if component type does not match, return v1
                    if (!componentType.equals(task.reconf_component)) {
                        myPrint("parameter " + para + " returns v1 " + task.reconf_v1 + 
                            " as none-considered component " + componentType + "." + componentId);
                        return task.reconf_v1;
                    } else { // component type matches; determine value by component id
                        if (task.reconf_point_int == -1) { // set odd-id instances with v2
                            if ((componentId % 2) == 1)
                                return returnHelper(para, task.reconf_v2, componentType, componentId);
                            else
                                return returnHelper(para, task.reconf_v1, componentType, componentId);
                        } else if (task.reconf_point_int == -2) { // set even-id instances with v2
                            if ((componentId % 2) == 0)
                                return returnHelper(para, task.reconf_v2, componentType, componentId);
                            else
                                return returnHelper(para, task.reconf_v1, componentType, componentId);
                        } else if (task.reconf_point_int == -3) { // set all instances with v2
                            return returnHelper(para, task.reconf_v2, componentType, componentId);
                        } else { // set instance whose index is exactly reconf_point_int with v2
                            if (componentId == task.reconf_point_int)
                                return returnHelper(para, task.reconf_v2, componentType, componentId);
                            else
                                return returnHelper(para, task.reconf_v1, componentType, componentId);
                        }
                    }
                } 
            }
        }
        
        // when vvmode is none (h_list is empty) or para not included in any htask, return the original value
        myPrint("parameter " + para + " returns original " + v);
        return v;
    }
    
    public static void myPrint(String str) { 
        if (msxConfEnable) {
            logger.warn("[msx-confagent] " + str);
        }
    }
}
