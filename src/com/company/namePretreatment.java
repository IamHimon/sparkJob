package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by HM on 2015/10/9.
 */
public class namePretreatment {

    public static final String ENCODE = "UTF-8";
    public static final String ENCODE1 = "GBK";

    private FileInputStream fis = null;
    private InputStreamReader isw = null;
    private BufferedReader br = null;


    public namePretreatment(String filename) throws Exception {
        fis = new FileInputStream(filename);
        isw = new InputStreamReader(fis, ENCODE);
        br = new BufferedReader(isw);
    }

    /*
    * build txt file (company's ID,simplified company's name)
    * */

    public void buildMapCSV(String fileName) throws Exception {
        String stemp;
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            fw = new FileWriter(new File("D://E/files/step2/" + fileName));
            writer = new BufferedWriter(fw);
            String name = null;
            String id = null;

            while ((stemp = br.readLine()) != null) {
                if (stemp.length() != 1) {
                    String[] s = stemp.split(",");

                    nameCut nc = new nameCut();

                    name = s[1].toString();
                    id = s[0].toString();
                    if ((name.length() > 1) && (id.length() > 0)) {
                        if (nc.rebuildName2(nc.startSeg(name)).size() > 1) {
                            System.out.println(s[0] + "," +name+"," + nc.rebuildName2(nc.startSeg(name)).get(1) + "," + nc.rebuildName2(nc.startSeg(name)).get(0));
                            writer.write(s[0] + "," +name+","+ nc.rebuildName2(nc.startSeg(name)).get(1) + "," + nc.rebuildName2(nc.startSeg(name)).get(0));
                            writer.newLine();
                        } else {
                            writer.write(s[0] + "," + name+","+"k" + "," + nc.rebuildName2(nc.startSeg(name)).get(0));
                            writer.newLine();
                        }
                    }
                }
                writer.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fw.close();
            writer.close();
        }
    }

    //make sure the name is not covered by any other name. If it is covered, then we put the nearest removed name into the company name.

    public void checkNameWithOtherNames(String path) throws Exception {
        FileWriter fw = null;
        BufferedWriter writer = null;
        List<String> name = new ArrayList<>();
        String stemp;
        String[] str1;
        String[] str2;
        String temp1 = null;
        String temp2 = null;
        try {
            fw = new FileWriter(new File("D://E/files/step2/" + path));
            writer = new BufferedWriter(fw);
            while ((stemp = br.readLine()) != null) {
                name.add(stemp);
            }
            int i,j;
            flagB:
            for (i = 0; i < name.size(); i++) {
                str1 = name.get(i).split(",");
                temp1 = str1[3].toString();     //temp1:company's name will be write into file.
                flagA:
                for (j = 0; j < name.size(); j++) {
                    str2 = name.get(j).split(",");
                    temp2 = str2[3].toString(); //temp2:company's used to judge if it contains temp1,if so,write temp1,if not continue the loop.

                    if (temp2.contains(temp1) && (j != i)) {
//                        System.out.println("write:" + temp2 + "contains:" + temp1+"  nearest:" +str1[2].toString()+"  length:"+ str1[2].toString().length());
                        if (str1[2].toString().length() != 1) {
                            temp1 = (str1[2].toString()).concat(temp1);
                        }

//                        System.out.println(str1[2].toString() + "contains:::" + temp1);
                        System.out.println(str1[0].toString() + "," + temp1);
                        writer.write(str1[0].toString() + ","+str1[1].toString()+"," + temp1);
                        writer.newLine();
                        continue flagB;
                    }

                }
                if(i != j) {
                    writer.write(str1[0].toString() + "," + str1[1].toString() + "," + temp1);
                    writer.newLine();
                    writer.flush();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fw.close();
            writer.close();
        }

    }

}
