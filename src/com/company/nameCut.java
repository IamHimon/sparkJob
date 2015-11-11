package com.company;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by HM on 2015/10/9.
 */
public class nameCut {
    private static final List<String> DIC = new ArrayList<>();  //read fixDic2.txt
    private static final List<String> DIC1 = new ArrayList<>(); //read placeDic.txt where only contain all place name

    private static int MAX_LENGTH;

    static {
        try {
            int max = 1;
            int count = 0;
            List<String> lines = Files.readAllLines(Paths.get("sourceFiles/fixedDic2.txt"), Charset.forName("utf-8"));
            for (String line : lines) {
                DIC.add(line);
                count++;
                if (line.length() > max) {
                    max = line.length();
                }
            }
            //read all place name to memory
            List<String> lines1 = Files.readAllLines(Paths.get("sourceFiles/placeDic.txt"), Charset.forName("utf-8"));
            for (String line : lines1) {
                DIC1.add(line);
            }
            MAX_LENGTH = max;
        } catch (IOException ex) {
            System.err.println("read dic failed" + ex.getMessage());
        }
    }

    //list to string
    public static String listToString(List<String> list) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            sb.append(list.get(i));
        }
        return sb.toString().substring(0, sb.toString().length());
    }

    //processing name according dic originally
    public static String processNameAccordingDic(String originalName) throws IOException {
        String temp = null;
        String result = null;
        List<String> lines = Files.readAllLines(Paths.get("sourceFiles/dic.txt"), Charset.forName("utf-8"));
        for (String line : lines) {
            result = originalName.replaceAll(line, "");
            temp = result;
            originalName = temp;
        }
        return result;
    }

    //    judge whether the company's name is start with place-name,if so,turn to seg(),if not,break the process then
    //   retain the complete name to the List.
    public static Map<String, List> startSeg(String text) throws IOException {
        String processedName = processNameAccordingDic(text);
        Map<String, List> map = new HashMap<>();
        List<String> retained = new ArrayList<>();        //retained words list
        List<String> removed = new ArrayList<>();       //removed words list

        //judge if the processed company's name is start with place name
        String temp = null;
        for (int i = 0; i <= processedName.length(); i++) {
            temp = processedName.substring(0, i);
            if (DIC1.contains(temp)) {
                break;
            }
        }
        retained.add(temp);
        if (!temp.equals(processedName)) {
            map = seg2(processedName);
        } else {
            map.put("retained", retained);
        }
        return map;
    }

    //divide the processed company's name，retain these place-name that is inside the company's name(version 2)
    public static Map<String, List> seg2(String text) {
        int length = text.length();
        Map<String, List> map = new HashMap<>();
        List<String> retained = new ArrayList<>();        //retained words list
        List<String> removed = new ArrayList<>();       //removed words list
        String temp = null;
        String result = null;
        int i, j;
        labelA:
        for (i = 0; i < length - 1; i = j) {             //not i+=j,
            lableB:
            for (j = length; j >= i; j--) {
                temp = text.substring(i, j);
                if (DIC1.contains(temp)) {
                    removed.add(temp);
                    break lableB;         //break : break from the current for-loop
                }
                if (i == j) {
                    result = text.substring(i, length);
                    break labelA;
                }
            }
        }
        retained.add(result);
        map.put("retained", retained);
        map.put("removed", removed);
        return map;
    }

    // make further processing of the map that got from startSeg ,then return the ultimate result<simplifiedName,nearestPlaceName> (version 2)
    public static List<String> rebuildName2(Map<String, List> map) {
        List<String> result = new ArrayList<>();
        String simpleName = null;
        List<String> removed = map.get("removed");
        List<String> retained = map.get("retained");

        if (removed != null) {

            if (removed.size() > 2) {
                result.add(removed.get(removed.size() - 2));
            } else {
                result.add(removed.get(0));
            }

            //if the length of removed list is more than 1,add the nearest place-name to retained.
            if (removed.size() > 1) {
                retained.add(0, removed.get(removed.size() - 1));
                simpleName = listToString(retained);
            } else {
                simpleName = listToString(retained);
            }
        } else {
            simpleName = listToString(retained);
        }
        result.add(0,simpleName);
        return result;
    }

    public static void main(String[] args) throws IOException {

//        String text1 = "江苏省苏州市平江区万向德农股份有限公司";
        String text1 = "江苏省苏州市沧浪区中天集团有限公司";
        System.out.println(text1);
        System.out.println(startSeg(text1).get("retained"));
        System.out.println(startSeg(text1).get("removed"));
        System.out.println(rebuildName2(startSeg(text1)));
        System.out.println(rebuildName2(startSeg(text1)).get(1));

    }
}
