package com.company;


/**
 * Created by HM on 2015/10/9.
 */
public class runMain {
    public static void main(String[] args)throws Exception{
        long  start =  System.currentTimeMillis();

//        namePretreatment np = new namePretreatment("D:\\E\\files\\shen\\maininfo1.txt");
//        np.buildMapCSV("resultSpark.txt");

        namePretreatment np1 = new namePretreatment("D:\\E\\files\\step2\\resultSpark.txt");
        np1.checkNameWithOtherNames("lastResultSpark.txt");

        long end = System.currentTimeMillis();
        System.out.println("consume:"+(end-start)/1000+"second");
    }
}
