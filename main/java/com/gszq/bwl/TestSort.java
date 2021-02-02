package com.gszq.bwl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

public class TestSort {

    // 获取市场行情的价格，不能获取的取1.0
    public static Double getMarketPrice(String url) {
        StringBuilder json = new StringBuilder();
        Double marketPrice = 1.0;
        try {
            URL urlObject = new URL(url);
            URLConnection uc = urlObject.openConnection();
            // 设置为utf-8的编码 才不会中文乱码
            BufferedReader in = new BufferedReader(new InputStreamReader(uc
                    .getInputStream(), "utf-8"));
            String inputLine = null;
            while ((inputLine = in.readLine()) != null) {
                json.append(inputLine);
            }

            JSONObject jsonObject = JSONObject.parseObject(json.toString());
            JSONArray jsonArray = jsonObject.getJSONArray("results");
            if(jsonArray.size()>0){

                marketPrice = Double.parseDouble(jsonArray.getJSONArray(0).getString(0));
            }

            in.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return marketPrice;
    }

    public static void main(String[] args) {



        DecimalFormat df = new DecimalFormat("0.0000");
        Double a =1.0;
        Double b=1.0;
        if(Double.valueOf(df.format(-(a-b)))==0.0) {

            System.out.println("step1:"+df.format(-(a-b)));
        } else{
            System.out.println("step2:"+df.format(-(a-b)));
        }

        Date currentTime = new Date();
        SimpleDateFormat sysTime = new SimpleDateFormat("yyyyMMdd");
        String dateString = sysTime.format(currentTime);
        System.out.println("step2:"+dateString);
        //分组排序
        //appleList.sort(comparing(entrust::getId).thenComparing(comparing(entrust::getNum).reversed()));
        //Map<Integer, List<entrust>> groupBy = appleList.stream().collect(Collectors.groupingBy(entrust::getId));

        //System.err.println("groupBy:" + groupBy);
        //取第一条
//        Map<Integer, entrust> appleMap = appleList.stream().collect(Collectors.toMap(entrust::getId, a -> a, (k1, k2) -> k1));
//        System.out.println("appleMap:" + appleMap);
//
//        for(Map.Entry<Integer, entrust> entry : appleMap.entrySet()){
//            System.out.println("key:"+entry.getKey());
//            System.out.println("value:"+entry.getValue().getNum());
//        }

//        Double price = getMarketPrice("http://183.57.43.58:8887/market/json?funcno=20000&version=1&stock_list=SZ:000001&field=2:24");
//        System.out.println("price:" + price);

    }

}
