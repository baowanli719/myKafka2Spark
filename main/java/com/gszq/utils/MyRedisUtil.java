package com.gszq.utils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.gszq.pojo.StockTodayIncome;
import org.apache.spark.sql.execution.datasources.json.JsonUtils;
import redis.clients.jedis.*;

import java.text.DecimalFormat;
import java.util.*;

public final  class MyRedisUtil {
    //Redis服务器IP
    private static String ADDR = "192.168.0.100";

    //Redis的端口号
    private static int PORT = 6379;

    //访问密码
    private static String AUTH = "admin";


    private static JedisCluster jedisCluster = null;



    private static Properties properties = MyPropertiesUtil.load();
    private static String hostAndPorts = properties.getProperty("redis.hostAndPort");
    private static String password = properties.getProperty("redis.password");


    public synchronized static JedisCluster getJedisCluster(){

        if(jedisCluster==null){
            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(200);
            poolConfig.setMaxIdle(50);
            poolConfig.setMaxWaitMillis(1000 * 100);
            poolConfig.setTestOnBorrow(false);

            String[] hosts = hostAndPorts.split(",");
            for(String hostport:hosts){
                String[] ipport = hostport.split(":");
                String ip = ipport[0];
                int port = Integer.parseInt(ipport[1]);
                nodes.add(new HostAndPort(ip, port));
            }
            jedisCluster = new JedisCluster(nodes,3000,3000,5,password, poolConfig);
        }
        return jedisCluster;
    }

    public static void set(String key,String value){
        JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.set(key, value);
    }

    public static void hmset(String key,Map<String, String> data){
        JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.hmset(key, data);

    }

    public static String get(String key){
        String value = null;
        JedisCluster jedisCluster = getJedisCluster();
        value = jedisCluster.get(key);
        return value;
    }

    public static void del(String key){
        String value = null;
        JedisCluster jedisCluster = getJedisCluster();
        jedisCluster.del(key);
    }


    public static String hget(String key,String field){
        String value = null;
        JedisCluster jedisCluster = getJedisCluster();
        value = jedisCluster.hget(key,field);
        return value;
    }



    public static void hscanKey(String key1,String key2,String key3,String fundacct){
        JedisCluster jedisCluster = getJedisCluster();
        // 游标初始值为0
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams();
        scanParams.count(1000);
        scanParams.match(fundacct+"*");
//        String key1 = "realtime-crdtstockreal-market-stock";
//        String key2 = "realtime-crdt-crdtrealtime";
//        String key3 = "lastday_crdtstockreal_market_value";
        //使用hscan命令获取500条数据，使用cursor游标记录位置，下次循环使用
        ScanResult<Map.Entry<String, String>> hscanResult1 =
                jedisCluster.hscan(key1, cursor, scanParams);

        ScanResult<Map.Entry<String, String>> hscanResult2 =
                jedisCluster.hscan(key2, cursor, scanParams);

        ScanResult<Map.Entry<String, String>> hscanResult3 =
                jedisCluster.hscan(key3, cursor, scanParams);

        //cursor = hscanResult.getStringCursor();// 返回0 说明遍历完成
        List<Map.Entry<String, String>> scanResult1 =
                hscanResult1.getResult();
        List<StockTodayIncome> stockTodayincomeList= new ArrayList();
        long t1 = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("0.0000");

        for(int m = 0;m < scanResult1.size();m++){
            StockTodayIncome res = new StockTodayIncome();
            Map.Entry<String, String> mapentry = scanResult1.get(m);
            //jedisCluster.hdel(key, mapentry.getKey());
            String stockCode = mapentry.getKey().toString().split("_")[1];
            String lastDayAsset = hget(key3, mapentry.getKey());
            String tradValue = hget(key2, mapentry.getKey());
            String realAsset = mapentry.getValue();
            res.setStockCode(stockCode);

            if(realAsset!=null){
                res.setRealAsset(df.format(Double.valueOf(realAsset)));
            } else{
                res.setRealAsset("0.0000");
            }

            if(lastDayAsset!=null){
                res.setLastDayAsset(df.format(Double.valueOf(lastDayAsset)));
            } else{
                res.setLastDayAsset("0.0000");
            }

            if(tradValue!=null){
                res.setTradeValue(df.format(Double.valueOf(tradValue)));
            } else{
                res.setTradeValue("0.0000");
            }

            Double tradValueTemp = 0.0;
            if(tradValue!=null){
                tradValueTemp = Double.valueOf(tradValue);
            }

            Double lastDayValueTemp = 0.0;
            if(lastDayAsset!=null){
                lastDayValueTemp = Double.valueOf(lastDayAsset);
            }

            Double realValueTemp = 0.0;
            if(mapentry.getValue()!=null){
                realValueTemp = Double.valueOf(mapentry.getValue());
            }


            if(Double.valueOf(df.format(realValueTemp - tradValueTemp - lastDayValueTemp))==0.0) {
                res.setTodayIncome("0.0000");
                System.out.println("setStockTodayIncome3:"+df.format(realValueTemp - tradValueTemp - lastDayValueTemp));
            } else{
                res.setTodayIncome(df.format(realValueTemp - tradValueTemp - lastDayValueTemp));
                //System.out.println("setStockTodayIncome4:"+df.format(realValueTemp - tradValueTemp - lastDayValueTemp));
            }
            //System.out.println("结果：" + mapentry.getKey()+":"+mapentry.getValue());
            stockTodayincomeList.add(res);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("查询" + scanResult1.size()
                + "条数据，耗时: " + (t2-t1) + "毫秒,cursor:" + cursor);

        String json = new Gson().toJson(stockTodayincomeList);
        String jsonString = JSONObject.toJSONString(stockTodayincomeList);
        System.out.println("返回结果："+jsonString);
        System.out.println(json);
    }



    public static void main(String[] args) throws Exception {

        //使用pipeline hmset

//        Map<String,Double> data = new HashMap<String,Double>();
//        long start = System.currentTimeMillis();
//
//        Map<String, String> inviteePhone = new HashMap<String, String>();
//        inviteePhone.put("inviterID", "1001");
//        inviteePhone.put("status", "0");
//        jedisCluster.hmset("inviteePhone", inviteePhone);
//
//        System.out.println(jedisCluster.hget("inviteePhone", "inviterID"));
//        System.out.println(jedisCluster.hget("inviteePhone", "status"));


        //long end = System.currentTimeMillis();
        //System.out.println("hmset with pipeline used [" + (end - start) / 1000 + "] seconds ..");

//        set("name-1", "value-1");
//        set("name-2", "value-2");
//        set("name-3", "value-3");
//        System.out.println(get("name-1"));
//        System.out.println(get("name-2"));
//        System.out.println(get("name-3"));

//        System.out.println(hget("realtime-stockreal","10001_0"));
//        JedisCluster jedisCluster = getJedisCluster();
//
//        ScanParams scanParams = new ScanParams();
//
//        scanParams.match("10001_0*");
//        scanParams.count(500);

//        ScanResult<String> sr = jedisCluster.scan(cursor, scanParams);
//        List<String> resultList = sr.getResult();
//
//        for (String result : resultList) {
//            System.out.println("key: " + result);
//
//            //对key的操作，或者先放到一个集合里面，然后再进行后续操作
//        }
//
//        cursor = sr.getStringCursor();
//        System.out.println("cursor: " + cursor);
        hscanKey("realtime-crdtstockreal-market-stock","realtime-crdt-crdtrealtime","lastday_crdtstockreal_market_value","10001");
        hscanKey("realtime-stockreal-market-stock","realtime-secu-realtime","lastday_stockreal_market_value","10001");


    }
}
