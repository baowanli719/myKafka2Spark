package com.gszq.bwl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.gszq.pojo.UserCode;
import com.gszq.utils.MyPropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GetMarketPrice {

    private static Properties properties = MyPropertiesUtil.load();
    private static Admin admin =null;
    private static String zookeeperQuorum =  properties.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = "2181";
    private static String principal = properties.getProperty("hbase.master.principal");
    private static String kerberosConfPath = properties.getProperty("java.security.krb5.conf");
    private static String keytabPath = properties.getProperty("hbase.master.keytab.file");
    private static Configuration krbConf = getKrbHbaseConf();
    private static final String priceUrl = properties.getProperty("price.url");

    public static Configuration getKrbHbaseConf(){

        System.setProperty("java.security.krb5.conf", kerberosConfPath);
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        final Configuration conf = HBaseConfiguration.create();
        File corefile=new File(properties.getProperty("core-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hbasefile=new File(properties.getProperty("hdfs-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hdfsfile=new File(properties.getProperty("hbase-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        try
        {
            FileInputStream coreStream=new FileInputStream(corefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(coreStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }
        try
        {
            FileInputStream hbaseStream=new FileInputStream(hbasefile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hbaseStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }

        try
        {
            FileInputStream hdfsStream=new FileInputStream(hdfsfile);//与根据File类对象的所代表的实际文件建立链接创建fileInputStream对象
            conf.addResource(hdfsStream);
        }
        catch (FileNotFoundException e)
        {

            System.out.println("文件不存在或者文件不可读或者文件是目录");
        }
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("hbase.client.operation.timeout", 30000);
        conf.setInt("hbase.client.scanner.timeout.period", 200000);

//        try {
//            // 使用票据登陆Kerberos
//            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
//            ExecutorService executor = Executors.newFixedThreadPool(10);
//            connection = ConnectionFactory.createConnection(conf, executor);
//            //connection = ConnectionFactory.createConnection(conf);
//            System.out.println(connection.hashCode());
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(1);
//        }

        return conf;

    }

    // 获取市场行情的价格
    public static void getMarketPrice(String stockCodes,String exchangeType) {
        Connection connection = createConnection();
        StringBuilder json = new StringBuilder();
        Double marketPrice = 0.0;
        String stockCode = "";
        TableName tableName = TableName.valueOf("RL_ARM:HS_USER_PRICE");

        if (stockCodes.endsWith("|")) {
            stockCodes = stockCodes.substring(0,stockCodes.length() - 1);
        }
        List<Put> batPut = new ArrayList<Put>();
        try {
            Table table = connection.getTable(tableName);
            System.out.println(priceUrl+stockCodes);

            URL urlObject = new URL(priceUrl+stockCodes+"&field=2:24");

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

                for(int j=0;j<jsonArray.size();j++){
                    marketPrice = Double.parseDouble(jsonArray.getJSONArray(j).getString(0));
                    stockCode = jsonArray.getJSONArray(j).getString(1);
                    Put put = new Put(Bytes.toBytes(stockCode+"_"+exchangeType));

                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("market_price"), Bytes.toBytes(String.valueOf(marketPrice)));
                    //单记录put

                    batPut.add(put);
                }
                table.put(batPut);
            }

            in.close();
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取场外金融产品价格
    public static List<UserCode>getUserCode() {

        Connection connection = createConnection();

        String tableName = "RL_ARM:HS_USER_PRICE";
        String FAMILY = "info";
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_EXCHANGE_TYPE = "exchange_type";
        String COLUM_ASSET_PRICE = "asset_price";
        String COLUM_MONEY_TYPE = "money_type";


        //Configuration conf= noKerberos();
        Configuration conf= krbConf;

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_EXCHANGE_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ASSET_PRICE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));

        HTable table = null;
        ResultScanner scan_1 = null;
        String stock_code = null;
        String exchange_type = null;
        String money_type = null;
        Double asset_price = 0.0;
        HashMap<String, String> map = new HashMap<>();
        List<UserCode> stockList =   new ArrayList<UserCode>();

        try{
            //Admin admin = connection.getAdmin();
            table = (HTable)connection.getTable(TableName.valueOf(tableName));
            scan_1 = table.getScanner(new Scan());
            for(Result rs : scan_1){
                UserCode stock_one = new UserCode();
                for(Cell cell : rs.rawCells()){
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength());
                    if (colName.equals("asset_price")) {
                        asset_price = Double.parseDouble(value);
                    }
                    if (colName.equals("stock_code")) {
                        stock_code = value;
                    }
                    if (colName.equals("exchange_type")) {
                        exchange_type = value;
                    }
                    if (colName.equals("money_type")) {
                        money_type = value;
                    }

                }
                //map.put(stock_code,exchange_type);
                if(stock_code != null && stock_code.length() != 0){
                    stock_one.setStockCcode(stock_code);
                    stock_one.setMoneyType(money_type);
                    stock_one.setExchangeType(exchange_type);
                    stock_one.setMarketPrice(0.0);
                    stockList.add(stock_one);
                }

            }
            System.out.println("123");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return stockList;
    }

    //调行情接口获取real_in_market_value(证券市值)值
    public static void queryTableprice(List<UserCode> stockList) throws IOException, JSONException {

        String stockListMap_G="";
        String stockListMap_S="";
        String stockListMap_2="";
        String stockListMap_9="";
        String stockListMap_1="";
        for(int i=0;i<stockList.size();i++) {
            if (stockList.get(i).getExchangeType().equals("G")) {
                stockListMap_G = stockListMap_G + "HK:" + stockList.get(i).getStockCcode();
                stockListMap_G = stockListMap_G + "|";
                if(stockListMap_G.length()>600){
                    getMarketPrice(stockListMap_G,"G");
                    stockListMap_G = "";
                }
            } else if (stockList.get(i).getExchangeType().equals("S")) {
                stockListMap_S = stockListMap_S + "HK:" + stockList.get(i).getStockCcode();
                stockListMap_S = stockListMap_S + "|";
                if(stockListMap_S.length()>600){
                    getMarketPrice(stockListMap_S,"S");
                    stockListMap_S = "";
                }
            } else if ( stockList.get(i).getExchangeType().equals("2")) {
                stockListMap_2 = stockListMap_2 + "SZ:" + stockList.get(i).getStockCcode();
                stockListMap_2 = stockListMap_2 + "|";

                if(stockListMap_2.length()>700){
                    getMarketPrice(stockListMap_2,"2");
                    stockListMap_2 = "";
                }
            }else if ( stockList.get(i).getExchangeType().equals("9")) {
                stockListMap_9 = stockListMap_9 + "SZ:" + stockList.get(i).getStockCcode();
                stockListMap_9 = stockListMap_9 + "|";
                if(stockListMap_9.length()>700){
                    getMarketPrice(stockListMap_9,"9");
                    stockListMap_9 = "";
                }
            } else if ( stockList.get(i).getExchangeType().equals("1")) {
                stockListMap_1 = stockListMap_1 + "SH:" + stockList.get(i).getStockCcode();
                stockListMap_1 = stockListMap_1 + "|";
                if(stockListMap_1.length()>700){
                    getMarketPrice(stockListMap_1,"1");
                    stockListMap_1 = "";
                }
            }


        }
        if(stockListMap_G.length()>0){
            getMarketPrice(stockListMap_G,"G");
        }

        if(stockListMap_S.length()>0){
            getMarketPrice(stockListMap_S,"S");
        }

        if(stockListMap_2.length()>0){
            getMarketPrice(stockListMap_2,"2");
        }

        if(stockListMap_9.length()>0){
            getMarketPrice(stockListMap_9,"9");
        }

        if(stockListMap_1.length()>0){
            getMarketPrice(stockListMap_1,"1");
        }
    }

    public static synchronized Connection  createConnection(){

        Connection conn = null;
        try {
            // 使用票据登陆Kerberos
            Configuration conf = getKrbHbaseConf();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            conn = ConnectionFactory.createConnection(conf, executor);
            System.out.println(conn.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    //主函数依次计算
    public static void main(String[] args){


        String Point11 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());


        //增加循环实时计算
        try {

            List<UserCode> stockList = getUserCode();
//            queryTableprice(stockList);
            System.out.println("11");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("12");
        } finally {

            System.out.println("15");
        }
    }
}
