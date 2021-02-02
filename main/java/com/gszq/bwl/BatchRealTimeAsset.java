package com.gszq.bwl;
import com.gszq.pojo.Entrust;
import com.gszq.utils.MyPropertiesUtil;
import com.gszq.utils.MyRedisUtil;
import com.gszq.utils.OraConn;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import parquet.example.data.simple.IntegerValue;
import scala.Tuple2;
import org.apache.hadoop.hbase.KeyValue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import com.gszq.utils.OraSqlProxy;
import scala.Tuple4;

import static java.lang.Math.abs;


/**
 * 通过hfile形式获取HBASE数据，进行批量计算客户资产，最后汇总写入mongodb。
 */
public class BatchRealTimeAsset {

    private static JavaPairRDD<ImmutableBytesWritable, Result> rdd;
    private static Properties properties = MyPropertiesUtil.load();
    private static Admin admin = null;
    private static Connection connection = null;

    private static String zookeeperQuorum = properties.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = "2181";
    private static String principal = properties.getProperty("hbase.master.principal");

    private static String kerberosConfPath = properties.getProperty("java.security.krb5.conf");
    private static String keytabPath = properties.getProperty("hbase.master.keytab.file");
    private static String[] contentId = properties.getProperty("content.id").split(",");
    private static String[] business_flag_rule = {"2041", "2042", "2141", "2142"};//业务品种
    private static String appName = properties.getProperty("app.name");
    private static Integer batchNum = Integer.valueOf(properties.getProperty("batch.num"));
    private static final String[] debenture = {"w", "a", "+", "-", "9", "I", "U", "X", "Y", "u", "Q", "Z", "1", "6", "L", "T", "j", "l", "G", "4"};


    public static synchronized Configuration noKerberos() {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "cdh1.bigdata.com,cdh2.bigdata.com,cdh3.bigdata.com,cdh4.bigdata.com");

        return conf;
    }

    //取普通和信用的持仓，计算市值
    public static void stockreal(JavaSparkContext sc, String tableName, String redisTableName) {

        //String tableName = "RL_ARM:hs_secu_stockreal";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_EXCHANGE_TYPE = "exchange_type";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_CURRENT_AMOUNT = "current_amount";
        String COLUM_CORRECT_AMOUNT = "correct_amount";
        String COLUM_REAL_BUY_AMOUNT = "real_buy_amount";
        String COLUM_REAL_SELL_AMOUNT = "real_sell_amount";


        //Configuration conf= noKerberos();
        Configuration conf = krbConf;

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_EXCHANGE_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_AMOUNT));

        JavaPairRDD<String, Double> reduceByKey = null;


        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            String Point11 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName + "数据总条数：" + hbaseRDD.count());

            String Point12 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.1HS_SECU_STOCKREAL-newAPIHadoopRDD", Point11, Point12);
            //获取产品价格
            HashMap<String, Double> userPrice = getUserPrice();

            String Point13 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.2HS_SECU_STOCKREAL-getUserPrice", Point12, Point13);

            //将Hbase数据转换成PairRDD，资金账户,币种
            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_EXCHANGE_TYPE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_AMOUNT));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_AMOUNT));//取列的值
                    byte[] o6 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_AMOUNT));//取列的值
                    byte[] o7 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_AMOUNT));//取列的值
                    byte[] o8 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));//取列的值

                    String rowkey = new String(o1) + "_" + new String(o2) + "_" + new String(o8);
                    Double amount = 0.0;
                    Double current_amount = 0.0;

                    Double correct_amount = 0.0;
                    Double real_buy_amount = 0.0;
                    Double real_sell_amount = 0.0;

                    if (o4.length > 0) {
                        current_amount = new Double(new String(o4));
                    }
                    //System.out.println("o5数据长度：" + o5.length);

                    if (o5.length > 0) {
                        correct_amount = new Double(new String(o5));
                    }

                    if (o6.length > 0) {
                        real_buy_amount = new Double(new String(o6));
                    }

                    if (o7.length > 0) {
                        real_sell_amount = new Double(new String(o7));
                    }


                    amount = current_amount + correct_amount + real_buy_amount - real_sell_amount;
                    String price_key = new String(o2) + "_" + new String(o3);
                    Double inMarketValue = 0.0;
                    Double userPriceVal = 0.0;

                    if (userPrice.containsKey(price_key)) {
                        userPriceVal = userPrice.get(price_key);
                    }

                    if (userPriceVal == null) {
                        userPriceVal = 0.0;
                    }

                    inMarketValue = amount * userPriceVal;


                    return new Tuple2<String, Double>(rowkey, inMarketValue);
                    //待补充股价相乘得到市值
                }
            });

            String Point14 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.3HS_SECU_STOCKREAL-mapToPair", Point13, Point14);

            JavaPairRDD<String, Double> fundaccountPair = mapToPair.mapToPair(w -> new Tuple2<String, Double>(
                    w._1.split("_")[0] + "_" + w._1.split("_")[2], w._2)
            );

            String Point15 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.4HS_SECU_STOCKREAL-fundaccountPair", Point14, Point15);

            //按年龄降序排序
            //JavaPairRDD<String, Integer> sortByKey = mapToPair.sortByKey(false);
            //按账户+币种汇总
            reduceByKey = fundaccountPair.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });
            String Point16 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.5HS_SECU_STOCKREAL-reduceByKey", Point15, Point16);

            //清空表
            //MyRedisUtil.del(redisTableName+"-market");

            //打印出最终结果，写入redis
            List<Tuple2<String, Double>> result = null;
            Map<String, String> stockrealData = new HashMap<String, String>();
            Map<String, String> stockrealStockData = new HashMap<String, String>();

            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    stockrealData.put(each._1.toString(), each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if (stockrealData.size() > batchNum) {
                        MyRedisUtil.hmset(redisTableName + "-market", stockrealData);
                        stockrealData.clear();
                    }
                });

                if (stockrealData.size() > 0) {
                    MyRedisUtil.hmset(redisTableName + "-market", stockrealData);
                    stockrealData.clear();
                }
            });

            String Point17 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.6HS_SECU_STOCKREAL-stockrealData", Point16, Point17);

            mapToPair.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    stockrealStockData.put(each._1.toString(), each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if (stockrealStockData.size() > batchNum) {
                        MyRedisUtil.hmset(redisTableName + ":market:stock", stockrealStockData);
                        stockrealStockData.clear();
                    }
                });

                if (stockrealStockData.size() > 0) {
                    MyRedisUtil.hmset(redisTableName + ":market:stock", stockrealStockData);
                    stockrealStockData.clear();
                }
            });

            String Point18 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
            insertOracle("1.7HS_SECU_STOCKREAL-stockrealStockData", Point17, Point18);

//            //打印出最终结果，将持仓写入mongodb
//            List<Tuple2<String, Double>> output2 = mapToPair.collect();
//            Map<String, String> stockrealStockData = new HashMap<String, String>();
//            for (Tuple2 tuple : output2) {
//                stockrealStockData.put(tuple._1.toString(),tuple._2.toString());
//                //System.out.println(tuple._1 + ": " + tuple._2);
//                if(stockrealStockData.size()==50000){
//                    MyRedisUtil.hmset(redisTableName+"-market-stock",stockrealStockData);
//                    stockrealStockData.clear();
//                }
//            }
//            MyRedisUtil.hmset(redisTableName+"-market-stock",stockrealStockData);
//            stockrealStockData.clear();
//
//            String Point18= new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
//            insertOracle("1.7HS_SECU_STOCKREAL-stockrealStockData",Point17,Point18);
//

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        //return reduceByKey;
    }

    //获取场外金融产品价格
    public static HashMap<String, Double> getProdPrice() {

        String tableName = "RL_ARM:HS_PROD_PRODCODE";
        String FAMILY = "info";
        String COLUM_PRODE_CODE = "prod_code";
        String COLUM_PRODA_NO = "prodta_no";
        String COLUM_NAV = "nav";


        //Configuration conf= noKerberos();
        Configuration conf = krbConf;

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_PRODE_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_PRODA_NO));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAV));

        UserGroupInformation.setConfiguration(conf);
        try {
            //UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/cdh3@HD.COM", "D:\\myconf\\hbase.keytab");
            //UserGroupInformation.setLoginUser(ugi);
            HBaseAdmin.checkHBaseAvailable(conf);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }

        HTable table = null;
        ResultScanner scan_1 = null;
        String prod_code = null;
        String prodta_no = null;
        Double navVal = 0.0;
        HashMap<String, Double> map = new HashMap<>();
        try {
            //Admin admin = connection.getAdmin();
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            scan_1 = table.getScanner(new Scan());
            for (Result rs : scan_1) {
                for (Cell cell : rs.rawCells()) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength());
                    if (colName.equals("nav")) {
                        navVal = Double.parseDouble(value);
                    }
                    if (colName.equals("prodta_no")) {
                        prodta_no = value;
                    }
                    if (colName.equals("prod_code")) {
                        prod_code = value;
                    }

                }
                map.put(prod_code + "_" + prodta_no, navVal);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                if (null != connection) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return map;
    }


    //获取场外金融产品价格
    public static HashMap<String, Double> getUserPrice() {

        String tableName = "RL_ARM:HS_USER_PRICE";
        String FAMILY = "info";
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_EXCHANGE_TYPE = "exchange_type";
        String COLUM_ASSET_PRICE = "asset_price";
        String COLUM_MARKET_PRICE = "market_price";

        //Configuration conf= noKerberos();
        Configuration conf = krbConf;

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_EXCHANGE_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ASSET_PRICE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MARKET_PRICE));

        UserGroupInformation.setConfiguration(conf);
        try {
            //UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/cdh3@HD.COM", "D:\\myconf\\hbase.keytab");
            //UserGroupInformation.setLoginUser(ugi);
            HBaseAdmin.checkHBaseAvailable(conf);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }

        HTable table = null;
        ResultScanner scan_1 = null;
        String stock_code = null;
        String exchange_type = null;
        BigDecimal price_temp = null;
        HashMap<String, Double> map = new HashMap<>();
        try {
            //Admin admin = connection.getAdmin();
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
            scan_1 = table.getScanner(new Scan());
            Double asset_price = 0.0;
            Double market_price = 0.0;
            List<String> colnames = new ArrayList<String>();
            colnames.add("123");
            for (Result rs : scan_1) {
                //System.out.println("--------" + Bytes.toString(rs.getRow()));

                for (Cell cell : rs.rawCells()) {
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength());
                    if (colName.equals("asset_price")) {
                        price_temp = new BigDecimal(value);
                        asset_price = price_temp.doubleValue();
                    }
                    if (colName.equals("stock_code")) {
                        stock_code = value;
                    }
                    if (colName.equals("exchange_type")) {
                        exchange_type = value;
                    }
                    if (colName.equals("market_price")&&colName!=null) {
                        market_price = Double.parseDouble(value);
                    }
                    if(colName!=null&&!colName.isEmpty()){
                        colnames.add(colName);
                       // System.out.println("------------" + colName+":"+value);
                    }

                    //System.out.println("------------" + colName);
                }

                if (market_price > 0.0 && colnames.contains("market_price")) {
                    asset_price = market_price;
                }
                //System.out.println("---------" + asset_price+":"+market_price);
                map.put(stock_code + "_" + exchange_type, asset_price);
                colnames.clear();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                if (null != connection) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return map;
    }
    //获取场外金融产品持仓
    public static void secumreal(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_PROD_SECUMREAL";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_CURRENT_AMOUNT = "current_amount";
        String COLUM_CORRECT_AMOUNT = "correct_amount";
        String COLUM_REAL_BUY_AMOUNT = "real_buy_amount";
        String COLUM_REAL_SELL_AMOUNT = "real_sell_amount";
        String COLUM_RPODTA_NO = "prodta_no";
        String COLUM_PROD_CODE = "prod_code";

        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_AMOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_RPODTA_NO));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_PROD_CODE));

        JavaPairRDD<String, Double> reduceByKey  = null;



        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());
            //获取产品价格
            HashMap<String, Double> prodPrice = getProdPrice();
            //System.out.println(prodPrice);

            //将Hbase数据转换成PairRDD，资金账户,币种
            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {
                private static final long serialVersionUID = -2437063503351644147L;

                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_AMOUNT));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_AMOUNT));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_AMOUNT));//取列的值
                    byte[] o6 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_AMOUNT));//取列的值
                    byte[] o7 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_RPODTA_NO));//取列的值
                    byte[] o8 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_PROD_CODE));//取列的值

                    String rowkey = new String(o1)+"_"+new String(o2);
                    Double amount = 0.0;
                    amount = new Double(new String(o3))+new Double(new String(o4))+new Double(new String(o5))-new Double(new String(o6));
                    String price_key = new String(o8)+"_"+new String(o7);
                    Double outMarketValue = 0.0;
                    Double prodPriceVal =0.0;

                    if(prodPrice.containsKey(price_key)){
                        prodPriceVal = prodPrice.get(price_key);
                    }

                    if(prodPriceVal ==  null){
                        prodPriceVal = 0.0;
                    }

                    outMarketValue = amount * prodPriceVal;

                    //System.out.println(rowkey+ ": " + amount+":"+prodPriceVal);
                    return new Tuple2<String, Double>(rowkey, outMarketValue);
                }
            });

            //按年龄降序排序
            //JavaPairRDD<String, Integer> sortByKey = mapToPair.sortByKey(false);
            reduceByKey = mapToPair.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });

            //打印出最终结果
            Map<String, String> secumrealData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    secumrealData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(secumrealData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:secumreal:market",secumrealData);
                        secumrealData.clear();

                    }
                });

                if(secumrealData.size()>0){
                    MyRedisUtil.hmset("realtime:secumreal:market",secumrealData);
                    secumrealData.clear();
                }

            });


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        //return reduceByKey;
    }


    //获取资金余额
    public static void fundreal(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_FUND_FUNDREAL";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_CURRENT_BALANCE = "current_balance";
        String COLUM_CORRECT_BALANCE = "correct_balance";
        String COLUM_REAL_BUY_BALANCE = "real_buy_balance";
        String COLUM_REAL_SELL_BALANCE = "real_sell_balance";


        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_BALANCE));


        JavaPairRDD<String, Double> reduceByKey  = null;



        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());


            //将Hbase数据转换成PairRDD，资金账户,币种
            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {
                private static final long serialVersionUID = -2437063503351644147L;

                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CURRENT_BALANCE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_CORRECT_BALANCE));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_BUY_BALANCE));//取列的值
                    byte[] o6 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_SELL_BALANCE));//取列的值

                    String rowkey = new String(o1)+"_"+new String(o2);
                    Double balance = 0.0;
                    balance = new Double(new String(o3))+new Double(new String(o4))-new Double(new String(o5))+new Double(new String(o6));


                    return new Tuple2<String, Double>(rowkey, balance);
                }
            });

            //汇总

//            reduceByKey = mapToPair.reduceByKey(new Function2<Double, Double, Double>() {
//                public Double call(Double i1, Double i2) {
//                    return i1 + i2;
//                }
//            });

            reduceByKey = mapToPair.filter(new Function<Tuple2<String, Double>, Boolean>() {
                public Boolean call(Tuple2<String, Double> subTuple2) throws Exception {
                    if (abs(subTuple2._2) > 0.0){
                        return true;
                    }
                    return false;
                }
            });

            //打印出最终结果
            Map<String, String> fundrealData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    fundrealData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(fundrealData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:fund:fundreal",fundrealData);
                        fundrealData.clear();
                    }
                });

                if(fundrealData.size()>0){
                    MyRedisUtil.hmset("realtime:fund:fundreal",fundrealData);
                    fundrealData.clear();
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        //return reduceByKey;
    }

    //计算信用净资产
    public static void crdtasset(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_CRDT_COMPACT";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_COMPACT_TYPE = "compact_type";
        String COLUM_COMPACT_BALANCE = "real_compact_balance";
        String COLUM_COMPACT_FARE = "real_compact_fare";



        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_FARE));

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUM_COMPACT_TYPE),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("0"));
        scan.setFilter(filter1);

        JavaPairRDD<String, Double> reduceByKey  = null;

        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());


            //将Hbase数据转换成PairRDD，资金账户,币种
            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {
                private static final long serialVersionUID = -2437063503351644147L;

                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_TYPE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_BALANCE));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_COMPACT_FARE));//取列的值

                    String rowkey = new String(o1)+"_"+new String(o2);
                    Double balance = 0.0;
                    balance = new Double(new String(o4))+new Double(new String(o5));

                    return new Tuple2<String, Double>(rowkey, balance);
                }
            });

            //按年龄降序排序
            //JavaPairRDD<String, Integer> sortByKey = mapToPair.sortByKey(false);
            reduceByKey = mapToPair.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });

            //打印出最终结果
            Map<String, String> compactData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    compactData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(compactData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:crdt:compact",compactData);
                        compactData.clear();

                    }
                });

                if(compactData.size()>0){
                    MyRedisUtil.hmset("realtime:crdt:compact",compactData);
                    compactData.clear();

                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //计算资金净转入
    public static void fundjour(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_ASSET_FUNDJOUR";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_OCCUR_BALANCE = "occur_balance";
        String COLUM_INIT_DATE = "init_date";



        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_OCCUR_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_INIT_DATE));

        Date currentTime = new Date();
        SimpleDateFormat sysTime = new SimpleDateFormat("yyyyMMdd");
        String dateString = sysTime.format(currentTime);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUM_INIT_DATE),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(dateString));
        scan.setFilter(filter1);


        JavaPairRDD<String, Double> reduceByKey  = null;

        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());


            //将Hbase数据转换成PairRDD，资金账户,币种
            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_OCCUR_BALANCE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_INIT_DATE));//取列的值

                    String rowkey = new String(o1)+"_"+new String(o2);
                    Double balance = 0.0;
                    balance = new Double(new String(o3));

                    return new Tuple2<String, Double>(rowkey, balance);
                }
            });

            //按年龄降序排序
            //JavaPairRDD<String, Integer> sortByKey = mapToPair.sortByKey(false);
            reduceByKey = mapToPair.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });

            //打印出最终结果
            Map<String, String> fundjourData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    fundjourData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(fundjourData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:fund:jour",fundjourData);
                        fundjourData.clear();

                    }
                });

                if(fundjourData.size()>0){
                    MyRedisUtil.hmset("realtime:fund:jour",fundjourData);
                    fundjourData.clear();
                }

            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //计算实时成交
    public static void realtime(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_SECU_REALTIME";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_BUSINESS_BALANCE = "business_balance";
        String COLUM_INIT_DATE = "init_date";
        String COLUM_REAL_TYPE = "real_type";
        String COLUM_REAL_STATUS = "real_status";
        String COLUM_ENTRUST_BS = "entrust_bs";
        String[] real_status_rule = {"0","4"};//处理标志
        String[] real_type_rule1 = {"0"};//成交类型
        String[] real_type_rule2 = {"6","7","8","9"};//成交类型


        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_BUSINESS_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_INIT_DATE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_STATUS));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ENTRUST_BS));

        Date currentTime = new Date();
        SimpleDateFormat sysTime = new SimpleDateFormat("yyyyMMdd");
        String dateString = sysTime.format(currentTime);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUM_INIT_DATE),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(dateString));
        scan.setFilter(filter1);

        JavaPairRDD<String, Double> reduceByKey  = null;

        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());


            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {

                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_BUSINESS_BALANCE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TYPE));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_STATUS));//取列的值
                    byte[] o6 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ENTRUST_BS));//取列的值
                    String rowkey = new String(o1)+"_"+new String(o2)+"_"+new String(o4)+"_"+new String(o5);
                    Double balance = 0.0;
                    String trad_drection = new String(o6);
                    double trad_flag = 1.0;
                    if(trad_drection.equals("2")){
                        trad_flag=-1.0;
                    }

                    balance = new Double(new String(o3))*trad_flag;

                    return new Tuple2<String, Double>(rowkey, balance);
                }
            });


            JavaPairRDD<String, Double> realtimePair1 = mapToPair.filter(new Function<Tuple2<String, Double>, Boolean>() {
                public Boolean call(Tuple2<String, Double> stringStringTuple2) throws Exception {

                    String realType = stringStringTuple2._1.split("_")[2];
                    String realStatus = stringStringTuple2._1.split("_")[3];
                    if (Arrays.asList(real_status_rule).contains(realStatus)
                            &&Arrays.asList(real_type_rule1).contains(realType)){
                        return true;
                    }
                    return false;
                }
            });

            JavaPairRDD<String, Double> realtimePair2 = realtimePair1.mapToPair(w -> new Tuple2<String, Double>(
                    w._1.split("_")[0]+"_"+w._1.split("_")[1] , w._2)
            );

            //按年龄降序排序
            reduceByKey = realtimePair2.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });



            //打印出最终结果
            Map<String, String> realtimeData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    realtimeData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(realtimeData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:secu:realtime",realtimeData);
                        realtimeData.clear();

                    }
                });

                if(realtimeData.size()>0){
                    MyRedisUtil.hmset("realtime:secu:realtime",realtimeData);
                    realtimeData.clear();
                }

            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //计算资金净转入
    public static void crdtrealtime(JavaSparkContext sc ) {

        String tableName = "RL_ARM:HS_CRDT_CRDTREALTIME";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_BUSINESS_BALANCE = "business_balance";
        String COLUM_INIT_DATE = "init_date";
        String COLUM_REAL_TYPE = "real_type";
        String COLUM_REAL_STATUS = "real_status";
        String COLUM_ENTRUST_BS = "entrust_bs";
        String[] real_status_rule = {"0","4"};//处理标志
        String[] real_type_rule1 = {"0"};//成交类型
        String[] real_type_rule2 = {"6","7","8","9"};//成交类型


        //Configuration conf= noKerberos();
        Configuration conf= krbConf;
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_BUSINESS_BALANCE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_INIT_DATE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_STATUS));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ENTRUST_BS));

        Date currentTime = new Date();
        SimpleDateFormat sysTime = new SimpleDateFormat("yyyyMMdd");
        String dateString = sysTime.format(currentTime);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes(FAMILY),Bytes.toBytes(COLUM_INIT_DATE),CompareFilter.CompareOp.EQUAL,Bytes.toBytes(dateString));
        scan.setFilter(filter1);

        JavaPairRDD<String, Double> reduceByKey  = null;

        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            conf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println(tableName+"数据总条数：" + hbaseRDD.count());


            JavaPairRDD<String, Double> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, Double>() {

                @Override
                public Tuple2<String, Double> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2)throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_STOCK_CODE));//取列的值
                    byte[] o3 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_BUSINESS_BALANCE));//取列的值
                    byte[] o4 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TYPE));//取列的值
                    byte[] o5 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_STATUS));//取列的值
                    byte[] o6 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_ENTRUST_BS));//取列的值
                    String rowkey = new String(o1)+"_"+new String(o2)+"_"+new String(o4)+"_"+new String(o5);
                    Double balance = 0.0;
                    String trad_drection = new String(o6);
                    double trad_flag = 1.0;
                    if(trad_drection.equals("2")){
                        trad_flag=-1.0;
                    }

                    balance = new Double(new String(o3))*trad_flag;

                    return new Tuple2<String, Double>(rowkey, balance);
                }
            });


            JavaPairRDD<String, Double> crdtrealtimePair1 = mapToPair.filter(new Function<Tuple2<String, Double>, Boolean>() {
                public Boolean call(Tuple2<String, Double> stringStringTuple2) throws Exception {

                    String realType = stringStringTuple2._1.split("_")[2];
                    String realStatus = stringStringTuple2._1.split("_")[3];
                    if (Arrays.asList(real_status_rule).contains(realStatus)
                            &&Arrays.asList(real_type_rule2).contains(realType)){
                        return true;
                    }
                    return false;
                }
            });

            JavaPairRDD<String, Double> crdtrealtimePair2 = crdtrealtimePair1.mapToPair(w -> new Tuple2<String, Double>(
                    w._1.split("_")[0]+"_"+w._1.split("_")[1] , w._2)
            );

            //按年龄降序排序
            reduceByKey = crdtrealtimePair2.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double i1, Double i2) {
                    return i1 + i2;
                }
            });



            //打印出最终结果
            Map<String, String> crdtrealtimeData = new HashMap<String, String>();
            reduceByKey.foreachPartition(partition -> {
                partition.forEachRemaining(each -> {
                    crdtrealtimeData.put(each._1.toString(),each._2.toString());
                    //System.out.println(each._1.toString() + ": " + each._2.toString());
                    //插入数据
                    if(crdtrealtimeData.size()>batchNum){
                        MyRedisUtil.hmset("realtime:crdt:crdtrealtime",crdtrealtimeData);
                        crdtrealtimeData.clear();

                    }
                });

                if(crdtrealtimeData.size()>0){
                    MyRedisUtil.hmset("realtime:crdt:crdtrealtime",crdtrealtimeData);
                    crdtrealtimeData.clear();
                }

            });


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Configuration krbConf = getKrbHbaseConf();

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

        try {
            // 使用票据登陆Kerberos
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            connection = ConnectionFactory.createConnection(conf, executor);
            //connection = ConnectionFactory.createConnection(conf);
            System.out.println(connection.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return conf;

    }
    public  synchronized Connection  createConnection(){
        try {
            // 使用票据登陆Kerberos
            Configuration conf = getKrbHbaseConf();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            connection = ConnectionFactory.createConnection(conf, executor);
            System.out.println(connection.hashCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
    /**
     * 判断时间是否在时间段内
     *
     * @param nowTime
     * @param beginTime
     * @param endTime
     * @return
     */
    public static boolean belongCalendar(Date nowTime, Date beginTime,
                                         Date endTime) {
        Calendar date = Calendar.getInstance();
        date.setTime(nowTime);

        Calendar begin = Calendar.getInstance();
        begin.setTime(beginTime);

        Calendar end = Calendar.getInstance();
        end.setTime(endTime);

        Calendar cal = Calendar.getInstance();
        cal.setTime(nowTime);
        Boolean weekendFlag = false;
        if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
            weekendFlag = false;
        } else{
            weekendFlag = true;
        }


        if (date.after(begin) && date.before(end) && weekendFlag) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean insertOracle(String step_name, String beginTime, String endTime) {
        try{
            OraSqlProxy oraconn = new OraSqlProxy();
            java.sql.Connection OraClient = OraConn.getConnection();
            oraconn.executeQuery(OraClient
                    ,"INSERT INTO spark_realtime_runtime_log (step_name,start_time,end_time) values ('"+step_name+"','"+beginTime+"','"+endTime+"')");
            oraconn.shutdown(OraClient);
        } catch (SQLException e) {
            e.printStackTrace();
            return  false;
        }
        return true;
    }



    //主函数依次计算
    public static void main(String[] args) throws IOException {


        SparkConf conf1=new SparkConf()
                .setAppName(appName)
                //.setMaster("local[*]");//设置spark app名称和运行模式（此为local模式）
                ;
        JavaSparkContext sc=new JavaSparkContext(conf1);

        while(true) {
        //for(int i=0;i<1;i++){


            //增加循环实时计算
            try {


                //----01
                if(Arrays.asList(contentId).contains("1")){
                    String Point11 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    stockreal(sc,"RL_ARM:HS_SECU_STOCKREAL","realtime:stockreal");
                    String Point12= new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("1HS_SECU_STOCKREAL",Point11,Point12);
                }

                //----02
                if(Arrays.asList(contentId).contains("2")){
                    String Point21 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    stockreal(sc,"RL_ARM:HS_CRDT_CRDTSTOCKREAL","realtime:crdtstockreal");
                    String Point22 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("2HS_CRDT_CRDTSTOCKREAL",Point21,Point22);

                }

                //----03
                if(Arrays.asList(contentId).contains("3")){
                    String Point31 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    secumreal(sc);
                    String Point32 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("3HS_PROD_SECUMREAL",Point31,Point32);

                }

                //----04
                if(Arrays.asList(contentId).contains("4")) {
                    String Point41 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    fundreal(sc);
                    String Point42 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("4HS_FUND_FUNDREAL",Point41,Point42);

                }

                //----05
                if(Arrays.asList(contentId).contains("5")) {
                    String Point51 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    crdtasset(sc);
                    String Point52 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("5HS_CRDT_COMPACT",Point51,Point52);
                }

                //----06
                if(Arrays.asList(contentId).contains("6")) {
                    String Point61 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    fundjour(sc);
                    String Point62 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("6HS_ASSET_FUNDJOUR",Point61,Point62);
                }

                //----07
                if(Arrays.asList(contentId).contains("7")) {
                    String Point71 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    realtime(sc);
                    String Point72 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("7HS_SECU_REALTIME",Point71,Point72);
                }

                //----08
                if(Arrays.asList(contentId).contains("8")) {
                    String Point81 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    crdtrealtime(sc);
                    String Point82 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
                    insertOracle("8HS_CRDT_CRDTREALTIME",Point81,Point82);
                }


            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }
//            //打印日志
//            String Point12 = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date());
//            System.out.println("Timecal:stime:"+Point11+",endtime:"+Point12+",interval:");
//            System.out.println("INSERT INTO spark_realtime_runtime_log (start_time,end_time) values ("+Point11+","+Point12+")");
//

            //判断查询时间
            SimpleDateFormat df = new SimpleDateFormat("HH:mm");// 设置日期格式
            Date now = null;
            Date beginTime = null;
            Date endTime = null;

            try {
                now = df.parse(df.format(new Date()));
                beginTime = df.parse("09:10");
                endTime = df.parse("19:00");
                Boolean flag = belongCalendar(now, beginTime, endTime);
                if(!flag){
                    Thread.sleep(600000);
                }

                beginTime = df.parse("08:00");
                endTime = df.parse("09:00");
                Boolean flag2 = belongCalendar(now, beginTime, endTime);
                if(flag2){
                    Thread.sleep(600000);
                    MyRedisUtil.del("realtime:stockreal:market:stock");
                    MyRedisUtil.del("realtime:stockreal:market");
                    MyRedisUtil.del("realtime:crdtstockreal:market:stock");
                    MyRedisUtil.del("realtime:crdtstockreal:market");
                    MyRedisUtil.del("realtime:secumreal:market");
                    MyRedisUtil.del("realtime:crdt:compact");
                    MyRedisUtil.del("realtime:fund:fundreal");
                    MyRedisUtil.del("realtime:secu:realtime");
                    MyRedisUtil.del("realtime:crdt:crdtrealtime");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

}