package com.gszq.bwl;

import com.gszq.utils.HbaseConn;
import com.gszq.utils.MyPropertiesUtil;
import com.gszq.utils.MyRedisUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
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
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



/**
 * 通过hfile形式获取HBASE数据，进行批量计算客户资产，最后汇总写入mongodb。
 */
public class LoadLastDayStockValue {

    private static JavaPairRDD<ImmutableBytesWritable, Result> rdd;
    private static Properties properties = MyPropertiesUtil.load();

    private static String [] contentId = properties.getProperty("content.id").split(",");
    private static String [] business_flag_rule = {"2041", "2042", "2141", "2142"};//业务品种

    private static String kerberosConfPath = properties.getProperty("java.security.krb5.conf");

    private static Properties hbaseproperties = MyPropertiesUtil.load();
    private static Admin admin =null;
    private static Connection connection =null;

    private static String zookeeperQuorum =  hbaseproperties.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = "2181";
    private static String principal = hbaseproperties.getProperty("hbase.master.principal");


    private static String keytabPath = hbaseproperties.getProperty("hbase.master.keytab.file");

    public static Configuration getKrbHbaseConf(){


        final Configuration conf = HBaseConfiguration.create();
        File corefile=new File(hbaseproperties.getProperty("core-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        System.out.println(hbaseproperties.getProperty("core-site.path"));
        File hbasefile=new File(hbaseproperties.getProperty("hdfs-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
        File hdfsfile=new File(hbaseproperties.getProperty("hbase-site.path")); //根据路径创建File类对象--这里路径即使错误也不会报错，因为只是产生File对象，还并未与计算机文件读写有关联
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
    public static synchronized Configuration noKerberos() {

        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum","cdh1.bigdata.com,cdh2.bigdata.com,cdh3.bigdata.com,cdh4.bigdata.com");

        return conf;
    }

    //取普通和信用的持仓，计算市值
    public static void loadAsset2Redis(JavaSparkContext sc,String tableName,String redisTableName) {

        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_REAL_TOTAL_ASSET = "lastday_stockreal_market_value";

        //Configuration conf= noKerberos();
        Configuration conf = getKrbHbaseConf();

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_FUND_ACCOUNT));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_MONEY_TYPE));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TOTAL_ASSET));

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
                    String o1 = Bytes.toString(resultTuple2._2.getRow());//取列的值
                    byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_REAL_TOTAL_ASSET));//取列的值

                    String rowkey = o1;
                    Double last_day_asset = 0.0;

                    if(o2.length>0){
                        last_day_asset = new Double(new String(o2));
                    }


                    return new Tuple2<String, Double>(rowkey, last_day_asset);
                    //待补充股价相乘得到市值
                }
            });

            //清空表
            MyRedisUtil.del(redisTableName);

            //打印出最终结果，写入redis
            List<Tuple2<String, Double>> output = mapToPair.collect();
            Map<String, String> lastAssetData = new HashMap<String, String>();
            for (Tuple2 tuple : output) {
                if(Double.valueOf(tuple._2.toString())!=0.0){
                    lastAssetData.put(tuple._1.toString(),tuple._2.toString());
                }

                //System.out.println(tuple._1 + ": " + tuple._2);
                if(lastAssetData.size()==50000){
                    MyRedisUtil.hmset(redisTableName,lastAssetData);
                    lastAssetData.clear();
                }
            }
            MyRedisUtil.hmset(redisTableName,lastAssetData);

            lastAssetData.clear();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //主函数依次计算
    public static void main(String[] args) throws IOException {

        System.setProperty("java.security.krb5.conf", kerberosConfPath);
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        SparkConf conf1=new SparkConf()
                .setAppName("spark_load_lastday_stockreal_market_value_baowanli")
                .setMaster("local[*]")//设置spark app名称和运行模式（此为local模式）
                ;
        JavaSparkContext sc=new JavaSparkContext(conf1);

        //增加循环实时计算
        try {

            loadAsset2Redis(sc,"RL_ARM:STOCKREAL_MARKET_VALUE","lastday:stockreal:market:value");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}