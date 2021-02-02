package com.gszq.zzw;


import com.gszq.pojo.Entrust;
import com.gszq.utils.OraConn;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gszq.utils.MyPropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


public class RealTimeEntrust {

    private static  Properties properties = MyPropertiesUtil.load();
    private static org.apache.hadoop.hbase.client.Connection connection =null;
    private static Admin admin =null;

    private static String zookeeperQuorum =  properties.getProperty("hbase.zookeeper.quorum");
    private static String clientPort = "2181";
    private static String principal = properties.getProperty("hbase.master.principal");

    private static String kerberosConfPath = properties.getProperty("java.security.krb5.conf");
    private static String keytabPath = properties.getProperty("hbase.master.keytab.file");



    static {
        System.setProperty("java.security.krb5.conf", kerberosConfPath);
        System.setProperty("java.security.auth.login.config", properties.getProperty("java.security.auth.login.config"));
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

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
            UserGroupInformation.setLoginUser(ugi);
            HBaseAdmin.checkHBaseAvailable(conf);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //插入结果表数据
    public static void insertSum(List<Map<String, Object>> list,List<Date> dates,String tablename) throws Exception{

        Map<String,Double> result1 = new HashMap<>();
        Map<String,Integer> result2 = new HashMap<>();
        Multimap<String,Object> multimap1 = ArrayListMultimap.create();
        Multimap<String,Object> multimap2 = ArrayListMultimap.create();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



        list.forEach(p-> {
            multimap1.put(p.get("rowTime").toString(), p.get("rowValue"));
            multimap2.put(p.get("rowTime").toString(), p.get("rowValue"));
        });

        //分组求和
        multimap1.keySet().forEach(key->{
            Double rowSum = multimap1.get(key).stream().map(p->{
                return Double.parseDouble(p.toString());
            }).reduce(0.0,Double::sum);
            result1.put(key,rowSum);
        });

        //分组计数
        multimap2.keySet().forEach(key->{
            Integer rowCount = multimap2.get(key).stream().map(p->{
                return Integer.valueOf("1").intValue();
            }).reduce(0,Integer::sum);
            result2.put(key,rowCount);
        });

        Connection conn=null;
        Statement exsql=null;
        conn = OraConn.getConnection();

        try {
            //获得连接
            exsql=conn.createStatement();

            Iterator<String> iter1 = result1.keySet().iterator();
            while (iter1.hasNext()){
                String multimap1key = iter1.next();
                Double multimap1value = result1.get(multimap1key);  //求和
                Integer multimap2value = result2.get(multimap1key);  //计数



                //创建插入的sql语句
                //String sql="insert into "+tablename+" (INIT_DATE,FUND_ACCOUNT) values('"+p.get("position_str").toString()+"',"+"'"+p.get("fund_account")+"')";
                //String sql="insert into "+tablename+" (INIT_DATE,LOAD_TIME,FUND_ACCOUNT) values('"+p.get("position_str").toString().substring(0,8)+"',"+"'"+p.get("position_str").toString().substring(10,16)+"',"+"'"+p.get("fund_account")+"')";
                String sql = "insert into " + tablename + " (INIT_DATE,LOAD_TIME,ENTRUST_NUM,BALANCE) values('" + multimap1key.substring(0,8) + "'," + "'" + multimap1key.substring(8,14) + "'," + "'" + multimap2value + "'," + "'" + multimap1value + "')";
                System.out.println(sql);

                //返回一个进行此操作的结果，要么成功，要么失败，如果返回的结果>0就是成功，反之失败
                int ora_insert_result = exsql.executeUpdate(sql);
                if (ora_insert_result > 0) {
                    System.out.println("-----------添加result成功----------");
                } else {
                    System.out.println("!!!!!!!!!!添加result失败!!!!!!!!!!");
                }
            }


        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            if (exsql!=null) exsql.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }




    }


    //插入key值数据
    public static void insertDetail(String rowkey) throws Exception{

        String tableName1 = "RL_ARM:ENTRUST_KES";

        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        List<Put> batPut = new ArrayList<Put>();

        //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("the_key"), Bytes.toBytes(rowkey));

        batPut.add(put);
        table.put(batPut);

    }


    //检查key值是否已存在
    public static boolean checkRowKey(String rowkey) {
        ResultScanner scann = null;
        HTable table = null;
        try {
            table = (HTable)connection.getTable(TableName.valueOf("RL_ARM:ENTRUST_KES"));

            Result rs = table.get(new Get(rowkey.getBytes()));
            System.out.println("the value of rowkey:"+rowkey);
            if(rs.isEmpty()){
                return false;
            } else{
                return true;
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally{
            if(null != table){
                try{
                    table.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return  false;
    }


    //private static  ArrayList listRowKey = new ArrayList<String>();  //存放RowKey，已写入的key值


    public static void main(String[] args) throws InterruptedException {


        System.setProperty("java.security.auth.login.config", properties.getProperty("java.security.auth.login.config"));
        System.setProperty("java.security.krb5.conf", properties.getProperty("java.security.krb5.conf"));


        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf()
                .setAppName("RealTimeEntrust")
                //.setMaster("local[*]")
                ;

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //jssc.checkpoint("D:\\streaming_checkpoint");

        //创建map类型以传参
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", properties.getProperty("bootstrap.servers"));
        kafkaParams.put("group.id", "zhouziwei");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //kerberos安全认证
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");



        String topics = properties.getProperty("kafka_topics");
        String[] topicArr = topics.split(",");

        Collection<String> topicSet = Arrays.asList(topicArr);
        //HashSet topicSet = new HashSet<String>();
        //topicSet.add(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS));

        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);



        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> Dstream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                    );



            //表名要改回委托表
            String[] tablename_rule1 = {"ENTRUST","CBSENTRUST","CBPENTRUST"};
            String[] tablename_rule2 = {"","CRDTENTRUST"};
            String[] operType_rule1 = {"I","U"};
            String[] operType_rule2 = {"U"};
            //List<String> listRowKey = new ArrayList<String>();  //存放RowKey，已写入的key值
            //operType  D：delete;I:insert;U:update:DT:truncate;


            //逐一处理每条消息
            Dstream.foreachRDD(rdd-> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                //System.out.print("one-----------------------" + offsetRanges + "-----------------------------------\n");
                rdd.foreachPartition(partitions -> {

                    List<Entrust> EntrustList = new ArrayList<>();
                    //List<Map<String, Object>> list_entrust = new ArrayList<Map<String, Object>>();
                    List<Date> dates = new ArrayList<Date>();

                    partitions.forEachRemaining(line -> {
                        //System.out.print("***************************" + line.value() + "***************************\n");
                        //List<String> list2 = new ArrayList<>();
                        //todo 获取到kafka的每条数据 进行操作
                        //System.out.print("***************************" + s.value() + "***************************");
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


                        JSONObject res1 = JSON.parseObject(line.value());
                        //System.out.print("two-----------------------" + res1.values() + "-----------------------------------\n");
                        String tablename = res1.getString("Tablename");
                        String timeload = res1.getString("timeload");
//                        String regEx = "[^0-9]";
//                        Pattern p = Pattern.compile(regEx);
//                        Matcher m = p.matcher(timeload);
//                        timeload = m.replaceAll("").trim();  //正则表达式转换日期时间格式
                        String operType = res1.getString("operType");
                        try{
                            dates.add(format.parse(timeload));
                        }catch (Exception e){
                            e.printStackTrace();
                        }


                        JSONObject res2 = null;

                        //此处有有修改，原为columnInfo，现改为columns
                        res2 = JSON.parseObject(res1.getString("columnInfo"));
                        System.out.print("test---------------------"+res2);

                        //计算委托数以及委托金额
                        if(Arrays.asList(operType_rule1).contains(operType)
                                &Arrays.asList(tablename_rule1).contains(tablename)) {
                            //处理普通和综合
                            String rowKey1 = res2.getString("POSITION_STR");
                            String timeload1 = timeload;
                            String init_date1 = res2.getString("INIT_DATE");
                            String curr_time_format = StringUtils.leftPad(res2.get("CURR_TIME").toString(),9,"0");
                            String time_id = res2.get("INIT_DATE").toString()+curr_time_format;
                            String rowTime1 = time_id.substring(0,14);
                            BigDecimal rowValue1 = new  BigDecimal(res2.getString("BUSINESS_BALANCE"));
                            Entrust entrust_data1 = new Entrust();
                            entrust_data1.setRowkey(rowKey1);
                            entrust_data1.setTimeload(timeload1);
                            entrust_data1.setInit_date(init_date1);
                            entrust_data1.setRowTime(rowTime1);
                            entrust_data1.setRowValue(rowValue1);
                            boolean isexists = false;
                            try{
                                isexists = checkRowKey(rowKey1);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if(!isexists){

                                EntrustList.add(entrust_data1);
                                try{
                                    insertDetail(rowKey1);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }



                            //System.out.println("数值--" + rowKey1 + "--" + timeload1+ "--" + init_date1 + "--" + rowTime1 +"--" + rowValue1 + "--");
                            //System.out.println("有写入操作--------普通");
                        } else if(Arrays.asList(operType_rule1).contains(operType)
                                &Arrays.asList(tablename_rule2).contains(tablename)){
                            //处理信用
                            String rowKey2 = res2.getString("POSITION_STR");
                            String timeload2 = timeload;
                            String init_date2 = res2.getString("INIT_DATE");
                            String curr_time_format = StringUtils.leftPad(res2.get("CURR_TIME").toString(),9,"0");
                            String time_id = res2.get("INIT_DATE").toString()+curr_time_format;
                            String rowTime2 = time_id.substring(0,14);
                            BigDecimal rowValue2 = new  BigDecimal(res2.getString("BUSINESS_BALANCE"));
                            Entrust entrust_data2 = new Entrust();
                            entrust_data2.setRowkey(rowKey2);
                            entrust_data2.setTimeload(timeload2);
                            entrust_data2.setInit_date(init_date2);
                            entrust_data2.setRowTime(rowTime2);
                            entrust_data2.setRowValue(rowValue2);
                            boolean isexists = false;
                            try{
                                isexists = checkRowKey(rowKey2);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            if(!isexists){

                                EntrustList.add(entrust_data2);
                                try{
                                    insertDetail(rowKey2);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            //System.out.println("数值--" + rowKey2 + "--" + timeload2+ "--" + init_date2 + "--" + rowTime2 +"--" + rowValue2 + "--");
                            //System.out.println("有写入操作--------信用");
                        }


                    });

                    if (EntrustList.size()>0) {
                        //System.out.println("three---------"+EntrustList+"-------");
                        //Iterator it1 = EntrustList.iterator();
                        for (int i=0;i<EntrustList.size();i++){
                            System.out.println("four---------"+EntrustList.get(i).getRowKey()+"-------");
                        }

                    }

                    Date currentTime = new Date();
                    SimpleDateFormat sysTime = new SimpleDateFormat("yyyyMMddHHmmss");
                    String dataString = sysTime.format(currentTime);
                    String timeString = dataString.substring(8,14);

                    //分组排序
                    EntrustList.sort(Comparator.comparing(Entrust::getRowKey).thenComparing(Comparator.comparing(Entrust::getTimeload).reversed()));

                    //降序取第一条数据
                    Map<String,Entrust> EntrustMap = EntrustList.stream().collect(Collectors.toMap(Entrust::getRowKey,a->a,(k1,k2)->k1));
                    //System.out.println("EntrustMap:"+EntrustMap);

                    List<Map<String, Object>> list_entrust = new ArrayList<Map<String, Object>>();


                    for (Map.Entry<String,Entrust> entry : EntrustMap.entrySet()) {
                        Map<String, Object> DistinctMap = new HashMap<String, Object>();
                        DistinctMap.put("timeload", entry.getValue().getRowKey());
                        DistinctMap.put("rowKey", entry.getValue().getTimeload());
                        DistinctMap.put("init_date", entry.getValue().getInit_date());
                        DistinctMap.put("rowTime", entry.getValue().getRowTime());
                        DistinctMap.put("rowValue", entry.getValue().getRowValue());
                        list_entrust.add(DistinctMap);

                    }

                    System.out.println("去重------------"+list_entrust);


                    //调用方法处理数据
                    try{
                        //insertMany(hbtable,list);
                        if (list_entrust.size()>0) {

                            insertSum(list_entrust, dates,"realtime_entrust_amount");
                            System.out.println("###############完成表插入################");


                        } else{
                            System.out.println("没有符合条件的数据");
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                });
                ((CanCommitOffsets) Dstream.inputDStream()).commitAsync(offsetRanges);
            });


            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}