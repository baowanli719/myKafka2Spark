package com.gszq.bwl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class HbaseTest {


    private static Connection connection =null;
    private static Admin admin =null;

    static {
        System.setProperty("java.security.krb5.conf", "D:\\myconf\\krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        final Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh6.bigdata.com,cdh7.bigdata.com,cdh8.bigdata.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("bigdata@BIGDATA.COM", "D:\\myconf\\bigdata.keytab");
            UserGroupInformation.setLoginUser(ugi);
            HBaseAdmin.checkHBaseAvailable(conf);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tablename,String... cf1) throws Exception{
        //获取admin对象
        Admin admin = connection.getAdmin();
        //创建tablename对象描述表的名称信息
        TableName tname = TableName.valueOf(tablename);//bd17:mytable
        //创建HTableDescriptor对象，描述表信息
        HTableDescriptor tDescriptor = new HTableDescriptor(tname);
        //判断是否表已存在
        if(admin.tableExists(tname)){
            System.out.println("表"+tablename+"已存在");
            return;
        }
        //添加表列簇信息
        for(String cf:cf1){
            HColumnDescriptor famliy = new HColumnDescriptor(cf);
            tDescriptor.addFamily(famliy);
        }
        //调用admin的createtable方法创建表
        admin.createTable(tDescriptor);
        System.out.println("表"+tablename+"创建成功");
    }
    //删除表
    public void deleteTable(String tablename) throws Exception{
        Admin admin = connection.getAdmin();
        TableName tName = TableName.valueOf(tablename);
        if(admin.tableExists(tName)){
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println("删除表"+tablename+"成功！");
        }else{
            System.out.println("表"+tablename+"不存在。");
        }
    }

    public void query(){
        HTable table = null;
        ResultScanner scan = null;
        try{
            //Admin admin = connection.getAdmin();
            table = (HTable)connection.getTable(TableName.valueOf("yangsy"));

            scan = table.getScanner(new Scan());

            for(Result rs : scan){
                System.out.println("rowkey:" + new String(rs.getRow()));

                for(Cell cell : rs.rawCells()){
                    System.out.println("column:" + new String(CellUtil.cloneFamily(cell)));

                    System.out.println("columnQualifier:"+new String(CellUtil.cloneQualifier(cell)));

                    System.out.println("columnValue:" + new String(CellUtil.cloneValue(cell)));

                    System.out.println("----------------------------");
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                table.close();
                if(null != connection) {
                    connection.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void queryByRowKey(){
        ResultScanner scann = null;
        HTable table = null;
        try {
            table = (HTable)connection.getTable(TableName.valueOf("yangsy"));

            Result rs = table.get(new Get("1445320222118".getBytes()));
            System.out.println("yangsy the value of rokey:1445320222118");
            for(Cell cell : rs.rawCells()){
                System.out.println("family" + new String(CellUtil.cloneFamily(cell)));
                System.out.println("value:"+new String(CellUtil.cloneValue(cell)));
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
    }

    public static String getRandomChar(int length) {
//        char[] chr = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
//                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
//                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
        char[] chr = { '1', '0',};

        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            buffer.append(chr[random.nextInt(1)]);
        }
        return buffer.toString();
    }
    public static String getRandomChar() {
        return getRandomChar(10);
    }


    public static String getRandomFundaccount() {
        String[] chr = {"10001", "10001", "10002", "10003", "10004", "10005", "10006", "10007", "10008", "10009"};
        int length =1;

        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            buffer.append(chr[random.nextInt(10)]);
        }
        return buffer.toString();
    }

    public static String getRandomProd() {
        String[] chr = {"P10001", "P10001", "P10002", "P10003", "P10004", "P10005", "P10006", "P10007", "P10008", "P10009"};
        int length =1;

        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            buffer.append(chr[random.nextInt(10)]);
        }
        return buffer.toString();
    }

    public static String getRandomStockCode() {
        String[] chr = {"600000"
                ,"600004"
                ,"600006"
                ,"600007"
                ,"600008"
                ,"600009"
                ,"600010"
                ,"600011"
                ,"600012"
                ,"600015"
                ,"600016"
                ,"600017"
                ,"600018"
                ,"600019"
                ,"600020"
                ,"600021"
                ,"600022"
                ,"600023"
                ,"600025"
                ,"600026"};

        int length =1;

        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < length; i++) {
            buffer.append(chr[random.nextInt(20)]);
        }
        return buffer.toString();
    }

    //新增数据到表里面Put
    public void putData(String table_name) throws Exception{
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();
        for(int i=0;i<20000;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(getRandomFundaccount()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock_code"), Bytes.toBytes(getRandomStockCode()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock_type"), Bytes.toBytes(getRandomChar(1)));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("exchange_type"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("current_amount"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("correct_amount"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_buy_amount"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_sell_amount"), Bytes.toBytes(""));

            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //prodPrice
    public void putProdPriceData(String table_name) throws Exception{
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();
        String COLUM_PRODE_CODE = "prod_code";
        String COLUM_PRODA_NO = "prodta_no";
        String COLUM_NAV = "nav";
        for(int i=0;i<200;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("prod_code"), Bytes.toBytes(getRandomProd()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("prodta_no"), Bytes.toBytes("prodta"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("nav"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //prodPrice
    public void putUserPriceData(String table_name) throws Exception{
        TableName tableName = TableName.valueOf(table_name);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();
        String COLUM_STOCK_CODE = "stock_code";
        String COLUM_EXCHANGE_TYPE = "exchange_type";

        for(int i=0;i<200;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock_code"), Bytes.toBytes(getRandomStockCode()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("exchange_type"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("asset_price"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }
    //prodPrice
    public void putSecumProdData() throws Exception{

        String tableName1 = "RL_ARM:hs_prod_secumreal";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_CURRENT_AMOUNT = "current_amount";
        String COLUM_CORRECT_AMOUNT = "correct_amount";
        String COLUM_REAL_BUY_AMOUNT = "real_buy_amount";
        String COLUM_REAL_SELL_AMOUNT = "real_sell_amount";
        String COLUM_RPODTA_NO = "prodta_no";
        String COLUM_PROD_CODE = "prod_code";


        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();



        for(int i=0;i<20000;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(getRandomFundaccount()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("prodta_no"), Bytes.toBytes("prodta"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("prod_code"), Bytes.toBytes(getRandomProd()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("current_amount"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("correct_amount"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_buy_amount"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_sell_amount"), Bytes.toBytes("0"));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }


    //fundReal
    public void putFundrealData() throws Exception{

        String tableName1 = "RL_ARM:HS_FUND_FUNDREAL";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";
        String COLUM_CURRENT_BALANCE = "current_balance";
        String COLUM_CORRECT_BALANCE = "correct_balance";
        String COLUM_REAL_BUY_BALANCE = "real_buy_balance";
        String COLUM_REAL_SELL_BALANCE = "real_sell_balance";



        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();



        for(int i=0;i<100;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(getRandomFundaccount()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("current_balance"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("correct_balance"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_buy_balance"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_sell_balance"), Bytes.toBytes("0"));
            //单记录put
//            table.put(put);
            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //fundReal
    public void putCrdtCompactData() throws Exception{

        String tableName1 = "RL_ARM:HS_CRDT_COMPACT";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";

        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();

        for(int i=0;i<20000;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            Put put = new Put(Bytes.toBytes("rowkey_"+i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(getRandomFundaccount()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("compact_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_compact_balance"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_compact_fare"), Bytes.toBytes("0"));
            //单记录put

            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }



    //putRealtimeData
    public void putRealtimeData() throws Exception{

        String tableName1 = "RL_ARM:HS_SECU_REALTIME";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";

        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();

        for(int i=0;i<200;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            String fundaccount = getRandomFundaccount();
            String stockcode = getRandomStockCode();
            Put put = new Put(Bytes.toBytes(fundaccount+stockcode));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(fundaccount));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock_code"), Bytes.toBytes(stockcode));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_status"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("business_balance"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            //单记录put

            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //putRealtimeData
    public void putCrdtRealtimeData() throws Exception{

        String tableName1 = "RL_ARM:HS_CRDT_CRDTREALTIME";
        String FAMILY = "info";
        String COLUM_FUND_ACCOUNT = "fund_account";
        String COLUM_MONEY_TYPE = "money_type";

        TableName tableName = TableName.valueOf(tableName1);
        Table table = connection.getTable(tableName);
        Random random = new Random();
        List<Put> batPut = new ArrayList<Put>();
        Random rand = new Random();

        for(int i=0;i<200;i++){
            //构建put的参数是rowkey   rowkey_i (Bytes工具类，各种java基础数据类型和字节数组之间的相互转换)
            String fundaccount = getRandomFundaccount();
            String stockcode = getRandomStockCode();
            Put put = new Put(Bytes.toBytes(fundaccount+stockcode));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fund_account"), Bytes.toBytes(fundaccount));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stock_code"), Bytes.toBytes(stockcode));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_status"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("real_type"), Bytes.toBytes("0"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("business_balance"), Bytes.toBytes(String.valueOf(random.nextInt(900))));
            //单记录put

            batPut.add(put);
        }
        table.put(batPut);
        System.out.println("表插入数据成功！");
    }

    //关闭连接
    public void cleanUp() throws Exception{
        connection.close();
    }
    /**
     *@paramargs
     */
    public static void main(String[] args) {

        HbaseTest hbaseTest = new HbaseTest();
//        try{
//            hbaseTest.createTable("baowl:test","i","j");
//        }catch (Exception e){
//            e.printStackTrace();
//        }

        try{
//            hbaseTest.putData("RL_ARM:HS_SECU_STOCKREAL");
//            hbaseTest.putData("RL_ARM:HS_CRDT_CRDTSTOCKREAL");
//            hbaseTest.putProdPriceData("RL_ARM:HS_PROD_PRODCODE");
//            hbaseTest.putSecumProdData();
//            hbaseTest.putUserPriceData("RL_ARM:HS_USER_PRICE");
//            hbaseTest.putFundrealData();
//            hbaseTest.putCrdtCompactData();


            hbaseTest.putRealtimeData();
            hbaseTest.putCrdtRealtimeData();

        }catch (Exception e){
            e.printStackTrace();
        }
        try{
            hbaseTest.cleanUp();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}