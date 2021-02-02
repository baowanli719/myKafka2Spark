package com.gszq.utils;


import java.util.function.Consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Arrays;
import java.util.List;

public class  MongodbConn {


//    private static MongoDatabase database= null;
//    static {
//        try {
//
//            MongoClient client = MongoClients.create("mongodb://172.50.100.234:27017,172.50.100.217:27017,172.50.100.145:27017/realtime");
//            database = client.getDatabase("realtime");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//        public static void main(String[] args) {
//        try {
//
//            //MongoClient client =  MongoClients.create("mongodb://172.50.100.234:27017,172.50.100.217:27017,172.50.100.145:27017/realtime");
//            //MongoDatabase database = client.getDatabase("realtime");
//            MongoCollection<Document> coll = database.getCollection("stockreal");
//            for (String name : database.listCollectionNames()) {
//                System.out.println(name);
//            }
//
//            Consumer<Document> printConsumer = new Consumer<Document>() {
//                @Override
//                public void accept(final Document document) {
//                    System.out.println(document.toJson());
//                }
//            };
//
//            coll.find().forEach(printConsumer);
//            coll.insertOne(new Document("name","张三").append("age", 20));
//
//            FindIterable findIterable= coll.find();
//
//            MongoCursor mongoCursor = findIterable.iterator();
//
//            while(mongoCursor.hasNext()){
//
//                System.out.println(mongoCursor.next());
//
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }



}
