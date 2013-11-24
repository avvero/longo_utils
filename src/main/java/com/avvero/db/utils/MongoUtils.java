package com.avvero.db.utils;

import com.mongodb.*;

import java.net.UnknownHostException;

/**
 * User: avvero
 * Date: 17.11.13
 */
public class MongoUtils {

    private static final String NATURAL_ORDER = "$natural";

    public static DB getDB(String host, int port, String databaseName) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient(host, port);
        DB db = mongoClient.getDB(databaseName);
        return db;
    }

    public static DBCursor getTailableCursor(DB db, String collectionName) throws UnknownHostException {
        db.requestStart();
        DBCollection collection = db.getCollection(collectionName);
        DBCursor cur = collection.find().sort(new BasicDBObject(NATURAL_ORDER, 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        return cur;
    }

    public static DBCursor getTailableCursor(DB db, String collectionName, BasicDBObject query) throws UnknownHostException {
        db.requestStart();
        DBCollection collection = db.getCollection(collectionName);
        DBCursor cur = collection.find(query).sort(new BasicDBObject(NATURAL_ORDER, 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        return cur;
    }

    public static void tail(DB db, DBCursor cur, CursorListener listener) {
        try {
            while (cur.hasNext()) {
                BasicDBObject doc = (BasicDBObject)cur.next();
                listener.afterInsert(doc);
            }
        } finally {
            try {
                if (cur != null) cur.close();
            } catch (final Throwable t) { /* nada */ }
            db.requestDone();
        }
    }

    private MongoUtils () {}
}
