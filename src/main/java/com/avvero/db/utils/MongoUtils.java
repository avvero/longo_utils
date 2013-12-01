package com.avvero.db.utils;

import com.mongodb.*;

import java.net.UnknownHostException;

/**
 * User: avvero
 * Date: 17.11.13
 */
public class MongoUtils {

    public static final long DEF_CAPPET_COL_SIZE = 1000000;

    private static final String NATURAL_ORDER = "$natural";

    public static DB getDB(String host, int port, String databaseName) throws UnknownHostException {
        MongoClient mongoClient = new MongoClient(host, port);
        DB db = mongoClient.getDB(databaseName);
        return db;
    }

    public static DBCollection getCappedCollection(DB db, String collectionName, boolean makeCapped, long size) {
        DBCollection cappedCollection = db.getCollection(collectionName);
        if (!cappedCollection.isCapped()) {
            if (makeCapped) {
                DBObject cmd = new BasicDBObject();
                cmd.put("convertToCapped", collectionName);
                cmd.put("size", size);
                db.command(cmd);
            } else {
                throw new MongoException("Collection is not capped.");
            }
        }
        return cappedCollection;
    }

    public static DBCollection getCappedCollection(DB db, String collectionName, boolean makeCapped) {
        return getCappedCollection(db, collectionName, makeCapped, DEF_CAPPET_COL_SIZE);
    }

    public static DBCollection getCappedCollection(DB db, String collectionName) {
        return getCappedCollection(db, collectionName, true, DEF_CAPPET_COL_SIZE);
    }

    public static DBCursor getTailableCursor(DB db, String collectionName) throws UnknownHostException {
        db.requestStart();
        DBCollection cappedCollection = getCappedCollection(db, collectionName);
        DBCursor cur = cappedCollection.find().sort(new BasicDBObject(NATURAL_ORDER, 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        return cur;
    }

    public static DBCursor getTailableCursor(DB db, String collectionName, BasicDBObject query) throws UnknownHostException {
        db.requestStart();
        DBCollection cappedCollection = getCappedCollection(db, collectionName);
        DBCursor cur = cappedCollection.find(query).sort(new BasicDBObject(NATURAL_ORDER, 1)).addOption(Bytes.QUERYOPTION_TAILABLE).addOption(Bytes.QUERYOPTION_AWAITDATA);
        return cur;
    }

    public static void tail(DB db, DBCursor cur, CursorListener listener) {
        try {
            while (cur.hasNext()) {
                BasicDBObject doc = (BasicDBObject) cur.next();
                listener.afterInsert(doc);
            }
        } finally {
            try {
                if (cur != null) cur.close();
            } catch (final Throwable t) { /* nada */ }
            db.requestDone();
        }
    }

    private MongoUtils() {
    }
}
