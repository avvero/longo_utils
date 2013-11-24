import com.avvero.db.utils.MongoUtils
import com.avvero.db.utils.CursorListener
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCursor

DB db = MongoUtils.getDB("127.0.0.1", 27017, "physicians")

BasicDBObject query = new BasicDBObject("first", "Sem");

DBCursor cursor = MongoUtils.getTailableCursor(db, "doctor", query);
MongoUtils.tail(db, cursor, new CursorListener() {
    @Override
    public void afterInsert(Object o) {
        BasicDBObject dbObject = (BasicDBObject)o;
        System.out.println("-----");
        System.out.print(dbObject.get("_id").toString());
        System.out.print(" ");
        System.out.println(dbObject.get("first"));
    }
});
db.requestDone();