package Util;

import Util.MongoDBUtil;
import Util.Props;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.bson.types.ObjectId;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-19
 * Time: 下午3:17
 * To change this template use File | Settings | File Templates.
 */
public class MongoDBUtilTest extends TestCase {

    private String DBHost = Props.getProperty("MongoDBHost");
    private String DBPort = Props.getProperty("MongoDBPort");
    private String DBName = Props.getProperty("MongoDBName");
    private String TestCollection = Props.getProperty("TestCollection");

    private MongoDBUtil dbUtil = new MongoDBUtil(DBHost, DBPort, DBName);

    public void testMongoDBUtilInsert(){

        dbUtil.getConnection();

        dbUtil.drop(TestCollection);

        for (int i = 0; i < 10; ++i){
            DBObject insertionObj = new BasicDBObject();
            insertionObj.put("id", i);
            insertionObj.put("Processed", "N");
            dbUtil.insert(insertionObj, TestCollection);
        }
        assertEquals(10, dbUtil.getCollectionSize(TestCollection));
    }

    public void testMongoDBUtilFind(){

        refreshDB();

        DBObject findObj = new BasicDBObject();
        findObj.put("id", 0);
        DBObject result = dbUtil.findOne(findObj, TestCollection);

        System.out.println(result.toString());

        assertEquals("N", result.get("Processed"));
    }

    public void testMongoDBUtilUpdate(){

        refreshDB();

        DBObject findObj = new BasicDBObject();
        findObj.put("id", 4);
        DBObject result = dbUtil.findOne(findObj, TestCollection);
        DBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(result.get("_id").toString()));
        DBObject updateObj = new BasicDBObject();
        updateObj.put("Processed", "Y");
        updateObj = new BasicDBObject().append("$set", updateObj);
        dbUtil.update(query, updateObj, TestCollection);

        result = dbUtil.findOne(query, TestCollection);
        assertEquals("Y", result.get("Processed"));
    }

    public void testMongoDBUtilRemove(){

        refreshDB();

        DBObject queryApple = new BasicDBObject();
        queryApple.put("id", 9);
        dbUtil.remove(queryApple, TestCollection);
        DBObject result = dbUtil.findOne(queryApple, TestCollection);
        assertNull(result);
    }

    private void refreshDB(){
        dbUtil.getConnection();

        dbUtil.drop(TestCollection);

        for (int i = 0; i < 10; ++i){
            DBObject insertionObj = new BasicDBObject();
            insertionObj.put("id", i);
            insertionObj.put("Processed", "N");
            dbUtil.insert(insertionObj, TestCollection);
        }
    }
}
