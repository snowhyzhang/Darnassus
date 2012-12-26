package DocumentProcessor.DocumentVectorProcessing;

import DocumentProcessor.DocumentWordProcessing.DocumentWordProcessor;
import DocumentProcessor.iDocumentProcessor;
import DocumentProcessor.iDocumentWordProcessor;
import Util.MongoDBUtil;
import Util.Props;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import javax.sound.midi.SysexMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-21
 * Time: 下午3:39
 * To change this template use File | Settings | File Templates.
 */
public class DocumentVectorProcessing implements iDocumentProcessor {
    private final String mongoDBHost = Props.getProperty("MongoDBHost");
    private final String mongoDBPort = Props.getProperty("MongoDBPort");
    private final String mongoDBName = Props.getProperty("MongoDBName");

    private final int sleepTime = Integer.parseInt(Props.getProperty("MapperSleepTime"));

    private final String documentSet = Props.getProperty("DocumentSet");
    private final String documentQueue = Props.getProperty("DocumentQueue");

    private final String documentFilter = Props.getProperty("DocumentFilter");

    private MongoDBUtil dbUtil = null;
    private iDocumentWordProcessor documentWordProcessor = null;
    private String documentProcessingQueue = null;


    public void setDBConnection(String MongoDBHost, String MongoDBPort, String MongoDBName) {
        if (dbUtil == null){
            dbUtil = new MongoDBUtil(MongoDBHost, MongoDBPort, MongoDBName);
        } else {
            dbUtil.setMongoDBHost(MongoDBHost);
            dbUtil.setMongoDBPort(MongoDBPort);
            dbUtil.setMongoDBName(MongoDBName);
        }
    }

    @Override
    public void processDocument(String processingQueue) {
        setDBConnection(mongoDBHost, mongoDBPort, mongoDBName);
        dbUtil.getConnection();
        documentProcessingQueue = processingQueue;
        documentWordProcessor = new DocumentWordProcessor();

        boolean shutdownFlag = false;
        while (!shutdownFlag){
            List<String> processingIdList = getProcessingDoc();
            if (processingIdList == null || processingIdList.isEmpty()){
                System.out.println("No document to be processed.");
                try {
                    sleep (sleepTime);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                continue;
            }
            for (String processingId: processingIdList){
                if (processingId.equalsIgnoreCase("SHUTDOWN")){
                    shutdownFlag = true;
                    DBObject shutdownQuery = new BasicDBObject("ProcessingId", processingId);
                    DBObject shutdownUpdate = new BasicDBObject("Processed", "Y");
                    shutdownUpdate = new BasicDBObject().append("$set", shutdownUpdate);
                    dbUtil.update(shutdownQuery, shutdownUpdate, documentProcessingQueue);
                    System.out.println("Processing is SHUTDOWN!");
                } else {
                    processingDocument(processingId);
                }
            }
            System.out.println("Processed Document:" + processingIdList.size());
        }

        dbUtil.closeConnection();
    }

    private void processingDocument(String processingId) {
        DBObject processingQuery = new BasicDBObject("_id", new ObjectId(processingId));
        DBObject documentMessage = dbUtil.findOne(processingQuery, documentQueue);
        String documentContent = (String)documentMessage.get("Content");
        String documentName = (String)documentMessage.get("DocumentName");
        Map<String, Integer> documentWordsMapper = documentWordProcessor.getProcessingDocument(documentContent, documentFilter);
        DBObject documentWords = new BasicDBObject();
        documentWords.put("DocumentId", processingId);
        documentWords.put("DocumentName", documentName);
        documentWords.putAll(documentWordsMapper);
        dbUtil.insert(documentWords, documentSet);

        DBObject documentProcessedUpdateQuery = new BasicDBObject("ProcessingId", processingId);
        DBObject documentProcessedUpdateStatus = new BasicDBObject("Processed", "Y");
        documentProcessedUpdateStatus = new BasicDBObject().append("$set", documentProcessedUpdateStatus);
        dbUtil.update(documentProcessedUpdateQuery, documentProcessedUpdateStatus, documentProcessingQueue);
    }

    public List<String> getProcessingDoc() {
        DBObject query = new BasicDBObject();
        query.put("Processed", "N");
        List<DBObject> dbObjectList =  dbUtil.findAll(query, documentProcessingQueue);
        List<String> processingIdList = new ArrayList<String>();
        for (DBObject obj: dbObjectList){
            processingIdList.add((String)obj.get("ProcessingId"));
        }
        return processingIdList;
    }
}
