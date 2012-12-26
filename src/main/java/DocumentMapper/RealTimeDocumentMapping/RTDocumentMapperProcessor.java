package DocumentMapper.RealTimeDocumentMapping;

import DocumentMapper.iDocumentMapper;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-20
 * Time: 下午4:12
 * To change this template use File | Settings | File Templates.
 */
public class RTDocumentMapperProcessor {

    public static void main(String args[]){
        RTDocumentMapperProcessor RTDocMap = new RTDocumentMapperProcessor();
        RTDocMap.startMapper();
    }

    public void startMapper(){
        iDocumentMapper documentMapper = new RTDocumentMapping();
        documentMapper.mapToDocumentProcessorQueue();
    }
}
