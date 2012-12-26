package Util;

import Util.Props;
import junit.framework.TestCase;

/**
 * Created with IntelliJ IDEA.
 * User: snowhyzhang
 * Date: 12-12-12
 * Time: 下午4:56
 * To change this template use File | Settings | File Templates.
 */
public class PropsTest extends TestCase {
    Props prop = new Props();

    public void testGetProperty(){
        String pro = prop.getProperty("TestPro");

        assertEquals("snow", pro);
    }
}
