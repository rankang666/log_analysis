package rk.logs;

import org.apache.log4j.Logger;
import org.junit.Test;


/**
 * @Author rk
 * @Date 2018/11/19 15:45
 * @Description:
 *  Log4jTest
 *      DEBUG --> INFO --> WARN --> ERROR -->
 **/
public class Log4jTest {

    private Logger logger = Logger.getLogger(Log4jTest.class);
//    private Logger logger = Logger.getLogger(Log4jTest1.class);

    @Test
    public void testLog4j(){
        //系统日志
        //自定义的日志
        logger.debug("------DEBUG------");
        logger.info("------INFO------");
        logger.warn("------WARN------");
        logger.error("------ERROR------");

        new Thread(new Runnable() {
            @Override
            public void run() {
                logger.debug("------THRED--DEBUG------");
                logger.info("------THRED--INFO------");
                logger.warn("------THRED--WARN------");
                logger.error("------THRED--ERROR------");
            }
        },"线程") .start();

        System.out.println("------------------------------------------------------------------------");

        //用户自定义日志的获取
        logger = Logger.getLogger("access");
        logger.debug("----access--DEBUG------");
        logger.info("----access--INFO------");
        logger.warn("----access--WARN------");
        logger.error("----access--ERROR------");


    }

}
