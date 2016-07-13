package cn.crxy.spider.web.listener;

import cn.crxy.spider.web.domain.Goods;
import cn.crxy.spider.web.index.SolrIndex;
import cn.crxy.spider.web.utils.HbaseUtils;
import cn.crxy.spider.web.utils.RedisUtils;
import cn.crxy.spider.web.utils.SleepUtils;
import cn.crxy.spider.web.utils.SolrUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Created by tonye0115 on 2016/7/13.
 */
public class MyListener implements ServletContextListener {
    private MyThread myThread;

    public void contextDestroyed(ServletContextEvent e) {
        if (myThread != null && myThread.isInterrupted()) {
            myThread.interrupt();
        }
    }

    public void contextInitialized(ServletContextEvent e) {
        String str = null;
        if (str == null && myThread == null) {
            myThread = new MyThread();
            myThread.start(); // servlet 上下文初始化时启动 socket
        }
    }


}


class MyThread extends Thread {
    static final Logger logger = LoggerFactory.getLogger(SolrIndex.class);
    private static final String SOLR_INDEX = "solr_index";
    static RedisUtils redis = new RedisUtils();
    public void run() {
        final String[] goodsId = {""};
        final HbaseUtils hbaseUtils = new HbaseUtils();
        goodsId[0] = redis.poll(SOLR_INDEX);
        while (!this.isInterrupted()) {// 线程未中断执行循环
            if(StringUtils.isNotBlank(goodsId[0])){
                Goods goods = hbaseUtils.get(HbaseUtils.TABLE_NAME, goodsId[0]);
                if(goods==null){
                    logger.error("id为{}的商品索引建立失败!原因：没有在hbase数据库中查询到数据", goodsId[0]);
                }else{
                    if(goodsId[0].startsWith("_jd")){
                        goods.setFrom("京东");
                    }else if(goodsId[0].startsWith("_yx")){
                        goods.setFrom("易迅");
                    }else{
                        goods.setFrom("未知");
                    }
                    try {
                        SolrUtil.addIndex(goods);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                goodsId[0] = redis.poll(SOLR_INDEX);
            }else{
                System.out.println("暂时没有需要索引的数据,休息一会");
                SleepUtils.sleep(5000);
            }
        }
    }
}