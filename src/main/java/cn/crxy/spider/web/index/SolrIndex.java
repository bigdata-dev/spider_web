package cn.crxy.spider.web.index;
import cn.crxy.spider.web.domain.Goods;
import cn.crxy.spider.web.utils.HbaseUtils;
import cn.crxy.spider.web.utils.RedisUtils;
import cn.crxy.spider.web.utils.SleepUtils;
import cn.crxy.spider.web.utils.SolrUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 建立商品索引，
 * 从redis中查询需要建索引的商品ID
 * 然后查询hbase，建立索引
 * 如果建立索引的时候出现异常，则把当前商品id重新放入索引表中
 *
 */
public class SolrIndex{
	static final Logger logger = LoggerFactory.getLogger(SolrIndex.class);
	private static final String SOLR_INDEX = "solr_index";
	static RedisUtils redis = new RedisUtils();
	/**
	 * 建立索引
	 */
	public static void doIndex(){
		final String[] goodsId = {""};
		try {
			final HbaseUtils hbaseUtils = new HbaseUtils();
			goodsId[0] = redis.poll(SOLR_INDEX);
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
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
			}).run();
		} catch (Exception e) {
			logger.error("id为{}的商品索引建立失败!{}", goodsId[0], e);
			redis.add(SOLR_INDEX, goodsId[0]);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		doIndex();
	}
}
