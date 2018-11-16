package day19;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * creat by newforesee 2018/10/17
 * 连接池
 */
public class RedisAPI02 {
    public static void main(String[] args) {
        //创建JedisPool
        JedisPool jedisPool = new JedisPool("master", 6379);
        //通过pool获取jedis实例
        Jedis jedis = jedisPool.getResource();
        jedis.auth("123");
        System.out.println(jedis.ping());
        //测试
        jedis.set("a2","jedis");
        System.out.println(jedis.get("a2"));
        jedis.close();
    }
}
