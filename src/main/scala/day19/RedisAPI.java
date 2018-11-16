package day19;

import redis.clients.jedis.Jedis;

/**
 * creat by newforesee 2018/10/17
 * 单机连接Redis
 */
public class RedisAPI {
    public static void main(String[] args) {
//        创建jedis连接
        //我们需要更改Redis的配置文件信息,将bind的值改为主机ip
        Jedis jedis = new Jedis("master", 6379);
        //注意:如果设置了密码需要密码验证
        jedis.auth("123");
        jedis.flushDB();
        System.out.println(jedis.ping());
        jedis.set("a1","jedis test");
        System.out.println(jedis.get("a1"));
        //关闭Redis连接
        jedis.close();

    }
}
