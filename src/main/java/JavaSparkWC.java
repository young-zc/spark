import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class JavaSparkWC {
    public static void main(String[] args) {
        //创建配置文件对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaSpark").setMaster("local");
        //创建上下文对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //读取数据文件
        JavaRDD<String> lines = javaSparkContext.textFile("/Users/newforesee/Intellij Project/Spark/src/main/scala/a.txt");
        //调用flatMap进行切分压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return (Iterator<String>) Arrays.asList(s.split(" "));
            }
        });
        //调用mapToPair算子进行单词计数
        JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //根据key聚合values
        JavaPairRDD<String, Integer> reduce = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //进行排序
        //Java接口汇总没有提供sortBy算子.如果需要以value进行排序时需要把数据进行反转一下排序后再翻转回来
        JavaPairRDD<Integer, String> swaped = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
               // return  new Tuple2<>(t._2,t._1);
                return t.swap();
            }
        });
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return t.swap();
            }
        });
        System.out.println(result.collect());
        //关闭
        javaSparkContext.close();

    }
}
