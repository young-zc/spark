import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 第二种方式 使用编程方式指定元数据
 */
public class JavaRDD2DataFrame2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DF2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext ssc = new SQLContext(sc);
        //创建一个普通RDD,但是注意类型一定是ROW类型,不像我们之前传入一类就行
        JavaRDD<String> files = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/students.txt");
        JavaRDD<Row> rows = files.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] splits = s.split(",");

                return RowFactory.create(
                        Integer.valueOf(splits[0]),
                        splits[1],
                        Integer.valueOf(splits[2]));
            }
        });
        //接下来动态构建元数据
        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        //构建StructType
        StructType structType = DataTypes.createStructType(fields);
        //开始使用动态构建的元数据将rows和structType传进去构建DF
        Dataset<Row> df = ssc.createDataFrame(rows, structType);
        //注册临时表
        df.registerTempTable("student");
        //开始执行sql
        Dataset<Row> sql = ssc.sql("select * from student where age>17");
        //转换成RDD
        List<Row> rowList = sql.javaRDD().collect();
        for (Row row : rowList) {
            System.out.println(row);
        }

    }
}
