import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 第一种 通过反射方式实现
 */
public class JavaRDD2DataFrame {
    public static void main(String[] args) {
    //创建一个普通的RDD
        SparkConf conf = new SparkConf().setAppName("JavaRDD2DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext ssc = new SQLContext(sc);
        JavaRDD<String> files = sc.textFile("/Users/newforesee/Intellij Project/Spark/src/students.txt");
        //对这个RDD进行处理
        JavaRDD<Student> student = files.map(new Function<String, Student>() {
            @Override
            public Student call(String s) throws Exception {
                //进行数据切分
                String[] splits = s.split(",");
                //将需要的类new出来赋值
                Student stu = new Student();
                stu.setId(Integer.valueOf(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.valueOf(splits[2]));
                return stu;
            }
        });
        //接下来开始使用反射方式将RDD转换为DataFrame
        //将Student传入,其实就是用反射的方式创建DataFrame
        Dataset<Row> df = ssc.createDataFrame(student, Student.class);
        //拿到了DF之后就可以将其注册为一个临时表,然后针对其中的数据执行SQL操作
        df.registerTempTable("student");
        //针对临时表执行SQL语句,查询年龄小于18 的学生
        Dataset<Row> sql = ssc.sql("SELECT * FROM student WHERE AGE <= 18");
        //sql.show();
        //操作这批数据转成RDD进行操作
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        //调用map算子进行处理
        JavaRDD<Student> studentJavaRDD = rowJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setId(row.getInt(1));
                stu.setName(row.getString(2));
                stu.setAge(row.getInt(0));
                return stu;
            }
        });
        //将数据收回
        List<Student> collect = studentJavaRDD.collect();
        //接下来将数据进行自定义展示
        for (Student stu : collect) {
            System.out.println(stu.getId() + " "+ stu.getName()+" "+stu.getAge());
        }
    }
}
