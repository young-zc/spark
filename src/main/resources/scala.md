#Scala语言的特点：
- 基于JVM（可以重用类库）
- 简洁优雅
- 面向对象 + 函数式编程（FP）

函数式编程的数学基础是 : λ演算
函数式编程中，所有的数据都是不可变的，不同的函数之间通过数据流来交换信息，函数作为FP中的一等公民，享有跟数据一样的地位，可以作为参数传递给下一个函数，同时也可以作为返回值。

#基础语法
## Scala基础 ##
- 程序文件的名称可以不与对象名称完全匹配；
- 程序从main()方法开始，**main 方法应该在object中**；
- $字符是Scala中的保留关键字；
- 语句在单行上分号可省略；
- 操作符是方法，没有提供 ++、--操作符；
- **使用val声明不可变的值，使用var声明可变的变量；鼓励使用val，避免使用var；**
- 需要给出值或者变量的类型，scala可以做自动类型推断。必要时可以指定类型；
- 拥有和java一样的数据类型，和java的数据类型的内存布局、精度完全一致;

## 控制结构和函数 ##
- **if 语句有返回值；**
- **基础类型都继承自AnyVal，应用类型都继承自AnyRef，AnyVal 与 AnyRef 继承自Any；**
- 块语句是一个包含于{}中的语句序列，**块中最后一个表达式的值就是块的值**；
- **字符串插值器：s、raw、f；**
- for循环中：to、until；很方便的写多重循环；
    for (i <-1 to 3; j<-1 to 3[;] if i != j) print((10*i+j) + " " )
- for循环中的守卫语句（guard 语句）；for推导式（yield）；
- Range对象：1 to 100 by 2; Range(1, 100, 2); (1 to 100).toArray;
- 只要函数不是递归的，不需要指定返回类型。Scala可以进行自动类型推断；函数的定义：
    def funcname(x: Int, y: Int): Int = {
      x + y
    }
- 默认参数、带名参数、变长参数；
- 懒值。当val被声明为lazy，它的初始化将被推迟，直到首次对它取值（只有val才能定义为lazy）

## 数组（Array） ##
Array：长度固定、元素可变、可索引、存放相同类型的集合
ArryBuffer：长度可以变；要导包；打印方便
toArray / toBuffer
增加元素：+=、:+、+:
减元素：-=
增加一个数组: ++= ++

map、foreach、reduce、flatMap
max、min、sorted、reverse
sum、product、size、length、indices
count、filter(Not)、distinct、take、takeRigth、takeWhile
drop、dropRigth、dropWhile
zip、unzip、head、tail、last、init
union、groupBy、contains

## Map ##
存放键值对的容器（集合）；
默认使用不可变的Map，这里的不可变是指值不可变；如果要给Map增加值应声明为var；
如果要使用可变的Map，要显示声明；import scala.collection.mutable.Map
getOrElse

## Tuple ##
可以存放不同类型的元素，元组是不可变的；最大的长度22；
访问使用x._1、x._2

# 面向对象 #
大处使用面向对象；
小处使用函数式编程；（操作数据）

用class声明类，用object声明单例对象；
用final阻止一个对象被继承；
用abstract阻止对象被实例化；
用this指代对象本身；
缺省的访问限定符为public；

## 类 & 对象 ##
定义类的成员变量访问属性：
class Student(var name: String, var age)
class Student(val name: String, val age)
class Student(private val name: String, val age)【示意】
class Student(private[this] val name: String, val age)【示意】
自定义getter、setter方法；
定义java风格的getter、setter方法；

主构造函数：就是类体本身

辅助构造函数：命名为this，跟不同的参数列表；第一行必须调用主构造器 或 其他辅助构造器；

用来定义常量和工具方法；
实现单例类；
与java中的static 类与方法类似；
没有参数列表；其他的性质与class类似；

程序的入口点，main方法定义在其中
apply方法也放在这里；

## 继承 & 特质 ##
scala采用object关键字实现单例对象，具备和Java静态方法同样的功能；
object本质上可以拥有类的所有特性，除了不能提供构造器参数；
对于任何在Java中用单例对象的地方，在scala中都可以用object实现：
- 作为存放工具函数或常量的地方
- 高效地共享单个不可变实例

当单例对象与某个类具有相同的名称时，它被称为这个类的“伴生对象”，类称为伴生类；
类和它的伴生对象必须存在于同一个文件中，而且可以相互访问私有成员（字段和方法）；
object的apply、unapply、update方法
定义伴生对象中的apply方法，主要用来解决简化对象的初始；

trait（接口），是scala代码重用的基本单元，可以同时拥有抽象方法和具体方法；

# 函数式编程 #
## case class & case object ##
样例类是scala中特殊的类。当声明样例类时，如下事情会自动发生：
- 构造器中每一个参数都成为val（不建议使用var）；
- 提供apply方法；
- 提供unapply方法；
- 将生成toString、equals、hashCode和copy方法；
- 继承了Product和Serializable；

case class和其他类型完全一样，可以添加方法和字段，可以继承；
case class最大的用处是用于模式匹配；

case class是多例的（后面要跟构造参数），case object是单例的

## 模式匹配 ##
模式匹配使用 match... { case ... }的语法结构，具有以下特点：
- match 语句有返回值；
- case分支语句不会贯穿到下一个case语句；
- 如果没有任何一个模式匹配上，会抛出异常（MatchError）；
- 通常最后一个语句与 _ 匹配；

模式匹配无处不在；

**## Option类型 ##**
Option是Scala编程中常用的一个类，用来避免指针异常；
包含两个实例：Some、None；
Option类型提供了getOrElse方法；
可将Option[T]看做是一个集合，这个集合只要么
- 只包含一个元素（被包装在Some中）；
- 要么就不存在元素（返回None）；

## 函数 ##
**函数字面量：函数的值；(x, str) => { x + str.length }
函数的类型：(Int, String) => Int
匿名函数：即Lambda表达式，没有名称的函数；**
可使用下划线作为一个或多个参数的占位符，只要每个参数在函数字面量内仅出现一次。
多个下划线指代多个参数；

**高阶函数：接收一个或多个函数作为输入 或 输出一个函数。
闭包是一种特殊函数，是在其上下文中引用了自由变量的函数；（闭包反映了一个从开放到封闭的过程）
柯里化：将原来接收两个参数的函数变成新的接收一个参数的函数的过程；
部分应用函数（偏应用函数）：是指缺少部分（甚至全部）参数的函数；
偏函数：只接受部分输入参数的函数；**

在偏函数中只能使用 case 语句，整个函数必须用大括号包围；通常使用偏函数进行类型转换，拆解；

## 集合 ##
Seq（序列）、Set（集）、map（映射）
集合的分类：有序、无序；可变、不可变；
默认提供不可变版本，使用可变版本需要显示声明；

### Seq ###
Seq：具有一定长度的可迭代访问的对象，每个元素均带有一个从0开始计数的固定索引位置。
常见的Seq类型包括：List、Vector、String、Range、Queue、Stack；
List : 列表是不可变的（元素不能通过赋值改变）；结构是递归的；
List递归的表示 ：head :: tial
常用操作符 ：Nil、::、:::（++）
Vector是以树形结构的形式实现，每个节点可以有不超过32个子节点。（4层可存放100W个节点）；
Range通常表示一个整数序列；

### set ###
与数学集合的概念较为类似。元素没有重复值，不保证元素的存放顺序；

### 高阶函数 ###
flatMap、flatten、reduce
fold、foldLeft、foldRight
aggregate

## 隐式转换 ##
隐式转换可以为现有的类库添加功能; 隐式转隐为隐藏了相应的细节;
隐式函数是指那种以implicit关键字声明的带有单个参数的函数;

隐式转换规则：
1、源 或 目标类型 的伴生对象中的隐式函数
2、当前作用域的隐式函数

隐式参数有点类似缺省参数，如果在调用方法时没有提供某个参数，编译器会在当前作用域查找是否有符合条件的 implicit 对象可以作为参数传入；

## Ordered、Ordering ##
Ordered 提供比较器模板，可以自定义多种比较方式（我们常用它来解决对象的排序问题）
Ordering 定义了相同类型间的比较方式【Ordering.Int、Ordering.String.reverse】

sorted、sortWith、sortBy

## 函数式编程的特点 ##
不变性
函数式一等公民
递归和尾递归

## 函数式编程相关的技术 ##
少用循环和遍历，多使用 map 、 reduce等函数；
柯里化（currying）；
高阶函数（higher order function）；
递归（recursing）和尾递归
管道（pipeline）

## 函数 与 方法 ##
使用 def 定义的是方法；使用 val 定义的是函数；
二者在大多数情况下可以认为是相等的；

方法是类的组成部分，不能单独存在；
函数是一个完整的对象，可以独立存在；
方法可以转化为函数；

下划线的用法：
导包
类成员变量赋值
可变参数
类型通配符
模式匹配
Tuple访问
简化函数字面量
定义setter方法
部分应用函数
方法转换为函数

+:、:+、++、::、:::

type：声明类型，提供可读性
