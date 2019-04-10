/**
 * xxx
 * creat by newforesee 2018/12/14
 */
public class test {
    static class Mythread extends Thread{
        public Mythread() {
            this.setName("Mythread");
            System.out.println(Thread.currentThread().getName());
        }

        @Override
        public void run() {
            System.out.println(Thread.currentThread().getName());
        }
    }
    public static void main(String[] args) {
        Thread thread = new Mythread();
        thread.run();
        thread.start();
    }
}
