package com.mt.hbase.chpt07.disruptor;

/**
 * @author pengxu
 * @Date 2018/12/19.
 */
public class PerformanceCompare implements Runnable{

    private static int THREAD_COUNT = 5;

    private static long LOOP_COUNT = 100000000;

    private static PaddingLong[] paddingLongArray = new PaddingLong[THREAD_COUNT];

    private int threadIndex;

    public PerformanceCompare(int threadIndex){
        this.threadIndex = threadIndex;
    }

    @Override public void run() {
        long i = LOOP_COUNT + 1;
        while (i-- > 0) {
                paddingLongArray[threadIndex].paddingLong = i;
        }
    }

    static class PaddingLong {

        /**
         * 下行为缓存行填充，注释掉跟保留运行时间差别很大
         * idea默认JVM内存配置下，mac 4核心2.6G机器执行耗时如下：
         * cost=5562040000 注释掉耗时
         * cost=1396560475 未注释耗时
         */
        public long p1, p2, p3, p4, p5, p6, p7;
        public volatile long paddingLong = 0L;
    }


    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < THREAD_COUNT; i++) {
            paddingLongArray[i] = new PaddingLong();
        }
        doLoop();
    }


    private static void doLoop() throws InterruptedException {
        Thread[] threads = new Thread[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread t = new Thread(new PerformanceCompare(i));
            threads[i] = t;
        }
        long start = System.nanoTime();
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        System.out.println("cost=" + (System.nanoTime() - start));
    }
}
