package com.cl;

import java.text.SimpleDateFormat;
import java.util.concurrent.*;

/**
 * Created By chenli
 * ON 17/11/15
 */
public class ProductorAndCustomerModel {
    //模拟消费线程池 同时4个线程处理
    private static final ThreadPoolExecutor THREAD_POOL = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    //模拟消息生产任务
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    //用于判断是否关闭订阅
    private static volatile boolean isClose = false;

    private static ThreadLocal<SimpleDateFormat> LOCAL_DATE_FORMAT = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };
    public static void main(String[] args) throws InterruptedException {

        //注册钩子方法
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("准备关闭了。。");
                close();
            }
        });
        //队列长度100
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(100);
        //生产者
        producer(queue);
        //消费者
        consumer(queue);

    }

    //模拟消息队列生产者
    private static void producer(final BlockingQueue  queue){
        //每200毫秒向队列中放入一个消息，1S放入5个
        SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(new Runnable() {
            public void run() {
                queue.offer(String.valueOf(Math.random()));
            }
        }, 0L, 200L, TimeUnit.MILLISECONDS);
    }

    //模拟消息队列消费者 生产者每秒生产5个   消费者4个线程消费1个1秒  每秒积压1个
    private static void consumer(final BlockingQueue queue) throws InterruptedException {
        while (!isClose){
            getPoolBacklogSize();
            //从队列中拿到消息
            final String msg = (String)queue.take();
            //放入线程池处理
            if(!THREAD_POOL.isShutdown()) {
                THREAD_POOL.execute(new Runnable() {
                    public void run() {
                        try {
                            System.out.println(msg);
                            TimeUnit.MILLISECONDS.sleep(1000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
    }

    //查看线程池堆积消息个数
    private static long getPoolBacklogSize(){
        long backlog = THREAD_POOL.getTaskCount()- THREAD_POOL.getCompletedTaskCount();
        System.out.println("时间:" + LOCAL_DATE_FORMAT.get().format(System.currentTimeMillis()) + ",任务总数:" + THREAD_POOL.getTaskCount() + ",完成数量:" + THREAD_POOL.getCompletedTaskCount() + ",剩余数量" + backlog);
        return backlog;
    }

    private static void close(){
        System.out.println("收到kill消息，执行关闭操作");
        //关闭订阅消费
        isClose = true;
        //关闭线程池，等待线程池积压消息处理
        THREAD_POOL.shutdown();
        //判断线程池是否关闭
        while (!THREAD_POOL.isTerminated()) {
            try {
                //每200毫秒 判断线程池积压数量
                getPoolBacklogSize();
                TimeUnit.MILLISECONDS.sleep(200L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("订阅者关闭，线程池处理完毕");
    }
}
