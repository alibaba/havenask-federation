package com.alibaba.search.common.arpc.perftest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.search.common.arpc.ANetRPCChannel;
import com.alibaba.search.common.arpc.ANetRPCChannelManager;
import com.alibaba.search.common.arpc.ANetRPCController;
import com.alibaba.search.common.arpc.DefaultRpcCallback;

public class ArpcClientPerfTest {
    private final static Logger logger = LoggerFactory
            .getLogger(ArpcClientPerfTest.class);
    
    public static int response_count = 0;
    public static long max_latency = 0;
    public static long min_latency = Long.MAX_VALUE;
    public static double ave_latency = 0;
    
    static class ArpcClientThread extends Thread {
        private ANetRPCChannelManager channelManager;
        private String host;
        private int port;
        private int request_size;
        private int request_count;
        private int queue_limit;
        
        public ArpcClientThread(ANetRPCChannelManager channelManager,
                String host, int port, int request_count, 
                int request_size, int queue_limit) {
            this.channelManager = channelManager;
            this.host = host;
            this.port = port;
            this.request_count = request_count;
            this.request_size = request_size;
            this.queue_limit = queue_limit;
        }
        
        @Override
        public void run() {
            ANetRPCChannel channel = channelManager.openChannel(host, port);
            channel.setPostQueueSizeLimit(queue_limit);
            Echo.EchoService.Stub service = Echo.EchoService.newStub(channel);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < request_size; ++i) {
                sb.append('a');
            }
            Echo.Request request = Echo.Request.newBuilder()
                    .setRequest(sb.toString()).build();
            
            for (int i = 0; i < request_count; ++i) {
                ANetRPCController controller = new ANetRPCController();
                service.echo(controller, request,
                        new MyRpcCallback<Echo.Response>(controller));
            }
        }
        
    }
    static class MyRpcCallback<MessageType> extends DefaultRpcCallback<MessageType> {
        private ANetRPCController controller;
        private long begin;
        
        public MyRpcCallback(ANetRPCController controller) {
            this.controller = controller;
            begin = System.currentTimeMillis();
        }

        @Override
        protected void doRun(MessageType parameter) {
            if (controller.failed()) {
                System.out.println("rpc call failed: " + controller.errorText());
                System.exit(-1);
            } else {
                long duration = System.currentTimeMillis() - begin;
                max_latency = Math.max(max_latency, duration);
                min_latency = Math.min(min_latency, duration);
                ave_latency += duration;
                response_count++;
            }
            
        }
    }
    
    public static void main(String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: host port request_count request_size " +
            		"thread_num queue_limit");
            System.exit(-1);
        }
        String host = args[0];
        int port = Integer.valueOf(args[1]);
        int request_count = Integer.valueOf(args[2]);
        int request_size = Integer.valueOf(args[3]);
        int thread_num = Integer.valueOf(args[4]);
        int queue_limit = Integer.valueOf(args[5]);
        Thread[] threads = new Thread[thread_num];
        
        ANetRPCChannelManager channelManager = new ANetRPCChannelManager();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < thread_num; ++i) {
            threads[i] = new ArpcClientThread(channelManager, host, port, 
                    request_count, request_size, queue_limit);
            threads[i].start();
        }          
        while (true) {
            if (response_count == request_count * thread_num) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        for (int i = 0; i < thread_num; ++i) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long total = request_count * thread_num;
        long time = end - begin;
        System.out.println("posted: " + total);
        System.out.println("size: " + request_size + " bytes");
        System.out.println("time: " + time + "ms");
        System.out.println("qps: " + 1000L * total / time);
        System.out.println("latency: min:" + min_latency + " max:" + 
                max_latency + " ave:" + ave_latency / total);
        System.out.printf("bandwidth: %.2fMB\n", (double)total * 
                request_size / 1024 / 1024 / time * 1000);
        channelManager.dispose();
    }
}
