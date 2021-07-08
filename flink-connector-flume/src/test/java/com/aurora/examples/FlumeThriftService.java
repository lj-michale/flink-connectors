package com.aurora.examples;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.ThriftSource;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lj.michale
 * @description Start Flume Source service.
 * @date 2021-07-08
 */
public class FlumeThriftService {

    private static String hostname = "localhost";
    private static int port = 9000;

    public static void main(String[] args) throws Exception {
        //Flume Source
        ThriftSource source = new ThriftSource();
        Channel ch = new MemoryChannel();
        Configurables.configure(ch, new Context());

        Context context = new Context();
        context.put("port", String.valueOf(port));
        context.put("bind", hostname);
        Configurables.configure(source, context);

        List<Channel> channels = new ArrayList<>();
        channels.add(ch);
        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);
        source.setChannelProcessor(new ChannelProcessor(rcs));
        source.start();
        System.out.println("ThriftSource service start.");

        while (true) {
            Transaction transaction = ch.getTransaction();
            transaction.begin();
            Event event = ch.take();
            if (null != event) {
                System.out.println(event);
                System.out.println(new String(event.getBody()).trim());
            }
            transaction.commit();
            transaction.close();
        }

    }
}
