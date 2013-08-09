package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The FORK protocol; multiplexes messages to different forks in a stack (https://issues.jboss.org/browse/JGRP-1613).
 * See doc/design/FORK.txt for details
 * @author Bela Ban
 * @since  3.4
 */
@MBean(description="Implementation of FORK protocol")
public class FORK extends Protocol {

    public static short ID=ClassConfigurator.getProtocolId(FORK.class);

    // mappings between fork-stack-ids and fork-stacks (bottom-most protocol)
    protected final ConcurrentMap<String,Protocol> fork_stacks=new ConcurrentHashMap<String,Protocol>();


    public Protocol get(String fork_stack_id)                        {return fork_stacks.get(fork_stack_id);}
    public Protocol putIfAbsent(String fork_stack_id, Protocol prot) {return fork_stacks.put(fork_stack_id, prot);}


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                ForkHeader hdr=(ForkHeader)msg.getHeader(id);
                if(hdr == null)
                    break;
                if(hdr.fork_stack_id == null)
                    throw new IllegalArgumentException("header has a null fork_stack_id");
                Protocol bottom_prot=get(hdr.fork_stack_id);
                return bottom_prot.up(evt);

            case Event.VIEW_CHANGE:
                for(Protocol bottom: fork_stacks.values())
                    bottom.up(evt);
                break;
        }
        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        // Sort fork messages by fork-stack-id
        Map<String,List<Message>> map=new HashMap<String,List<Message>>();
        for(Message msg: batch) {
            ForkHeader hdr=(ForkHeader)msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                List<Message> list=map.get(hdr.fork_stack_id);
                if(list == null) {
                    list=new ArrayList<Message>();
                    map.put(hdr.fork_stack_id, list);
                }
                list.add(msg);
            }
        }

        // Now pass fork messages up, batched by fork-stack-id
        for(Map.Entry<String,List<Message>> entry: map.entrySet()) {
            String fork_stack_id=entry.getKey();
            List<Message> list=entry.getValue();
            Protocol bottom_prot=get(fork_stack_id);
            MessageBatch mb=new MessageBatch(batch.dest(), batch.sender(), batch.clusterName(), batch.multicast(), list);
            try {
                bottom_prot.up(mb);
            }
            catch(Throwable t) {
                log.error("failed passing up batch", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public static class ForkHeader extends Header {
        protected String fork_stack_id, fork_channel_id;

        public ForkHeader() {
        }

        public ForkHeader(String fork_stack_id, String fork_channel_id) {
            this.fork_stack_id=fork_stack_id;
            this.fork_channel_id=fork_channel_id;
        }

        public String getForkStackId() {
            return fork_stack_id;
        }

        public void setForkStackId(String fork_stack_id) {
            this.fork_stack_id=fork_stack_id;
        }

        public String getForkChannelId() {
            return fork_channel_id;
        }

        public void setForkChannelId(String fork_channel_id) {
            this.fork_channel_id=fork_channel_id;
        }

        public int size() {return Util.size(fork_stack_id) + Util.size(fork_channel_id);}

        public void writeTo(DataOutput out) throws Exception {
            Util.writeString(fork_stack_id, out);
            Util.writeString(fork_channel_id, out);
        }

        public void readFrom(DataInput in) throws Exception {
            fork_stack_id=Util.readString(in);
            fork_channel_id=Util.readString(in);
        }

        public String toString() {return fork_stack_id + ":" + fork_channel_id;}
    }
}
