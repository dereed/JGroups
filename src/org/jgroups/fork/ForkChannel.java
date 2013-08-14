package org.jgroups.fork;

import org.jgroups.*;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of a ForkChannel, which is a light-weight channel. Not all methods are supported,
 * UnsupportedOperationExceptions will be thrown if an unsupported operation is called.
 * See doc/design/FORK.txt for details
 * @author Bela Ban
 * @since  3.4
 */
public class ForkChannel extends JChannel {
    protected final Channel           main_channel;
    protected final String            fork_channel_id;
    protected final ForkProtocolStack fork_stack;

    /**
     * Creates a new ForkStack instance
     * @param main_channel The main-channel. The lifetime of the newly created channel will be less than or equal to
     *                     the main-channel
     * @param fork_stack_id The ID to associate the fork-stack with in FORK
     * @param fork_channel_id The ID used to map fork-channel IDs to ForkChannels in the fork-channels protocol stack
     * @param create_fork_if_absent If true, and FORK doesn't exist, a new FORK protocol will be created and inserted
     *                              into the main-stack at the given position. If false, and FORK doesn't exist, an
     *                              exception will be thrown
     * @param position The position at which the newly created FORK will be inserted. {@link ProtocolStack#ABOVE} or
     *                 {@link ProtocolStack#BELOW} are accepted. Ignored if create_fork_if_absent is false.
     * @param neighbor The class of the neighbor protocol below or above which the newly created FORK protocol will
     *                 be inserted. Ignored if create_fork_if_absent is false.
     * @param protocols A list of protocols (<em>from bottom to top</em> !) to insert as the fork_stack in FORK under the
     *                  given fork_stack_id. If the fork-stack with fork_stack_id already exists, an exception will be
     *                  thrown.
     *                  Can be null if no protocols should be added. This may be the case when an app only wants to use
     *                  a ForkChannel to mux/demux messages, but doesn't need a different protocol stack.
     *
     * @throws Exception
     */
    public ForkChannel(final Channel main_channel, String fork_stack_id, String fork_channel_id,
                       boolean create_fork_if_absent, int position, Class<? extends Protocol> neighbor,
                       Protocol ... protocols) throws Exception {

        if(main_channel == null)    throw new IllegalArgumentException("main channel cannot be null");
        if(fork_stack_id == null)   throw new IllegalArgumentException("fork_stack_id cannot be null");
        if(fork_channel_id == null) throw new IllegalArgumentException("fork_channel_id cannot be null");

        this.main_channel=main_channel;
        this.fork_channel_id=fork_channel_id;

        main_channel.addChannelListener(new ChannelListener() {
            public void channelConnected(Channel channel)    {
                state=State.CONNECTED;
                local_addr=main_channel.getAddress();
                cluster_name=main_channel.getClusterName();
                name=main_channel.getName();
                Event evt=new Event(Event.SET_LOCAL_ADDRESS, local_addr);
                if(up_handler != null)
                    up_handler.up(evt);
            }
            public void channelDisconnected(Channel channel) {state=State.OPEN; local_addr=null;cluster_name=null;name=null;}
            public void channelClosed(Channel channel)       {state=State.CLOSED;}
        });

        // Find the FORK protocol
        ProtocolStack stack=main_channel.getProtocolStack();
        FORK fork=(FORK)stack.findProtocol(FORK.class);
        if(fork == null) {
            if(!create_fork_if_absent)
                throw new IllegalArgumentException("FORK not found in main stack");
            fork=new FORK();
            stack.insertProtocol(fork, position, neighbor);
        }
        // Now FORK has either been found or created and is not null

        // Create the fork-stack if absent
        Protocol bottom_prot=fork.get(fork_stack_id);
        if(bottom_prot == null)
            bottom_prot=fork.createForkStack(fork_stack_id, new ForkProtocolStack(), false, Arrays.asList(protocols));

        fork_stack=getForkStack(bottom_prot);

        Channel existing_ch=fork_stack.putIfAbsent(fork_channel_id,this);
        if(existing_ch != null && existing_ch != this)
            throw new IllegalArgumentException("fork-channel with id=" + fork_channel_id + " is already present");
    }



    public Address getAddress() {
        return main_channel.getAddress();
    }

    public String getName() {
        return main_channel.getName();
    }

    public String getName(Address member) {
        return main_channel.getName(member);
    }

    public void setName(String name) {
    }

    public JChannel name(String name) {
        return this;
    }

    public View getView() {
        return main_channel.getView();
    }

    public String getClusterName() {
        return main_channel.getClusterName();
    }

    public ProtocolStack getProtocolStack() {
        return null;
    }

    public boolean flushSupported() {
        return false;
    }

    public Map<String,Object> dumpStats() {
        return main_channel.dumpStats();
    }

    public void connect(String cluster_name) throws Exception {
    }

    public void connect(String cluster_name, Address target, long timeout) throws Exception {
    }

    public void disconnect() {
    }

    public void close() {
    }

    public void send(Message msg) throws Exception {
        checkClosedOrNotConnected();
        FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
        if(hdr != null)
            hdr.setForkChannelId(fork_channel_id);
        else {
            hdr=new FORK.ForkHeader(null, fork_channel_id);
            msg.putHeader(FORK.ID, hdr);
        }
        fork_stack.down(new Event(Event.MSG, msg));
    }

    public void send(Address dst, Object obj) throws Exception {
    }

    public void send(Address dst, byte[] buf) throws Exception {
    }

    public void send(Address dst, byte[] buf, int offset, int length) throws Exception {
    }

    public void startFlush(List<Address> flushParticipants, boolean automatic_resume) throws Exception {
    }

    public void startFlush(boolean automatic_resume) throws Exception {
    }

    public void stopFlush() {
    }

    public void stopFlush(List<Address> flushParticipants) {
    }

    public void getState(Address target, long timeout) throws Exception {
    }


    protected static ForkProtocolStack getForkStack(Protocol prot) {
        while(prot != null && !(prot instanceof ForkProtocolStack))
            prot=prot.getUpProtocol();
        return prot instanceof ForkProtocolStack? (ForkProtocolStack)prot : null;
    }

    protected static Protocol createForkStack(String fork_stack_id, Protocol... protocols) {
        ForkProtocolStack fork_stack=new ForkProtocolStack();
        Protocol bottom_prot=new ForkProtocol(fork_stack_id);
        List<Protocol> prots=new ArrayList<Protocol>();
        prots.add(fork_stack);
        prots.addAll(Arrays.asList(protocols));
        prots.add(bottom_prot);

        // Connect down protocols
        for(int i=0; i < prots.size(); i++) {
            Protocol current=prots.get(i);
            Protocol next=i+1 >= prots.size()? null : prots.get(i+1);
            if(next != null) {
                current.setDownProtocol(next);
                next.setUpProtocol(current);
            }
        }
        // bottom_prot.setDownProtocol(fork);
        return bottom_prot;
    }
}
