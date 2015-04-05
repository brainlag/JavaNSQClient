package io.nsq.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.nsq.frames.NSQFrame;

import java.util.List;

public class NSQDecoder extends ReplayingDecoder<NSQDecoder.MyDecoderState>{

	public enum MyDecoderState {
		READ_SIZE,
		READ_FRAME_ID,
		READ_DATA
	}
	
	private int size;
	private NSQFrame frame;
	
	 public NSQDecoder() {
	    // Set the initial state.
	    super(MyDecoderState.READ_SIZE);
	 }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		switch (state()) {
	     case READ_SIZE:
	       size = in.readInt();
	       checkpoint(MyDecoderState.READ_FRAME_ID);
	       break;
	     case READ_FRAME_ID:
	    	 int id = in.readInt();
	    	 frame = NSQFrame.instance(id);
	    	 if (frame == null) {
	    		 //uhh, bad response from server..  what should we do?
	    		 throw new Exception("Bad frame id from server (" + id + ").  disconnect!");
	    	 }
	    	 frame.setSize(size);
	    	 checkpoint(MyDecoderState.READ_DATA);
	    	 break;
	     case READ_DATA:
	    	 ByteBuf bytes = in.readBytes(frame.getSize()-4); //subtract 4 because the frame id is included
	    	 frame.setData(bytes.array());
	    	 checkpoint(MyDecoderState.READ_SIZE);
	    	 out.add(frame);
             break;
	     default:
	       throw new Error("Shouldn't reach here.");
	     }
	}

}