package com.twister.nio;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Iterator; 
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 * IOLoop handles acceptance events from selection in the same thread (supposed
 * to be the thread main sin its invoked from there). All other events returned
 * from a poll will be executed in a thread available in the poll.
 * 
 * TODO Another test is to dispatch execution to the pool, only code handled by
 * the user app. All the JTornado code may run in a single thread. Need to test.
 * 
 * @author zhouguoqing
 * 
 */
public class IOLoopEvent {

	private static final int MIN_SELECT_TIMEOUT = 1; 
	public static int SELECT_TIMEOUT = 3000;

	/**
	 * Receives a Selection Key previously registered for a given selector. At
	 * the moment of receive, the key will be already canceled if it is not a
	 * OP_ACCEPT.	 * 
	 */
	public static interface EventHandler {
		/**
		 * Handles the Selectable Channel. opts means the current ready
		 * operations.
		 * 
		 * @param opts
		 * @param selectionKey
		 * @throws Exception
		 */
		public void handleEvents(int opts, SelectionKey selectionKey)
				throws Exception;

	}
	 
	public static abstract class EventHandlerAdapter implements EventHandler {
		 
		@Override
		public void handleEvents(int opts, SelectionKey selectionKey)
				throws Exception {
			SelectableChannel channel = selectionKey.channel();
			switch (opts) {
			case SelectionKey.OP_READ:
				try {
					onRead(selectionKey);
				} catch (Exception e) {
					onReadError(e, selectionKey);
				}
				break;
			case SelectionKey.OP_WRITE:
				try {
					onWrite(selectionKey);
				} catch (Exception e) {
					onWriteError(e, selectionKey);
				}
				break;
			case SelectionKey.OP_ACCEPT:
				try {
					_onAccept(selectionKey);
				} catch (Exception e) {
					onAcceptError(e, selectionKey);
				}
				break;
			default:
				throw new UnsupportedOperationException();
			}

		}

		private void _onAccept(SelectionKey selectionKey) throws Exception {
			SelectableChannel channel = selectionKey.channel();
			SocketChannel clientChannel = ((ServerSocketChannel) channel)
					.accept();
			clientChannel.configureBlocking(false);
			onAccept(selectionKey);

		}

		protected void onAcceptError(Exception e,SelectionKey selectionKey) {
			onWriteError(e,selectionKey);
		}

		protected void onAccept(SelectionKey selectionKey) throws Exception {		 
			throw new UnsupportedOperationException();
		}

		/**
		 * Default behavior is close the channel - if open - and print the
		 * stack.
		 * 
		 * @param e
		 * @param channel
		 */
		protected void onWriteError(Exception e, SelectionKey selectionKey) {
			 
			try {
				SelectableChannel channel = selectionKey.channel();
				if (channel.isOpen())
					channel.close();
				e.printStackTrace();
			} catch (Exception _e) {
				_e.printStackTrace();
			}
		}

		protected void onReadError(Exception e, SelectionKey selectionKey) {
			onWriteError(e, selectionKey);
		}

		protected void onWrite(SelectionKey selectionKey) throws Exception {
			System.out.println("onWrite");
			throw new UnsupportedOperationException();
		}

		protected void onRead(SelectionKey selectionKey) throws Exception {
			System.out.println("onread");
			throw new UnsupportedOperationException();
		}

	}
	
	private Selector selector; 
	public IOLoopEvent(){
		try {
			this.selector = Selector.open();
		} catch (IOException e) {			 
			e.printStackTrace();
		}
	}
	
	private Selector getSelector() {
		return this.selector;
	}
}
