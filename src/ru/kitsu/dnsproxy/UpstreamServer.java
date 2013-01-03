package ru.kitsu.dnsproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import ru.kitsu.dnsproxy.parser.DNSMessage;
import ru.kitsu.dnsproxy.parser.DNSParseException;

public class UpstreamServer {
	// Maximum message should be 512 bytes
	// We accept up to 16384 bytes just in case
	private static final int MAX_PACKET_SIZE = 16384;

	private static final Random random = new Random();

	private final Lock lock = new ReentrantLock();
	private final Map<Short, UpstreamRequest> inflight = new HashMap<>();
	private final Map<ProxyRequest, UpstreamRequest> accepted = new HashMap<>();
	private final BlockingQueue<UpstreamRequest> requests = new LinkedBlockingQueue<>();
	private final BlockingQueue<UpstreamResponse> responses = new LinkedBlockingQueue<>();

	private final ProxyServer proxyServer;
	private final InetSocketAddress addr;
	private final DatagramChannel socket;
	private final Thread receiveThread;
	private final Thread sendThread;
	private final Thread responsesThread;

	private final short shuffleKey = (short) random.nextInt();
	private short nextId = 0;

	private class ReceiveWorker implements Runnable {
		@Override
		public void run() {
			ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_PACKET_SIZE);
			try {
				// Loop as long as channel is still open
				while (!Thread.interrupted()) {
					buffer.clear();
					final SocketAddress remote;
					try {
						remote = socket.receive(buffer);
					} catch (ClosedChannelException e) {
						stop();
						break;
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
					if (remote == null)
						continue; // shouldn't happen, but just in case
					buffer.flip();
					if (!addr.equals(remote))
						continue; // ignore packets from unexpected sources
					final DNSMessage message;
					try {
						message = DNSMessage.parse(buffer);
					} catch (BufferUnderflowException e) {
						continue; // message is severely truncated
					} catch (DNSParseException e) {
						continue; // cannot parse or whatever
					}
					if (!message.isResponse())
						continue; // ignore non-responses
					final UpstreamRequest request;
					lock.lockInterruptibly();
					try {
						final Short id = message.getId();
						request = inflight.get(id);
						if (request == null)
							continue; // not inflight, stale or cancelled
						if (!Arrays.equals(message.getQuestions(), request
								.getProxyRequest().getMessage().getQuestions()))
							continue; // skip if questions don't match
						inflight.remove(id);
						accepted.remove(request.getProxyRequest());
					} finally {
						lock.unlock();
					}
					buffer.rewind();
					final byte[] packet = new byte[buffer.limit()];
					buffer.get(packet);
					final UpstreamResponse response = new UpstreamResponse(
							remote, packet, message, request.getProxyRequest());
					// onResponse(response);
					responses.put(response);
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class SendWorker implements Runnable {
		@Override
		public void run() {
			ByteBuffer buffer = ByteBuffer.allocateDirect(MAX_PACKET_SIZE);
			try {
				// Loop until interrupted
				while (!Thread.interrupted()) {
					final UpstreamRequest request = requests.take();
					// FIXME: is this check really worth it?
					lock.lockInterruptibly();
					try {
						if (accepted.get(request.getProxyRequest()) != request)
							continue; // request is cancelled already!
					} finally {
						lock.unlock();
					}
					// Construct and send the message
					final byte[] packet = request.getProxyRequest().getPacket();
					if (packet.length < 12 || packet.length > MAX_PACKET_SIZE)
						continue;
					buffer.clear();
					buffer.putShort(request.getId());
					buffer.put(packet, 2, packet.length - 2);
					buffer.flip();
					try {
						socket.send(buffer, addr);
					} catch (ClosedChannelException e) {
						stop();
						break;
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class ResponsesWorker implements Runnable {
		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					final UpstreamResponse response = responses.take();
					onResponse(response);
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	public UpstreamServer(final ProxyServer proxyServer, final String host,
			final int port) throws IOException {
		this.proxyServer = proxyServer;
		addr = new InetSocketAddress(host, port);
		if (addr.isUnresolved()) {
			throw new IOException("Cannot resolve '" + host + "'");
		}
		socket = DatagramChannel.open(StandardProtocolFamily.INET);
		receiveThread = new Thread(new ReceiveWorker());
		sendThread = new Thread(new SendWorker());
		responsesThread = new Thread(new ResponsesWorker());
	}

	public InetSocketAddress getAddr() {
		return addr;
	}

	public int getInflightCount() {
		lock.lock();
		try {
			return inflight.size();
		} finally {
			lock.unlock();
		}
	}

	public void start() {
		receiveThread.start();
		sendThread.start();
		responsesThread.start();
	}

	public void stop() {
		receiveThread.interrupt();
		sendThread.interrupt();
		responsesThread.interrupt();
	}

	/**
	 * Generates a new request id, or 0 if no free slots are available
	 * 
	 * @return next free request id
	 */
	private short generateRequestId() {
		short id = nextId;
		do {
			short requestId = (short) (shuffleKey ^ id++);
			if (requestId != 0 && inflight.get(requestId) == null) {
				nextId = id;
				return requestId;
			}
		} while (id != nextId);
		return 0;
	}

	public boolean startRequest(ProxyRequest proxyRequest)
			throws InterruptedException {
		if (proxyRequest == null)
			throw new NullPointerException();
		UpstreamRequest upstreamRequest;
		lock.lock();
		try {
			upstreamRequest = accepted.get(proxyRequest);
			if (upstreamRequest != null)
				return false; // reject duplicate requests
			short id = generateRequestId();
			if (id == 0)
				return false; // no free slots left
			upstreamRequest = new UpstreamRequest(id, proxyRequest);
			inflight.put(id, upstreamRequest);
			accepted.put(proxyRequest, upstreamRequest);
		} finally {
			lock.unlock();
		}
		requests.put(upstreamRequest);
		return true;
	}

	public boolean cancelRequest(ProxyRequest proxyRequest) {
		lock.lock();
		try {
			UpstreamRequest upstreamRequest = accepted.get(proxyRequest);
			if (upstreamRequest == null)
				return false; // never existed or gone already
			accepted.remove(proxyRequest);
			inflight.remove(upstreamRequest.getId());
		} finally {
			lock.unlock();
		}
		// NOTE: we don't remove request from pending because normally
		// by the time cancel is called request is already sent and
		// send thread will ignore requests that are removed.
		return true;
	}

	protected void onResponse(UpstreamResponse response)
			throws InterruptedException {
		proxyServer.onResponse(response);
	}
}
