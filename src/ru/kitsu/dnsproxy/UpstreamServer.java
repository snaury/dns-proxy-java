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
import java.util.concurrent.atomic.AtomicInteger;

import ru.kitsu.dnsproxy.parser.DNSMessage;
import ru.kitsu.dnsproxy.parser.DNSParseException;

public class UpstreamServer {
	// Maximum message should be 512 bytes
	// We accept up to 16384 bytes just in case
	private static final int MAX_PACKET_SIZE = 16384;

	private static final Random random = new Random();

	private final AtomicInteger inflightCount = new AtomicInteger();
	private final AtomicInteger parseErrors = new AtomicInteger();
	private final Map<Short, UpstreamRequest> inflight = new HashMap<>();
	private final Map<ProxyRequest, UpstreamRequest> accepted = new HashMap<>();
	private final BlockingQueue<Object> incoming = new LinkedBlockingQueue<>();
	private final BlockingQueue<UpstreamRequest> outgoing = new LinkedBlockingQueue<>();

	private final ProxyServer proxyServer;
	private final InetSocketAddress addr;
	private final DatagramChannel socket;
	private final Thread processingThread;
	private final Thread receiveThread;
	private final Thread sendThread;

	private final short shuffleKey = (short) random.nextInt();
	private short nextId = 0;

	private static class CancelRequest {
		ProxyRequest proxyRequest;

		CancelRequest(ProxyRequest proxyRequest) {
			this.proxyRequest = proxyRequest;
		}
	}

	private class ProcessingWorker implements Runnable {
		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					final Object op = incoming.take();
					if (op instanceof ProxyRequest) {
						// start a new ProxyRequest
						final ProxyRequest proxyRequest = (ProxyRequest) op;
						if (null != accepted.get(proxyRequest))
							continue;
						short id = generateRequestId();
						if (id == 0)
							continue; // no free slots left
						final UpstreamRequest upstreamRequest = new UpstreamRequest(
								id, proxyRequest);
						inflight.put(id, upstreamRequest);
						accepted.put(proxyRequest, upstreamRequest);
						inflightCount.set(inflight.size());
						outgoing.put(upstreamRequest);
						continue;
					}
					if (op instanceof CancelRequest) {
						// cancel existing ProxyRequest
						final ProxyRequest proxyRequest = ((CancelRequest) op).proxyRequest;
						final UpstreamRequest upstreamRequest = accepted
								.get(proxyRequest);
						if (null == upstreamRequest)
							continue;
						final Short id = upstreamRequest.getId();
						inflight.remove(id);
						accepted.remove(proxyRequest);
						inflightCount.set(inflight.size());
						continue;
					}
					if (op instanceof UpstreamResponse) {
						// match upstream response to proxy request
						final UpstreamResponse response = (UpstreamResponse) op;
						final Short id = response.getMessage().getId();
						final UpstreamRequest upstreamRequest = inflight
								.get(id);
						if (null == upstreamRequest)
							continue; // no such request in flight
						final ProxyRequest proxyRequest = upstreamRequest
								.getProxyRequest();
						if (!Arrays.equals(
								response.getMessage().getQuestions(),
								proxyRequest.getMessage().getQuestions()))
							continue; // ids match, but questions don't
						inflight.remove(id);
						accepted.remove(proxyRequest);
						inflightCount.set(inflight.size());
						proxyServer.onUpstreamResponse(proxyRequest, response);
						continue;
					}
					System.err.println("Unexpected message: " + op);
					System.exit(1);
				}
			} catch (InterruptedException e) {
				// interrupted
			}
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
	}

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
						message = DNSMessage.parse(buffer, false);
					} catch (BufferUnderflowException e) {
						parseErrors.incrementAndGet();
						continue; // message is severely truncated
					} catch (DNSParseException e) {
						parseErrors.incrementAndGet();
						continue; // cannot parse or whatever
					}
					if (!message.isResponse()) {
						parseErrors.incrementAndGet();
						continue; // ignore non-responses
					}
					buffer.rewind();
					final byte[] packet = new byte[buffer.limit()];
					buffer.get(packet);
					final UpstreamResponse response = new UpstreamResponse(
							remote, packet, message);
					incoming.put(response);
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
					final UpstreamRequest request = outgoing.take();
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

	public UpstreamServer(final ProxyServer proxyServer, final String host,
			final int port) throws IOException {
		this.proxyServer = proxyServer;
		addr = new InetSocketAddress(host, port);
		if (addr.isUnresolved()) {
			throw new IOException("Cannot resolve '" + host + "'");
		}
		socket = DatagramChannel.open(StandardProtocolFamily.INET);
		final String prefix = "Upstream " + addr;
		processingThread = new Thread(new ProcessingWorker(), prefix
				+ " processing");
		receiveThread = new Thread(new ReceiveWorker(), prefix + " receive");
		sendThread = new Thread(new SendWorker(), prefix + " send");
	}

	public InetSocketAddress getAddr() {
		return addr;
	}

	public int getInflightCount() {
		return inflightCount.get();
	}

	public int getParseErrors() {
		return parseErrors.get();
	}

	public void start() {
		processingThread.start();
		receiveThread.start();
		sendThread.start();
	}

	public void stop() {
		processingThread.interrupt();
		receiveThread.interrupt();
		sendThread.interrupt();
	}

	public void startRequest(ProxyRequest proxyRequest)
			throws InterruptedException {
		if (null == proxyRequest)
			throw new NullPointerException();
		incoming.put(proxyRequest);
	}

	public void cancelRequest(ProxyRequest proxyRequest)
			throws InterruptedException {
		if (null == proxyRequest)
			throw new NullPointerException();
		incoming.put(new CancelRequest(proxyRequest));
	}
}
