package ru.kitsu.dnsproxy;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import ru.kitsu.dnsproxy.parser.DNSParseException;
import ru.kitsu.dnsproxy.parser.DNSMessage;

public class ProxyServer {
	private static final boolean DEBUG = false;
	// Maximum message should be 512 bytes
	// We accept up to 16384 bytes just in case
	private static final int MAX_PACKET_SIZE = 16384;
	// Date format in a log filename
	private static final SimpleDateFormat logNameDateFormat = new SimpleDateFormat(
			"yyyy-MM-dd-HH-mm");

	private final Lock lock = new ReentrantLock();
	private final Condition inflightAvailable = lock.newCondition();
	private final PriorityQueue<ProxyRequest> inflight = new PriorityQueue<>(
			11, new ProxyRequest.DeadlineComparator());
	private final BlockingQueue<ProxyResponse> responses = new LinkedBlockingQueue<>();
	private final BlockingQueue<RequestResponse> uresponses = new LinkedBlockingQueue<>();
	private final BlockingQueue<ProxyRequest> logged = new LinkedBlockingQueue<>();

	private final InetSocketAddress addr;
	private final DatagramChannel socket;
	private final Thread receiveThread;
	private final Thread sendThread;
	private final Thread responsesThread;
	private final Thread timeoutThread;
	private final Thread logThread;
	private final Thread statsThread;
	private final List<UpstreamServer> upstreams = new ArrayList<>();

	private static class RequestResponse {
		ProxyRequest request;
		UpstreamResponse response;

		RequestResponse(ProxyRequest request, UpstreamResponse response) {
			this.request = request;
			this.response = response;
		}
	}

	private class ReceiveWorker implements Runnable {
		@Override
		public void run() {
			final ByteBuffer buffer = ByteBuffer
					.allocateDirect(MAX_PACKET_SIZE);
			try {
				log("Accepting requests on " + addr);
				while (!Thread.interrupted()) {
					buffer.clear();
					final SocketAddress client;
					try {
						client = socket.receive(buffer);
					} catch (ClosedChannelException e) {
						log("Channel closed by " + e);
						stop();
						break;
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
					if (client == null)
						continue; // shouldn't happen, but just in case
					buffer.flip();
					final DNSMessage message;
					try {
						message = DNSMessage.parse(buffer);
					} catch (BufferUnderflowException e) {
						continue;
					} catch (DNSParseException e) {
						continue;
					}
					if (message.isResponse())
						continue; // only requests are accepted
					buffer.rewind();
					final byte[] packet = new byte[buffer.limit()];
					buffer.get(packet);
					final ProxyRequest request = new ProxyRequest(client,
							packet, message);
					onRequest(request);
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class SendWorker implements Runnable {
		@Override
		public void run() {
			final ByteBuffer buffer = ByteBuffer
					.allocateDirect(MAX_PACKET_SIZE);
			try {
				while (!Thread.interrupted()) {
					final ProxyResponse response = responses.take();
					final byte[] packet = response.getResponsePacket();
					if (packet.length < 12 || packet.length > MAX_PACKET_SIZE)
						continue;
					buffer.clear();
					buffer.putShort(response.getRequestId());
					buffer.put(packet, 2, packet.length - 2);
					buffer.flip();
					try {
						socket.send(buffer, response.getAddr());
					} catch (ClosedChannelException e) {
						log("Channel closed by " + e);
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
					RequestResponse uresponse = uresponses.take();
					onResponse(uresponse.request, uresponse.response);
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class TimeoutWorker implements Runnable {
		@Override
		public void run() {
			try {
				while (!Thread.interrupted()) {
					onTimeout(takeNextTimedOut());
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class LogWorker implements Runnable {
		@Override
		public void run() {
			try {
				long lastzone = -1;
				PrintStream output = null;
				StringBuilder sb = new StringBuilder();
				while (!Thread.interrupted()) {
					final ProxyRequest request = logged.take();
					// Current nanotime for latency of timed out requests
					final long nanotime = System.nanoTime();
					// Current system timestamp in milliseconds
					final long timestamp = System.currentTimeMillis() / 1000;
					// Current file zone, basically hourly file
					final long zone = timestamp - (timestamp % 3600);
					if (lastzone != zone) {
						if (output != null) {
							output.close();
							output = null;
						}
						try {
							output = new PrintStream(
									new FileOutputStream("resolve-"
											+ zone
											+ "-"
											+ logNameDateFormat
													.format(new Date(
															zone * 1000))
											+ ".log", true));
						} catch (FileNotFoundException e) {
							e.printStackTrace();
							Thread.sleep(1000);
							continue;
						}
						sb.setLength(0);
						sb.append("[time]");
						for (UpstreamServer upstream : upstreams) {
							sb.append("\t");
							sb.append(upstream.getAddr().getAddress()
									.getHostAddress());
							sb.append(":");
							sb.append(upstream.getAddr().getPort());
							sb.append("\t");
							sb.append("(latency)");
						}
						output.println(sb.toString());
						lastzone = zone;
					}
					// All upstream responses
					final UpstreamResponse[] responses = request.getResponses();
					// Start constructing a log line
					sb.setLength(0);
					sb.append(timestamp);
					for (UpstreamServer upstream : upstreams) {
						sb.append("\t");
						UpstreamResponse response = null;
						for (UpstreamResponse candidate : responses) {
							if (upstream.getAddr().equals(candidate.getAddr())) {
								response = candidate;
								break;
							}
						}
						if (response != null) {
							long delay = (response.getTimestamp() - request
									.getTimestamp()) / 1000000;
							sb.append(response.getMessage().getRcode());
							sb.append("\t");
							sb.append(delay);
							sb.append("ms");
						} else {
							long delay = (nanotime - request.getTimestamp()) / 1000000;
							sb.append("-\t");
							sb.append(delay);
							sb.append("ms");
						}
					}
					output.println(sb.toString());
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private class StatsWorker implements Runnable {
		@Override
		public void run() {
			try {
				StringBuilder sb = new StringBuilder();
				while (!Thread.interrupted()) {
					Thread.sleep(5000);
					long freeMemory = Runtime.getRuntime().freeMemory();
					long totalMemory = Runtime.getRuntime().totalMemory();
					sb.setLength(0);
					sb.append("Used memory: ");
					sb.append((totalMemory - freeMemory) / 1048576);
					sb.append("/");
					sb.append(totalMemory / 1048576);
					sb.append("MB, ");
					sb.append("Upstreams inflight: ");
					int index = 0;
					for (UpstreamServer upstream : upstreams) {
						if (index != 0) {
							sb.append(", ");
						}
						sb.append(upstream.getAddr().getAddress()
								.getHostAddress());
						sb.append(":");
						sb.append(upstream.getAddr().getPort());
						sb.append(": ");
						sb.append(upstream.getInflightCount());
						++index;
					}
					log(sb.toString());
				}
			} catch (InterruptedException e) {
				// interrupted
			}
		}
	}

	private static void log(String line) {
		System.out.format("[%s] %s\n", new Date(), line);
	}

	private ProxyServer(String host, int port) throws IOException {
		addr = new InetSocketAddress(host, port);
		if (addr.isUnresolved()) {
			throw new IOException("Cannot resolve '" + host + "'");
		}
		socket = DatagramChannel.open(StandardProtocolFamily.INET);
		socket.bind(addr);
		final String prefix = "Proxy " + addr;
		receiveThread = new Thread(new ReceiveWorker(), prefix + " receive");
		sendThread = new Thread(new SendWorker(), prefix + " send");
		responsesThread = new Thread(new ResponsesWorker(), prefix
				+ " responses");
		timeoutThread = new Thread(new TimeoutWorker(), prefix + " timeouts");
		logThread = new Thread(new LogWorker(), prefix + " logging");
		statsThread = new Thread(new StatsWorker(), prefix + " stats");
	}

	public void addUpstream(String host, int port) throws IOException {
		UpstreamServer upstream = new UpstreamServer(this, host, port);
		for (UpstreamServer currentUpstream : upstreams) {
			if (upstream.getAddr().equals(currentUpstream.getAddr()))
				throw new IOException(
						"Cannot add upstream with duplicate address "
								+ upstream.getAddr());
		}
		upstreams.add(upstream);
	}

	public void start() {
		for (UpstreamServer upstream : upstreams) {
			upstream.start();
		}
		receiveThread.start();
		sendThread.start();
		responsesThread.start();
		timeoutThread.start();
		logThread.start();
		statsThread.start();
	}

	public void stop() {
		receiveThread.interrupt();
		sendThread.interrupt();
		responsesThread.interrupt();
		timeoutThread.interrupt();
		logThread.interrupt();
		statsThread.interrupt();
		for (UpstreamServer upstream : upstreams) {
			upstream.stop();
		}
	}

	public void join() throws InterruptedException {
		receiveThread.join();
		sendThread.join();
		responsesThread.join();
		timeoutThread.join();
		logThread.join();
		statsThread.join();
	}

	private void addRequest(ProxyRequest request) throws InterruptedException {
		lock.lockInterruptibly();
		try {
			inflight.offer(request);
			inflightAvailable.signalAll();
		} finally {
			lock.unlock();
		}
	}

	private boolean removeRequest(ProxyRequest request)
			throws InterruptedException {
		lock.lockInterruptibly();
		try {
			return inflight.remove(request);
		} finally {
			lock.unlock();
		}
	}

	private ProxyRequest takeNextTimedOut() throws InterruptedException {
		// Unlike DelayQueue there's no Leader-Follower pattern,
		// but there's only one timeout thread, so it doesn't matter
		lock.lockInterruptibly();
		try {
			while (true) {
				ProxyRequest request = inflight.peek();
				if (request == null) {
					// Empty queue, wail indefinitely
					inflightAvailable.await();
					continue;
				}
				long delay = request.getDeadline() - System.nanoTime();
				if (delay <= 0) {
					request = inflight.poll();
					if (inflight.peek() != null)
						inflightAvailable.signalAll();
					return request;
				}
				inflightAvailable.awaitNanos(delay);
			}
		} finally {
			lock.unlock();
		}
	}

	protected void onRequest(ProxyRequest request) throws InterruptedException {
		if (DEBUG) {
			System.out.format("Request from %s: %s\n", request.getAddr(),
					request.getMessage());
		}
		addRequest(request);
		for (UpstreamServer upstream : upstreams) {
			upstream.startRequest(request);
		}
		if (request.isFinished()) {
			// Request was finished anywhere between
			// addRequest(request) and here, so we cannot be
			// sure it was cancelled on all upstreams.
			// Make sure it's really cancelled.
			// N.B.: chances of hitting this are very slim,
			// still better safe than sorry.
			for (UpstreamServer upstream : upstreams) {
				upstream.cancelRequest(request);
			}
		}
	}

	protected void onResponse(ProxyRequest request, UpstreamResponse response)
			throws InterruptedException {
		if (DEBUG) {
			System.out.format("Response from %s: %s\n", response.getAddr(),
					response.getMessage());
		}
		int index = request.addResponse(response);
		if (index == 0) {
			// First response is sent to the client
			responses.put(new ProxyResponse(request, response));
		}
		if (index == upstreams.size() - 1) {
			// Received last response, finish request
			if (request.setFinished()) {
				// Don't need timeout anymore
				removeRequest(request);
				// Send to logging
				logged.put(request);
			}
		}
	}

	protected void onTimeout(ProxyRequest request) throws InterruptedException {
		// Request was removed due to timeout
		if (request.setFinished()) {
			// Make sure it's cancelled
			for (UpstreamServer upstream : upstreams) {
				upstream.cancelRequest(request);
			}
			// Send to logging
			logged.put(request);
		}
	}

	public void onUpstreamResponse(ProxyRequest request,
			UpstreamResponse response) throws InterruptedException {
		uresponses.put(new RequestResponse(request, response));
	}

	private static void usage() {
		System.out
				.println("Usage: ProxyServer [-host host] [-port port] -config config");
		System.exit(1);
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		String host = "127.0.0.1";
		int port = 53;
		List<UpstreamConfig> upstreams = null;
		for (int i = 0; i < args.length; ++i) {
			switch (args[i]) {
			case "-host":
				if (++i >= args.length)
					usage();
				host = args[i];
				break;
			case "-port":
				if (++i >= args.length)
					usage();
				port = Integer.parseInt(args[i]);
				break;
			case "-config":
				if (++i >= args.length)
					usage();
				BufferedReader r = new BufferedReader(new InputStreamReader(
						new FileInputStream(args[i])));
				upstreams = new ArrayList<>();
				String line;
				while (null != (line = r.readLine())) {
					int index = line.indexOf('#');
					if (index != -1)
						line = line.substring(0, index);
					line = line.trim();
					if (line.length() == 0)
						continue;
					upstreams.add(UpstreamConfig.parseLine(line));
				}
				r.close();
				break;
			default:
				usage();
			}
		}
		if (upstreams == null) {
			upstreams = new ArrayList<>();
			upstreams.add(new UpstreamConfig("8.8.8.8"));
			upstreams.add(new UpstreamConfig("8.8.4.4"));
		}
		ProxyServer server = new ProxyServer(host, port);
		for (UpstreamConfig config : upstreams) {
			server.addUpstream(config.getHost(), config.getPort());
		}
		server.start();
		server.join();
	}
}
