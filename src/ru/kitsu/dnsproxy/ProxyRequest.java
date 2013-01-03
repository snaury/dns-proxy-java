package ru.kitsu.dnsproxy;

import java.net.SocketAddress;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import ru.kitsu.dnsproxy.parser.DNSMessage;

/**
 * Client request currently processed by the proxy
 * 
 * @author Alexey Borzenkov
 * 
 */
public final class ProxyRequest {
	private static final long defaultTimeout = TimeUnit.SECONDS.toNanos(5);

	private final SocketAddress addr;
	private final byte[] packet;
	private final DNSMessage message;
	private final long timestamp;
	private final long deadline;
	private final AtomicBoolean finished = new AtomicBoolean();
	private final List<UpstreamResponse> responses = new LinkedList<>();

	public static final class DeadlineComparator implements
			Comparator<ProxyRequest> {
		@Override
		public int compare(ProxyRequest o1, ProxyRequest o2) {
			if (o1.deadline < o2.deadline)
				return -1;
			if (o1.deadline == o2.deadline)
				return 0;
			return 1;
		}
	}

	public ProxyRequest(SocketAddress addr, byte[] packet, DNSMessage message) {
		this(addr, packet, message, defaultTimeout);
	}

	public ProxyRequest(SocketAddress addr, byte[] packet, DNSMessage message,
			long timeout) {
		this.addr = addr;
		this.packet = packet;
		this.message = message;
		this.timestamp = System.nanoTime();
		this.deadline = timestamp + timeout;
	}

	public SocketAddress getAddr() {
		return addr;
	}

	public byte[] getPacket() {
		return packet;
	}

	public DNSMessage getMessage() {
		return message;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getDeadline() {
		return deadline;
	}

	public synchronized UpstreamResponse[] getResponses() {
		UpstreamResponse[] r = new UpstreamResponse[responses.size()];
		return responses.toArray(r);
	}

	public synchronized UpstreamResponse[] getResponses(UpstreamResponse[] r) {
		return responses.toArray(r);
	}

	public synchronized int addResponse(UpstreamResponse response) {
		int index = responses.size();
		responses.add(response);
		return index;
	}

	public boolean isFinished() {
		return finished.get();
	}

	public boolean setFinished() {
		return !finished.getAndSet(true);
	}
}
