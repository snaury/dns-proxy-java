package ru.kitsu.dnsproxy;

import java.net.SocketAddress;

import ru.kitsu.dnsproxy.parser.DNSMessage;

/**
 * Response received from an upstream server
 * 
 * @author Alexey Borzenkov
 * 
 */
public class UpstreamResponse {
	private final SocketAddress addr;
	private final byte[] packet;
	private final DNSMessage message;
	private final ProxyRequest proxyRequest;
	private final long timestamp;

	public UpstreamResponse(SocketAddress addr, byte[] packet,
			DNSMessage message, ProxyRequest proxyRequest) {
		this.addr = addr;
		this.packet = packet;
		this.message = message;
		this.proxyRequest = proxyRequest;
		this.timestamp = System.nanoTime();
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

	public ProxyRequest getProxyRequest() {
		return proxyRequest;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
