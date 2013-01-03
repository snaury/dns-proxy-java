package ru.kitsu.dnsproxy;

import java.net.SocketAddress;

/**
 * Response that should be sent back to the client
 * 
 * @author Alexey Borzenkov
 * 
 */
public class ProxyResponse {
	private final SocketAddress addr;
	private final short requestId;
	private final byte[] responsePacket;

	public ProxyResponse(ProxyRequest request, UpstreamResponse response) {
		this.addr = request.getAddr();
		this.requestId = request.getMessage().getId();
		this.responsePacket = response.getPacket();
	}

	public SocketAddress getAddr() {
		return addr;
	}

	public short getRequestId() {
		return requestId;
	}

	public byte[] getResponsePacket() {
		return responsePacket;
	}
}
