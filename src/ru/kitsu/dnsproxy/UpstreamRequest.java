package ru.kitsu.dnsproxy;

/**
 * Tracks a request sent to an upstream server
 * 
 * @author Alexey Borzenkov
 * 
 */
public class UpstreamRequest {
	private final short id;
	private final ProxyRequest proxyRequest;

	public UpstreamRequest(short id, ProxyRequest proxyRequest) {
		this.id = id;
		this.proxyRequest = proxyRequest;
	}

	public short getId() {
		return id;
	}

	public ProxyRequest getProxyRequest() {
		return proxyRequest;
	}
}
