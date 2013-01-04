package ru.kitsu.dnsproxy;

/**
 * Parsed upstream configuration line
 * 
 * @author Alexey Borzenkov
 * 
 */
public class UpstreamConfig {
	private final String host;
	private final int port;

	public UpstreamConfig(String host) {
		this(host, 53);
	}

	public UpstreamConfig(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public static UpstreamConfig parseLine(String line) {
		int index = line.lastIndexOf(':');
		if (index != -1) {
			return new UpstreamConfig(line.substring(0, index),
					Integer.parseInt(line.substring(index + 1)));
		}
		return new UpstreamConfig(line);
	}
}
