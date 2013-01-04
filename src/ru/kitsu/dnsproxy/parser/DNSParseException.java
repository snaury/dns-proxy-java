package ru.kitsu.dnsproxy.parser;

/**
 * Thrown on invalid or unsupported DNS messages
 * 
 * @author Alexey Borzenkov
 * 
 */
public class DNSParseException extends Exception {

	private static final long serialVersionUID = 1L;

	public DNSParseException() {
		super();
	}

	public DNSParseException(String message) {
		super(message);
	}

}
