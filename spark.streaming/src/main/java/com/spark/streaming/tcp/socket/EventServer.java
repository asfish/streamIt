package com.spark.streaming.tcp.socket;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is an event generator, which generates fake events stream. This is the java class
 * for the data generating server. The server sets up a socket and generates data of the form
 * "username:event", where event could be "login" or "purchase".
 *
 * Run this class before any of the streaming programs so they have something to get data from!
 *
 *
 * Created by agebriel on 8/17/17.
 */
public class EventServer
{
	private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
	private static final int PORT = 9999;
	private static final String DELIMITER = ":";
	private static final long EVENT_PERIOD_SECONDS = 1;
	private static final Random random = new Random();

	public static void main(String[] args) throws IOException, InterruptedException
	{
		BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
		SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
		while (true)
		{
			eventQueue.put(generateEvent());
			TimeUnit.SECONDS.sleep(EVENT_PERIOD_SECONDS);
		}
	}

	private static String generateEvent() {
		int userNumber = random.nextInt(10);
		String event = random.nextBoolean() ? "login" : "purchase";

		// In production use a real schema like JSON or protocol buffers
		return String.format("user-%s", userNumber) + DELIMITER + event;
	}

	private static class SteamingServer implements Runnable {
		private final BlockingQueue<String> eventQueue;

		public SteamingServer(BlockingQueue<String> eventQueue) {
			this.eventQueue = eventQueue;
		}

		@Override
		public void run() {
			try (ServerSocket serverSocket = new ServerSocket(PORT);
			     Socket clientSocket = serverSocket.accept();
			     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
			) {
				while (true) {
					String event = eventQueue.take();
					System.out.println(String.format("Writing \"%s\" to the socket.", event));
					out.println(event);
				}
			} catch (IOException|InterruptedException e) {
				throw new RuntimeException("Server error", e);
			}
		}
	}
}
