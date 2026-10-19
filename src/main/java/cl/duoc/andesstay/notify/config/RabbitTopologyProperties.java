package cl.duoc.andesstay.notify.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Single runtime source for topology names is {@code application.yml}
 * ({@code andesstay.notify.topology.*}). {@link #defaults()} mirrors that file for unit tests
 * without a Spring context.
 */
@ConfigurationProperties(prefix = "andesstay.notify.topology")
public class RabbitTopologyProperties {

	private Exchanges exchanges = new Exchanges();
	private Queues queues = new Queues();
	private RoutingKeys routingKeys = new RoutingKeys();
	private TopicPatterns topicPatterns = new TopicPatterns();
	private String dlqSuffix;

	public Exchanges getExchanges() {
		return exchanges;
	}

	public void setExchanges(Exchanges exchanges) {
		this.exchanges = exchanges;
	}

	public Queues getQueues() {
		return queues;
	}

	public void setQueues(Queues queues) {
		this.queues = queues;
	}

	public RoutingKeys getRoutingKeys() {
		return routingKeys;
	}

	public void setRoutingKeys(RoutingKeys routingKeys) {
		this.routingKeys = routingKeys;
	}

	public TopicPatterns getTopicPatterns() {
		return topicPatterns;
	}

	public void setTopicPatterns(TopicPatterns topicPatterns) {
		this.topicPatterns = topicPatterns;
	}

	public String getDlqSuffix() {
		return dlqSuffix;
	}

	public void setDlqSuffix(String dlqSuffix) {
		this.dlqSuffix = dlqSuffix;
	}

	public String deadLetterQueue(String queue) {
		return queue + dlqSuffix;
	}

	/** Mirrors {@code application.yml} defaults for unit tests without Spring. */
	public static RabbitTopologyProperties defaults() {
		RabbitTopologyProperties topology = new RabbitTopologyProperties();
		topology.getExchanges().setDirect("cmd.direct");
		topology.getExchanges().setTopic("cmd.topic");
		topology.getExchanges().setDead("cmd.dead.dlx");
		topology.getQueues().setEmail("q.cmd.email");
		topology.getQueues().setHousekeeping("q.cmd.housekeeping");
		topology.getQueues().setVoucher("q.cmd.voucher");
		topology.getRoutingKeys().setEmail("email.send");
		topology.getRoutingKeys().setHousekeeping("housekeeping.ticket");
		topology.getRoutingKeys().setVoucher("voucher.gen");
		topology.getTopicPatterns().setEmail("email.*");
		topology.getTopicPatterns().setHousekeeping("housekeeping.#");
		topology.getTopicPatterns().setVoucher("voucher.*");
		topology.setDlqSuffix(".dlq");
		return topology;
	}

	public static class Exchanges {
		private String direct;
		private String topic;
		private String dead;

		public String getDirect() {
			return direct;
		}

		public void setDirect(String direct) {
			this.direct = direct;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getDead() {
			return dead;
		}

		public void setDead(String dead) {
			this.dead = dead;
		}
	}

	public static class Queues {
		private String email;
		private String housekeeping;
		private String voucher;

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getHousekeeping() {
			return housekeeping;
		}

		public void setHousekeeping(String housekeeping) {
			this.housekeeping = housekeeping;
		}

		public String getVoucher() {
			return voucher;
		}

		public void setVoucher(String voucher) {
			this.voucher = voucher;
		}
	}

	public static class RoutingKeys {
		private String email;
		private String housekeeping;
		private String voucher;

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getHousekeeping() {
			return housekeeping;
		}

		public void setHousekeeping(String housekeeping) {
			this.housekeeping = housekeeping;
		}

		public String getVoucher() {
			return voucher;
		}

		public void setVoucher(String voucher) {
			this.voucher = voucher;
		}
	}

	public static class TopicPatterns {
		private String email;
		private String housekeeping;
		private String voucher;

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String getHousekeeping() {
			return housekeeping;
		}

		public void setHousekeeping(String housekeeping) {
			this.housekeeping = housekeeping;
		}

		public String getVoucher() {
			return voucher;
		}

		public void setVoucher(String voucher) {
			this.voucher = voucher;
		}
	}
}
