package cl.duoc.andesstay.notify.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({RabbitTopologyProperties.class, NotifyRetryProperties.class})
public class AmqpMessageConverterConfig {

	@Bean
	public MessageConverter jackson2JsonMessageConverter(ObjectMapper objectMapper) {
		Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
		DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
		typeMapper.setTrustedPackages("cl.duoc.andesstay.notify.messaging");
		converter.setJavaTypeMapper(typeMapper);
		return converter;
	}
}
