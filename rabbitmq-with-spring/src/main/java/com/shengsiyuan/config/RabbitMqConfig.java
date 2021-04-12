package com.shengsiyuan.config;

import com.shengsiyuan.converter.CustomImageConverter;
import com.shengsiyuan.converter.CustomWordConverter;
import com.shengsiyuan.delegate.CustomMessageDelegate;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.ContentTypeDelegatingMessageConverter;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * @author gaowei
 * @date 2021/4/11 下午9:41
 */
@Configuration
public class RabbitMqConfig {

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setHost("localhost:5672");
        factory.setVirtualHost("test");
        factory.setUsername("steven");
        factory.setPassword("hadoop");
        factory.setConnectionTimeout(100000);
        factory.setCloseTimeout(100000);
        return factory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(){
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());
        rabbitTemplate.setReceiveTimeout(50000);
        return rabbitTemplate;
    }

    @Bean
    public RabbitAdmin rabbitAdmin() {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory());
        admin.setAutoStartup(true);
        return admin;
    }

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange("test.topic.exchange", true, false);
    }

    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange("test.direct.exchange", true, false);
    }

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange("test.fanout.exchange", true, false);
    }

    @Bean
    public Queue topicQueue(){
        return new Queue("test.topic.queue", true, false, false, null);
    }

    @Bean
    public Queue topicQueue2(){
        return new Queue("test.topic.queue2", true, false, false, null);
    }

    @Bean
    public Queue directQueue(){
        return new Queue("test.direct.queue", true, false, false, null);
    }

    @Bean
    public Queue fanoutQueue(){
        return new Queue("test.fanout.queue", true, false, false, null);
    }

    @Bean
    public Queue orderQueue(){
        return new Queue("test.order.queue", true, false, false, null);
    }

    @Bean
    public Queue addressQueue(){
        return new Queue("test.address.queue", true, false, false, null);
    }

    @Bean
    public Queue fileQueue(){
        return new Queue("test.file.queue", true, false, false, null);
    }

    @Bean
    public Binding topicBinding(){
        return BindingBuilder.bind(topicQueue()).to(topicExchange()).with("topic.#");
    }

    @Bean
    public Binding topicBinding2(){
        return BindingBuilder.bind(topicQueue2()).to(topicExchange()).with("topic.key.#");
    }

    @Bean
    public Binding directBinding(){
        return BindingBuilder.bind(directQueue()).to(directExchange()).with("direct.key");
    }

    @Bean
    public Binding orderQueueBinding(){
        return BindingBuilder.bind(orderQueue()).to(directExchange()).with("test.order.queue");
    }

    @Bean
    public Binding addressQueueBinding(){
        return BindingBuilder.bind(addressQueue()).to(directExchange()).with("test.address.queue");
    }

    @Bean
    public Binding fileQueueBinding(){
        return BindingBuilder.bind(fileQueue()).to(directExchange()).with("test.file.queue");
    }

    @Bean
    public Binding fanoutBinding(){
        return BindingBuilder.bind(fanoutQueue()).to(fanoutExchange());
    }

    @Bean
    public SimpleMessageListenerContainer simpleMessageListenerContainer(){
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory());
        container.setQueues(topicQueue(), directQueue(), fanoutQueue(), topicQueue2(), orderQueue(), addressQueue(), fileQueue());
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(10);
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);
        container.setDefaultRequeueRejected(false);
        container.setConsumerTagStrategy(s -> UUID.randomUUID().toString() + "-" + s);

        MessageListenerAdapter adapter = new MessageListenerAdapter(new CustomMessageDelegate());
        container.setMessageListener(adapter);

        /*
         * 处理json消息
         */
        adapter.setDefaultListenerMethod("consumeJsonMessage");
        adapter.setMessageConverter(new Jackson2JsonMessageConverter());
        container.setMessageListener(adapter);

        /*
         * 处理java对象消息
         */
        adapter.setDefaultListenerMethod("consumeJavaObjectMessage");
        DefaultJackson2JavaTypeMapper mapper = new DefaultJackson2JavaTypeMapper();
        mapper.setTrustedPackages("com.shengsiyuan.entity");
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
        converter.setJavaTypeMapper(mapper);
        adapter.setMessageConverter(converter);

        /*
         * 处理pdf/image型消息
         */
        adapter.setDefaultListenerMethod("consumeFileMessage");
        ContentTypeDelegatingMessageConverter typeDelegatingMessageConverter = new ContentTypeDelegatingMessageConverter();
        typeDelegatingMessageConverter.addDelegate("img/png", new CustomImageConverter());
        typeDelegatingMessageConverter.addDelegate("img/jpg", new CustomImageConverter());
        typeDelegatingMessageConverter.addDelegate("application/word", new CustomWordConverter());
        typeDelegatingMessageConverter.addDelegate("word", new CustomWordConverter());

        adapter.setMessageConverter(typeDelegatingMessageConverter);
        container.setMessageListener(adapter);
        return container;
    }
}
