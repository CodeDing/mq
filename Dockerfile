FROM rabbitmq:management

RUN rabbitmq-plugins enable rabbitmq_tracing
RUN rabbitmq-plugins enable rabbitmq_shovel rabbitmq_shovel_management
