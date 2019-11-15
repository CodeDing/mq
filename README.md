# mq
Message Queue client wrapper

# Basic Use

- Provide publisher and subscribe interface, only support reliable and unreliable message send/receive.

**Note**
- reliable: Publisher uses RabbitMQ confirm mode to make sure the RabbitMQ Server receive the message 
        and Consumer will ack each message after receive and handle it successfully.
- unreliable: Publisher will send the message and return immediately and Consumer will not ack the each
        message. 