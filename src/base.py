from inspect import iscoroutinefunction

from typing import Callable, Awaitable

from uuid import UUID as uuid, uuid4

from json import loads, dumps

from aiormq import connect, spec
from aiormq.abc import DeliveredMessage as Message

from asyncio import Event

from logging import getLogger, Logger

class Request:

    def __init__(self) -> None:
        self.guid: uuid = uuid4()
        self.event: Event = Event()
        self.data: object = None

    async def set(self, data: object):
        self.data = data
        self.event.set()

    async def get(self):
        await self.event.wait()
        return self.data

class AMQPRouter:

    def __init__(self) -> None:

        self.client: AMQPClient = None
        self.parent: AMQPRouter = None

        self.listeners: dict[str, dict[str, list[Callable[[object], None]]]] = {}
        self.handlers: dict[str, dict[str, Callable[[object], object]]] = {}

    def listen(self, entity: str, event: str) -> Callable[[str], Callable[[object], None]]:

        def decorate(callback: Callable[[object], None]) -> None:
            self.include_listener(entity, event, callback)
        
        return decorate

    def handle(self, entity: str, operation: str) -> Callable[[str], Callable[[object], object]]:
        
        def decorate(callback: Callable[[object], object]) -> None:
            self.include_handler(entity, operation, callback)
        
        return decorate

    def include_router(self, router: "AMQPRouter") -> None:

        router.parent = self
        
        for entity, events in router.listeners.items():
            for event, listeners in events.items():
                for listener in listeners:
                    self.include_listener(entity, event, listener)
        
        for entity, operations in router.handlers.items():
            for operation, listener in operations.items():
                self.include_handler(entity, operation, listener)

    def include_listener(self, entity: str, event: str, callback: Callable[[dict | str], None]) -> None:

        if entity in self.listeners:
            if event in self.listeners[entity]:
                self.listeners[entity][event].append(callback)
            else:
                self.listeners[entity][event] = [callback]
        else:
            self.listeners[entity] = {event: [callback]}

    def include_handler(self, entity: str, operation: str, callback: Callable[[dict | str], None]) -> None:

        if entity in self.handlers:
                self.handlers[entity][operation] = callback
        else:
            self.handlers[entity] = {operation: callback}

class AMQPClient:

    def __init__(self, host: str, port: int, username: str, password: str, application: str, service: str, logger: Logger = getLogger()) -> None:

        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password

        self.application: str = application
        self.service: str = service

        self.connection = None
        self.channel = None

        self.requests: dict[uuid, tuple[Event, object]] = {}

        self.listeners: dict[str, dict[str, list[Callable[[object], None]]]] = {}
        self.handlers: dict[str, dict[str, Callable[[object], object]]] = {}

        self.logger: Logger = logger

    async def start(self):

        self.connection = await connect(f'amqp://{self.username}:{self.password}@{self.host}:{self.port}')
        self.channel = await self.connection.channel()

        self.channel.closing.add_done_callback(self._handle_stop)

        await self._register()

        self.logger.info(f'Connected to AMQP Server at {self.host}:{self.port} using username {self.username}.')

    async def stop(self):
        await self.channel.close()
        await self.connection.close()

    async def notify(self, entity: str, event: str, data: object) -> None:

        self.logger.info(f'Send notification of event {event} on {entity}.')

        message: bytes
        try:
            message = dumps(data).encode('utf-8')
        except ValueError:
            message: bytes = str(data).encode('utf-8')

        # send the notification to the exchange using an appropriate routing key
        await self.channel.basic_publish(
            exchange=self.application,
            routing_key=f'notify.{entity}.{event}',
            body=message
        )

    async def request(self, entity: str, operation: str, data: object) -> object:
        
        request: Request = Request()

        # put request into list of requests to match response
        self.requests[request.guid] = request

        self.logger.info(f'Send request of operation {operation} on {entity} ({request.guid}).')
        
        message: bytes
        try:
            message = dumps(data).encode('utf-8')
        except ValueError:
            message: bytes = str(data).encode('utf-8')

        # send the request to the exchange using an appropriate routing key
        await self.channel.basic_publish(
            exchange = self.application,
            routing_key = f'request.{entity}.{operation}',
            properties = spec.Basic.Properties(
                correlation_id = str(request.guid),
                reply_to = f'response.{self.service}'
            ),
            body=message
        )

        # wait until the result of the request was recieved and return the data
        return await request.get()

    def listen(self, entity: str, event: str) -> Callable[[str], Callable[[object], None]]:

        def decorate(callback: Callable[[object], None]) -> None:
            self.include_listener(entity, event, callback)
        
        return decorate

    def handle(self, entity: str, operation: str) -> Callable[[str], Callable[[object], object]]:
        
        def decorate(callback: Callable[[object], object]) -> None:
            self.include_handler(entity, operation, callback)
        
        return decorate

    async def _register(self):
        await self._declare_exchange()
        await self._declare_queues()
        await self._bind_queues()
        await self._consume_queues()

    async def _declare_exchange(self):
        await self.channel.exchange_declare(self.application, exchange_type='topic', durable=True)

    async def _declare_queues(self):
        await self.channel.queue_declare(f'{self.application}.{self.service}.notify', durable=True)
        await self.channel.queue_declare(f'{self.application}.{self.service}.request', durable=True)
        await self.channel.queue_declare(f'{self.application}.{self.service}.response', durable=True)

    async def _bind_queues(self):

        for (entity, events) in self.listeners.items():
            for (event, _) in events.items():
                await self.channel.queue_bind(f'{self.application}.{self.service}.notify', self.application, f'notify.{entity}.{event}')

        for (entity, operations) in self.handlers.items():
            for (operation, _) in operations.items():
                await self.channel.queue_bind(f'{self.application}.{self.service}.request', self.application, f'request.{entity}.{operation}')
        
        await self.channel.queue_bind(f'{self.application}.{self.service}.response', self.application, f'response.{self.service}')
    
    async def _consume_queues(self):
        await self.channel.basic_consume(f'{self.application}.{self.service}.notify', self._handle_notify)
        await self.channel.basic_consume(f'{self.application}.{self.service}.request', self._handle_request)
        await self.channel.basic_consume(f'{self.application}.{self.service}.response', self._handle_response)

    def _handle_stop(self, test):
        self.logger.info(f'Disconnected from AMQP Server.')

    async def _handle_request(self, message: Message):

        # retrieve entity and operation based on the routing key of the message
        _, entity, operation = message.routing_key.split('.')

        # try to parse content of message to json otherwise retain original type
        data: dict[str, object] | list[object] | object
        try:
            data = loads(message.body)
        except ValueError:
            data = message.body

        self.logger.info(f'Recieved request of operation {operation} on {entity} ({message.header.properties.correlation_id}).')
        
        try:

            # retrieve the handler method
            handler: Callable | Awaitable = self.handlers[entity][operation]
            result: object = None

            # execute the handler method either synchronously or asynchronously to retrieve a response
            if iscoroutinefunction(handler):
                result = await handler(data)
            else:
                result = handler(data)

            # send the response to the exchange using the given response routing key
            await message.channel.basic_publish(
                exchange = self.application,
                routing_key = message.header.properties.reply_to,
                body = result,
                properties = spec.Basic.Properties(
                    correlation_id = message.header.properties.correlation_id
                )
            )

        except Exception:

            self.logger.info(f'Request {message.header.properties.correlation_id} could not be processed.')
        
            # reject the message that raised the exception for processing by another consumer
            await message.channel.basic_reject(
                delivery_tag = message.delivery.delivery_tag,
                requeue = True
            )
            
        else:

            # confirm the successfull processing of the message
            await message.channel.basic_ack(
                delivery_tag = message.delivery.delivery_tag
            )

    async def _handle_response(self, message: Message):

        # retrieve the matching request of the response
        request: Request = self.requests[uuid(message.header.properties.correlation_id)]

        # try to parse content of message to json otherwise retain original type
        data: dict[str, object] | list[object] | object
        try:
            data = loads(message.body)
        except ValueError:
            data = message.body

        self.logger.info(f'Recieved response to request {request.guid}.')

        # match the resulting data to the request and unlock the process
        request.set(data)

        # confirm the successfull processing of the message
        await message.channel.basic_ack(
            delivery_tag = message.delivery.delivery_tag
        )

    async def _handle_notify(self, message: Message):

        # retrieve entity and event based on the routing key of the message
        _, entity, event = message.routing_key.split('.')

        self.logger.info(f'Recieved notification of event {event} on {entity}.')
        
        # try to parse content of message to json otherwise retain original type
        data: dict[str, object] | list[object] | object
        try:
            data = loads(message.body)
        except ValueError:
            data = message.body

        try:

            # iterate through all listeners and execute the corresponding callback method
            for listener in self.listeners[entity][event]:

                # execute the listener method either synchronously or asynchronously to retrieve a response
                if iscoroutinefunction(listener):
                    await listener(data)
                else:
                    listener(data)

        except Exception as exception:

            print(entity, event, self.listeners)

            self.logger.info(f'Notification could not be processed.')
        
            # reject the message that raised the exception for processing by another consumer
            await message.channel.basic_reject(
                delivery_tag = message.delivery.delivery_tag,
                requeue = True
            )
            
        else:

            # confirm the successfull processing of the message
            await message.channel.basic_ack(
                delivery_tag = message.delivery.delivery_tag
            )

    def include_router(self, router: "AMQPRouter") -> None:

        router.client = self
        
        for entity, events in router.listeners.items():
            for event, listeners in events.items():
                for listener in listeners:
                    self.include_listener(entity, event, listener)
        
        for entity, operations in router.handlers.items():
            for operation, listener in operations.items():
                self.include_handler(entity, operation, listener)

    def include_listener(self, entity: str, event: str, callback: Callable[[dict | str], None]) -> None:

        if entity in self.listeners:
            if event in self.listeners[entity]:
                self.listeners[entity][event].append(callback)
            else:
                self.listeners[entity][event] = [callback]
        else:
            self.listeners[entity] = {event: [callback]}

    def include_handler(self, entity: str, operation: str, callback: Callable[[dict | str], None]) -> None:

        if entity in self.handlers:
                self.handlers[entity][operation] = callback
        else:
            self.handlers[entity] = {operation: callback}