# lara-sqs-ext
This package offers extended queue functionality for Amazon SQS queues in Laravel. Out of
the box it adds support for long polling but it is also a great starting point for
further extensions.

## Install

	composer require mehr-it/lara-sqs-ext
	
This package uses Laravel's package auto-discovery, so the service provider will be loaded 
automatically.

## Queue configuration

Just configure a queue connection as you would configure any other SQS queue in Laravel but use
`sqs-ext` as driver.

	'sqs-conn' => [
		'driver' => 'sqs-ext',
		'key'    => '112233445566778899',
		'secret' => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
		'prefix' => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
		'queue'  => 'msgs',
		'region' => 'eu-central-1',
	],
	
### Long polling

To enable long polling, you may add the option `message_wait_timeout` to the queue
configuration. This sets the `WaitTimeSeconds` parameter to the configured amount of time.

	'sqs-conn' => [
		'driver'               => 'sqs-ext',
		'key'                  => '112233445566778899',
		'secret'               => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
		'prefix'               => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
		'queue'                => 'msgs',
		'region'               => 'eu-central-1',
		'message_wait_timeout' => 20,
	],
	
Valid wait timeouts are between 0 and 20 seconds. Long polling might not be a suitable
configuration if you query multiple queues with a single worker.

For more information about long polling see the [AWS SDK documentation](https://docs.aws.amazon.com/de_de/sdk-for-php/v3/developer-guide/sqs-examples-enable-long-polling.html).


## Extending

The classes in this library offer good entry points for extending the classes. Have a look
at the source code if you wish to add further functionality.

If you only need to extend the Job class, you can also do this in the queue configuration:

	'sqs-conn' => [
		'driver'   => 'sqs-ext',
		'key'      => '112233445566778899',
		'secret'   => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
		'prefix'   => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
		'queue'    => 'msgs',
		'region'   => 'eu-central-1',
		'job_type' => 'myJobClass'
	],
	
The `myJobClass` is resolved via the application service container. Following parameters
are passed to the resolver:

	[
		'container'      => $container, 		// the application container
		'sqs'            => $sqs,				// the SQS client
		'job'            => $job,				// the raw SQS message
		'connectionName' => $connectionName,	// the queue connection name
		'queue'          => $queue,				// the queue URL
		'queueOptions'   => $this->options,		// the queue options (configuration)
	]

