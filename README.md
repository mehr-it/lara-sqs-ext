# lara-sqs-ext
This package offers extended queue functionality for Amazon SQS queues in Laravel. Out of
the box it adds support for long polling and automatically sets the visibility timeout to
job's timeout. Of course you may set the visibility timeout manually at any time. 

This package is also a great starting point for further extensions.

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


## Visibility timeout

The visibility timeout is one of the key concepts in AWS SQS but us not well used in Laravel's default
SQS implementation. This package provides advanced usage of this feature.

For more information about visibility timeout see the [AWS documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html).

### Automatically set visibility timeout to job timeout

If your jobs have a timeout, the SQS messages should be invisible to other subscribers exactly the
same time. Unfortunately laravel does not set it automatically.

However **SqsExtJob automatically sets the visibility timeout of the SQS messages to the job
timeout** if a timeout is specified.

We think this makes sense for all SQS jobs. That's why this behaviour is activated by default.

However you may set a custom time value or deactivate this behaviour by setting
`$automaticQueueVisibility = false`:

	class MyJob extends SqsExtJob	{
		
		// will set visibility timeout to 45sec, regardless of job's timeout
		protected $automaticQueueVisibility = 45;
	}

You may even add an extra amount to job's timeout using the `$automaticQueueVisibilityExtra`
property.

### Manually setting visibility timeout

The `SqsExtJob` class has a new method `setVisibilityTimeout` which allows you to set the
visibility timeout manually. This is especially useful if you want to acquire more time for job
processing.

**If you manually set the visibility timeout, be aware that the job timeout still applies and your
worker processes will stop running after that amount of time**

The `InteractsWithSqsQueue` trait implements the `setVisibilityTimeout` method, as the `InteractsWithQueue`
trait does for other methods.

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

