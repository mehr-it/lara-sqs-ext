# lara-sqs-ext
[![Latest Version on Packagist](https://img.shields.io/packagist/v/mehr-it/lara-sqs-ext.svg?style=flat-square)](https://packagist.org/packages/mehr-it/lara-sqs-ext)
[![Build Status](https://travis-ci.org/mehr-it/lara-sqs-ext.svg?branch=master)](https://travis-ci.org/mehr-it/lara-sqs-ext)

This package offers extended queue functionality for Amazon SQS queues in Laravel. Out of
the box it adds support for long polling, automatically sets the visibility timeout to
job's timeout and allows longer delays (SQS maximum is 15min). Of course you may set the visibility
timeout manually at any time.

It also adds support for listen locks, to only poll a queue with a single worker and
save unnecessary costs.

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

### Extended job delay

Amazon SQS has a maximum delay of 15 minutes for dispatched messages. For some use cases, this might
not be enough. The `extend_delay` option allows to use longer delays when dispatching jobs:

	'sqs-conn' => [
        'driver'               => 'sqs-ext',
        'key'                  => '112233445566778899',
        'secret'               => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
        'prefix'               => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
        'queue'                => 'msgs',
        'region'               => 'eu-central-1',
        'message_wait_timeout' => 20,
        'extend_delay'         => true,
    ],

When extended delay is enabled, the job will be delayed up to 15 minutes at SQS. For greater delays
the job will be released back to the queue on each receive until the delay has elapsed.

Note: messages delayed longer than 15 minutes will be "in flight" until the delay has elapsed. There
is a [limit of 120,000 inflight messages for SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html#quotas-queues).

### Listen locks

When using long polling and multiple workers on the same queue, you should set the `listen_lock`
option to `true`. This synchronizes the worker processes polling the same queue and allows only
one worker at a time to poll the queue. This can save you a lot of money when using many workers.

	'sqs-conn' => [
        'driver'               => 'sqs-ext',
        'key'                  => '112233445566778899',
        'secret'               => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
        'prefix'               => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
        'queue'                => 'msgs',
        'region'               => 'eu-central-1',
        'message_wait_timeout' => 20,
        'listen_lock'          => true,
        // optionally specify custom lock file
        'listen_lock_file'     => '/path/to/file',
        // optionally specify listen lock timeout (in seconds)
        'listen_lock_timeout'  => 5
    ],

As soon as the long polling API request returns (with message received or not) the listen lock
is released and another process can acquire it.

The `listen_lock_timeout` value specifies how long the queue driver tries to obtain the listen lock 
before returning an empty reply to the worker loop. This value should not be too high, so that the
worker regularly can check for restart and other signals. 

## Message encryption
If you want to encrypt any messages end-to-end, you can set the `encrypt` to `true` for your queue:

    'sqs-conn' => [
		'driver'  => 'sqs-ext',
		'key'     => '112233445566778899',
		'secret'  => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
		'prefix'  => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
		'queue'   => 'msgs',
		'region'  => 'eu-central-1',
		'encrypt' => true,
	],
	
Laravel's internal encryption function will be used to encrypt the complete message string. Note
that encryption uses Base64 encoding, which increases the message size by ~36%.

## Visibility timeout

The visibility timeout is one of the key concepts in AWS SQS but is not well used in Laravel's default
SQS implementation. This package provides advanced usage of this feature.

For more information about visibility timeout see the [AWS documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html).

### Automatically set visibility timeout to job timeout

If your jobs have a timeout, the SQS messages should be invisible to other subscribers exactly the
same time. Unfortunately Laravel does not set it automatically.


However **SqsExtJob automatically sets the visibility timeout of the SQS messages to the job
timeout** if a timeout is specified.

We think this makes sense for all SQS jobs. That's why this behaviour is activated by default.

However you may set a custom time value or deactivate this behaviour by setting
`$automaticQueueVisibility = false`. Following example manually sets a visibility timeout which
has precedence over job timeout:

	class MyJob implements ShouldQueue {
		
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
worker processes will stop running after that amount of time!**

The `InteractsWithSqsQueue` trait implements the `setVisibilityTimeout` method, as the `InteractsWithQueue`
trait does for other methods.

## Rate limiting
You may configure rate limiting for your queues by passing a throttle in your
queue config. It uses the ["Token bucket" algorithm]([https://en.wikipedia.org/wiki/Token_bucket)
implemented by [mehr-it/lara-token-bucket](https://github.com/mehr-it/lara-token-bucket).

**For rate-limiting to work correctly in distributed systems, a centralized cache is required.**

```php
'sqs-conn' => [
    'driver'    => 'sqs-ext',
    'key'       => '112233445566778899',
    'secret'    => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
    'prefix'    => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
    'queue'     => 'msgs',
    'region'    => 'eu-central-1',
    'cache'     => 'the-centralized-cache',
    'throttles'=> [
        // queue name or wildcard as key
        'msgs' => [
            // the number of tokens added per second
            'rate'  => 0.5,
            // the maximum number of tokens in the bucket
            'burst' => 4,
            // the initial number of tokens in the bucket
            'initial' => 0,        
        ]       
    ]
],
```

If working with multiples queues per connection or if you are using wildcards
in queue workers, you may specify a throttle for each queue. Throttles even
support wildcards, which means that you may apply a throttle config to each
queue which matches a wildcard:

```php
'sqs-conn' => [
    'driver'    => 'sqs-ext',
    'key'       => '112233445566778899',
    'secret'    => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
    'prefix'    => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
    'queue'     => 'apiQueue1',
    'region'    => 'eu-central-1',
    // use a centralized cache
    'cache'     => 'the-centralized-cache',
    'throttles' => [
        // queue name or wildcard as key
        'apiQueue*' => [
            // the number of tokens added per second
            'rate'  => 0.5,
            // the maximum number of tokens in the bucket
            'burst' => 4,     
        ]       
    ]
],
```
Even if using a wildcard, each queue is throttled individually. However, you
may pass the `bucketName` option, to explicitly specify a shared token bucket
for multiple queues.


## Wildcard queue workers
When starting workers, laravel natively allows specifying which queues the 
worker should work on. Even though, multiple queue names can be given, workers
don't behave very smart when dealing with multiple queues.

This package allows to pass a wildcard as "queue" to workers:

```shell
# you should set sleep to zero, to efficiently switch between queues
./artisan queue:work sqs-conn --queue=apiQueue* --sleep=0
``` 

Given the example above, the worker will fetch the list of all available
queues starting with "apiQueue" and will poll them one after another for
new messages. But when selecting the next queue to poll, it takes to 
following into account:

### Throttled queues

If a queue is to be throttled due to rate-limits, it is not polled
until quota is available again.

### Empty queues

If a queue did not return a message on last poll, **polling is paused** on this
until either a new job is sent or the pause timeout elapses. The pause time
can be configured within the queue config.
This prevents your worker from wasting time with empty queues while other
queues are full.

Resuming paused workers on job sending is implemented using cache. **So 
centralized cache is crucial for distributed systems.**

**See following configuration example for a suitable configuration to be used
with wildcard workers**:

```php
'sqs-conn' => [
    'driver'                     => 'sqs-ext',
    'key'                        => '112233445566778899',
    'secret'                     => 'xxxxxxxxxxxxxxxxxxxxxxxxxx',
    'prefix'                     => 'https://sqs.eu-central-1.amazonaws.com/11223344556677',
    'queue'                      => 'apiQueue1',
    'region'                     => 'eu-central-1',
    // Long polling must not be used with wildcard workers, as this disables
    // queue pausing which is more efficient than long polling when working
    // on multiple queues. 
    'message_wait_timeout'       => 0,
    // use a centralized cache
    'cache'                      => 'the-centralized-cache',
    // interval for regular queue list updates (in seconds) 
    'throttles' => [
        /* define throttles here */
    ],   
    'queue_list_update_interval' => 60,
    // Maximum pause time, for empty queues (in seconds).
    // This value must not be too high, since delayed or in-the-flight
    // messages are not visible. The queue might look empty but will
    // have available messages later without the worker pause being aborted.
    'queue_pause_time'           => 20,
],
```


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

