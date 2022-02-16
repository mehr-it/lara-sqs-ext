<?php

	namespace MehrItLaraSqsExtTest\Cases\Unit\Queue;

	use Aws\Result;
	use Aws\Sqs\SqsClient;
	use Carbon\Carbon;
	use MehrIt\LaraSqsExt\Queue\SqsQueueSelector;
	use MehrItLaraSqsExtTest\Cases\TestCase;

	class SqsQueueSelectorTest extends TestCase
	{

		public function testSelectQueue_queueName() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock);
			$selector->setContainer($this->app);

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));

		}
		
		public function testSelectQueue_wildcard_returnsAllAvailableQueuesInSequence() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue3',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock);
			$selector->setContainer($this->app);


			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));

		}

		public function testSelectQueue_wildcard_updatesQueueListAfterTimeout() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->twice()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturnValues([
					new Result([
						'QueueUrls' => [
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue3',
						]
					]),
					new Result([
						'QueueUrls' => [
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue3',
							'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue4',
						]
					])
				]);


			$selector = new SqsQueueSelector($sqsMock, [], 60);
			$selector->setContainer($this->app);


			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));

			Carbon::setTestNow(Carbon::now()->addSeconds(61));

			$this->assertSame('thePrefixQueue4', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue4', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));

		}

		public function testSelectQueue_queueName_pausingInactive_alwaysReturns() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 0);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('theQueue'), 'theQueue');

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));

		}

		public function testSelectQueue_queueName_paused_doesNotReturnIfPaused() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('theQueue'), 'theQueue');

			$this->assertSame(null, $selector->selectQueue('theQueue'));
			
		}

		public function testSelectQueue_queueName_paused_returnsIfWoken() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('theQueue'), 'theQueue');

			$this->assertSame(null, $selector->selectQueue('theQueue'));
			
			$selector->wake('theQueue');

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));

		}
		
		public function testSelectQueue_queueName_paused_pauseEndsAfterTimeout() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('theQueue'), 'theQueue');

			$this->assertSame(null, $selector->selectQueue('theQueue'));
			
			Carbon::setTestNow(Carbon::now()->addSeconds(21));

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));

		}

		public function testSelectQueue_wildcard_paused_doesNotReturnPausedQueues() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('thePrefixQueue1'), 'thePrefixQueue1');

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}

		public function testSelectQueue_wildcard_paused_returnsNullIfAllQueuesArePaused() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('thePrefixQueue1'), 'thePrefixQueue1');
			$selector->pauseQueue($selector->lastWake('thePrefixQueue2'), 'thePrefixQueue2');

			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}

		public function testSelectQueue_wildcard_paused_returnsWokenQueues() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('thePrefixQueue2'), 'thePrefixQueue2');

			// all still sleeping
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));

			$selector->wake('thePrefixQueue2');

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}

		public function testSelectQueue_wildcard_paused_pauseEndsAfterTimeout() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('thePrefixQueue2'), 'thePrefixQueue2');

			// all still sleeping
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame(null, $selector->selectQueue('thePrefix*'));

			// shift time +20s
			Carbon::setTestNow(Carbon::now()->addSeconds(21));

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}

		public function testSelectQueue_wildcard_paused_testPauseWakeSequence() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [], 60, null, 'sqs', 20);
			$selector->setContainer($this->app);

			$selector->pauseQueue($selector->lastWake('thePrefixQueue2'), 'thePrefixQueue2');

			// all still sleeping
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));

			// wake within pausing-sequence
			$lastBeforeWake = $selector->lastWake('thePrefixQueue2');
			$selector->wake('thePrefixQueue2');
			$selector->pauseQueue($lastBeforeWake, 'thePrefixQueue2');

			// the last pause should be ignored, because it was based on an older wake-state
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			// pause the queue again
			$selector->pauseQueue($selector->lastWake('thePrefixQueue2'), 'thePrefixQueue2');

			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$selector->wake('thePrefixQueue2');
			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));

		}

		public function testSelectQueue_wildcard_throttled_throttlesQueue() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [
				'thePrefixQueue1' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
			]);
			$selector->setContainer($this->app);
			
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			// quota exceeded again
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			// quota available again
			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			
		}
		
		public function testSelectQueue_queueName_throttled_throttlesQueue() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [
				'theQueue' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
			]);
			$selector->setContainer($this->app);
			
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			// quota exceeded again
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			// quota available again
			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));
			
		}
		
		public function testSelectQueue_queueName_throttledWithPrefix_throttlesQueue() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldNotReceive('listQueues');


			$selector = new SqsQueueSelector($sqsMock, [
				'the*' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
			]);
			$selector->setContainer($this->app);
			
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			// quota exceeded again
			$this->assertSame(null, $selector->selectQueue('theQueue'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			// quota available again
			$this->assertSame('theQueue', $selector->selectQueue('theQueue'));
		}
		
		public function testSelectQueue_wildcard_throttledWithPrefix_throttlesQueue() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [
				'the*' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
			]);
			$selector->setContainer($this->app);
			
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());

			// quota exceeded again
			$this->assertSame(null, $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			// quota available again
			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			
		}
		
		public function testSelectQueue_wildcard_throttled_doesNotReturnQueuesForWhichNeedToWait() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue1',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue2',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueue3',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [
				'thePrefixQueue1' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
				'thePrefixQueue2' => [
					'rate'    => 1,
					'burst'   => 4,
					'initial' => 1,
				],
				'thePrefixQueue3' => [
					'rate'    => 1,
					'burst'   => 4,
					'initial' => 1,
				],
			]);
			$selector->setContainer($this->app);

			$this->assertSame('thePrefixQueue2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame('thePrefixQueue3', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSeconds(2));
			
			$this->assertSame('thePrefixQueue1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}
		
		public function testSelectQueue_wildcard_throttledWithPrefix_doesNotReturnQueuesForWhichNeedToWait() {

			Carbon::setTestNow(Carbon::now());

			$sqsMock = \Mockery::mock(SqsClient::class);
			$sqsMock
				->shouldReceive('listQueues')
				->once()
				->with([
					'QueueNamePrefix' => 'thePrefix',
					'MaxResults'      => 1000,
				])
				->andReturn(new Result([
					'QueueUrls' => [
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueueA1',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueueB2',
						'https://sqs.eu-central-1.amazonaws.com/1111111111/thePrefixQueueB3',
					]
				]));


			$selector = new SqsQueueSelector($sqsMock, [
				'thePrefixQueueA1' => [
					'rate'    => 0.5,
					'burst'   => 4,
					'initial' => 0,
				],
				'thePrefixQueueB*' => [
					'rate'    => 1,
					'burst'   => 4,
					'initial' => 1,
				]
			]);
			$selector->setContainer($this->app);

			$this->assertSame('thePrefixQueueB2', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
			
			$this->assertSame('thePrefixQueueB3', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSeconds(2));
			
			$this->assertSame('thePrefixQueueA1', $selector->selectQueue('thePrefix*'));
			Carbon::setTestNow(Carbon::now()->addSecond());
		}
	}