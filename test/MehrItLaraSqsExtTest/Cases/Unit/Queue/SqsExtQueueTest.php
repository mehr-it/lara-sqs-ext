<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 19:14
	 */

	namespace MehrItLaraSqsExtTest\Cases\Unit\Queue;

	use Aws\Result;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;
	use MehrIt\LaraSqsExt\Queue\SqsExtQueue;
	use Mockery as m;
	use Aws\Sqs\SqsClient;
	use Illuminate\Support\Carbon;
	use Illuminate\Container\Container;
	use MehrItLaraSqsExtTest\Cases\TestCase;

	class SqsExtQueueTest extends TestCase
	{
		protected $temporaryFiles = [];

		public function tearDown() : void {
			m::close();

			foreach($this->temporaryFiles as $curr) {
				if (file_exists($curr))
					unlink($curr);
			}
		}

		public function setUp() : void {
			parent::setUp();

			// Use Mockery to mock the SqsClient
			$this->sqs       = m::mock(SqsClient::class);
			$this->account   = '1234567891011';
			$this->queueName = 'emails';
			$this->baseUrl   = 'https://sqs.someregion.amazonaws.com';
			// This is how the modified getQueue builds the queueUrl
			$this->prefix                                 = $this->baseUrl . '/' . $this->account . '/';
			$this->queueUrl                               = $this->prefix . $this->queueName;
			$this->mockedJob                              = 'foo';
			$this->mockedData                             = ['data'];
			$this->mockedPayload                          = json_encode(['job' => $this->mockedJob, 'data' => $this->mockedData]);
			$this->mockedDelay                            = 10;
			$this->mockedMessageId                        = 'e3cd03ee-59a3-4ad8-b0aa-ee2e3808ac81';
			$this->mockedReceiptHandle                    = '0NNAq8PwvXuWv5gMtS9DJ8qEdyiUwbAjpp45w2m6M4SJ1Y+PxCh7R930NRB8ylSacEmoSnW18bgd4nK\/O6ctE+VFVul4eD23mA07vVoSnPI4F\/voI1eNCp6Iax0ktGmhlNVzBwaZHEr91BRtqTRM3QKd2ASF8u+IQaSwyl\/DGK+P1+dqUOodvOVtExJwdyDLy1glZVgm85Yw9Jf5yZEEErqRwzYz\/qSigdvW4sm2l7e4phRol\/+IjMtovOyH\/ukueYdlVbQ4OshQLENhUKe7RNN5i6bE\/e5x9bnPhfj2gbM';
			$this->mockedSendMessageResponseModel         = new Result([
				'Body'          => $this->mockedPayload,
				'MD5OfBody'     => md5($this->mockedPayload),
				'ReceiptHandle' => $this->mockedReceiptHandle,
				'MessageId'     => $this->mockedMessageId,
				'Attributes'    => ['ApproximateReceiveCount' => 1],
			]);
			$this->mockedReceiveMessageResponseModel      = new Result([
				'Messages' => [
					0 => [
						'Body'          => $this->mockedPayload,
						'MD5OfBody'     => md5($this->mockedPayload),
						'ReceiptHandle' => $this->mockedReceiptHandle,
						'MessageId'     => $this->mockedMessageId,
					],
				],
			]);
			$this->mockedReceiveEmptyMessageResponseModel = new Result([
				'Messages' => null,
			]);
			$this->mockedQueueAttributesResponseModel     = new Result([
				'Attributes' => [
					'ApproximateNumberOfMessages' => 1,
				],
			]);
		}

		public function testPopProperlyPopsJobOffOfSqs() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveMessageResponseModel);
			$result = $queue->pop($this->queueName);
			$this->assertInstanceOf(SqsExtJob::class, $result);

		}

		public function testPopUsesWaitTimeout() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['message_wait_timeout' => 20]])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount'], 'WaitTimeSeconds' => 20])->andReturn($this->mockedReceiveMessageResponseModel);
			$result = $queue->pop($this->queueName);
			$this->assertInstanceOf(SqsExtJob::class, $result);

		}

		public function testPopProperlyPopsJobOffOfSqsUsingListenLock() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['listen_lock' => true]])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveMessageResponseModel);
			$result = $queue->pop($this->queueName);
			$this->assertInstanceOf(SqsExtJob::class, $result);

		}

		public function testPopProperlyPopsJobOffOfSqsUsingListenLock_customLockFile() {

			$this->temporaryFiles[] = $lockFileName = tempnam(sys_get_temp_dir(), 'sqsExtQueueTest');

			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['listen_lock' => true, 'listen_lock_file' => $lockFileName]])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveMessageResponseModel);
			$result = $queue->pop($this->queueName);
			$this->assertInstanceOf(SqsExtJob::class, $result);

			// check that log file was created
			$this->assertFileExists($lockFileName);

		}

		public function testPopWaitsForListenLock() {

			$this->temporaryFiles[] = $lockFileName = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestQueueLock');
			$this->temporaryFiles[] = $readyFile = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestReady');

			$tsBefore = microtime(true);

			$pid = pcntl_fork();
			if ($pid == -1) {
				$this->fail('Could not fork test process');
			}
			else if ($pid) {
				$count = 0;
				while (@file_get_contents($readyFile) != 'READY') {
					if ($count > 30)
						$this->fail('Waited 3sec for child process to become ready, but not signaled');
					usleep(100000);
				}

				$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['listen_lock' => true, 'listen_lock_file' => $lockFileName]])->getMock();
				$queue->setContainer(m::mock(Container::class));
				$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
				$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveMessageResponseModel);
				$result = $queue->pop($this->queueName);
				$this->assertInstanceOf(SqsExtJob::class, $result);

				// we should have waited 2sec, until child died and lock was released
				$this->assertGreaterThan(2,microtime(true) - $tsBefore);

				pcntl_wait($status);
			}
			else {

				if (!($fp = fopen($lockFileName, 'w+')))
					throw new \RuntimeException('Could not open lock file ' . $lockFileName);

				if (!flock($fp, LOCK_EX))
					throw new \RuntimeException('Could not lock file ' . $lockFileName);

				if (!file_put_contents($readyFile, 'READY'))
					throw new \RuntimeException('Could not create ready file ' . $readyFile);

				sleep(2);
				die();
			}

		}


		public function testPopReturnsAfterListenLockTimeoutElapsed() {

			$this->temporaryFiles[] = $lockFileName = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestQueueLock');
			$this->temporaryFiles[] = $readyFile = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestReady');

			$tsBefore = microtime(true);

			$pid = pcntl_fork();
			if ($pid == -1) {
				$this->fail('Could not fork test process');
			}
			else if ($pid) {
				$count = 0;
				while (@file_get_contents($readyFile) != 'READY') {
					if ($count > 30)
						$this->fail('Waited 3sec for child process to become ready, but not signaled');
					usleep(100000);
				}

				$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['listen_lock' => true, 'listen_lock_file' => $lockFileName, 'listen_lock_timeout' => 2]])->getMock();
				$queue->setContainer(m::mock(Container::class));
				$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
				$this->sqs->shouldNotReceive('receiveMessage');
				$result = $queue->pop($this->queueName);
				$this->assertNull($result);

				// we should have waited 2sec, until child died and lock was released
				$this->assertGreaterThan(2,microtime(true) - $tsBefore);
				$this->assertLessThan(4,microtime(true) - $tsBefore);

				pcntl_wait($status);
			}
			else {

				if (!($fp = fopen($lockFileName, 'w+')))
					throw new \RuntimeException('Could not open lock file ' . $lockFileName);

				if (!flock($fp, LOCK_EX))
					throw new \RuntimeException('Could not lock file ' . $lockFileName);

				if (!file_put_contents($readyFile, 'READY'))
					throw new \RuntimeException('Could not create ready file ' . $readyFile);

				sleep(8);
				die();
			}

		}

		public function testPopAbortsOnQueueRestart() {


			$this->temporaryFiles[] = $lockFileName = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestQueueLock');
			$this->temporaryFiles[] = $readyFile = tempnam(sys_get_temp_dir(), 'sqsExtQueueTestReady');


			$pid = pcntl_fork();


			if ($pid == -1) {
				$this->fail('Could not fork test process');
			}
			else if ($pid) {

				$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, ['listen_lock' => true, 'listen_lock_file' => $lockFileName]])->getMock();
				$queue->setContainer(m::mock(Container::class));
				$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
				$this->sqs->shouldNotReceive('receiveMessage');

				if (!file_put_contents($readyFile, 'READY'))
					throw new \RuntimeException('Could not create ready file ' . $readyFile);

				pcntl_wait($status);

				$result = $queue->pop($this->queueName);

				// we should not have received any job
				$this->assertNull( $result);

			}
			else {

				$count = 0;
				while (@file_get_contents($readyFile) != 'READY') {
					if ($count > 30)
						$this->fail('Waited 3sec for child process to become ready, but not signaled');
					usleep(100000);
				}

				$this->artisan('queue:restart');
				die();
			}

		}

		public function testPopReturnsConfiguredJobType() {
			$job = new \stdClass();
			$queueOptions = ['job_type' => 'my_job_type'];


			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account, $queueOptions])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveMessageResponseModel);

			app()->bind('my_job_type', function ($app, $params) use ($queue, $queueOptions, $job) {
				$this->assertInstanceOf(Container::class, $params['container']);
				$this->assertSame($this->sqs, $params['sqs']);
				$this->assertSame($this->mockedReceiveMessageResponseModel['Messages'][0], $params['job']);
				$this->assertSame($this->queueUrl, $params['queue']);
				$this->assertSame($queueOptions, $params['queueOptions']);

				return $job;
			});


			$result = $queue->pop($this->queueName);

			$this->assertSame($job, $result);
		}



		public function testPopProperlyHandlesEmptyMessage() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account])->getMock();
			$queue->setContainer(m::mock(Container::class));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('receiveMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateReceiveCount']])->andReturn($this->mockedReceiveEmptyMessageResponseModel);
			$result = $queue->pop($this->queueName);
			$this->assertNull($result);
		}


		public function testDelayedPushWithDateTimeProperlyPushesJobOntoSqs() {
			$now   = Carbon::now();
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['createPayload', 'secondsUntil', 'getQueue'])->setConstructorArgs([$this->sqs, 'message_handler', $this->queueName, $this->account])->getMock();
			$queue->expects($this->once())->method('createPayload')->with($this->mockedJob, $this->queueName, $this->mockedData)->will($this->returnValue($this->mockedPayload));
			$queue->expects($this->once())->method('secondsUntil')->with($now)->will($this->returnValue(5));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('sendMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'MessageBody' => $this->mockedPayload, 'DelaySeconds' => 5])->andReturn($this->mockedSendMessageResponseModel);
			$id = $queue->later($now->addSeconds(5), $this->mockedJob, $this->mockedData, $this->queueName);
			$this->assertEquals($this->mockedMessageId, $id);
		}

		public function testDelayedPushProperlyPushesJobOntoSqs() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['createPayload', 'secondsUntil', 'getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account])->getMock();
			$queue->expects($this->once())->method('createPayload')->with($this->mockedJob, $this->queueName, $this->mockedData)->will($this->returnValue($this->mockedPayload));
			$queue->expects($this->once())->method('secondsUntil')->with($this->mockedDelay)->will($this->returnValue($this->mockedDelay));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('sendMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'MessageBody' => $this->mockedPayload, 'DelaySeconds' => $this->mockedDelay])->andReturn($this->mockedSendMessageResponseModel);
			$id = $queue->later($this->mockedDelay, $this->mockedJob, $this->mockedData, $this->queueName);
			$this->assertEquals($this->mockedMessageId, $id);
		}

		public function testPushProperlyPushesJobOntoSqs() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['createPayload', 'getQueue'])->setConstructorArgs([$this->sqs, $this->queueName, $this->account])->getMock();
			$queue->expects($this->once())->method('createPayload')->with($this->mockedJob, $this->queueName, $this->mockedData)->will($this->returnValue($this->mockedPayload));
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('sendMessage')->once()->with(['QueueUrl' => $this->queueUrl, 'MessageBody' => $this->mockedPayload])->andReturn($this->mockedSendMessageResponseModel);
			$id = $queue->push($this->mockedJob, $this->mockedData, $this->queueName);
			$this->assertEquals($this->mockedMessageId, $id);
		}

		public function testSizeProperlyReadsSqsQueueSize() {
			$queue = $this->getMockBuilder(SqsExtQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->sqs,  $this->queueName, $this->account])->getMock();
			$queue->expects($this->once())->method('getQueue')->with($this->queueName)->will($this->returnValue($this->queueUrl));
			$this->sqs->shouldReceive('getQueueAttributes')->once()->with(['QueueUrl' => $this->queueUrl, 'AttributeNames' => ['ApproximateNumberOfMessages']])->andReturn($this->mockedQueueAttributesResponseModel);
			$size = $queue->size($this->queueName);
			$this->assertEquals($size, 1);
		}

		public function testGetQueueProperlyResolvesUrlWithPrefix() {
			$queue = new SqsExtQueue($this->sqs, $this->queueName, $this->prefix);
			$this->assertEquals($this->queueUrl, $queue->getQueue(null));
			$queueUrl = $this->baseUrl . '/' . $this->account . '/test';
			$this->assertEquals($queueUrl, $queue->getQueue('test'));
		}

		public function testGetQueueProperlyResolvesUrlWithoutPrefix() {
			$queue = new SqsExtQueue($this->sqs,  $this->queueUrl);
			$this->assertEquals($this->queueUrl, $queue->getQueue(null));
			$queueUrl = $this->baseUrl . '/' . $this->account . '/test';
			$this->assertEquals($queueUrl, $queue->getQueue($queueUrl));
		}

		public function testConfigurationOptionsPassed() {
			$options = ['x' => 1, 'b' => 4];
			$queue = new SqsExtQueue($this->sqs,  $this->queueUrl, '', $options);
			$this->assertEquals($options, $queue->getOptions());
		}

		public function testCreateObjectPayload() {

			$this->expectNotToPerformAssertions();

			$job = new TestExtQueueJob();

			$queue = new SqsExtQueue($this->sqs, $this->queueUrl);
			$this->sqs->shouldReceive('sendMessage')->once()->withArgs(function($args) use ($job) {
				if ($args['QueueUrl'] != $this->queueUrl)
					return false;

				$messageBody = json_decode($args['MessageBody'], true);
				if ($messageBody['automaticQueueVisibility'] != $job->automaticQueueVisibility)
					return false;
				if ($messageBody['automaticQueueVisibilityExtra'] != $job->automaticQueueVisibilityExtra)
					return false;


				return true;
			})->andReturn($this->mockedSendMessageResponseModel);
			$queue->push($job);

		}

	}

	class TestExtQueueJob  {

		public $timeout = 0;

		/**
		 * @var bool|int Determines if the SQS visibility timeout is automatically set to the job's timeout
		 */
		public $automaticQueueVisibility = true;

		/**
		 * @var int Extra amount if time added to job's timeout when setting SQS visibility timeout automatically
		 */
		public $automaticQueueVisibilityExtra = 0;


	}

