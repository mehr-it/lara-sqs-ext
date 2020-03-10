<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 07:23
	 */

	namespace MehrItLaraSqsExtTest\Cases\Unit\Queue\Jobs;


	use Carbon\Carbon;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;
	use stdClass;
	use Mockery as m;
	use Aws\Sqs\SqsClient;
	use Illuminate\Queue\SqsQueue;
	use Illuminate\Container\Container;
	use MehrItLaraSqsExtTest\Cases\TestCase;

	class SqsExtJobTest extends TestCase
	{


		public function setUp() :void {
			Carbon::setTestNow(Carbon::now());

			$this->key          = 'AMAZONSQSKEY';
			$this->secret       = 'AmAz0n+SqSsEcReT+aLpHaNuM3R1CsTr1nG';
			$this->service      = 'sqs';
			$this->region       = 'someregion';
			$this->account      = '1234567891011';
			$this->queueName    = 'emails';
			$this->baseUrl      = 'https://sqs.someregion.amazonaws.com';
			$this->releaseDelay = 0;
			// This is how the modified getQueue builds the queueUrl
			$this->queueUrl = $this->baseUrl . '/' . $this->account . '/' . $this->queueName;
			// Get a mock of the SqsClient
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['deleteMessage'])
				->disableOriginalConstructor()
				->getMock();
			// Use Mockery to mock the IoC Container
			$this->mockedContainer     = m::mock(Container::class);
			$this->mockedJob           = 'foo';
			$this->mockedData          = ['data'];
			$this->mockedPayload       = json_encode(['job' => $this->mockedJob, 'data' => $this->mockedData, 'attempts' => 1]);
			$this->mockedMessageId     = 'e3cd03ee-59a3-4ad8-b0aa-ee2e3808ac81';
			$this->mockedReceiptHandle = '0NNAq8PwvXuWv5gMtS9DJ8qEdyiUwbAjpp45w2m6M4SJ1Y+PxCh7R930NRB8ylSacEmoSnW18bgd4nK\/O6ctE+VFVul4eD23mA07vVoSnPI4F\/voI1eNCp6Iax0ktGmhlNVzBwaZHEr91BRtqTRM3QKd2ASF8u+IQaSwyl\/DGK+P1+dqUOodvOVtExJwdyDLy1glZVgm85Yw9Jf5yZEEErqRwzYz\/qSigdvW4sm2l7e4phRol\/+IjMtovOyH\/ukueYdlVbQ4OshQLENhUKe7RNN5i6bE\/e5x9bnPhfj2gbM';
			$this->mockedJobData       = [
				'Body'          => $this->mockedPayload,
				'MD5OfBody'     => md5($this->mockedPayload),
				'ReceiptHandle' => $this->mockedReceiptHandle,
				'MessageId'     => $this->mockedMessageId,
				'Attributes'    => ['ApproximateReceiveCount' => 1, 'SentTimestamp' => \Illuminate\Support\Carbon::now()->format('Uv')],
			];
		}

		public function tearDown() : void {
			m::close();
		}

		public function testFireProperlyCallsTheJobHandler() {
			$this->expectNotToPerformAssertions();

			$job = $this->getJob();
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();

		}

		public function testFireWaitsUntilNotBeforeTimestamp() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			Carbon::setTestNow(Carbon::now());

			$ts = Carbon::now()->timestamp;

			$job = $this->getJob(['timeout' => 15, 'notBefore' => $ts + 7]);
			$job->getContainer()->shouldReceive('make')->never();
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 7]);
			$job->fire();
		}

		public function testFireWaitsUntilNotBeforeTimestamp_maxVisibilityTimeoutReached() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			Carbon::setTestNow(Carbon::now());

			$ts = Carbon::now()->timestamp;

			$job = $this->getJob(['timeout' => 15, 'notBefore' => $ts + 99999]);
			$job->getContainer()->shouldReceive('make')->never();
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 43200]);
			$job->fire();
		}

		public function testFireSetsSqsVisibilityTimeout() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $this->getJob(['timeout' => 15]);
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 15]);
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testFireSetsSqsVisibilityTimeoutIfSetManually() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $this->getJob(['automaticQueueVisibility' => 23]);
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 23]);
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testFireManualVisibilityTimeoutOverridesJobTimeout() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $job = $this->getJob(['timeout' => 13, 'automaticQueueVisibility' => 12]);
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 12]);
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testFireDoesNotSqsVisibilityTimeoutIfDeactivated() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $job = $this->getJob(['timeout' => 15, 'automaticQueueVisibility' => false]);
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->never())->method('changeMessageVisibility');
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testFireDoesNotSqsVisibilityTimeoutIfNoTimeout() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $job = $this->getJob();
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->never())->method('changeMessageVisibility');
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testFireSetsSqsVisibilityTimeoutWithExtra() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $this->getJob(['timeout' => 15, 'automaticQueueVisibilityExtra' => 3]);
			$job->getContainer()->shouldReceive('make')->once()->with('foo')->andReturn($handler = m::mock(stdClass::class));
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 18]);
			$handler->shouldReceive('fire')->once()->with($job, ['data']);
			$job->fire();
		}

		public function testDeleteRemovesTheJobFromSqs() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['deleteMessage'])
				->disableOriginalConstructor()
				->getMock();
			$queue                 = $this->getMockBuilder(SqsQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->mockedSqsClient, $this->queueName, $this->account])->getMock();
			$queue->setContainer($this->mockedContainer);
			$job = $this->getJob();
			$job->getSqs()->expects($this->once())->method('deleteMessage')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle]);
			$job->delete();
		}

		public function testReleaseProperlyReleasesTheJobOntoSqs() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();
			$queue                 = $this->getMockBuilder(SqsQueue::class)->setMethods(['getQueue'])->setConstructorArgs([$this->mockedSqsClient, $this->queueName, $this->account])->getMock();
			$queue->setContainer($this->mockedContainer);
			$job = $this->getJob();
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => $this->releaseDelay]);
			$job->release($this->releaseDelay);
			$this->assertTrue($job->isReleased());
		}

		public function testSetVisibilityTimeout() {
			$this->mockedSqsClient = $this->getMockBuilder(SqsClient::class)
				->setMethods(['changeMessageVisibility'])
				->disableOriginalConstructor()
				->getMock();

			$job = $this->getJob();
			$job->getSqs()->expects($this->once())->method('changeMessageVisibility')->with(['QueueUrl' => $this->queueUrl, 'ReceiptHandle' => $this->mockedReceiptHandle, 'VisibilityTimeout' => 13]);
			$job->setVisibilityTimeout(13);
		}

		public function testGetSentTimestampMs() {

			Carbon::setTestNow(Carbon::now());

			$job = $this->getJob();
			$this->assertEquals(Carbon::createFromTimestampMs(Carbon::now()->format('Uv')), Carbon::createFromTimestampMs($job->sentTimestampMs()));

		}

		public function testGetSentDate() {

			Carbon::setTestNow(Carbon::now());

			$job = $this->getJob();
			$this->assertEquals(Carbon::createFromTimestampMs(Carbon::now()->format('Uv')), $job->sentDate());

		}

		protected function getJob($payloadMerge = []) {
			$payload = json_decode($this->mockedPayload, true);
			$payload = json_encode(array_merge($payload, $payloadMerge));

			$jobData         = $this->mockedJobData;
			$jobData['Body'] = $payload;

			return new SqsExtJob(
				$this->mockedContainer,
				$this->mockedSqsClient,
				$jobData,
				'connection-name',
				$this->queueUrl
			);
		}


	}

	class TestSqsExtJob extends SqsExtJob{

		public function setAutomaticQueueVisibility($value) {
			$this->automaticQueueVisibility = $value;
		}

		public function setAutomaticQueueVisibilityExtra($value) {
			$this->automaticQueueVisibilityExtra = $value;
		}

	}

