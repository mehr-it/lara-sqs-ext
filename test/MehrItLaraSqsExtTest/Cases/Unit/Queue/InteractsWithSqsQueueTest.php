<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 07:54
	 */

	namespace MehrItLaraSqsExtTest\Cases\Unit\Queue;


	use Illuminate\Queue\InteractsWithQueue;
	use MehrIt\LaraSqsExt\Queue\InteractsWithSqsQueue;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;
	use MehrItLaraSqsExtTest\Cases\TestCase;

	class InteractsWithSqsQueueTest extends TestCase
	{

		public function testSetVisibilityTimeout() {

			$cls = new TestInteractsWithSqsQueueClass();

			$job = $this->getMockBuilder(SqsExtJob::class)->disableOriginalConstructor()->getMock();
			$job->expects($this->once())
				->method('setVisibilityTimeout')
				->with(12);

			$cls->setJob($job);
			$cls->setVisibilityTimeout(12);

		}


	}

	class TestInteractsWithSqsQueueClass {
		use InteractsWithSqsQueue;
		use InteractsWithQueue;

	}
