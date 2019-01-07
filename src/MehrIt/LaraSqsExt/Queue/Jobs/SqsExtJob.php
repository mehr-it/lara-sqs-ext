<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 06:59
	 */

	namespace MehrIt\LaraSqsExt\Queue\Jobs;


	use Illuminate\Queue\Jobs\SqsJob;

	class SqsExtJob extends SqsJob
	{

		/**
		 * @var bool|int Determines if the SQS visibility timeout is automatically set to the job's timeout
		 */
		protected $automaticQueueVisibility = true;

		/**
		 * @var int Extra amount if time added to job's timeout when setting SQS visibility timeout automatically
		 */
		protected $automaticQueueVisibilityExtra = 0;

		/**
		 * Sets the time the job remains invisible to other queue workers
		 * @param int $time Time in seconds
		 */
		public function setVisibilityTimeout($time) {
			$this->sqs->changeMessageVisibility([
				'QueueUrl'          => $this->queue,
				'ReceiptHandle'     => $this->job['ReceiptHandle'],
				'VisibilityTimeout' => $time,
			]);
		}

		/**
		 * Fire the job.
		 *
		 * @return void
		 */
		public function fire() {

			// set SQS visibility timeout to job timeout
			if ($t = $this->getAutomaticVisibilityTimeout())
				$this->setVisibilityTimeout($t + $this->automaticQueueVisibilityExtra);

			// fire the job
			parent::fire();
		}

		/**
		 * Gets the visibility timeout to set automatically for the job
		 * @return int|null The time in seconds or 0 if
		 */
		public function getAutomaticVisibilityTimeout() {

			if ($this->automaticQueueVisibility) {

				if (is_int($this->automaticQueueVisibility))
					return $this->automaticQueueVisibility;

				if ($t = $this->timeout())
					return $t;
			}

			return null;
		}

	}