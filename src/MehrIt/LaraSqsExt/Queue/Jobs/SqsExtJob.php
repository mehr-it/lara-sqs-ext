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

			// set SQS visibility timeout
			if ($t = $this->getAutomaticVisibilityTimeout())
				$this->setVisibilityTimeout($t);

			// fire the job
			parent::fire();
		}

		/**
		 * Gets the visibility timeout to set automatically for the job
		 * @return int|null The time in seconds or 0 if
		 */
		public function getAutomaticVisibilityTimeout() {

			if ($av = $this->automaticQueueVisibility()) {

				if (is_int($av))
					return $av;

				if ($t = $this->timeout())
					return $t + $this->automaticQueueVisibilityExtra();
			}

			return null;
		}

		/**
		 * Gets the automatic queue visibility time
		 *
		 * @return int|boolean|null
		 */
		public function automaticQueueVisibility() {
			return $this->payload()['automaticQueueVisibility'] ?? true;
		}

		/**
		 * Gets the automatic queue visibility time to add to job timeout
		 *
		 * @return int
		 */
		public function automaticQueueVisibilityExtra() {
			return $this->payload()['automaticQueueVisibilityExtra'] ?? 0;
		}



	}