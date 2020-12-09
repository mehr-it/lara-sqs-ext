<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 07:51
	 */

	namespace MehrIt\LaraSqsExt\Queue;


	use Carbon\Carbon;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;

	trait InteractsWithSqsQueue
	{
		/**
		 * Sets the time the job remains invisible to other queue workers
		 * @param int $time Time in seconds
		 */
		public function setVisibilityTimeout($time) {
			/** @var SqsExtJob $job */
			$job = $this->job ?? null;

			if ($job && $job instanceof SqsExtJob) {

				$job->setVisibilityTimeout($time);
			}
		}

		/**
		 * Gets the job's sent date
		 * @return Carbon|null The job's sent date
		 */
		public function sentDate(): ?Carbon {
			/** @var SqsExtJob $job */
			$job = $this->job ?? null;

			if (!$job || !($job instanceof SqsExtJob))
				return null;

			return $job->sentDate();
		}
	}