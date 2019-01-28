<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 07:51
	 */

	namespace MehrIt\LaraSqsExt\Queue;


	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;

	trait InteractsWithSqsQueue
	{
		/**
		 * Sets the time the job remains invisible to other queue workers
		 * @param int $time Time in seconds
		 */
		public function setVisibilityTimeout($time) {
			/** @var SqsExtJob $job */
			$job = isset($this->job) ? $this->job : null;

			if ($job && $job instanceof SqsExtJob) {

				$job->setVisibilityTimeout($time);
			}
		}
	}