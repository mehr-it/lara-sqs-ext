<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 07.01.19
	 * Time: 06:59
	 */

	namespace MehrIt\LaraSqsExt\Queue\Jobs;


	use Carbon\Carbon;
	use Illuminate\Queue\Jobs\SqsJob;

	class SqsExtJob extends SqsJob
	{
		const SQS_MAX_VISIBILITY_TIMEOUT = 43200; // 12h (12 * 60 * 60s), see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html

		public $notBefore = null;

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

			// check if execution should still be delayed
			$notBefore = $this->notBefore();
			$ts = $this->currentTime();
			if ($notBefore && $notBefore > $ts) {
				$this->release(min($notBefore - $ts, self::SQS_MAX_VISIBILITY_TIMEOUT - 1 /* add 1 second safety margin */));
				return;
			}

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

		/**
		 * Gets the timestamp until which the job should be delayed
		 * @return int|null The timestamp or null
		 */
		public function notBefore() {
			return $this->payload()['notBefore'] ?? null;
		}

		/**
		 * Gets the messages sent timestamp as an integer representing the epoch time in milliseconds
		 *
		 * @return int|null
		 */
		public function sentTimestampMs(): ?int {

			$ms = $this->job['Attributes']['SentTimestamp'] ?? null;

			return $ms ? (int)$ms : null;
		}

		/**
		 * Gets the message sent date
		 * @return Carbon|null
		 */
		public function sentDate(): ?Carbon {

			$ms = $this->sentTimestampMs();
			if ($ms === null)
				return null;

			return Carbon::createFromTimestampMs($ms);
		}

	}