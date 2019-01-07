<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 17:31
	 */

	namespace MehrIt\LaraSqsExt\Queue;


	use Aws\Sqs\SqsClient;
	use Illuminate\Contracts\Container\Container;
	use Illuminate\Contracts\Queue\Job;
	use Illuminate\Queue\Jobs\SqsJob;
	use Illuminate\Queue\SqsQueue;
	use Illuminate\Support\Arr;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;

	class SqsExtQueue extends SqsQueue
	{
		const DEFAULT_JOB_TYPE = SqsExtJob::class;

		protected $options;
		protected $messageWaitTimeout;

		/**
		 * Create a new Amazon SQS extended queue instance.
		 *
		 * @param \Aws\Sqs\SqsClient $sqs
		 * @param string $default Default queue
		 * @param string $prefix Queue prefix
		 * @param array $options Queue configuration options
		 */
		public function __construct(SqsClient $sqs, $default, $prefix = '', $options = []) {
			parent::__construct($sqs, $default, $prefix);

			$this->options = $options;

			$this->messageWaitTimeout = Arr::get($this->options, 'message_wait_timeout', null);
		}

		/**
		 * Gets the queue options
		 * @return array The configured options
		 */
		public function getOptions() {
			return $this->options;
		}

		/**
		 * Gets the message wait timeout
		 * @return int|null The message wait timeout
		 */
		public function getMessageWaitTimeout() {
			return $this->messageWaitTimeout;
		}




		/**
		 * @inheritdoc
		 */
		public function pop($queue = null) {
			$queue = $this->getQueue($queue);

			$response = $this->receiveMessage($queue);

			if (!is_null($response['Messages']) && count($response['Messages']) > 0)
				return $this->makeJob($this->container, $this->sqs, $response['Messages'][0], $this->connectionName, $queue);

			return null;
		}

		/**
		 * Creates a new job instance
		 * @param Container $container The application container
		 * @param SqsClient $sqs The SQS client
		 * @param array $job The job data
		 * @param string $connectionName The connection name
		 * @param string $queue The queue name
		 * @return SqsJob|Job The job instance
		 */
		protected function makeJob(Container $container, SqsClient $sqs, array $job, $connectionName, $queue) {
			$jobClass = Arr::get($this->options, 'job_type', static::DEFAULT_JOB_TYPE);

			return app()->make($jobClass, [
				'container'      => $container,
				'sqs'            => $sqs,
				'job'            => $job,
				'connectionName' => $connectionName,
				'queue'          => $queue,
				'queueOptions'   => $this->options,
			]);
		}


		/**
		 * Receives messages from the SQS client
		 * @param string $queueUrl The queue URL
		 * @return \Aws\Result
		 */
		protected function receiveMessage($queueUrl) {
			$params = [
				'QueueUrl'       => $queueUrl,
				'AttributeNames' => ['ApproximateReceiveCount'],
			];

			// set wait time
			if ($this->messageWaitTimeout !== null)
				$params['WaitTimeSeconds'] = $this->messageWaitTimeout;

			// allow child classes to modify parameters
			$this->processReceiveMessageParams($params);

			return $this->sqs->receiveMessage($params);
		}


		/**
		 * May be used by child classes to override message receive parameters passed to the SQS client's receiveMessage function
		 * @param array $params The parameters
		 */
		protected function processReceiveMessageParams(&$params) {

		}
	}