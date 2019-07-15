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
	use Illuminate\Support\Facades\Cache;
	use MehrIt\LaraSqsExt\Queue\Jobs\SqsExtJob;
	use RuntimeException;

	class SqsExtQueue extends SqsQueue
	{
		const DEFAULT_JOB_TYPE = SqsExtJob::class;

		protected $options;

		protected $messageWaitTimeout;

		protected $listenLockFile;

		protected $listenLock = false;

		protected $lastQueueRestart;

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
			$this->listenLock         = (bool)Arr::get($this->options, 'listen_lock', false);
			$this->listenLockFile     = Arr::get($this->options, 'listen_lock_file', null);

			// create locks directory if no listen lock file specified
			if ($this->listenLock && !$this->listenLockFile && !file_exists(storage_path('locks')))
				mkdir(storage_path('locks'));

			// remember last queue restart timestamp
			if ($this->listenLock)
				$this->lastQueueRestart = $this->getTimestampOfLastQueueRestart();
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

			if ($this->listenLock)
				$response = $this->receiveMessageWithLock($queue);
			else
				$response = $this->receiveMessage($queue);

			if (!is_null($response['Messages']) && count($response['Messages']) > 0)
				return $this->makeJob($this->container, $this->sqs, $response['Messages'][0], $this->connectionName, $queue);

			return null;
		}/** @noinspection PhpDocMissingThrowsInspection */

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

			/** @noinspection PhpUnhandledExceptionInspection */
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
		 * Performs a message receive with a listen lock. Process will wait until lock can be acquired or signal is sent
		 * @param string The queue URL $queueUrl
		 * @return \Aws\Result|null
		 */
		protected function receiveMessageWithLock($queueUrl) {

			$lockFile = $this->getListenLockFile($queueUrl);

			try {
				$fp = fopen($lockFile, 'w+');
				if ($fp === false)
					throw new RuntimeException("Could not open queue listen lock file \"{$lockFile}\".");

				// Wait for lock file access. This might be interrupted by signals an return false. In this case
				// we simply do nothing and return without receive, so the worker process can handle the signal first
				$locked = flock($fp, LOCK_EX);
				if (!$locked)
					return null;

				// Maybe we have waited very long, and queue was asked to restart meanwhile. In this case
				// we simply do nothing and return without receive, so the worker process can shut down
				if ($this->queueShouldRestart())
					return null;

				return $this->receiveMessage($queueUrl);
			}
			finally {
				// close lock and file handle
				@flock($fp, LOCK_UN);
				@fclose($fp);
			}

		}

		/**
		 * Gets the listen lock file path
		 * @param string $queueUrl The queue URL
		 * @return string The listen lock file path
		 */
		protected function getListenLockFile($queueUrl) {
			return $this->listenLockFile ?: storage_path('locks/sqsListenLock_' . sha1($queueUrl));
		}

		/**
		 * Determines if the queue should restart
		 * @return bool True if should restart. Else false.
		 */
		protected function queueShouldRestart() {
			return $this->lastQueueRestart != $this->getTimestampOfLastQueueRestart();
		}

		/**
		 * Get the last queue restart timestamp, or null.
		 *
		 * @return int|null
		 */
		protected function getTimestampOfLastQueueRestart() {
			return Cache::get('illuminate:queue:restart');
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
		 * @inheritdoc
		 */
		protected function createObjectPayload($job, $queue){
            $payload = parent::createObjectPayload($job, $queue);

            // we add some extra data to the payload
			$payload['automaticQueueVisibility']      = $job->automaticQueueVisibility ?? true;
			$payload['automaticQueueVisibilityExtra'] = $job->automaticQueueVisibilityExtra ?? 0;

            return $payload;
		}


		/**
		 * May be used by child classes to override message receive parameters passed to the SQS client's receiveMessage function
		 * @param array $params The parameters
		 */
		protected function processReceiveMessageParams(&$params) {

		}
	}