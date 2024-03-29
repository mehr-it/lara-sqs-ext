<?php /** @noinspection PhpComposerExtensionStubsInspection */

	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 17:31
	 */

	namespace MehrIt\LaraSqsExt\Queue;


	use Aws\Sqs\Exception\SqsException;
	use Aws\Sqs\SqsClient;
	use ErrorException;
	use Illuminate\Contracts\Container\Container;
	use Illuminate\Contracts\Encryption\Encrypter;
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

		const SQS_MAX_MESSAGE_DELAY = 900; // 15min (15 * 60s), see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html

		protected $options;

		protected $messageWaitTimeout;

		protected $listenLockFile;

		protected $listenLock = false;

		protected $listenLockTimeout;

		protected $lastQueueRestart;

		protected $extendMessageDelay;

		protected $nextNotBefore;

		protected $encrypt;

		protected $crypt;
		
		protected $queueThrottles;

		protected $queueListUpdateInterval;

		protected $noQueueAvailableSleep;

		/**
		 * @var SqsQueueSelector
		 */
		protected $queueSelector;

		protected $queueCacheStore;

		protected $queueCachePrefix;

		protected $queuePauseTime;


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

			$this->messageWaitTimeout      = Arr::get($this->options, 'message_wait_timeout');
			$this->listenLock              = (bool)Arr::get($this->options, 'listen_lock', false);
			$this->listenLockFile          = Arr::get($this->options, 'listen_lock_file', null);
			$this->listenLockTimeout       = Arr::get($this->options, 'listen_lock_timeout', 5);
			$this->extendMessageDelay      = Arr::get($this->options, 'extend_delay', false);
			$this->encrypt                 = Arr::get($this->options, 'encrypt', false);
			$this->queueThrottles          = Arr::get($this->options, 'throttles', []);
			$this->noQueueAvailableSleep   = Arr::get($this->options, 'no_queue_available_sleep', 1);
			$this->queueListUpdateInterval = Arr::get($this->options, 'queue_list_update_interval', 60);
			$this->queueCacheStore         = Arr::get($this->options, 'cache');
			$this->queueCachePrefix        = Arr::get($this->options, 'cache_prefix', 'sqs');
			$this->queuePauseTime          = Arr::get($this->options, 'queue_pause_time', 20);


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

		public function later($delay, $job, $data = '', $queue = null) {
			
			// If the message delay should be extended and the delay is above SQS' maximum delay,
			// we set job's notBefore timestamp and delay with maximum supported delay
			if ($this->extendMessageDelay) {

				$delaySeconds = $this->secondsUntil($delay);
				if ($delaySeconds > self::SQS_MAX_MESSAGE_DELAY) {

					$this->nextNotBefore = $this->currentTime() + $delaySeconds;

					$delay = self::SQS_MAX_MESSAGE_DELAY;
				}
			}

			try {
				return parent::later($delay, $job, $data, $queue);
			}
			finally {
				$this->nextNotBefore = null;
				
				$this->queueSelector()->wake($queue ?: $this->default);
			}
		}

		/**
		 * @inheritDoc
		 */
		public function pushRaw($payload, $queue = null, array $options = []) {
			
			$ret = parent::pushRaw($payload, $queue, $options);

			$this->queueSelector()->wake($queue ?: $this->default);
			
			return $ret;
		}


		/**
		 * @inheritdoc
		 */
		public function pop($queue = null) {
						
			// Select a queue. The queue selector handles a single queue name as well as wildcards.
			$queue = $this->queueSelector()->selectQueue($queue);

			if ($queue !== null) {
				
				$queueUrl = $this->getQueue($queue);

				// Get the last wake. We need it later, when we eventually pause the queue.
				$lastWake = $this->queueSelector()->lastWake($queue);
				
				try {
					if ($this->listenLock)
						$response = $this->receiveMessageWithLock($queueUrl);
					else
						$response = $this->receiveMessage($queueUrl);
				}
				catch(SqsException $ex) {
					
					// if we tried to select from a non-existent queue, we should update our queue list
					if ($ex->getAwsErrorCode() === 'AWS.SimpleQueueService.NonExistentQueue')
						$this->queueSelector()->clearQueueListCache();
					
					throw $ex;
				}

				if ($response && !is_null($response['Messages']) && count($response['Messages']) > 0)
					return $this->makeJob($this->container, $this->sqs, $response['Messages'][0], $this->connectionName, $queueUrl);

				// No job is available. We try to pause the queue for a certain amount of time, to prevent
				// unnecessary API requests
				$this->queueSelector()->pauseQueue($lastWake, $queue);
			}
			else {
				// no queue available

				// we wait some time, to give queues a chance to become available again
				sleep($this->noQueueAvailableSleep ?: 1);
			}

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

			// decrypt if encryption is used
			if ($this->encrypt)
				$job['Body'] = $this->crypt()->decryptString($job['Body']);

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

			// check for pcntl extension required for listen lock
			if (!extension_loaded('pcntl'))
				throw new RuntimeException('Extension "pcntl" is required when using listen_lock.');

			$lockFile = $this->getListenLockFile($queueUrl);

			$fp = null;
			try {
				try {
					$fp = fopen($lockFile, 'w+');
					if ($fp === false)
						throw new RuntimeException("Could not open queue listen lock file \"{$lockFile}\".");
				}
				catch (ErrorException $ex) {
					throw new RuntimeException("Could not open queue listen lock file \"{$lockFile}\".", 0, $ex);
				}

				// Here we set a timeout for flock which is used below. This way we return to the loop within
				// an endless time and do not wait here forever blind for any external events
				pcntl_signal(SIGALRM, function () {
				});
				pcntl_alarm($this->listenLockTimeout);

				// Wait for lock file access. This might be interrupted by the alarm signal an return false. In this case
				// we simply do nothing and return without receive, so the worker process loop iterates again
				$locked = flock($fp, LOCK_EX);
				if (!$locked)
					return null;

				// Maybe we have waited very long, and queue was asked to restart meanwhile. In this case
				// we simply do nothing and return without receive, so the worker process can shut down
				if ($this->queueShouldRestart())
					return null;

				return $this->receiveMessage($queueUrl);
			}
			catch (RuntimeException $ex) {
				report($ex);

				$this->listenLock = false;
				logger()->warning("Disabling listen lock for worker on {$queueUrl} due to previous error with locking file. Now running without listen lock.");

				return null;
			}
			finally {
				// close lock and file handle
				if ($fp !== null) {
					@flock($fp, LOCK_UN);
					@fclose($fp);
				}
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
				'AttributeNames' => ['ApproximateReceiveCount', 'SentTimestamp'],
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
		protected function createObjectPayload($job, $queue) {
			$payload = parent::createObjectPayload($job, $queue);

			// we add some extra data to the payload
			$payload['automaticQueueVisibility']      = $job->automaticQueueVisibility ?? true;
			$payload['automaticQueueVisibilityExtra'] = $job->automaticQueueVisibilityExtra ?? 0;

			// set job's not before timestamp
			if ($this->nextNotBefore)
				$payload['notBefore'] = $this->nextNotBefore;

			return $payload;
		}


		/**
		 * May be used by child classes to override message receive parameters passed to the SQS client's receiveMessage function
		 * @param array $params The parameters
		 */
		protected function processReceiveMessageParams(&$params) {

		}
		
		public function queueSelector(): SqsQueueSelector {
			if (!$this->queueSelector) {
				$this->queueSelector = $this->container->make(SqsQueueSelector::class, [
					'sqs'                     => $this->sqs,
					'throttles'               => $this->queueThrottles,
					'queueListUpdateInterval' => $this->queueListUpdateInterval ?: 20,
					'cache'                   => $this->queueCacheStore,
					'cachePrefix'             => $this->queueCachePrefix,
					// disable queue pausing, if long-polling is active
					'queuePauseTime'          => !$this->messageWaitTimeout ? $this->queuePauseTime : 0,
				]);

				$this->queueSelector->setContainer($this->container);
			}
			
			return $this->queueSelector;
		}

		/**
		 * @inheritDoc
		 */
		protected function createPayload($job, $queue, $data = '') {
			$payload = parent::createPayload($job, $queue, $data);

			// encrypt if configured to do so
			if ($this->encrypt)
				$payload = $this->crypt()->encryptString((string)$payload);

			return $payload;
		}

		/**
		 * Gets a crypto instance
		 * @return Encrypter The crypto instance
		 */
		protected function crypt(): Encrypter {
			if (!$this->crypt)
				$this->crypt = $this->container->make('encrypter');

			return $this->crypt;
		}

	}