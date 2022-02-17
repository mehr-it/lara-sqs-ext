<?php

	namespace MehrIt\LaraSqsExt\Queue;

	use Aws\Sqs\SqsClient;
	use Carbon\Carbon;
	use Illuminate\Cache\CacheManager;
	use Illuminate\Contracts\Container\Container;
	use MehrIt\LaraTokenBucket\TokenBucket;
	use MehrIt\LaraTokenBucket\TokenBucketManager;

	/**
	 * Implements queue selection logic, to select one of multiple queues and to throttle queues.
	 */
	class SqsQueueSelector
	{

		/**
		 * @var SqsClient The Amazon SQS instance.
		 */
		protected $sqs;

		/**
		 * @var string[][] The list of queues matching a prefix grouped by prefix
		 */
		protected $prefixQueueList = [];

		/**
		 * @var int[][] The timestamps when a prefix queue list was last updated
		 */
		protected $prefixQueueListUpdatedAt = [];

		/**
		 * @var int Interval in seconds for refreshing the queue list
		 */
		protected $queueListUpdateInterval;

		/**
		 * @var array|array[] Throttle definitions for queues. Queue name or wildcard as key
		 */
		protected $queueThrottles = [];

		/**
		 * @var int The time to pause polling if a queue is empty. (in seconds)
		 */
		protected $queuePauseTime;

		/**
		 * @var int[] Timestamps when a queue was last selected. Queue name as key. (in milliseconds)
		 */
		protected $lastSelectedTimestamps = [];

		/**
		 * @var string|null The cache store name
		 */
		protected $cache;

		/**
		 * @var string The cache key prefix
		 */
		protected $cachePrefix;

		/**
		 * @var TokenBucketManager
		 */
		protected $tokenBucketManager;

		/**
		 * @var CacheManager
		 */
		protected $cacheManager;

		/**
		 * @var Container
		 */
		protected $container;


		/**
		 * Creates a new instance
		 * @param SqsClient $sqs The SQS client
		 * @param array{bucketName: string|null, rate:float, burst: int, initial:int|null}[] $throttles Throttle definitions for queues. The key may either be a queue name or a queue prefix ending with an asterisk.
		 * @param int $queueListUpdateInterval The interval for updating prefixes
		 */
		public function __construct(SqsClient $sqs, array $throttles = [], int $queueListUpdateInterval = 60, string $cache = null, string $cachePrefix = 'sqs', int $queuePauseTime = 20) {
			$this->sqs                     = $sqs;
			$this->queueListUpdateInterval = $queueListUpdateInterval;
			$this->queueThrottles          = $throttles;
			$this->cache                   = $cache;
			$this->cachePrefix             = $cachePrefix;
			$this->queuePauseTime          = $queuePauseTime;
		}

		/**
		 * Set the IoC container instance.
		 *
		 * @param \Illuminate\Container\Container $container
		 * @return void
		 */
		public function setContainer(\Illuminate\Container\Container $container) {
			$this->container = $container;
		}

		/**
		 * @return array|array[]
		 */
		public function getQueueThrottles(): array {
			return $this->queueThrottles;
		}

		/**
		 * @return int
		 */
		public function getQueuePauseTime(): int {
			return $this->queuePauseTime;
		}

		/**
		 * @return int
		 */
		public function getQueueListUpdateInterval(): int {
			return $this->queueListUpdateInterval;
		}


		/**
		 * @return string|null
		 */
		public function getCache(): ?string {
			return $this->cache;
		}

		/**
		 * @return string
		 */
		public function getCachePrefix(): string {
			return $this->cachePrefix;
		}

		/**
		 * @return Container
		 */
		public function getContainer(): Container {
			return $this->container;
		}


		/**
		 * Selects a queue matching the given name prefix
		 * @param string $name The queue name or a wildcard ending with '*'
		 * @return string|null The queue name or null if none selectable
		 */
		public function selectQueue(string $name): ?string {

			if (substr($name, -1) === '*') {

				$namePrefix = substr($name, 0, -1);

				// update the queue list, if necessary
				$lastUpdated = $this->prefixQueueListUpdatedAt[$namePrefix] ?? null;
				if ($lastUpdated === null || $lastUpdated < Carbon::now()->getTimestamp() - $this->queueListUpdateInterval) {

					$this->prefixQueueList[$namePrefix]          = $this->listQueues($namePrefix);
					$this->prefixQueueListUpdatedAt[$namePrefix] = Carbon::now()->getTimestamp();
				}

				$queueCandidates = $this->prefixQueueList[$namePrefix] ?? [];
			}
			else {

				// we don't have wildcard but a single queue name => this is our only candidate
				$queueCandidates = [$name];
			}


			// filter out queues which are paused
			if ($this->queuePauseTime > 0) {

				// get pausing states
				$pausingStates = $this->cacheManager()->store($this->cache)->getMultiple(array_merge(
					array_map(function ($queueName) {
						return "{$this->cachePrefix}Paused{$queueName}";
					}, $queueCandidates),
					array_map(function ($queueName) {
						return "{$this->cachePrefix}WakeNum{$queueName}";
					}, $queueCandidates)
				));

				// filter
				$nowTs           = Carbon::now()->getTimestamp();
				$queueCandidates = array_filter($queueCandidates, function ($queueName) use ($pausingStates, $nowTs) {

					// if paused, we don't return the queue
					$currPaused = $pausingStates["{$this->cachePrefix}Paused{$queueName}"] ?? [];
					if ($currPaused) {
						$currPausedUntil = $currPaused['until'] ?? 0;
						$wakeNum         = $currPaused['wakeNum'] ?? 0;

						// The pause is only effective, if the pause wakeNum is the same as the queue wakeNum. If these
						// values don't equal, the queue has already been woken up in-between.
						if ($currPausedUntil && $currPausedUntil > $nowTs && $wakeNum == $pausingStates["{$this->cachePrefix}WakeNum{$queueName}"])
							return false;
					}

					return true;
				});
			}

			// sort queues by the time of their last selection (the oldest first)
			usort($queueCandidates, function ($queueA, $queueB) {

				$lastSelectTsA = $this->lastSelectedTimestamps[$queueA] ?? 0;
				$lastSelectTsB = $this->lastSelectedTimestamps[$queueB] ?? 0;

				return $lastSelectTsA <=> $lastSelectTsB ?:
					$queueA <=> $queueB;
			});


			// return the first candidate for which we could take a fetch token, if throttled
			foreach ($queueCandidates as $currCandidate) {

				$currThrottle = $this->queueThrottle($currCandidate);
				if (!$currThrottle || $currThrottle->tryTake()) {

					$this->lastSelectedTimestamps[$currCandidate] = Carbon::now()->getTimestampMs();

					return $currCandidate;
				}
			}

			return null;

		}

		/**
		 * Wakes the given queue in case it is sleeping
		 * @param string $queueName The queue name
		 * @return SqsQueueSelector
		 */
		public function wake(string $queueName): SqsQueueSelector {

			// we only need this, if pausing is active
			if ($this->queuePauseTime > 0)
				$this->cacheManager()->store($this->cache)->increment("{$this->cachePrefix}WakeNum{$queueName}");

			return $this;
		}

		/**
		 * Gets tha last wake identifier
		 * @param string $queueName The queue name
		 * @return int
		 */
		public function lastWake(string $queueName): int {

			// we return a dummy value, if pausing is not active
			if ($this->queuePauseTime <= 0)
				return 0;

			return $this->cacheManager()->store($this->cache)->get("{$this->cachePrefix}WakeNum{$queueName}") ?: 0;
		}


		/**
		 * Pauses the given queue until next wait or timeout
		 * @param int $lastWake The last wake identifier fetched before the decision to pause was made
		 * @param string $queueName The queue name
		 * @return $this
		 */
		public function pauseQueue(int $lastWake, string $queueName): SqsQueueSelector {

			// only if pausing is active
			if ($this->queuePauseTime > 0) {

				$this->cacheManager()->store($this->cache)
					->forever("{$this->cachePrefix}Paused{$queueName}", [
						'until'   => Carbon::now()->getTimestamp() + $this->queuePauseTime,
						'wakeNum' => $lastWake,
					]);
			}

			return $this;
		}

		/**
		 * Clears the cache of existing queues
		 * @return $this
		 */
		public function clearQueueListCache(): SqsQueueSelector {
			$this->prefixQueueListUpdatedAt = [];

			return $this;
		}

		/**
		 * Gets the throttling token bucket, if one exists for the given queue
		 * @param string $queueName The queue name
		 * @return TokenBucket|null The token bucket if exists. Else false.
		 */
		protected function queueThrottle(string $queueName): ?TokenBucket {

			foreach ($this->queueThrottles as $throttleExp => $throttle) {

				// check if the current throttle matches the queue
				if (
					$throttleExp === $queueName ||
					(substr($throttleExp, -1) === '*' && strncmp($throttleExp, $queueName, strlen($throttleExp) - 1) === 0)
				) {
					
					$bucketName = "sqs{$queueName}";
					if (($throttle['bucketName'] ?? null) !== null)
						$bucketName = str_replace('{queueName}', $queueName, $throttle['bucketName']);					

					return $this->tokenBucketManager()->bucket(
						$bucketName,
						$throttle['rate'],
						$throttle['burst'],
						($throttle['initial'] ?? 0) ?: 0,
						$this->cache
					);
				}

			}

			return null;
		}


		/**
		 * Lists the SQS queues with the given name prefix
		 * @param string $namePrefix The name prefix
		 * @return string[] The queue names
		 */
		protected function listQueues(string $namePrefix): array {

			$queueNames = [];

			$nextToken = null;
			do {

				$args = [
					'QueueNamePrefix' => $namePrefix,
					'MaxResults'      => 1000,
				];

				if ($nextToken)
					$args['NextToken'] = $nextToken;

				$response = $this->sqs->listQueues($args);

				foreach ((array)($response['QueueUrls'] ?? null) as $currQueueUrl) {
					$lastIndex    = strrpos($currQueueUrl, '/');
					$queueNames[] = substr($currQueueUrl, $lastIndex + 1);
				}

				$nextToken = $response['NextToken'] ?? null;

			} while ($nextToken);

			return $queueNames;
		}

		/**
		 * Resolves the token bucket manager
		 * @return TokenBucketManager The token bucket manager
		 */
		protected function tokenBucketManager(): TokenBucketManager {

			if (!$this->tokenBucketManager)
				$this->tokenBucketManager = $this->container->make(TokenBucketManager::class);

			return $this->tokenBucketManager;

		}

		/**
		 * Resolves the cache manager
		 * @return CacheManager The cache manager
		 */
		protected function cacheManager(): CacheManager {

			if (!$this->cacheManager)
				$this->cacheManager = $this->container->make('cache');

			return $this->cacheManager;

		}
	}