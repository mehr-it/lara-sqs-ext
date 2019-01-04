<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 17:28
	 */

	namespace MehrIt\LaraSqsExt\Queue\Connectors;


	use Aws\Sqs\SqsClient;
	use Illuminate\Contracts\Queue\Queue;
	use Illuminate\Queue\Connectors\SqsConnector;
	use Illuminate\Support\Arr;
	use MehrIt\LaraSqsExt\Queue\SqsExtQueue;

	class SqsExtConnector extends SqsConnector
	{
		const DEFAULT_QUEUE_TYPE = SqsExtQueue::class;

		/**
		 * Establish a queue connection.
		 *
		 * @param  array $config
		 * @return Queue
		 */
		public function connect(array $config) {

			// prepare configuration
			$this->prepareConfig($config);

			return $this->makeQueue(new SqsClient($config), $config['queue'], Arr::get($config, 'prefix', ''), $config);
		}

		/**
		 * Prepare the queue configuration
		 * @param array $config The configuration
		 */
		protected function prepareConfig(array &$config) {

			$config = $this->getDefaultConfiguration($config);
			if ($config['key'] && $config['secret'])
				$config['credentials'] = Arr::only($config, ['key', 'secret']);

		}


		/**
		 * Creates a new queue instance
		 * @param \Aws\Sqs\SqsClient $sqs
		 * @param string $default Default queue
		 * @param string $prefix Queue prefix
		 * @param array $options Queue configuration options
		 * @return Queue|SqsExtQueue The new queue instance
		 */
		protected function makeQueue(SqsClient $sqs, $default, $prefix = '', $options = []) {

			return app()->make($this->getQueueClass(), [
				'sqs'     => $sqs,
				'default' => $default,
				'prefix'  => $prefix,
				'options' => $options,
			]);
		}

		/**
		 * Gets the queue class to use
		 * @return string The queue class
		 */
		protected function getQueueClass() {
			return static::DEFAULT_QUEUE_TYPE;
		}

	}