<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 19:47
	 */

	namespace MehrItLaraSqsExtTest\Cases\Unit\Queue\Connectors;


	use Aws\Sqs\SqsClient;
	use MehrIt\LaraSqsExt\Queue\Connectors\SqsExtConnector;
	use MehrIt\LaraSqsExt\Queue\SqsExtQueue;
	use MehrItLaraSqsExtTest\Cases\TestCase;

	class SqsExtConnectorTest extends TestCase
	{
		public function testConnectPassesOptionsToQueue() {
			$conn = new SqsExtConnector();

			$options = [
				'driver'          => 'sqs-ext',
				'key'             => 'AMAZONSQSKEY',
				'secret'          => 'AmAz0n+SqSsEcReT+aLpHaNuM3R1CsTr1nG',
				'queue'           => 'https://sqs.someregion.amazonaws.com/123123123123/QUEUE',
				'region'          => 'someregion',
				'another-option'  => 'another-value'
			];

			/** @var SqsExtQueue $ret */
			$ret = $conn->connect($options);


			$this->assertInstanceOf(SqsExtQueue::class, $ret);

			foreach($options as $key => $value) {
				$this->assertSame($ret->getOptions()[$key] ?? null, $value);
			}

		}

		public function testConnectUsingContainer() {
			$conn = new SqsExtConnector();

			$queue = new \stdClass();

			$options = [
				'driver'         => 'sqs-ext',
				'key'            => 'AMAZONSQSKEY',
				'secret'         => 'AmAz0n+SqSsEcReT+aLpHaNuM3R1CsTr1nG',
				'queue'          => 'https://sqs.someregion.amazonaws.com/123123123123/QUEUE',
				'region'         => 'someregion',
				'prefix'         => 'my-prefix',
				'another-option' => 'another-value'
			];

			app()->bind(SqsExtQueue::class, function($app, $params) use ($options, $queue) {

				$this->assertInstanceOf(SqsClient::class, $params['sqs']);
				$this->assertSame($options['queue'], $params['default']);
				$this->assertSame($options['prefix'], $params['prefix']);

				foreach ($options as $key => $value) {
					$this->assertSame($params['options'][$key] ?? null, $value);
				}

				return $queue;

			});

			/** @var SqsExtQueue $ret */
			$ret = $conn->connect($options);


			$this->assertSame($queue, $ret);

		}
	}