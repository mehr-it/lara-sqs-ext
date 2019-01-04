<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 17:26
	 */

	namespace MehrIt\LaraSqsExt\Provider;


	use Illuminate\Queue\QueueManager;
	use Illuminate\Support\ServiceProvider;
	use MehrIt\LaraSqsExt\Queue\Connectors\SqsExtConnector;

	class SqsExtServiceProvider extends ServiceProvider
	{

		public function boot() {
			/** @var QueueManager $manager */
			$manager = $this->app['queue'];

			$manager->addConnector('sqs-ext', function () {
				return new SqsExtConnector();
			});
		}


	}