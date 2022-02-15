<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 19:13
	 */

	namespace MehrItLaraSqsExtTest\Cases;


	use Carbon\Carbon;
	use Illuminate\Cache\CacheManager;
	use MehrIt\LaraSqsExt\Provider\SqsExtServiceProvider;
	use MehrIt\LaraTokenBucket\Provider\TokenBucketServiceProvider;

	class TestCase extends \Orchestra\Testbench\TestCase
	{
		protected function getPackageProviders($app) {
			return [
				SqsExtServiceProvider::class,
				TokenBucketServiceProvider::class,
			];
		}


		protected function setUp(): void {

			parent::setUp();

			Carbon::setTestNow();

			/** @var CacheManager $cacheManager */
			$cacheManager = app('cache');

			$cacheManager->clear();
		}

	}