<?php
	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 04.01.19
	 * Time: 19:13
	 */

	namespace MehrItLaraSqsExtTest\Cases;


	use Carbon\Carbon;

	class TestCase extends \Orchestra\Testbench\TestCase
	{
		protected function setUp(): void {

			parent::setUp();

			Carbon::setTestNow();
		}

	}