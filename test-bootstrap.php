<?php

	$lockFile = sys_get_temp_dir() . '/phpUnit' . sha1(__DIR__);

	$fh = fopen($lockFile, 'w+');
	echo "Waiting for lock ($lockFile)...\n";
	flock($fh, LOCK_EX);


	$testPackages = getenv('TEST_PACKAGES');


	echo "Update dependencies before testing\n";
	$output = [];
	if (trim($testPackages)) {
		echo "Requiring $testPackages\n";
		copy(__DIR__ . '/composer.json', __DIR__ . '/composer-test.json');
		exec("cd '" . __DIR__ . "' && export COMPOSER=\"composer-test.json\" && composer require --no-interaction --with-all-dependencies $testPackages && composer dump-autoload --no-interaction", $output, $returnVar);
	}
	else {
		echo "Using default composer.json\n";
		exec("cd '" . __DIR__ . "' && && composer update --with-all-dependencies && composer dump-autoload", $output, $returnVar);
	}

	echo implode("\n", $output) . "\n";

	if ($returnVar !== 0) {
		echo "Updating dependencies failed\n";
		die(1);
	}


	return require __DIR__ . '/vendor/autoload.php';
