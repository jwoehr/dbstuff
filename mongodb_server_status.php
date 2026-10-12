<?php
/* assumes you did composer require mongodb/mongodb */
require_once 'vendor/autoload.php';
/* just pass in a valid MongoDB URI for your server */
$mongodb_client = new MongoDB\Client($argv[1]);
/* of course in production you'd use a .env file */
$db = $mongodb_client->selectDatabase('admin');
$cursor = $db->command(["serverStatus" => 1]);
var_dump($cursor->toArray()[0]);
?>