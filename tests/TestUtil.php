<?php
/**
 * This file is part of the prooph/event-store-pdo-adapter.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Adapter\PDO;

use PDO;

class TestUtil
{
    /**
     * List of URL schemes from a database URL and their mappings to driver.
     */
    private static $driverSchemeAliases = [
        'pdo_mysql'  => 'mysql',
        'pdo_pgsql'  => 'postgres',
        'pdo_sqlite' => 'sqlite',
    ];

    public static function getConnection(): PDO
    {
        $connectionParams = self::getConnectionParams();
        $dsn = self::$driverSchemeAliases[$connectionParams['driver']] . ':';
        $dsn .= 'host=' . $connectionParams['host'] . ';';
        $dsn .= 'port=' . $connectionParams['port'] . ';';
        $connection = new PDO($dsn, $connectionParams['user'], $connectionParams['password']);

        return $connection;
    }

    public static function getDatabaseName(): string
    {
        return $GLOBALS['db_name'] ?? 'event_store_tests';
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    private static function hasRequiredConnectionParams(): bool
    {
        return isset(
            $GLOBALS['db_type'],
            $GLOBALS['db_username'],
            $GLOBALS['db_password'],
            $GLOBALS['db_host'],
            $GLOBALS['db_name'],
            $GLOBALS['db_port']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            'driver' => $GLOBALS['db_type'],
            'user' => $GLOBALS['db_username'],
            'password' => $GLOBALS['db_password'],
            'host' => $GLOBALS['db_host'],
            'dbname' => $GLOBALS['db_name'],
            'port' => $GLOBALS['db_port']
        ];
    }
}
