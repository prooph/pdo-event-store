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
        if (isset($connectionParams['driver'], $connectionParams['memory'])
            && $connectionParams['memory']
        ) {
            $connection = new PDO('sqlite::memory:');
        } else {
            $dsn = self::$driverSchemeAliases[$connectionParams['driver']] . ':';
            $dsn .= 'host=' . $connectionParams['host'] . ';';
            $dsn .= 'port=' . $connectionParams['port'] . ';';
            $dsn .= 'dbname=' . $connectionParams['dbname'];
            $connection = new PDO($dsn, $connectionParams['user'], $connectionParams['password']);
        }

        return $connection;
    }

    private static function getConnectionParams()
    {
        if (self::hasRequiredConnectionParams()) {
            return self::getSpecifiedConnectionParams();
        }

        return self::getFallbackConnectionParams();
    }

    private static function hasRequiredConnectionParams()
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

    private static function getFallbackConnectionParams()
    {
        return [
            'driver' => 'pdo_sqlite',
            'memory' => true
        ];
    }
}
