<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use PDO;

abstract class TestUtil
{
    /**
     * List of URL schemes from a database URL and their mappings to driver.
     */
    private static $driverSchemeAliases = [
        'pdo_mysql' => 'mysql',
        'pdo_pgsql' => 'pgsql',
    ];

    private static $driverSchemeSeparators = [
        'pdo_mysql' => ';',
        'pdo_pgsql' => ' ',
    ];

    /**
     * @var PDO
     */
    private static $connection;

    public static function getConnection(): PDO
    {
        if (! isset(self::$connection)) {
            $connectionParams = self::getConnectionParams();
            $separator = self::$driverSchemeSeparators[$connectionParams['driver']];
            $dsn = self::$driverSchemeAliases[$connectionParams['driver']] . ':';
            $dsn .= 'host=' . $connectionParams['host'] . $separator;
            $dsn .= 'port=' . $connectionParams['port'] . $separator;
            $dsn .= 'dbname=' . $connectionParams['dbname'] . $separator;
            $dsn = rtrim($dsn);
            self::$connection = new PDO($dsn, $connectionParams['user'], $connectionParams['password']);
        }

        try {
            self::$connection->rollBack();
        } catch (\PDOException $e) {
            // ignore
        }

        return self::$connection;
    }

    public static function getDatabaseName(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return $GLOBALS['db_name'];
    }

    public static function getDatabaseVendor(): string
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return $GLOBALS['db_type'];
    }

    public static function getConnectionParams(): array
    {
        if (! self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function initDefaultDatabaseTables(PDO $connection): void
    {
        $vendor = self::getDatabaseVendor();

        if ($vendor === 'pdo_mysql') {
            $vendor = 'mysql';
        } elseif ($vendor === 'pdo_pgsql') {
            $vendor = 'postgres';
        } else {
            throw new \RuntimeException('Invalid database vendor');
        }

        $connection->exec(file_get_contents(__DIR__.'/../scripts/' . $vendor . '/01_event_streams_table.sql'));
        $connection->exec(file_get_contents(__DIR__.'/../scripts/' . $vendor . '/02_projections_table.sql'));
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
            'port' => $GLOBALS['db_port'],
        ];
    }
}
