<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\PDO\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use PDO;
use Prooph\Common\Event\ActionEventEmitter;
use Prooph\Common\Event\ProophActionEventEmitter;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\ActionEventEmitterAware;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;
use Prooph\EventStore\PDO\MySQLEventStore;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreProjection;
use Prooph\EventStore\PDO\Projection\MySQLEventStoreReadModelProjection;
use Prooph\EventStore\PDO\TableNameGeneratorStrategy\Sha1;

final class MySQLEventStoreReadModelProjectionFactory implements RequiresConfig, RequiresConfigId, ProvidesDefaultOptions, RequiresMandatoryOptions
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * @var array
     */
    private $driverSchemeAliases = [
        'pdo_mysql'  => 'mysql',
        'pdo_pgsql'  => 'pgsql',
    ];

    private $driverSchemeSeparators = [
        'pdo_mysql'  => ';',
        'pdo_pgsql'  => ' ',
    ];

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     MySQLEventStoreProjection::class => [MySQLEventStoreProjectionFactory::class, 'service_name'],
     *     PostgresEventStoreProjection::class => [PostgresEventStoreProjectionFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): MySQLEventStoreReadModelProjection
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }
        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId)
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): MySQLEventStoreReadModelProjection
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        if (isset($config['connection_service'])) {
            $connection = $container->get($config['connection_service']);
        } else {
            $separator = $this->driverSchemeSeparators[$config['connection_options']['driver']];
            $dsn = $this->driverSchemeAliases[$config['connection_options']['driver']] . ':';
            $dsn .= 'host=' . $config['connection_options']['host'] . $separator;
            $dsn .= 'port=' . $config['connection_options']['port'] . $separator;
            $dsn .= 'dbname=' . $config['connection_options']['dbname'] . $separator;
            $dsn = rtrim($dsn);
            $user = $config['connection_options']['user'];
            $password = $config['connection_options']['password'];
            $connection = new PDO($dsn, $user, $password);
        }

        $eventStoreName = $config['event_store'];
        $eventStore = MySQLEventStoreFactory::$eventStoreName($container);

        return new MySQLEventStoreReadModelProjection(
            $eventStore,
            $connection,
            $this->configId,
            $container->get($config['read_model']),
            $config['event_streams_table'],
            $config['projections_table'],
            $config['lock_timeout_ms']
        );
    }

    public function dimensions()
    {
        return ['prooph', 'event_store_projection'];
    }

    public function defaultOptions(): array
    {
        return [
            'connection_options' => [
                'driver' => 'pdo_mysql',
                'user' => 'root',
                'password' => '',
                'host' => '127.0.0.1',
                'dbname' => 'event_store',
                'port' => 3306,
            ],
            'event_streams_table' => 'event_streams',
            'projections_table' => 'projection',
            'lock_timeout_ms' => 1000,
        ];
    }

    public function mandatoryOptions(): array
    {
        return [
            'read_model',
        ];
    }
}
