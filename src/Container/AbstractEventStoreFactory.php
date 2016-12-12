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
use Prooph\EventStore\ActionEventEmitterEventStore;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\PDO\Exception\InvalidArgumentException;

abstract class AbstractEventStoreFactory implements
    ProvidesDefaultOptions,
    RequiresConfig,
    RequiresConfigId,
    RequiresMandatoryOptions
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
        'pdo_mysql' => 'mysql',
        'pdo_pgsql' => 'pgsql',
    ];

    private $driverSchemeSeparators = [
        'pdo_mysql' => ';',
        'pdo_pgsql' => ' ',
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
     *     MySQLEventStore::class => [MySQLEventStoreFactory::class, 'service_name'],
     *     PostgresEventStore::class => [PostgresEventStoreFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): EventStore
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): EventStore
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

        $eventStoreClassName = $this->eventStoreClassName();

        $eventStore = new $eventStoreClassName(
            $container->get($config['message_factory']),
            $messageConverter = $container->get($config['message_converter']),
            $connection,
            $container->get($config['persistence_strategy']),
            $config['load_batch_size'],
            $config['event_streams_table']
        );

        if (! $config['wrap_action_event_emitter']) {
            return $eventStore;
        }

        return $this->createActionEventEmitterEventStore($eventStore);
    }

    abstract protected function createActionEventEmitterEventStore(EventStore $eventStore): ActionEventEmitterEventStore;

    abstract protected function eventStoreClassName(): string;

    public function dimensions(): iterable
    {
        return ['prooph', 'event_store'];
    }

    public function mandatoryOptions(): iterable
    {
        return [
            'persistence_strategy',
        ];
    }
}
