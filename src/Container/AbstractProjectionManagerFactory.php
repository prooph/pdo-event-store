<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2019 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2019 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\Exception\InvalidArgumentException;
use Prooph\EventStore\Projection\ProjectionManager;
use Psr\Container\ContainerInterface;

abstract class AbstractProjectionManagerFactory implements
    ProvidesDefaultOptions,
    RequiresConfigId,
    RequiresMandatoryOptions
{
    use ConfigurationTrait;

    private $configId;

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     ProjectionManager::class => [MySqlProjectionManagerFactory::class, 'service_name'],
     *     // or
     *     ProjectionManager::class => [PostgresProjectionManagerFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): ProjectionManager
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                \sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }

        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    public function __invoke(ContainerInterface $container): ProjectionManager
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId);

        $projectionManagerClassName = $this->projectionManagerClassName();

        return new $projectionManagerClassName(
            $container->get($config['event_store']),
            $container->get($config['connection']),
            $config['event_streams_table'],
            $config['projections_table']
        );
    }

    abstract protected function projectionManagerClassName(): string;

    public function dimensions(): iterable
    {
        return ['prooph', 'projection_manager'];
    }

    public function mandatoryOptions(): iterable
    {
        return ['connection'];
    }

    public function defaultOptions(): iterable
    {
        return [
            'event_store' => EventStore::class,
            'event_streams_table' => 'event_streams',
            'projections_table' => 'projections',
        ];
    }
}
