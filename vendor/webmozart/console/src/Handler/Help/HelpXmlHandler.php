<?php

/*
 * This file is part of the webmozart/console package.
 *
 * (c) Bernhard Schussek <bschussek@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Webmozart\Console\Handler\Help;

use Symfony\Component\Console\Descriptor\XmlDescriptor;
use Webmozart\Console\Adapter\ApplicationAdapter;
use Webmozart\Console\Adapter\CommandAdapter;
use Webmozart\Console\Adapter\IOOutput;
use Webmozart\Console\Api\Args\Args;
use Webmozart\Console\Api\Command\Command;
use Webmozart\Console\Api\IO\IO;

/**
 * @since  1.0
 *
 * @author Bernhard Schussek <bschussek@gmail.com>
 */
class HelpXmlHandler
{
    /**
     * {@inheritdoc}
     */
    public function handle(Args $args, IO $io, Command $command)
    {
        $descriptor = new XmlDescriptor();
        $output = new IOOutput($io);
        $application = $command->getApplication();
        $applicationAdapter = new ApplicationAdapter($application);

        if ($args->isArgumentSet('command')) {
            $theCommand = $application->getCommand($args->getArgument('command'));
            $commandAdapter = new CommandAdapter($theCommand, $applicationAdapter);
            $descriptor->describe($output, $commandAdapter);
        } else {
            $descriptor->describe($output, $applicationAdapter);
        }

        return 0;
    }
}
