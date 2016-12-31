<?php
namespace Nocake\Config;

use Webmozart\Console\Api\Args\Format\Argument;
use Webmozart\Console\Api\Args\Format\Option;
use Webmozart\Console\Config\DefaultApplicationConfig;
use Nocake\Command\ProductImporterCommandHandler;
use Nocake\Service\AffilinetImporter;

/**
 * Configuration of arguments and options
 */
class ProductImporterConfig extends DefaultApplicationConfig
{
    protected function configure()
    {
        parent::configure();

        $this
          ->setName('productimporter')
          ->setVersion('0.1')
          ->beginCommand('import')
            ->setDescription('Imports product lists into nocake search database')
            ->setHandler(new ProductImporterCommandHandler(new AffilinetImporter()))
            ->addOption('download', null, Option::OPTIONAL_VALUE, 'Download XML files before importing', 1)
          ->end()
        ;
    }
}
