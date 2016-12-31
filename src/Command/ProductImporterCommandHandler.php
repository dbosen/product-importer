<?php

namespace Nocake\Command;

use Nocake\Service\ImporterInterface;
use Webmozart\Console\Api\Args\Args;
use Webmozart\Console\Api\IO\IO;
use Webmozart\Console\Api\Command\Command;

class ProductImporterCommandHandler
{
    protected $importer;

    /**
     * ProductImporterCommandHandler constructor.
     *
     * @param \Nocake\Service\ImporterInterface $importer
     *   The source loader service.
     */
    public function __construct(ImporterInterface $importer)
    {
        $this->importer = $importer;
    }

    /**
     * The command handler.
     *
     * @param \Webmozart\Console\Api\Args\Args $args
     *  The args given to the command.
     * @param \Webmozart\Console\Api\IO\IO $io
     *  Console console input and output.
     * @param \Webmozart\Console\Api\Command\Command $command
     *  The command.
     *
     * @return int
     *  Return value of the console command.
     */
    public function handle(Args $args, IO $io, Command $command)
    {
        $this->importer->import($args->getOption('download'));
//        $io->writeLine('Hello!');
        return 0;
    }
}
