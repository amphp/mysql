<?php

namespace Amp\Mysql\Internal;

final class GeneratorParser
{
    private ?\Generator $generator;

    private array $buffers = [];

    private int $bufferLength = 0;

    private int $length;

    public function __construct(\Generator $generator)
    {
        $this->generator = $generator;
        $this->length = $this->generator->current();

        if (!$this->generator->valid()) {
            $this->generator = null;
        }
    }

    final public function cancel(): void
    {
        $this->generator = null;
    }

    final public function isValid(): bool
    {
        return $this->generator !== null;
    }

    final public function push(string $data): void
    {
        if ($this->generator === null) {
            throw new \Error("The parser is no longer writable");
        }

        $this->buffers[] = $data;
        $this->bufferLength += \strlen($data);

        if ($this->bufferLength >= $this->length) {
            $offset = 0;
            $buffer = \implode('', $this->buffers);

            while ($this->bufferLength >= $this->length) {
                $send = \substr($buffer, $offset, $this->length);
                $offset += $this->length;
                $this->bufferLength -= $this->length;

                $this->length = $this->generator->send($send);
            }

            $this->buffers = [\substr($buffer, $offset)];
        }
    }
}