<?php

namespace Amp\Mysql\Test;

use Amp\Mysql\Internal\ConnectionProcessor;
use PHPUnit\Framework\TestCase;

final class ScrambleTest extends TestCase
{
    public function test()
    {
        $password = 'Ab12#$Cd56&*';
        $scramble = 'eF!@34gH%^78';

        $expected = "\x6a\x45\x37\x96\x6b\x29\x63\x59\x24\x8d\x64\x86\x0a\xd6\xcc\x2a\x06\x47\x8c\x26\xea\xaa\x3b\x02\x69\x4c\x85\x02\xf5\x5b\xc8\xdc";

        $actual = ConnectionProcessor::sha2Auth($password, $scramble);

        self::assertSame($expected, $actual);
    }
}