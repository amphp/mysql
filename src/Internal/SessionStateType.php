<?php

namespace Amp\Mysql\Internal;

/**
 * @internal
 *
 * @see 13.1.3.1.1 Session State Information
 */
enum SessionStateType: int
{
    public const SystemVariables = 0x00;
    public const Schema = 0x01;
    public const StateChange = 0x02;
    public const Gtids = 0x03;
    public const TransactionCharacteristics = 0x04;
    public const TransactionState = 0x05;
}
