<?php

namespace Mysql;

class QueryException extends Exception {
	protected $query = "";

	public function __construct($message, $query = "", \Exception $previous = null) {
		if ($query != "") {
			$this->query = $query;
		}
		parent::__construct($message, 0, $previous);
	}

	final public function getQuery() {
		return $this->query;
	}

	public function __toString() {
		if ($this->query == "") {
			return parent::__toString();
		}

		$msg = $this->message;
		$this->message .= "\nCurrent query was {$this->query}";
		$str = parent::__toString();
		$this->message = $msg;
		return $str;
	}
}