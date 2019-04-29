# leo_mq

[![Build Status](https://travis-ci.org/leo-project/leo_mq.svg?branch=develop)](https://travis-ci.org/leo-project/leo_mq)

## Overview

* "leo_mq" is local message-queueing library which is used for including in Erlang's applications.
* "leo_mq" uses [rebar](https://github.com/basho/rebar) as a build system. Makefile so that simply running "make" at the top level should work.
* "leo_mq" requires Erlang/OTP 19.3 or later.

## Usage in Leo Project

**leo_mq** is used in [**leo_storage**](https://github.com/leo-project/leo_storage), [**leo_gateway**](https://github.com/leo-project/leo_gateway) and [**leo_manager**](https://github.com/leo-project/leo_manager)
It is used to fix inconsistent data wish asynchrounous processing.

## Sponsors

* LeoProject/LeoFS was sponsored by [Rakuten, Inc.](http://global.rakuten.com/corp/) from 2012 to Dec of 2018.
* LeoProject/LeoFS is sponsored by [Lions Data, Inc](https://lions-data.com/) from Jan of 2019.
