#!/bin/sh

rm -rf doc/rst && mkdir doc/rst
pandoc --read=html --write=rst doc/leo_mq_api.html -o doc/rst/leo_mq_api.rst
pandoc --read=html --write=rst doc/leo_mq_server.html -o doc/rst/leo_mq_server.rst
