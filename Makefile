.PHONY: all compile xref eunit check_plt build_plt dialyzer doc callgraph graphviz clean distclean

REBAR := rebar3
APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl webtool ssl
PLT_FILE = .leo_mq_dialyzer_plt
COMMON_PLT_FILE = .common_dialyzer_plt
DOT_FILE = leo_mq.dot
CALL_GRAPH_FILE = leo_mq.png

all: compile xref eunit
compile:
	@$(REBAR) compile
xref:
	@$(REBAR) xref
eunit:
	@$(REBAR) eunit
check_plt:
	@$(REBAR) compile
	dialyzer --check_plt --plt $(PLT_FILE) --apps $(APPS)
build_plt:
	@$(REBAR) compile
	dialyzer --build_plt --output_plt $(PLT_FILE) --apps _build/default/lib/*/ebin
dialyzer:
	@$(REBAR) dialyzer
doc: compile
	@$(REBAR) edoc
callgraph: graphviz
	dot -Tpng -o$(CALL_GRAPH_FILE) $(DOT_FILE)
graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))
clean:
	@$(REBAR) clean
distclean:
	@rm -rf _build
	@$(REBAR) clean
