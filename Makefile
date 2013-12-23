# <variables>
PACKAGE=lineup  # the python module name
TESTS_VERBOSITY=2
export PYTHONPATH=$(pwd)
# </variables>

EXTRA_TEST_TASKS=
all: test

filename=lineup-`python -c 'import lineup.version;print lineup.version.version'`.tar.gz

export PYTHONPATH:=  ${PWD}

test: unit functional
run_test:
	@if [ -d tests/$(suite) ]; then \
		if [ "`ls tests/$(suite)/*.py`" = "tests/$(suite)/__init__.py" ] ; then \
			echo "No \033[0;32m$(suite)\033[0m tests..."; \
		else \
			echo "======================================="; \
			echo "* Running \033[0;32m$(suite)\033[0m test suite *"; \
			echo "======================================="; \
			nosetests --immediate --rednose --stop --with-coverage --cover-package=$(PACKAGE) \
				--cover-min-percentage=100% \
				--cover-branches  --cover-erase --verbosity=$(TESTS_VERBOSITY) -s tests/$(suite) ; \
		fi \
	fi

unit:
	@make run_test suite=unit

functional:
	@make run_test suite=functional

lint:
	@flake8 lineup

acceptance:
	@steadymark README.md
	@steadymark docs/*.md

docs: clean
	@steadymark docs/*.md
	@git co master && \
		(git br -D gh-pages || printf "") && \
		git checkout --orphan gh-pages && \
		markment -o . -t theme docs --sitemap-for=http://falcao.it/lineup && \
		cp quick-start.html index.html && \
		git add . && \
		git commit -am 'documentation' && \
		git push --force origin gh-pages && \
		git checkout master
clean:
	@printf "Cleaning up files that are already in .gitignore... "
	@for pattern in `cat .gitignore`; do rm -rf $$pattern; find . -name "$$pattern" -exec rm -rf {} \;; done
	@echo "OK!"

release: test
	@./.release
	@python setup.py sdist register upload
