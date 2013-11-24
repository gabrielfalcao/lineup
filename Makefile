all: test

filename=lineup-`python -c 'import lineup.version;print lineup.version.version'`.tar.gz

export PYTHONPATH:=  ${PWD}

test: clean
	@echo "Running tests"
	@nosetests --cover-branches --with-coverage  --cover-erase --cover-package=lineup --stop -v -s tests
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
