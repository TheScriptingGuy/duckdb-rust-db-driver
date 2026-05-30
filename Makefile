EXTENSION_NAME=rust_db_driver

# Called by extension-ci-tools: cd into the DuckDB submodule and switch to the
# requested version tag.  A shallow fetch is attempted first so the tag resolves
# even in a --depth 1 clone.
.PHONY: set_duckdb_version

set_duckdb_version:
	cd duckdb && \
	  git fetch --depth=1 origin refs/tags/$(DUCKDB_GIT_VERSION):refs/tags/$(DUCKDB_GIT_VERSION) 2>/dev/null || true && \
	  git checkout $(DUCKDB_GIT_VERSION)
