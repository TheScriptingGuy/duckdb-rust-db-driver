EXTENSION_NAME=rust_db_driver

.PHONY: configure_ci set_duckdb_version release

# Called by extension-ci-tools before and inside the Docker build step.
configure_ci:
	@echo "configure_ci is a no-op for this Rust extension"

# Called by extension-ci-tools: cd into the DuckDB submodule and switch to the
# requested version tag.  A shallow fetch is attempted first so the tag resolves
# even in a --depth 1 clone.
set_duckdb_version:
	cd duckdb && \
	  git fetch --depth=1 origin refs/tags/$(DUCKDB_GIT_VERSION):refs/tags/$(DUCKDB_GIT_VERSION) 2>/dev/null || true && \
	  git checkout $(DUCKDB_GIT_VERSION)

# Called by extension-ci-tools inside the Docker build container.
release:
	mkdir -p build/release
	cmake -DCMAKE_BUILD_TYPE=Release -S . -B build/release
	cmake --build build/release --config Release
