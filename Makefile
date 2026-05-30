EXTENSION_NAME=rust_db_driver

.PHONY: configure_ci set_duckdb_version release test_release

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

# Called by extension-ci-tools after the build step.
# Integration tests require live PostgreSQL/MySQL/MSSQL instances which are
# not available in the CI Docker container, so this target is a no-op.
test_release:
	@echo "test_release: skipped (external database services not available in CI)"
